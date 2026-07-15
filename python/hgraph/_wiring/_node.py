"""_PyNode: Python user-node wiring (signature binding + call
normalisation), @compute_node/@sink_node, lift, @generator, push_queue."""
import inspect

import _hgraph

from .._types import (_ContextExpr, _GenericTsExpr, _Required, _TsExpr,
                      _TypeVarSentinel, _type_var_name)
from ._core import (IncorrectTypeBinding, RequirementsNotMetWiringError,
                    WiringError, WiringPort, _current_wiring,
                    _resolve_context, _unwrap, wire)
from ._markers import (LOGGER, STATE, _INJECTABLE_MARKERS, _MISSING,
                       _RecordableStateExpr, _annotation_ts_kind,
                       _is_object_vt, _tsw_kind, _unbounded_tuple_kind)
from ._operator import _register_overload, _run_requires

class _PyNode:
    """@compute_node / @sink_node: a Python function as a runtime node. The
    function runs on the graph thread (both modes) under the GIL, receives
    time-series inputs as plain Python VALUES and wiring-time scalars as
    supplied; STATE/CLOCK/SCHEDULER/EvaluationEngineApi-annotated parameters
    are injected. A
    compute node's return value ticks its output (None = no tick). It must
    have no side effects beyond its output."""

    def __init__(self, fn, has_output, active=None, valid=None, resolvers=None,
                 node_type=None):
        self._wiring_signature = inspect.signature(fn)
        var_params = [p for p in self._wiring_signature.parameters.values()
                      if p.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)]
        if var_params:
            # hgraph parity (upstream PythonWiringNodeClass): star params
            # receive ONE packed time-series (a TSL/TSB view), so the code
            # object is rewritten to make every parameter keyword-only.
            import types

            co = fn.__code__
            kw_only_code = co.replace(
                co_flags=co.co_flags & ~(inspect.CO_VARARGS | inspect.CO_VARKEYWORDS),
                co_argcount=0,
                co_posonlyargcount=0,
                co_kwonlyargcount=len(self._wiring_signature.parameters),
            )
            rewritten = types.FunctionType(kw_only_code, fn.__globals__, name=fn.__name__,
                                           argdefs=fn.__defaults__, closure=fn.__closure__)
            # an EMPTY group must still bind: () / {} defaults.
            kw_defaults = dict(fn.__kwdefaults__ or {})
            for p in var_params:
                kw_defaults[p.name] = () if p.kind is inspect.Parameter.VAR_POSITIONAL else {}
            rewritten.__kwdefaults__ = kw_defaults
            fn = rewritten
        self.fn = fn
        self.has_output = has_output
        # active=/valid= accept name iterables OR wiring-time callables
        # (m, **scalars) evaluated once the call's scalars are known.
        self._active_fn = active if callable(active) else None
        self._valid_fn = valid if callable(valid) else None
        self._active = None if callable(active) else self._policy_names("active", active)
        self._valid = None if callable(valid) else self._policy_names("valid", valid)
        self._resolvers = dict(resolvers) if resolvers else None
        self._requires = None
        self._pins = {}
        self._node_type = node_type
        self._start_fn = None
        self._stop_fn = None
        self.__name__ = fn.__name__
        sig = self._wiring_signature
        self._signature = sig
        # Callers introspecting the NODE (eval_node's input typing) must see
        # the user function's signature, not _PyNode.__call__'s.
        self.__signature__ = sig
        self._out_tp = sig.return_annotation if has_output else None
        self._params = list(sig.parameters.values())
        self._recordable_state = next(
            (param.annotation for param in self._params
             if isinstance(param.annotation, _RecordableStateExpr)), None)
        ts_names = {
            param.name
            for param in self._params
            # Generic annotations (TIME_SERIES_TYPE, TSS[SCALAR], ...) are
            # time-series parameters too - they resolve from the wired ports.
            if isinstance(param.annotation, (_TsExpr, _ContextExpr, _GenericTsExpr, _TypeVarSentinel))
        }
        for policy, names in (("active", self._active), ("valid", self._valid)):
            if names is None:
                continue
            unknown = names - ts_names
            if unknown:
                rendered = ", ".join(sorted(unknown))
                raise TypeError(f"{self.__name__}: {policy}= contains non-time-series input(s): {rendered}")
        # Injectable parameters MUST default to None (hgraph convention):
        # the default guarantees user code in a graph never supplies them.
        for param in self._params:
            is_injectable = (param.annotation in _INJECTABLE_MARKERS or
                             isinstance(param.annotation, _RecordableStateExpr))
            if is_injectable and param.default is not None:
                raise TypeError(
                    f"injectable parameter '{param.name}' of '{self.__name__}' must default to None"
                )
        if sum(isinstance(param.annotation, _RecordableStateExpr)
               for param in self._params) > 1:
            raise TypeError(f"'{self.__name__}' supports at most one RECORDABLE_STATE parameter")
        if self._recordable_state is not None and any(
                param.annotation is STATE for param in self._params):
            raise TypeError(
                f"'{self.__name__}' cannot combine STATE with RECORDABLE_STATE")

    @staticmethod
    def _policy_names(policy, names):
        if names is None:
            return None
        if isinstance(names, str):
            raise TypeError(f"{policy}= must be an iterable of input names, not a string")
        try:
            result = frozenset(names)
        except TypeError as error:
            raise TypeError(f"{policy}= must be an iterable of input names") from error
        if any(not isinstance(name, str) for name in result):
            raise TypeError(f"{policy}= must contain only input names")
        return result

    def _set_lifecycle(self, phase, fn):
        if self._recordable_state is not None:
            raise TypeError(
                f"@{self.__name__}.{phase} is not supported with RECORDABLE_STATE")
        for param in inspect.signature(fn).parameters.values():
            if param.annotation in _INJECTABLE_MARKERS:
                if param.default is not None:
                    raise TypeError(
                        f"injectable parameter '{param.name}' of '{fn.__name__}' must default to None"
                    )
                continue
            if isinstance(param.annotation, (_TsExpr, _ContextExpr)) or param.name == "_output":
                raise TypeError(
                    f"@{self.__name__}.{phase} supports wiring-time scalars and injectables only"
                )
        setattr(self, f"_{phase}_fn", fn)
        return fn

    def start(self, fn):
        return self._set_lifecycle("start", fn)

    def stop(self, fn):
        return self._set_lifecycle("stop", fn)

    @property
    def signature(self):
        from .._signature import WiringNodeType, extract_signature

        node_type = self._node_type or (
            WiringNodeType.COMPUTE_NODE if self.has_output else WiringNodeType.SINK_NODE)
        return extract_signature(self.fn, node_type)

    def __getitem__(self, item):
        # node[TYPEVAR: TYPE] pre-resolution: the pins seed the call's
        # ResolutionScope (the C++ type-variable map) - a copied node so the
        # shared decorator object is never mutated.
        from .._types import _TypeVarSentinel, _TsExpr

        import copy

        pinned = copy.copy(self)
        pinned._pins = dict(self._pins)
        items = item if isinstance(item, tuple) else (item,)
        for entry in items:
            if isinstance(entry, slice) and isinstance(entry.start, _TypeVarSentinel):
                pinned._pins[_type_var_name(entry.start)] = entry.stop
        return pinned

    @staticmethod
    def _bind_resolved(scope, name, resolved):
        """Seed a pinned/resolved type variable into the C++ scope: a ts
        expression binds the ts var, a fixed-size value binds the size var,
        and a python scalar type binds the scalar var of the same name."""
        from .._types import _TsExpr, _value_type

        if isinstance(resolved, _TsExpr):
            scope.bind_ts(name, resolved.handle)
        elif isinstance(resolved, int) and not isinstance(resolved, bool):
            scope.bind_size(name, resolved)
        else:
            scope.bind_scalar(name, _value_type(resolved))

    @staticmethod
    def _eval_policy(policy, fn, scope, scalar_values):
        """Evaluate a wiring-time active=/valid= callable: (m, **scalars) ->
        None (all inputs) / iterable of input names."""
        params = list(inspect.signature(fn).parameters)
        call = {name: scalar_values.get(name) for name in params[1:]}
        result = fn(scope.bindings, **call)
        if result is None:
            return None
        return frozenset(result)

    def _check_binding(self, scope, param, value):
        """Match the wired port against the parameter's TYPE PATTERN,
        binding type variables into the scope. A mismatch on a concrete
        CompoundScalar annotation accepts SUBCLASS ports (python's
        inheritance is a py-frontend concept); anything else raises."""
        from .._types import TS, _pattern_of

        annotation = param.annotation
        if isinstance(annotation, _ContextExpr):
            return
        import typing

        if typing.get_origin(annotation) is typing.Union:
            members = typing.get_args(annotation)
            port_tp = _unwrap(value).ts_type
            for member in members:
                if isinstance(member, _TsExpr) and member.handle == port_tp:
                    return
            raise IncorrectTypeBinding(
                f"{self.__name__}: '{param.name}' expects one of {members!r}")
        if isinstance(annotation, _TsExpr):
            handle = annotation.handle
            # TS[object] widens over any payload (the py-any input rule).
            if handle.is_ts and _is_object_vt(_hgraph.ts_value_vt(handle)):
                return
            # TSW strictness is deferred until the duration/tick marker
            # normalisation lands (min-period defaults are applied at
            # wiring, so handle equality over-rejects).
            if handle.kind == _tsw_kind():
                return
        try:
            pattern = _pattern_of(annotation)
        except TypeError:
            return   # non-ts annotation reached with a port: leave to the runtime
        port_tp = _unwrap(value).ts_type
        if scope.match(pattern, port_tp):
            return
        if isinstance(annotation, _TsExpr) and annotation.handle.is_ts:
            # tuple[E, ...] widens over fixed tuples of E: re-match with the
            # C++ homogeneous-tuple pattern (the matcher owns that rule).
            vt = _hgraph.ts_value_vt(annotation.handle)
            if _hgraph.vt_kind(vt) == _unbounded_tuple_kind():
                homogeneous = _hgraph.type_pattern_ts(
                    _hgraph.scalar_pattern_homogeneous_tuple(
                        _hgraph.scalar_pattern_value(_hgraph.vt_element(vt))))
                if scope.match(homogeneous, port_tp):
                    return
        cs_class = getattr(annotation, "_cs_class", None)
        if cs_class is not None:
            stack = list(cs_class.__subclasses__())
            while stack:
                candidate = stack.pop()
                stack.extend(candidate.__subclasses__())
                if TS[candidate].handle == port_tp:
                    return   # subtype binding (auto-cast)
        raise IncorrectTypeBinding(
            f"{self.__name__}: '{param.name}' expects {annotation!r}, got {port_tp!r}")

    @staticmethod
    def _requested_input_shape(scope, annotation, value):
        """Resolve the exact input shape used when packing a Python node.

        Union inputs are validated member-by-member, so they do not have one
        TypePattern to resolve. Preserve the member that matched the port;
        ordinary inputs continue through the shared C++ resolution scope.
        """
        import types
        import typing
        from .._types import _pattern_of

        if typing.get_origin(annotation) in (typing.Union, types.UnionType):
            port_type = _unwrap(value).ts_type
            for member in typing.get_args(annotation):
                if isinstance(member, _TsExpr) and member.handle == port_type:
                    return member.handle
            return port_type
        return scope.resolve_ts(_pattern_of(annotation))

    def _apply_resolvers(self, scope, scalar_values):
        """resolvers={TYPEVAR: lambda mapping, <scalars...>: type}: bind the
        computed types into the scope (mapping = the scope's bindings)."""
        for sentinel, resolver in self._resolvers.items():
            params = list(inspect.signature(resolver).parameters)
            call = {name: scalar_values.get(name) for name in params[1:]}
            resolved = resolver(scope.bindings, **call)
            name = _type_var_name(sentinel)
            self._bind_resolved(scope, name, resolved)

    @staticmethod
    def _resolve_recordable_state(annotation, scope):
        from .._types import _TsExpr, _pattern_of, _value_type
        import typing

        schema = annotation.schema
        origin = typing.get_origin(schema) or schema
        args = typing.get_args(schema)
        for parameter, resolved in zip(getattr(origin, "__parameters__", ()), args):
            if isinstance(resolved, _TypeVarSentinel):
                continue
            name = _type_var_name(parameter)
            if isinstance(resolved, _TsExpr):
                scope.bind_ts(name, resolved.handle)
            else:
                scope.bind_scalar(name, _value_type(resolved))

        annotations = {}
        for klass in reversed(origin.__mro__):
            annotations.update(getattr(klass, "__annotations__", {}))
        fields = []
        for name, field_type in annotations.items():
            if isinstance(field_type, _TsExpr):
                resolved = field_type.handle
            else:
                resolved = scope.resolve_ts(_pattern_of(field_type))
            if resolved is None:
                raise TypeError(
                    f"cannot resolve recordable-state field '{name}' of {origin.__name__}")
            fields.append((name, resolved))

        state_name = origin.__name__
        qualname = getattr(origin, "__qualname__", state_name)
        if "<locals>" in qualname:
            state_name = f"{origin.__module__}.{qualname}"
        return _TsExpr(_hgraph.tsb(state_name, fields), f"TSB[{origin.__name__}]")

    def __call__(self, *args, **kwargs):
        from .._types import _pattern_of

        kwargs.pop("__recordable_id__", None)
        ref = _hgraph.node_ref(self.fn)
        layout, ports, scalars, reference_shapes = [], [], [], []
        # The wiring-time RESOLUTION SCOPE: the C++ type-variable map. Every
        # generic input pattern matches into it; outputs/resolvers/pins read
        # and write the same map - no python-side type classification.
        scope = _hgraph.ResolutionScope()
        for name, resolved in self._pins.items():
            self._bind_resolved(scope, name, resolved)
        bound = self._signature.bind_partial(*args, **kwargs)
        scalar_values = {}
        # Pre-collect scalar values so callable active=/valid= policies can
        # evaluate before layout letters are chosen.
        for param in self._params:
            if (isinstance(param.annotation, (_TsExpr, _ContextExpr, _GenericTsExpr,
                                              _TypeVarSentinel, _RecordableStateExpr))):
                continue
            if param.annotation in _INJECTABLE_MARKERS or param.annotation is LOGGER:
                continue
            value = bound.arguments.get(param.name, _MISSING)
            if value is _MISSING and param.default is not inspect.Parameter.empty:
                value = param.default
            if value is not _MISSING and not isinstance(value, WiringPort):
                scalar_values[param.name] = value
        active_policy = self._active
        valid_policy = self._valid
        if self._active_fn is not None:
            active_policy = self._eval_policy("active", self._active_fn, scope, scalar_values)
        if self._valid_fn is not None:
            valid_policy = self._eval_policy("valid", self._valid_fn, scope, scalar_values)
        # Var-args parity (upstream model): the *args group packs into ONE
        # TSL (or structural TSB when so annotated), **kwargs into ONE named
        # TSB (or TSD); the rewritten fn receives every parameter BY NAME.
        has_var_group = any(p.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
                            for p in self._params)
        by_name = has_var_group
        layout_names, layout_by_name = [], []   # parallel to ``layout``

        def _note(name):
            layout_names.append(name)
            layout_by_name.append(by_name)

        def _lift(entry):
            """A plain value in a variadic group lifts to an inferred const."""
            if isinstance(entry, WiringPort):
                return entry
            return wire("const", entry)

        def _group_layout(param):
            required = valid_policy is None or param.name in valid_policy
            is_active = active_policy is None or param.name in active_policy
            return {(True, True): "t", (True, False): "u",
                    (False, True): "T", (False, False): "U"}[(is_active, required)]

        for param in self._params:
            if param.kind is inspect.Parameter.VAR_POSITIONAL:
                entries = [_unwrap(_lift(entry)) for entry in bound.arguments.get(param.name, ())]
                if entries:
                    if _annotation_ts_kind(param.annotation) == _hgraph.TS_KIND_TSB:
                        packed_group = _hgraph.bundle_port(entries, [False] * len(entries))
                    else:
                        packed_group = _hgraph.tsl_port(entries)
                    layout.append(_group_layout(param))
                    _note(param.name)
                    ports.append(packed_group)
                    reference_shapes.append(False)
                continue
            if param.kind is inspect.Parameter.VAR_KEYWORD:
                extras = {k: _unwrap(_lift(v)) for k, v in bound.arguments.get(param.name, {}).items()}
                if extras:
                    if _annotation_ts_kind(param.annotation) == _hgraph.TS_KIND_TSD:
                        packed_group = _unwrap(wire(
                            "combine_tsd", tuple(extras.keys()),
                            *(WiringPort(v) for v in extras.values()), __strict__=False))
                    else:
                        tsb_type = _hgraph.un_named_tsb_type(
                            [(k, v.ts_type) for k, v in extras.items()])
                        packed_group = _hgraph.tsb_port(tsb_type, extras)
                    layout.append(_group_layout(param))
                    _note(param.name)
                    ports.append(packed_group)
                    reference_shapes.append(False)
                continue
            if param.kind is inspect.Parameter.KEYWORD_ONLY:
                by_name = True
            if isinstance(param.annotation, _RecordableStateExpr):
                if param.name in bound.arguments:
                    raise TypeError(
                        f"{self.__name__}: injectable '{param.name}' cannot be supplied")
                layout.append("R")
                _note(param.name)
                continue
            marker = _INJECTABLE_MARKERS.get(param.annotation)
            if marker is not None:
                if param.name in bound.arguments:
                    raise TypeError(f"{self.__name__}: injectable '{param.name}' cannot be supplied")
                layout.append(marker)
                _note(param.name)
                continue
            if param.annotation is LOGGER:
                if param.name in bound.arguments:
                    raise TypeError(f"{self.__name__}: injectable '{param.name}' cannot be supplied")
                import logging

                logger = logging.getLogger("hgraph")
                layout.append("s")   # process-wide logger: a plain object scalar
                _note(param.name)
                scalars.append(logger)
                scalar_values[param.name] = logger
                continue
            if isinstance(param.annotation, _ContextExpr):
                # Context-injected: resolved from the published stack by
                # type (and name); never supplied positionally.
                requirement = bound.arguments.get(param.name, param.default)
                name = None
                required = False
                if isinstance(requirement, _Required):
                    required, name = True, requirement.name
                elif isinstance(requirement, str):
                    name = requirement
                resolved = _resolve_context(param.annotation, name)
                if resolved is None:
                    where = f" with name {name}" if name else ""
                    if required or name is not None:
                        raise WiringError(
                            f"no context published for '{param.name}'{where} of '{self.__name__}'")
                    continue   # optional and absent: the fn sees its None default
                is_active = active_policy is None or param.name in active_policy
                layout.append("C" if is_active else "P")
                _note(param.name)
                ports.append(_unwrap(resolved))
                reference_shapes.append(False)
                continue
            if param.name == "_output":
                # hgraph's _output injection: the node's own output view.
                if param.name in bound.arguments:
                    raise TypeError(f"{self.__name__}: _output cannot be supplied")
                layout.append("o")
                _note(param.name)
                continue
            value = bound.arguments.get(param.name, _MISSING)
            if value is _MISSING:
                if param.default is inspect.Parameter.empty:
                    raise TypeError(f"{self.__name__}: missing argument '{param.name}'")
                value = param.default
                if value is None and isinstance(param.annotation, (_TsExpr,)):
                    # unwired optional ts input: a never-ticking source
                    value = wire("nothing", output_type=param.annotation)
            if not isinstance(value, WiringPort) and isinstance(
                    param.annotation, (_TsExpr, _GenericTsExpr)) and value is not None \
                    and not (value is _MISSING):
                # A plain VALUE on a time-series parameter lifts to const at
                # the declared type (hgraph's auto-const rule); conversion
                # errors surface as wiring errors.
                if isinstance(param.annotation, _TsExpr):
                    value = wire("const", value, output_type=param.annotation)
            if isinstance(value, WiringPort):
                self._check_binding(scope, param, value)
                required = valid_policy is None or param.name in valid_policy
                is_active = active_policy is None or param.name in active_policy
                layout.append({(True, True): "t", (True, False): "u",
                               (False, True): "T", (False, False): "U"}[(is_active, required)])
                _note(param.name)
                ports.append(_unwrap(value))
                requested = self._requested_input_shape(scope, param.annotation, value)
                reference_shapes.append(
                    requested
                    if requested is not None and _hgraph.ref_target(requested) != requested
                    else False)
            else:
                layout.append("s")
                _note(param.name)
                scalars.append(value)
                scalar_values[param.name] = value
        if self._requires is not None:
            try:
                verdict = _run_requires(self._requires, scope.bindings, scalar_values)
            except KeyError as error:
                raise RequirementsNotMetWiringError(
                    f"{self.__name__}: requires= references an unresolved type variable {error}") from error
            if verdict is not True:
                reason = verdict if isinstance(verdict, str) else "requirements not met"
                raise RequirementsNotMetWiringError(f"{self.__name__}: {reason}")
        if self._resolvers:
            self._apply_resolvers(scope, scalar_values)
        packed = WiringPort(_hgraph.bundle_port(ports, reference_shapes))
        config = "".join(layout)
        kw_names = [n for n, named in zip(layout_names, layout_by_name) if named]
        if kw_names:
            # ``layout|name,...``: the trailing entries fill BY NAME (all of
            # them when a star group rewrote the fn to keyword-only params,
            # else the keyword-only tail).
            config += "|" + ",".join(kw_names)
        node_kwargs = {"fn": ref, "config": config, "scalars": _hgraph.any_list(scalars)}
        recordable_state_type = None
        if self._recordable_state is not None:
            recordable_state_type = self._resolve_recordable_state(
                self._recordable_state, scope)
            node_kwargs["recordable_state_schema"] = recordable_state_type.handle
        for phase in ("start", "stop"):
            lifecycle_fn = getattr(self, f"_{phase}_fn")
            lifecycle_layout, lifecycle_scalars = [], []
            if lifecycle_fn is not None:
                for param in inspect.signature(lifecycle_fn).parameters.values():
                    marker = _INJECTABLE_MARKERS.get(param.annotation)
                    if marker is not None:
                        lifecycle_layout.append(marker)
                        continue
                    value = scalar_values.get(param.name, _MISSING)
                    if value is _MISSING:
                        if param.default is inspect.Parameter.empty:
                            raise TypeError(
                                f"{self.__name__}.{phase}: scalar '{param.name}' is not supplied by the node call"
                            )
                        value = param.default
                    lifecycle_layout.append("s")
                    lifecycle_scalars.append(value)
            node_kwargs[f"{phase}_fn"] = _hgraph.node_ref(lifecycle_fn or self.fn)
            node_kwargs[f"{phase}_enabled"] = lifecycle_fn is not None
            node_kwargs[f"{phase}_config"] = "".join(lifecycle_layout)
            node_kwargs[f"{phase}_scalars"] = _hgraph.any_list(lifecycle_scalars)
        if self.has_output:
            out_tp = self._out_tp
            if isinstance(out_tp, (_GenericTsExpr, _TypeVarSentinel)):
                resolved = scope.resolve_ts(_pattern_of(out_tp))
                if resolved is not None:
                    out_tp = _TsExpr(resolved, f"resolved[{out_tp!r}]")
            if not isinstance(out_tp, _TsExpr):
                raise TypeError(f"@compute_node '{self.__name__}' needs a TS[...] return annotation")
            op_name = ("__py_compute_recordable" if recordable_state_type is not None
                       else "__py_compute")
            return wire(op_name, packed, output_type=out_tp, **node_kwargs)
        return wire("__py_sink", packed, **node_kwargs)


def _make_py_node(fn, *, has_output, active, valid, resolvers, overloads, requires):
    node = _PyNode(fn, has_output=has_output, active=active, valid=valid, resolvers=resolvers)
    if overloads is not None:
        _register_overload(overloads, node, requires)
    elif requires is not None:
        # a plain node checks its own requires= at wiring (upstream raises
        # RequirementsNotMetWiringError on rejection).
        node._requires = requires
    return node


def compute_node(fn=None, *, active=None, valid=None, resolvers=None, overloads=None,
                 requires=None, label=None):
    """Python runtime compute node.

    ``active`` names the inputs that drive invocation; ``valid`` names the
    inputs required to be valid (either accepts a wiring-time callable
    ``(m, **scalars)``). ``resolvers`` maps type variables to wiring-time
    resolver callables. ``overloads=`` registers this node as a candidate
    of an @operator (or built-in operator) family; ``requires=`` guards its
    selection. ``label`` is accepted for hgraph parity (diagnostics)."""
    del label   # parity only: node naming rides the function identity
    if fn is None:
        return lambda f: _make_py_node(f, has_output=True, active=active, valid=valid,
                                       resolvers=resolvers, overloads=overloads, requires=requires)
    return _make_py_node(fn, has_output=True, active=active, valid=valid,
                         resolvers=resolvers, overloads=overloads, requires=requires)


def sink_node(fn=None, *, active=None, valid=None, resolvers=None, overloads=None,
              requires=None, label=None):
    del label
    if fn is None:
        return lambda f: _make_py_node(f, has_output=False, active=active, valid=valid,
                                       resolvers=resolvers, overloads=overloads, requires=requires)
    return _make_py_node(fn, has_output=False, active=active, valid=valid,
                         resolvers=resolvers, overloads=overloads, requires=requires)

def lift(fn, inputs=None, output=None, active=None, valid=None, all_valid=None,
         dedup_output=False, defaults=None):
    """hgraph's lift: wrap a plain scalar function as a compute node with a
    TS-lifted signature (each scalar annotation becomes TS[...]; ``inputs``/
    ``output`` override per name). Inputs cross into python nodes as plain
    values in this bridge, so the wrapped callable is the function itself."""
    from .._types import TS

    sig = inspect.signature(fn)

    def _scalar(arg):
        # python nodes receive live TimeSeries VIEWS; the lifted fn is a
        # plain scalar function (upstream passes a.value if a.valid).
        if isinstance(arg, _hgraph.TimeSeries):
            return arg.value if arg.valid else None
        return arg

    def _lifted(*args, **kwargs):
        return fn(*(_scalar(a) for a in args), **{k: _scalar(v) for k, v in kwargs.items()})

    def _ts(tp):
        return tp if isinstance(tp, (_TsExpr, _GenericTsExpr)) else TS[tp]

    parameters, annotations = [], {}
    for name, parameter in sig.parameters.items():
        tp = (inputs or {}).get(name, parameter.annotation)
        if tp is inspect.Parameter.empty:
            raise WiringError(f"lift: parameter '{name}' needs an annotation or an inputs= entry")
        ts_tp = _ts(tp)
        annotations[name] = ts_tp
        default = (defaults or {}).get(name, parameter.default)
        parameters.append(parameter.replace(annotation=ts_tp, default=default))
    return_annotation = output if output is not None else sig.return_annotation
    if return_annotation in (inspect.Signature.empty, None):
        raise WiringError(f"lift: '{getattr(fn, '__name__', fn)!r}' needs a return annotation or output=")
    annotations["return"] = _ts(return_annotation)
    _lifted.__name__ = getattr(fn, "__name__", "lifted")
    _lifted.__qualname__ = _lifted.__name__
    _lifted.__annotations__ = annotations
    _lifted.__signature__ = sig.replace(parameters=parameters,
                                        return_annotation=annotations["return"])
    node = _PyNode(_lifted, has_output=True, active=active, valid=valid)
    if not dedup_output:
        return node

    def _deduplicated(*args, **kwargs):
        return wire("dedup", node(*args, **kwargs))

    _deduplicated.__name__ = _lifted.__name__
    _deduplicated.__qualname__ = _lifted.__qualname__
    _deduplicated.__annotations__ = annotations
    _deduplicated.__signature__ = _lifted.__signature__

    from ._graph import graph

    return graph(_deduplicated)




class _Generator:
    """@generator: a Python generator function yielding (datetime, value)
    pairs; each pair is emitted at its ABSOLUTE time. Wiring-time arguments
    are captured per call (each call is a distinct source node)."""

    def __init__(self, fn):
        self.fn = fn
        self.__name__ = fn.__name__
        self._out_tp = inspect.signature(fn).return_annotation

    @property
    def signature(self):
        from .._signature import WiringNodeType, extract_signature

        return extract_signature(self.fn, WiringNodeType.PULL_SOURCE_NODE)

    def __call__(self, *args, **kwargs):
        if not isinstance(self._out_tp, _TsExpr):
            raise TypeError(f"@generator '{self.__name__}' needs a TS[...] return annotation")
        fn, call_args, call_kwargs = self.fn, args, kwargs

        def bound():
            return fn(*call_args, **call_kwargs)

        ref = _hgraph.node_ref(bound)
        return wire("__py_generator", fn=ref, output_type=self._out_tp)


def generator(fn=None, *, overloads=None, requires=None):
    def _make(f):
        wrapped = _Generator(f)
        if overloads is not None:
            _register_overload(overloads, wrapped, requires)
        return wrapped

    if fn is None:
        return _make
    return _make(fn)

class _PushQueue:
    """hgraph's @push_queue: the wrapped function is the node's START
    lifecycle hook - called with the sender callable as its first argument
    (plus any wiring-time scalars as kwargs) once the real-time graph is
    running. sender(value) is thread-safe from any Python thread."""

    def __init__(self, fn, tp, conflate):
        self.fn = fn
        self.tp = tp
        self.conflate = conflate
        self.__name__ = fn.__name__

    def __call__(self, **scalars):
        w = _current_wiring()
        fn = self.fn

        def on_start(sender):
            fn(sender.send, **scalars)

        port, _sender = w.push_source(_unwrap(self.tp), self.conflate, on_start)
        return WiringPort(port)


def push_queue(tp, conflate=False):
    def decorator(fn):
        return _PushQueue(fn, tp, conflate)

    return decorator

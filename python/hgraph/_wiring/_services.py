"""Service/adaptor decorators, impl registration and the wiring-scope
``context``."""
import inspect

import _hgraph

from .._types import (_GenericTsExpr, _TsExpr, _TypeVarSentinel,
                      _pattern_of, _type_var_is_scalar, _type_var_name,
                      _value_type)
from ._core import WiringError, WiringPort, _current_wiring, _unwrap
from ._graph import _wrap_graph_fn
from ._node import _PyNode, _warn_deprecated


_TS_ANNOTATIONS = (_TsExpr, _GenericTsExpr)


def _is_ts_annotation(annotation):
    return isinstance(annotation, _TS_ANNOTATIONS)


def _resolved_service_path(stub, path):
    resolver = getattr(stub, "_resolved_path", None)
    return resolver(path) if resolver is not None else path


def _apply_service_resolvers(resolution, resolvers):
    for sentinel, resolver in (resolvers or {}).items():
        name = _type_var_name(sentinel)
        if name in resolution.bindings:
            continue
        resolved = resolver(resolution.bindings)
        _PyNode._bind_resolved(resolution, name, resolved)
    return resolution


def _specialization_label(resolution):
    segments = []
    for name, value in resolution.bindings.items():
        label = value.name if isinstance(value, _hgraph.ValueType) else repr(value)
        segments.append(f"{name}={label}")
    return ",".join(sorted(segments))


def _specialization(item, owner, resolvers=None):
    items = item if isinstance(item, tuple) else (item,)
    resolution = _hgraph.ResolutionScope()
    for binding in items:
        if not isinstance(binding, slice) or binding.step is not None:
            raise TypeError(
                f"{owner} specialization requires TYPEVAR: concrete entries")
        variable, concrete = binding.start, binding.stop
        if not isinstance(variable, _TypeVarSentinel):
            raise TypeError(f"{owner} specialization key is not a type variable")
        name = _type_var_name(variable)
        if _type_var_is_scalar(variable):
            meta = _value_type(concrete)
            constraints = tuple(getattr(variable, "__constraints__", ()))
            if constraints and all(meta != _value_type(constraint) for constraint in constraints):
                allowed = ", ".join(getattr(value, "__name__", repr(value)) for value in constraints)
                raise TypeError(f"{name} must be one of {allowed}, got {meta.name}")
            resolution.bind_scalar(name, meta)
        else:
            meta = concrete.handle if isinstance(concrete, _TsExpr) else concrete
            resolution.bind_ts(name, meta)
    _apply_service_resolvers(resolution, resolvers)
    return resolution, _specialization_label(resolution)


def _inferred_specialization(fn, request_annotation, request, resolvers=None):
    resolution = _hgraph.ResolutionScope()
    actual = _unwrap(request).ts_type
    if not resolution.match(_pattern_of(request_annotation), actual):
        raise TypeError(
            f"generic adaptor '{fn.__name__}' request does not match its type pattern")
    _apply_service_resolvers(resolution, resolvers)
    specialization = _specialization_label(resolution)
    if not specialization:
        raise TypeError(
            f"generic adaptor '{fn.__name__}' could not infer its type specialization")
    return resolution, specialization


def _resolve_annotation(annotation, resolution):
    if isinstance(annotation, _TsExpr):
        return annotation.handle
    if isinstance(annotation, _GenericTsExpr) and resolution is not None:
        return resolution.resolve_ts(_pattern_of(annotation))
    return None


class context:
    """Publish a port as a named context for the wiring scope within:
    ``with hg.context("name", port): ...``; consume with
    ``hg.context.get("name")`` / test with ``hg.context.has("name")``.
    Same-wiring only (the design record's semantics)."""

    def __init__(self, name, port):
        self._name, self._port = name, port

    def __enter__(self):
        _hgraph.push_context(_current_wiring(), self._name, _unwrap(self._port))
        return self

    def __exit__(self, *exc):
        _hgraph.pop_context()
        return False

    @staticmethod
    def get(name):
        return WiringPort(_hgraph.get_context(_current_wiring(), name))

    @staticmethod
    def has(name):
        return _hgraph.has_context(_current_wiring(), name)


class _ServiceStub:
    """A service interface stub (hgraph's service decorators): calling it
    wires a CLIENT; register_service registers an implementation."""

    def __init__(self, fn, flavour, *, resolution=None, specialization="",
                 resolvers=None, deprecated=False):
        self.fn = fn
        self.__name__ = fn.__name__
        self.flavour = flavour
        self._specialization = specialization
        self._resolvers = dict(resolvers) if resolvers else None
        self._deprecated = deprecated
        sig = inspect.signature(fn)
        params = [p for p in sig.parameters.values() if _is_ts_annotation(p.annotation)]
        self._request_annotation = params[0].annotation if params else None
        out = sig.return_annotation
        if not _is_ts_annotation(out):
            raise TypeError(f"@{flavour}_service '{self.__name__}' requires a time-series return annotation")
        path_param = sig.parameters.get("path")
        default_path = (
            path_param.default
            if path_param is not None and isinstance(path_param.default, str)
            else f"{fn.__name__}_default"
        )
        if specialization:
            default_path = f"{default_path}[{specialization}]"
        kwargs = {
            "name": fn.__name__,
            "flavour": flavour,
            "default_path": default_path,
            "specialization": specialization,
        }

        if flavour == "reference":
            kwargs["output"] = _resolve_annotation(out, resolution)
        elif flavour == "subscription":
            if not params:
                raise TypeError(f"@subscription_service '{self.__name__}' needs a TS[key] parameter")
            kwargs["key_ts"] = _resolve_annotation(params[0].annotation, resolution)
            kwargs["value"] = _resolve_annotation(out, resolution)
        elif flavour == "request_reply":
            if not params:
                raise TypeError(f"@request_reply_service '{self.__name__}' needs a request parameter")
            kwargs["request"] = _resolve_annotation(params[0].annotation, resolution)
            kwargs["response"] = _resolve_annotation(out, resolution)
        unresolved = [name for name, value in kwargs.items()
                      if name in {"output", "key_ts", "value", "request", "response"}
                      and value is None]
        self.descriptor = None if unresolved else _hgraph.service_descriptor(**kwargs)

    def __getitem__(self, item):
        if self.descriptor is not None and not self._specialization:
            raise TypeError(f"service '{self.__name__}' is not generic")

        resolution, specialization = _specialization(
            item, f"service '{self.__name__}'", self._resolvers)
        result = _ServiceStub(
            self.fn, self.flavour, resolution=resolution,
            specialization=specialization, resolvers=self._resolvers,
            deprecated=self._deprecated)
        if result.descriptor is None:
            raise TypeError(
                f"service '{self.__name__}' specialization leaves an unresolved time-series type")
        return result

    def _require_descriptor(self):
        if self.descriptor is None:
            raise TypeError(
                f"generic service '{self.__name__}' must be specialized, for example "
                f"{self.__name__}[NUMBER:int]")
        return self.descriptor

    def _resolved_path(self, path):
        if not path or not self._specialization:
            return path
        suffix = f"[{self._specialization}]"
        return path if path.endswith(suffix) else f"{path}{suffix}"

    def __call__(self, ts=None, *, path=""):
        _warn_deprecated(self.__name__, self._deprecated)
        if isinstance(ts, str):
            # hgraph's reference-service call shape: the interface takes
            # ``path`` as its (only) positional parameter.
            path, ts = ts, None
        stub = self
        if self.descriptor is None:
            if ts is not None and self._request_annotation is not None:
                resolution, specialization = _inferred_specialization(
                    self.fn, self._request_annotation, ts, self._resolvers)
            else:
                resolution = _apply_service_resolvers(
                    _hgraph.ResolutionScope(), self._resolvers)
                specialization = _specialization_label(resolution)
            stub = _ServiceStub(
                self.fn, self.flavour, resolution=resolution,
                specialization=specialization, resolvers=self._resolvers,
                deprecated=self._deprecated)
        w = _current_wiring()
        port = _hgraph.service_client(w, stub._require_descriptor(), stub._resolved_path(path),
                                      None if ts is None else _unwrap(ts))
        return WiringPort(port)

    def wire_impl_inputs_stub(self, path=""):
        """hgraph parity: the interface inputs inside a service impl."""
        return _ServiceInputs(impl_input(self, path))

    def wire_impl_out_stub(self, path, out):
        """hgraph parity: publish this interface's output inside an impl."""
        impl_output(self, out, path)


def reference_service(fn=None, resolvers=None):
    """The service interface stub for a reference service: the return
    annotation is the shared output type; calling the stub wires a client."""
    if fn is None:
        return lambda f: _ServiceStub(f, "reference", resolvers=resolvers)
    return _ServiceStub(fn, "reference", resolvers=resolvers)


def subscription_service(fn=None, resolvers=None):
    """Subscription-service stub: first TS param = the subscription key,
    return annotation = the per-key value; call with the key time-series."""
    if fn is None:
        return lambda f: _ServiceStub(f, "subscription", resolvers=resolvers)
    return _ServiceStub(fn, "subscription", resolvers=resolvers)


def request_reply_service(fn=None, resolvers=None):
    """Request/reply stub: first TS param = the request, return annotation
    = the response; call with the request time-series."""
    if fn is None:
        return lambda f: _ServiceStub(f, "request_reply", resolvers=resolvers)
    return _ServiceStub(fn, "request_reply", resolvers=resolvers)


class _AdaptorStub:
    """@adaptor: an adaptor interface stub - the first TS parameter is the
    graph-side input (optional), the return annotation the graph-side output
    (optional). Calling the stub wires a CLIENT."""

    def __init__(self, fn, *, resolution=None, specialization="",
                 resolvers=None):
        self.fn = fn
        self.__name__ = fn.__name__
        self.flavour = "adaptor"
        self._specialization = specialization
        self._resolvers = dict(resolvers) if resolvers else None
        sig = inspect.signature(fn)
        params = [p for p in sig.parameters.values() if _is_ts_annotation(p.annotation)]
        self._request_annotation = params[0].annotation if params else None
        out = sig.return_annotation
        path_param = sig.parameters.get("path")
        default_path = (
            path_param.default
            if path_param is not None and isinstance(path_param.default, str)
            else f"{fn.__name__}_default"
        )
        if specialization:
            default_path = f"{default_path}[{specialization}]"
        kwargs = {
            "name": fn.__name__,
            "flavour": "adaptor",
            "default_path": default_path,
            "specialization": specialization,
        }
        if params:
            kwargs["request"] = _resolve_annotation(params[0].annotation, resolution)
        if _is_ts_annotation(out):
            kwargs["output"] = _resolve_annotation(out, resolution)
        unresolved = [
            name for name in ("request", "output")
            if name in kwargs and kwargs[name] is None
        ]
        self.descriptor = None if unresolved else _hgraph.service_descriptor(**kwargs)

    def __getitem__(self, item):
        if self.descriptor is not None and not self._specialization:
            raise TypeError(f"adaptor '{self.__name__}' is not generic")
        resolution, specialization = _specialization(
            item, f"adaptor '{self.__name__}'", self._resolvers)
        result = _AdaptorStub(
            self.fn, resolution=resolution, specialization=specialization,
            resolvers=self._resolvers)
        if result.descriptor is None:
            raise TypeError(
                f"adaptor '{self.__name__}' specialization leaves an unresolved time-series type")
        return result

    def _require_descriptor(self):
        if self.descriptor is None:
            raise TypeError(f"generic adaptor '{self.__name__}' must be specialized")
        return self.descriptor

    def _resolved_path(self, path):
        if not path or not self._specialization:
            return path
        suffix = f"[{self._specialization}]"
        return path if path.endswith(suffix) else f"{path}{suffix}"

    def __call__(self, ts=None, *, path=""):
        stub = self
        if self.descriptor is None:
            if ts is not None and self._request_annotation is not None:
                resolution, specialization = _inferred_specialization(
                    self.fn, self._request_annotation, ts, self._resolvers)
            else:
                resolution = _apply_service_resolvers(
                    _hgraph.ResolutionScope(), self._resolvers)
                specialization = _specialization_label(resolution)
            stub = _AdaptorStub(
                self.fn, resolution=resolution, specialization=specialization,
                resolvers=self._resolvers)
        port = _hgraph.adaptor_client(_current_wiring(), stub._require_descriptor(), stub._resolved_path(path),
                                      None if ts is None else _unwrap(ts))
        return None if port is None else WiringPort(port)


def adaptor(fn=None, resolvers=None):
    if fn is None:
        return lambda f: _AdaptorStub(f, resolvers=resolvers)
    return _AdaptorStub(fn, resolvers=resolvers)


class _ServiceAdaptorStub:
    """@service_adaptor: one request time-series per client, multiplexed as
    ``TSD[int, request]`` for the implementation and demultiplexed from its
    ``TSD[int, response]`` result by the native runtime."""

    def __init__(self, fn, *, resolution=None, specialization="",
                 resolvers=None):
        self.fn = fn
        self.__name__ = fn.__name__
        self.flavour = "service_adaptor"
        self._specialization = specialization
        self._resolvers = dict(resolvers) if resolvers else None
        sig = inspect.signature(fn)
        params = [p for p in sig.parameters.values() if _is_ts_annotation(p.annotation)]
        if len(params) != 1:
            raise TypeError(
                f"@service_adaptor '{self.__name__}' requires exactly one time-series request parameter"
            )
        if not _is_ts_annotation(sig.return_annotation):
            raise TypeError(f"@service_adaptor '{self.__name__}' requires a time-series return annotation")
        self._request_name = params[0].name
        self._request_annotation = params[0].annotation
        path_param = sig.parameters.get("path")
        default_path = (
            path_param.default
            if path_param is not None and isinstance(path_param.default, str)
            else ""
        )
        if specialization:
            default_path = f"{default_path}[{specialization}]"
        request = _resolve_annotation(params[0].annotation, resolution)
        output = _resolve_annotation(sig.return_annotation, resolution)
        self.descriptor = None if request is None or output is None else _hgraph.service_descriptor(
            name=fn.__name__, flavour="service_adaptor",
            request=request, output=output,
            default_path=default_path, specialization=specialization)

    def __getitem__(self, item):
        if self.descriptor is not None and not self._specialization:
            raise TypeError(f"service adaptor '{self.__name__}' is not generic")
        resolution, specialization = _specialization(
            item, f"service adaptor '{self.__name__}'", self._resolvers)
        result = _ServiceAdaptorStub(
            self.fn, resolution=resolution, specialization=specialization,
            resolvers=self._resolvers)
        if result.descriptor is None:
            raise TypeError(
                f"service adaptor '{self.__name__}' specialization leaves an unresolved time-series type")
        return result

    def _require_descriptor(self):
        if self.descriptor is None:
            raise TypeError(
                f"generic service adaptor '{self.__name__}' must be specialized")
        return self.descriptor

    def _resolved_path(self, path):
        if not path or not self._specialization:
            return path
        suffix = f"[{self._specialization}]"
        return path if path.endswith(suffix) else f"{path}{suffix}"

    def __call__(self, *args, path="", **kwargs):
        if args and isinstance(args[0], str):
            if path:
                raise TypeError(f"{self.__name__} received path twice")
            path, args = args[0], args[1:]
        if len(args) > 1:
            raise TypeError(f"{self.__name__} accepts one time-series request")
        if args:
            if self._request_name in kwargs:
                raise TypeError(f"{self.__name__} received its request twice")
            request = args[0]
        elif self._request_name in kwargs:
            request = kwargs.pop(self._request_name)
        else:
            raise TypeError(f"{self.__name__} requires '{self._request_name}'")
        if kwargs:
            raise TypeError(f"{self.__name__} got unexpected arguments {sorted(kwargs)!r}")
        stub = self
        if self.descriptor is None:
            resolution, specialization = _inferred_specialization(
                self.fn, self._request_annotation, request, self._resolvers)
            stub = _ServiceAdaptorStub(
                self.fn, resolution=resolution, specialization=specialization,
                resolvers=self._resolvers)
        return WiringPort(_hgraph.service_adaptor_client(
            _current_wiring(), stub._require_descriptor(), stub._resolved_path(path), _unwrap(request)))


class _AdaptorImplGroup:
    """One public registration token backed by concrete generic specializations."""

    def __init__(self, *implementations):
        self.implementations = implementations

    def wire_impl_inputs_stub(self, path=""):
        return _ServiceInputs(impl_input(self, self._resolved_path(path)))

    def wire_impl_out_stub(self, path, out):
        impl_output(self, out, self._resolved_path(path))


def service_adaptor(fn=None, resolvers=None):
    if fn is None:
        return lambda f: _ServiceAdaptorStub(f, resolvers=resolvers)
    return _ServiceAdaptorStub(fn, resolvers=resolvers)


def from_graph(stub, path=""):
    """Impl-side: the client input of ``stub`` (inside a registered impl)."""
    descriptor = stub._require_descriptor() if hasattr(stub, "_require_descriptor") else stub.descriptor
    path = _resolved_service_path(stub, path)
    if stub.flavour == "service_adaptor":
        return WiringPort(_hgraph.service_adaptor_from_graph(
            _current_wiring(), descriptor, path))
    return WiringPort(_hgraph.adaptor_from_graph(_current_wiring(), descriptor, path))


def to_graph(stub, out, path=""):
    """Impl-side: publish the adaptor output of ``stub`` back to clients."""
    descriptor = stub._require_descriptor() if hasattr(stub, "_require_descriptor") else stub.descriptor
    path = _resolved_service_path(stub, path)
    if stub.flavour == "service_adaptor":
        _hgraph.service_adaptor_to_graph(
            _current_wiring(), descriptor, path, out=_unwrap(out))
        return
    _hgraph.adaptor_to_graph(_current_wiring(), descriptor, path, out=_unwrap(out))


def register_adaptor(path, implementation, resolution_dict=None, **kwargs):
    """Bind an adaptor or service-adaptor implementation to ``path``."""
    if isinstance(implementation, _AdaptorImplGroup):
        for concrete in implementation.implementations:
            register_adaptor(
                path, concrete, resolution_dict=resolution_dict, **kwargs)
        return
    if not isinstance(implementation, _ServiceImpl):
        raise WiringError("register_adaptor requires an @adaptor_impl-decorated implementation")
    implementation = _resolve_registered_implementation(
        implementation, resolution_dict, "register_adaptor")
    flavours = {stub.flavour for stub in implementation.interfaces}
    if not flavours <= {"adaptor", "service_adaptor"}:
        raise WiringError("register_adaptor requires adaptor interfaces")
    if len(implementation.interfaces) > 1:
        if flavours != {"service_adaptor"}:
            raise WiringError(
                "multi-interface Python registration is supported for service adaptors only")
        resolved_paths = {
            _resolved_service_path(stub, path) for stub in implementation.interfaces
        }
        if len(resolved_paths) != 1:
            raise WiringError("multi-interface adaptors require one shared type specialization")
        resolved_path = resolved_paths.pop()
        impl_fn = _bind_registered_impl(implementation, resolved_path, kwargs)
        _hgraph.register_multi_service_impl(
            _current_wiring(), [stub.descriptor for stub in implementation.interfaces], resolved_path,
            _wrap_graph_fn(impl_fn))
        return
    stub = implementation.interfaces[0]
    resolved_path = _resolved_service_path(stub, path)
    impl_fn = _bind_registered_impl(implementation, resolved_path, kwargs)
    if stub.flavour == "service_adaptor":
        _hgraph.register_service_adaptor_impl(
            _current_wiring(), stub.descriptor, resolved_path, _wrap_graph_fn(impl_fn))
    else:
        _hgraph.register_adaptor_impl(
            _current_wiring(), stub.descriptor, resolved_path, _wrap_graph_fn(impl_fn))


def adaptor_impl(fn=None, *, interfaces=None, resolvers=None,
                 deprecated=False):
    """@adaptor_impl: declares the adaptor interfaces an implementation
    supports; the impl takes no wired inputs - it calls from_graph/to_graph."""
    if fn is None:
        return lambda f: _ServiceImpl(
            f, interfaces, resolvers=resolvers, deprecated=deprecated)
    return _ServiceImpl(
        fn, interfaces, resolvers=resolvers, deprecated=deprecated)


def service_adaptor_impl(fn=None, *, interfaces=None, resolvers=None,
                         label=None, deprecated=False):
    """Implementation of a service adaptor. A single-interface implementation
    consumes and returns dictionaries keyed by the native client id."""
    if fn is None:
        return lambda f: _ServiceImpl(
            f, interfaces, resolvers=resolvers, label=label,
            deprecated=deprecated)
    return _ServiceImpl(
        fn, interfaces, resolvers=resolvers, label=label,
        deprecated=deprecated)


_FLAVOUR_TS_ARITY = {
    "reference": 0,
    "subscription": 1,
    "request_reply": 1,
    "adaptor": 0,
    "service_adaptor": 1,
}


class _ServiceImpl:
    """@service_impl: an implementation declaring WHICH interfaces it
    supports - validated at decoration (signature shape per flavour) and
    used at registration (hgraph parity)."""

    def __init__(self, fn, interfaces, *, resolvers=None, label=None,
                 deprecated=False):
        self.fn = fn
        self.__name__ = fn.__name__
        self.resolvers = dict(resolvers) if resolvers else None
        self.label = label
        self.deprecated = deprecated
        if interfaces is None:
            raise TypeError(f"@service_impl '{self.__name__}' requires interfaces=")
        if not isinstance(interfaces, (tuple, list)):
            interfaces = (interfaces,)
        self.interfaces = tuple(self._resolve(stub) for stub in interfaces)
        target = getattr(fn, "fn", fn)   # unwrap @graph/@compute_node wrappers
        ts_params = [
            p for p in inspect.signature(target).parameters.values()
            if p.name != "path"
            and (_is_ts_annotation(p.annotation) or p.annotation is inspect.Signature.empty)
        ]
        if len(self.interfaces) > 1:
            # Multi-interface implementations take NO wired inputs: they
            # fetch each interface's input via impl_input and publish via
            # impl_output inside the body.
            if ts_params:
                raise TypeError(
                    f"@service_impl '{self.__name__}': a multi-interface implementation takes no "
                    "time-series parameters (use impl_input/impl_output)")
        else:
            for stub in self.interfaces:
                expected = _FLAVOUR_TS_ARITY[stub.flavour]
                if len(ts_params) != expected:
                    raise TypeError(
                        f"@service_impl '{self.__name__}': a {stub.flavour} implementation takes "
                        f"{expected} time-series parameter(s), found {len(ts_params)}"
                    )

    @staticmethod
    def _resolve(stub):
        if isinstance(stub, str):
            descriptor = _hgraph.find_service(stub)
            if descriptor is None:
                raise WiringError(f"no service interface named '{stub}'")

            class _CppStub:
                pass

            resolved = _CppStub()
            resolved.descriptor = descriptor
            resolved.flavour = descriptor.flavour
            return resolved
        if not isinstance(stub, (_ServiceStub, _AdaptorStub, _ServiceAdaptorStub)):
            raise TypeError(f"@service_impl interfaces must be service stubs, got {stub!r}")
        return stub


def service_impl(fn=None, *, interfaces=None, resolvers=None,
                 deprecated=False):
    """hgraph's @service_impl: declares (and validates) the interfaces an
    implementation supports; register with ``register_service(path, impl)``.
    Interfaces may be stubs or the NAMES of C++-defined interfaces (the
    ruled direction: Python impls for C++ stubs)."""
    if fn is None:
        return lambda f: _ServiceImpl(
            f, interfaces, resolvers=resolvers, deprecated=deprecated)
    return _ServiceImpl(
        fn, interfaces, resolvers=resolvers, deprecated=deprecated)


class _ServiceInputs:
    """hgraph's get_service_inputs result: the interface inputs, exposed as
    ``.ts`` (the single input time-series)."""

    __slots__ = ("ts",)

    def __init__(self, ts):
        self.ts = ts


def _bind_registered_impl(implementation, path, config):
    """Bind path/config while leaving only native service ports in the signature."""
    impl_fn = implementation.fn
    target = getattr(impl_fn, "fn", impl_fn)
    signature = inspect.signature(target)
    parameters = list(signature.parameters.values())
    expected_ports = 0 if len(implementation.interfaces) > 1 else _FLAVOUR_TS_ARITY[
        implementation.interfaces[0].flavour
    ]
    port_parameters = [
        param for param in parameters
        if param.name != "path"
        and (_is_ts_annotation(param.annotation) or param.annotation is inspect.Signature.empty)
    ]
    if len(port_parameters) != expected_ports:
        raise WiringError(
            f"implementation '{implementation.__name__}' requires {expected_ports} native service input(s)"
        )
    port_names = {param.name for param in port_parameters}
    scalar_parameters = [
        param for param in parameters if param.name != "path" and param.name not in port_names
    ]
    scalar_names = {param.name for param in scalar_parameters}
    unknown = set(config) - scalar_names
    if unknown:
        raise WiringError(
            f"implementation '{implementation.__name__}' has no scalar configuration {sorted(unknown)!r}"
        )
    for param in scalar_parameters:
        if param.name not in config and param.default is inspect.Parameter.empty:
            raise WiringError(
                f"implementation '{implementation.__name__}' requires scalar configuration '{param.name}'"
            )

    def bound(*ports):
        if len(ports) != len(port_parameters):
            raise WiringError(
                f"implementation '{implementation.__name__}' received {len(ports)} native service inputs"
            )
        arguments = dict(zip((param.name for param in port_parameters), ports))
        if any(param.name == "path" for param in parameters):
            arguments["path"] = path
        for param in scalar_parameters:
            arguments[param.name] = config.get(param.name, param.default)
        return impl_fn(**arguments)

    bound.__name__ = implementation.__name__
    bound.__signature__ = inspect.Signature(
        parameters=[
            param.replace(kind=inspect.Parameter.POSITIONAL_OR_KEYWORD)
            for param in port_parameters
        ],
        return_annotation=signature.return_annotation,
    )
    if implementation.resolvers or implementation.deprecated or implementation.label:
        from ._graph import _GraphFn

        return _GraphFn(
            bound,
            resolvers=implementation.resolvers,
            label=implementation.label,
            deprecated=implementation.deprecated,
        )
    return bound


def _resolve_registered_implementation(implementation, resolution_dict, operation):
    """Apply an upstream ``resolution_dict`` to unresolved interface stubs.

    The result is a shallow call-local implementation token. The decorated
    object remains reusable for other concrete specializations.
    """
    unresolved = [
        stub for stub in implementation.interfaces
        if getattr(stub, "descriptor", None) is None
    ]
    if not unresolved:
        return implementation
    if not resolution_dict:
        names = ", ".join(stub.__name__ for stub in unresolved)
        raise WiringError(
            f"{operation} requires resolution_dict for generic interface(s): {names}")
    entries = tuple(slice(key, value) for key, value in resolution_dict.items())
    import copy

    resolved = copy.copy(implementation)
    resolved.interfaces = tuple(
        stub[entries] if getattr(stub, "descriptor", None) is None else stub
        for stub in implementation.interfaces
    )
    return resolved


def get_service_inputs(path, stub):
    """hgraph parity: the interface's inputs inside a service impl."""
    return _ServiceInputs(impl_input(stub, path))


def set_service_output(path, stub, out):
    """hgraph parity: publish an interface's output inside a service impl."""
    impl_output(stub, out, path)


def impl_input(stub, path=""):
    """Inside a multi-interface implementation: the interface's input
    (subscription key set / request dictionary)."""
    descriptor = stub._require_descriptor() if hasattr(stub, "_require_descriptor") else stub.descriptor
    return WiringPort(_hgraph.service_impl_input(
        _current_wiring(), descriptor, _resolved_service_path(stub, path)))


def impl_output(stub, out, path=""):
    """Inside a multi-interface implementation: publish the interface's
    output explicitly."""
    descriptor = stub._require_descriptor() if hasattr(stub, "_require_descriptor") else stub.descriptor
    _hgraph.service_impl_output(
        _current_wiring(), descriptor, _resolved_service_path(stub, path), out=_unwrap(out))


def register_service(path, implementation, resolution_dict=None, **kwargs):
    """Bind ``implementation`` (an @service_impl) to ``path`` (hgraph's
    signature: path first). A SINGLE-interface impl wires with its input
    supplied and output captured automatically; a MULTI-interface impl
    takes no wired inputs and uses impl_input/impl_output per interface."""
    if not isinstance(implementation, _ServiceImpl):
        raise WiringError("register_service requires an @service_impl-decorated implementation")
    implementation = _resolve_registered_implementation(
        implementation, resolution_dict, "register_service")
    if len(implementation.interfaces) > 1:
        resolved_paths = {
            _resolved_service_path(stub, path) for stub in implementation.interfaces
        }
        if len(resolved_paths) != 1:
            raise WiringError("multi-interface services require one shared type specialization")
        resolved_path = resolved_paths.pop()
        impl_fn = _bind_registered_impl(implementation, resolved_path, kwargs)
        _hgraph.register_multi_service_impl(
            _current_wiring(), [stub.descriptor for stub in implementation.interfaces], resolved_path,
            _wrap_graph_fn(impl_fn))
        return
    stub = implementation.interfaces[0]
    resolved_path = _resolved_service_path(stub, path)
    impl_fn = _bind_registered_impl(implementation, resolved_path, kwargs)
    _hgraph.register_service_impl(
        _current_wiring(), stub.descriptor, resolved_path, _wrap_graph_fn(impl_fn))

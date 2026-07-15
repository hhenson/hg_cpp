"""Service/adaptor decorators, impl registration and the wiring-scope
``context``."""
import inspect

import _hgraph

from .._types import _TsExpr
from ._core import WiringError, WiringPort, _current_wiring, _unwrap
from ._graph import _wrap_graph_fn

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

    def __init__(self, fn, flavour):
        self.fn = fn
        self.__name__ = fn.__name__
        self.flavour = flavour
        sig = inspect.signature(fn)
        params = [p for p in sig.parameters.values() if isinstance(p.annotation, _TsExpr)]
        out = sig.return_annotation
        kwargs = {"name": fn.__name__, "flavour": flavour}
        if flavour == "reference":
            kwargs["output"] = _unwrap(out)
        elif flavour == "subscription":
            if not params:
                raise TypeError(f"@subscription_service '{self.__name__}' needs a TS[key] parameter")
            kwargs["key_ts"] = _unwrap(params[0].annotation)
            kwargs["value"] = _unwrap(out)
        elif flavour == "request_reply":
            if not params:
                raise TypeError(f"@request_reply_service '{self.__name__}' needs a request parameter")
            kwargs["request"] = _unwrap(params[0].annotation)
            kwargs["response"] = _unwrap(out)
        self.descriptor = _hgraph.service_descriptor(**kwargs)

    def __call__(self, ts=None, *, path=""):
        if isinstance(ts, str):
            # hgraph's reference-service call shape: the interface takes
            # ``path`` as its (only) positional parameter.
            path, ts = ts, None
        w = _current_wiring()
        port = _hgraph.service_client(w, self.descriptor, path,
                                      None if ts is None else _unwrap(ts))
        return WiringPort(port)

    def wire_impl_inputs_stub(self, path=""):
        """hgraph parity: the interface inputs inside a service impl."""
        return _ServiceInputs(impl_input(self, path))

    def wire_impl_out_stub(self, path, out):
        """hgraph parity: publish this interface's output inside an impl."""
        impl_output(self, out, path)


def reference_service(fn):
    """The service interface stub for a reference service: the return
    annotation is the shared output type; calling the stub wires a client."""
    return _ServiceStub(fn, "reference")


def subscription_service(fn):
    """Subscription-service stub: first TS param = the subscription key,
    return annotation = the per-key value; call with the key time-series."""
    return _ServiceStub(fn, "subscription")


def request_reply_service(fn):
    """Request/reply stub: first TS param = the request, return annotation
    = the response; call with the request time-series."""
    return _ServiceStub(fn, "request_reply")


class _AdaptorStub:
    """@adaptor: an adaptor interface stub - the first TS parameter is the
    graph-side input (optional), the return annotation the graph-side output
    (optional). Calling the stub wires a CLIENT."""

    def __init__(self, fn):
        self.fn = fn
        self.__name__ = fn.__name__
        self.flavour = "adaptor"
        sig = inspect.signature(fn)
        params = [p for p in sig.parameters.values() if isinstance(p.annotation, _TsExpr)]
        out = sig.return_annotation
        kwargs = {"name": fn.__name__, "flavour": "adaptor"}
        if params:
            kwargs["request"] = _unwrap(params[0].annotation)   # the adaptor input slot
        if isinstance(out, _TsExpr):
            kwargs["output"] = _unwrap(out)
        self.descriptor = _hgraph.service_descriptor(**kwargs)

    def __call__(self, ts=None, *, path=""):
        port = _hgraph.adaptor_client(_current_wiring(), self.descriptor, path,
                                      None if ts is None else _unwrap(ts))
        return None if port is None else WiringPort(port)


def adaptor(fn):
    return _AdaptorStub(fn)


class _ServiceAdaptorStub:
    """@service_adaptor: one request time-series per client, multiplexed as
    ``TSD[int, request]`` for the implementation and demultiplexed from its
    ``TSD[int, response]`` result by the native runtime."""

    def __init__(self, fn):
        self.fn = fn
        self.__name__ = fn.__name__
        self.flavour = "service_adaptor"
        sig = inspect.signature(fn)
        params = [p for p in sig.parameters.values() if isinstance(p.annotation, _TsExpr)]
        if len(params) != 1:
            raise TypeError(
                f"@service_adaptor '{self.__name__}' requires exactly one time-series request parameter"
            )
        if not isinstance(sig.return_annotation, _TsExpr):
            raise TypeError(f"@service_adaptor '{self.__name__}' requires a time-series return annotation")
        self._request_name = params[0].name
        path_param = sig.parameters.get("path")
        default_path = (
            path_param.default
            if path_param is not None and isinstance(path_param.default, str)
            else ""
        )
        self.descriptor = _hgraph.service_descriptor(
            name=fn.__name__, flavour="service_adaptor",
            request=_unwrap(params[0].annotation), output=_unwrap(sig.return_annotation),
            default_path=default_path)

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
        return WiringPort(_hgraph.service_adaptor_client(
            _current_wiring(), self.descriptor, path, _unwrap(request)))

    def wire_impl_inputs_stub(self, path=""):
        return _ServiceInputs(impl_input(self, path))

    def wire_impl_out_stub(self, path, out):
        impl_output(self, out, path)


def service_adaptor(fn):
    return _ServiceAdaptorStub(fn)


def from_graph(stub, path=""):
    """Impl-side: the client input of ``stub`` (inside a registered impl)."""
    if stub.flavour == "service_adaptor":
        return WiringPort(_hgraph.service_adaptor_from_graph(
            _current_wiring(), stub.descriptor, path))
    return WiringPort(_hgraph.adaptor_from_graph(_current_wiring(), stub.descriptor, path))


def to_graph(stub, out, path=""):
    """Impl-side: publish the adaptor output of ``stub`` back to clients."""
    if stub.flavour == "service_adaptor":
        _hgraph.service_adaptor_to_graph(
            _current_wiring(), stub.descriptor, path, out=_unwrap(out))
        return
    _hgraph.adaptor_to_graph(_current_wiring(), stub.descriptor, path, out=_unwrap(out))


def register_adaptor(path, implementation, **kwargs):
    """Bind an adaptor or service-adaptor implementation to ``path``."""
    if not isinstance(implementation, _ServiceImpl):
        raise WiringError("register_adaptor requires an @adaptor_impl-decorated implementation")
    impl_fn = _bind_registered_impl(implementation, path, kwargs)
    flavours = {stub.flavour for stub in implementation.interfaces}
    if not flavours <= {"adaptor", "service_adaptor"}:
        raise WiringError("register_adaptor requires adaptor interfaces")
    if len(implementation.interfaces) > 1:
        if flavours != {"service_adaptor"}:
            raise WiringError(
                "multi-interface Python registration is supported for service adaptors only")
        _hgraph.register_multi_service_impl(
            _current_wiring(), [stub.descriptor for stub in implementation.interfaces], path,
            _wrap_graph_fn(impl_fn))
        return
    stub = implementation.interfaces[0]
    if stub.flavour == "service_adaptor":
        _hgraph.register_service_adaptor_impl(
            _current_wiring(), stub.descriptor, path, _wrap_graph_fn(impl_fn))
    else:
        _hgraph.register_adaptor_impl(
            _current_wiring(), stub.descriptor, path, _wrap_graph_fn(impl_fn))


def adaptor_impl(fn=None, *, interfaces=None):
    """@adaptor_impl: declares the adaptor interfaces an implementation
    supports; the impl takes no wired inputs - it calls from_graph/to_graph."""
    if fn is None:
        return lambda f: _ServiceImpl(f, interfaces)
    return _ServiceImpl(fn, interfaces)


def service_adaptor_impl(fn=None, *, interfaces=None):
    """Implementation of a service adaptor. A single-interface implementation
    consumes and returns dictionaries keyed by the native client id."""
    if fn is None:
        return lambda f: _ServiceImpl(f, interfaces)
    return _ServiceImpl(fn, interfaces)


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

    def __init__(self, fn, interfaces):
        self.fn = fn
        self.__name__ = fn.__name__
        if interfaces is None:
            raise TypeError(f"@service_impl '{self.__name__}' requires interfaces=")
        if not isinstance(interfaces, (tuple, list)):
            interfaces = (interfaces,)
        self.interfaces = tuple(self._resolve(stub) for stub in interfaces)
        target = getattr(fn, "fn", fn)   # unwrap @graph/@compute_node wrappers
        ts_params = [
            p for p in inspect.signature(target).parameters.values()
            if p.name != "path"
            and (isinstance(p.annotation, _TsExpr) or p.annotation is inspect.Signature.empty)
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


def service_impl(fn=None, *, interfaces=None):
    """hgraph's @service_impl: declares (and validates) the interfaces an
    implementation supports; register with ``register_service(path, impl)``.
    Interfaces may be stubs or the NAMES of C++-defined interfaces (the
    ruled direction: Python impls for C++ stubs)."""
    if fn is None:
        return lambda f: _ServiceImpl(f, interfaces)
    return _ServiceImpl(fn, interfaces)


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
        and (isinstance(param.annotation, _TsExpr) or param.annotation is inspect.Signature.empty)
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
    return bound


def get_service_inputs(path, stub):
    """hgraph parity: the interface's inputs inside a service impl."""
    return _ServiceInputs(impl_input(stub, path))


def set_service_output(path, stub, out):
    """hgraph parity: publish an interface's output inside a service impl."""
    impl_output(stub, out, path)


def impl_input(stub, path=""):
    """Inside a multi-interface implementation: the interface's input
    (subscription key set / request dictionary)."""
    return WiringPort(_hgraph.service_impl_input(_current_wiring(), stub.descriptor, path))


def impl_output(stub, out, path=""):
    """Inside a multi-interface implementation: publish the interface's
    output explicitly."""
    _hgraph.service_impl_output(_current_wiring(), stub.descriptor, path, out=_unwrap(out))


def register_service(path, implementation, **kwargs):
    """Bind ``implementation`` (an @service_impl) to ``path`` (hgraph's
    signature: path first). A SINGLE-interface impl wires with its input
    supplied and output captured automatically; a MULTI-interface impl
    takes no wired inputs and uses impl_input/impl_output per interface."""
    if not isinstance(implementation, _ServiceImpl):
        raise WiringError("register_service requires an @service_impl-decorated implementation")
    impl_fn = _bind_registered_impl(implementation, path, kwargs)
    if len(implementation.interfaces) > 1:
        _hgraph.register_multi_service_impl(
            _current_wiring(), [stub.descriptor for stub in implementation.interfaces], path,
            _wrap_graph_fn(impl_fn))
        return
    stub = implementation.interfaces[0]
    _hgraph.register_service_impl(
        _current_wiring(), stub.descriptor, path, _wrap_graph_fn(impl_fn))

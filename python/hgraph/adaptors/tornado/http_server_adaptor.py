"""Tornado HTTP server adaptor and its request/response values."""

import asyncio
import inspect
import logging
import threading
from dataclasses import dataclass

import _hgraph
import tornado.iostream
from frozendict import frozendict

from hgraph import (
    CompoundScalar,
    GlobalState,
    TS,
    TSD,
    adaptor,
    adaptor_impl,
    feedback,
    from_graph,
    graph,
    map_,
    push_queue,
    register_adaptor,
    sink_node,
    to_graph,
)
from hgraph._types import _TsExpr
from hgraph._wiring import _GraphFn, _PyNode
from hgraph._wiring._markers import (
    LOGGER,
    _INJECTABLE_MARKERS,
    _RecordableStateExpr,
)

from ._tornado_web import BaseHandler, TornadoWeb

logger = logging.getLogger(__name__)


def _handler_parameters(signature):
    return tuple(
        parameter
        for name, parameter in signature.parameters.items()
        if name != "request"
        and parameter.annotation not in _INJECTABLE_MARKERS
        and parameter.annotation is not LOGGER
        and not isinstance(parameter.annotation, _RecordableStateExpr)
    )


def _ts_expr(handle) -> _TsExpr:
    return _TsExpr(handle, repr(handle))


def _bundle_field_types(annotation) -> dict[str, _TsExpr] | None:
    if not isinstance(annotation, _TsExpr) or not annotation.handle.is_tsb:
        return None
    return {
        name: _ts_expr(field_type)
        for name, field_type in _hgraph.ts_field_types(annotation.handle)
    }


def _keyed_bundle_field_types(annotation) -> dict[str, _TsExpr] | None:
    if not isinstance(annotation, _TsExpr) or not annotation.handle.is_tsd:
        return None
    element_type = _hgraph.tsd_element_ts(annotation.handle)
    if not element_type.is_tsb:
        return None
    return {
        name: _ts_expr(field_type)
        for name, field_type in _hgraph.ts_field_types(element_type)
    }


@dataclass(frozen=True)
class HttpRequest(CompoundScalar, abstract=True):
    """Base request value; construct one of the concrete method leaves."""

    url: str
    url_parsed_args: tuple[str, ...] = ()
    query: dict[str, str] = frozendict()
    headers: dict[str, str] = frozendict()
    cookies: dict[str, dict[str, object]] = frozendict()
    auth: object = None
    connect_timeout: float = 20.0
    request_timeout: float = 20.0


@dataclass(frozen=True)
class HttpGetRequest(HttpRequest):
    pass


@dataclass(frozen=True)
class HttpDeleteRequest(HttpRequest):
    pass


@dataclass(frozen=True)
class HttpPutRequest(HttpRequest):
    body: str = ""


@dataclass(frozen=True)
class HttpPostRequest(HttpRequest):
    body: str = ""


@dataclass(frozen=True)
class HttpResponse(CompoundScalar):
    status_code: int
    headers: dict[str, str] = frozendict()
    cookies: dict[str, dict[str, object]] = frozendict()
    body: bytes = b""

    async def write(self, stream) -> None:
        if self.body:
            stream.write(self.body)

    def __repr__(self) -> str:
        return (
            f"HttpResponse(status_code={self.status_code}, headers={self.headers}, "
            f"cookies={self.cookies}, body_length={len(self.body)})"
        )


class HttpAdaptorManager:
    """Coordinate request futures and graph queues for one listening port."""

    _instances: dict[int, "HttpAdaptorManager"] = {}
    _instances_lock = threading.Lock()

    @classmethod
    def instance(cls, port: int) -> "HttpAdaptorManager":
        with cls._instances_lock:
            manager = cls._instances.get(port)
            if manager is None:
                manager = cls(port)
                cls._instances[port] = manager
            return manager

    def __init__(self, port: int):
        self._web = TornadoWeb.instance(port)
        self._queues = {}
        self._registered_paths = set()
        self._requests = {}
        self._next_request_id = 1

    def set_queue(self, path: str, sender) -> None:
        self._queues[path] = sender

    def register_path(self, path: str) -> None:
        if path not in self._registered_paths:
            self._web.add_handler(path, HttpHandler, {"path": path, "manager": self})
            self._registered_paths.add(path)

    def start(self, path: str) -> None:
        self.register_path(path)
        self._web.start()

    def stop(self, path: str) -> None:
        completed = threading.Event()

        def cleanup():
            try:
                self._queues.pop(path, None)
                for request_id, (request_path, future) in tuple(self._requests.items()):
                    if request_path == path:
                        if not future.done():
                            future.cancel()
                        self._requests.pop(request_id, None)
            finally:
                completed.set()

        TornadoWeb.get_loop().add_callback(cleanup)
        if not completed.wait(timeout=5.0):
            raise RuntimeError(f"HTTP route {path!r} did not stop")
        self._web.stop()

    def add_request(self, path: str, request: HttpRequest):
        sender = self._queues.get(path)
        if sender is None:
            raise RuntimeError(f"HTTP graph queue for {path!r} is not running")
        request_id = self._next_request_id
        self._next_request_id += 1
        future = asyncio.get_running_loop().create_future()
        self._requests[request_id] = (path, future)
        sender({request_id: request})
        return request_id, future

    def complete_request(self, request_id: int, response: HttpResponse) -> None:
        pending = self._requests.get(request_id)
        if pending is None:
            return
        future = pending[1]
        if not future.done():
            future.set_result(response)

    def remove_request(self, request_id: int) -> None:
        pending = self._requests.pop(request_id, None)
        if pending is not None and not pending[1].done():
            pending[1].cancel()


class HttpHandler(BaseHandler):
    """Translate Tornado requests into the closed HTTP request union."""

    def initialize(self, path: str, manager: HttpAdaptorManager) -> None:
        self._path = path
        self._manager = manager

    def _common_arguments(self, args) -> dict:
        return {
            "url": self._path,
            "url_parsed_args": tuple(args),
            "query": frozendict(
                (key, "".join(value.decode() for value in values))
                for key, values in self.request.query_arguments.items()
            ),
            "headers": frozendict(self.request.headers.items()),
            "cookies": frozendict(
                (
                    key,
                    frozendict({"value": morsel.value, **dict(morsel.items())}),
                )
                for key, morsel in self.request.cookies.items()
            ),
            "auth": getattr(self, "current_user", None),
        }

    async def get(self, *args) -> None:
        await self._handle_request(HttpGetRequest(**self._common_arguments(args)))

    async def delete(self, *args) -> None:
        await self._handle_request(HttpDeleteRequest(**self._common_arguments(args)))

    async def post(self, *args) -> None:
        await self._handle_request(
            HttpPostRequest(
                **self._common_arguments(args),
                body=self.request.body.decode("utf-8"),
            )
        )

    async def put(self, *args) -> None:
        await self._handle_request(
            HttpPutRequest(
                **self._common_arguments(args),
                body=self.request.body.decode("utf-8"),
            )
        )

    async def _handle_request(self, request: HttpRequest) -> None:
        request_id, response_future = self._manager.add_request(self._path, request)
        try:
            response = await response_future
            self.set_status(response.status_code)
            for key, value in response.headers.items():
                self.set_header(key, value)
            for key, value in response.cookies.items():
                if isinstance(value, str):
                    self.set_cookie(key, value)
                else:
                    self.set_cookie(key, **value)
            await response.write(self)
            await self.finish()
        except tornado.iostream.StreamClosedError:
            pass
        finally:
            self._manager.remove_request(request_id)


class _HttpServerHandler:
    def __init__(self, fn, url: str):
        # A handler is a graph or a Python-authored node. A bare function is
        # treated as a graph (matching rest_handler's `graph(fn)` and so the
        # later `map_(self._fn, ...)` accepts it); already-decorated graphs and
        # nodes pass through unchanged. The global _as_wired strictness (bare
        # named functions must be tagged) is intentionally left in place.
        self._fn = fn if isinstance(fn, (_GraphFn, _PyNode)) else graph(fn)
        self.url = url
        self.__name__ = getattr(fn, "__name__", "http_server_handler")

        target = getattr(fn, "fn", fn)
        signature = inspect.signature(target)
        request = signature.parameters.get("request")
        if request is None:
            raise TypeError("HTTP handler requires a 'request' time-series input")

        single_request = TS[HttpRequest]
        batch_request = TSD[int, TS[HttpRequest]]
        if request.annotation == single_request:
            self._single = True
        elif request.annotation == batch_request:
            self._single = False
        else:
            raise TypeError(
                "HTTP handler request must be TS[HttpRequest] or "
                "TSD[int, TS[HttpRequest]]"
            )

        expected_output = TS[HttpResponse] if self._single else TSD[int, TS[HttpResponse]]
        output = signature.return_annotation
        self._auxiliary_output = False
        if output != expected_output:
            fields = _bundle_field_types(output)
            if fields is not None and fields.get("response") == expected_output:
                self._auxiliary_output = True
            elif not self._single:
                fields = _keyed_bundle_field_types(output)
                if fields is not None and fields.get("response") == TS[HttpResponse]:
                    self._auxiliary_output = True
                else:
                    fields = None
            else:
                fields = None
        if output != expected_output and not self._auxiliary_output:
            raise TypeError(
                f"HTTP handler output must be {expected_output!r}, a TSB with a "
                f"'response' field of that type, or a keyed TSD of response bundles"
            )

        parameters = _handler_parameters(signature)
        self.__signature__ = signature.replace(parameters=parameters)
        self.auto_wire = not self._auxiliary_output and all(
            parameter.default is not inspect.Parameter.empty
            for parameter in parameters
        )

    def __call__(self, *args, **kwargs):
        bound = self.__signature__.bind(*args, **kwargs)
        bound.apply_defaults()

        response_feedback = feedback(TSD[int, TS[HttpResponse]])
        requests = http_server_adaptor(response_feedback(), path=self.url)
        if self._single:
            responses = map_(self._fn, requests, *bound.args, **bound.kwargs)
        else:
            responses = self._fn(request=requests, **bound.arguments)
        response_feedback(responses.response if self._auxiliary_output else responses)
        return responses


_HTTP_SERVER_HANDLERS: dict[str, _HttpServerHandler] = {}


def http_server_handler(fn=None, *, url: str):
    """Declare an HTTP route handled by a graph or Python-authored node.

    A handler accepts either one ``TS[HttpRequest]`` and returns one
    ``TS[HttpResponse]``, or accepts and returns the corresponding keyed
    ``TSD`` forms. The output may instead be a ``TSB`` with a field named
    ``response`` of the primary response type; keyed batch handlers may also
    return a ``TSD`` of those bundles. Auxiliary-output handlers are wired
    explicitly so their full output remains observable. Response-only handlers
    without additional required inputs are wired by
    :func:`register_http_server_adaptor`.
    """
    if fn is None:
        return lambda decorated: http_server_handler(decorated, url=url)

    handler = _HttpServerHandler(fn, url)
    _HTTP_SERVER_HANDLERS[url] = handler
    return handler


@adaptor
def http_server_adaptor(
    response: TSD[int, TS[HttpResponse]],
    path: str = "http_server",
) -> TSD[int, TS[HttpRequest]]:
    """Expose a graph request/response stream as an HTTP route."""


@adaptor_impl(interfaces=http_server_adaptor)
def http_server_adaptor_impl(path: str, port: int = 80) -> None:
    """Run one HTTP route on the shared Tornado server for ``port``."""
    queue_key = f"http_server_adaptor://{port}/{path}/queue"
    manager = HttpAdaptorManager.instance(port)

    @push_queue(TSD[int, TS[HttpRequest]])
    def from_web(sender) -> None:
        GlobalState.instance()[queue_key] = sender

    @sink_node
    def to_web(responses: TSD[int, TS[HttpResponse]]) -> None:
        loop = TornadoWeb.get_loop()
        for request_id, response in responses.modified_items():
            loop.add_callback(manager.complete_request, request_id, response.value)

    @to_web.start
    def to_web_start(_global_state: GlobalState = None) -> None:
        manager.set_queue(path, _global_state[queue_key])
        manager.start(path)

    @to_web.stop
    def to_web_stop(_global_state: GlobalState = None) -> None:
        try:
            manager.stop(path)
        finally:
            if queue_key in _global_state:
                del _global_state[queue_key]

    requests = from_web()
    to_web(from_graph(http_server_adaptor, path=path))
    to_graph(http_server_adaptor, requests, path=path)


def register_http_server_adaptor(port: int) -> None:
    """Register the HTTP server implementation and wire automatic handlers."""
    handlers = tuple(_HTTP_SERVER_HANDLERS.items())
    manager = HttpAdaptorManager.instance(port)

    # A shared listener may accept requests as soon as the first adaptor starts.
    # Install the complete route table before any adaptor can open that listener.
    for path, _ in handlers:
        manager.register_path(path)

    for path, handler in handlers:
        register_adaptor(path, http_server_adaptor_impl, port=port)
        if handler.auto_wire:
            handler()


__all__ = (
    "HttpAdaptorManager",
    "HttpDeleteRequest",
    "HttpGetRequest",
    "HttpHandler",
    "HttpPostRequest",
    "HttpPutRequest",
    "HttpRequest",
    "HttpResponse",
    "http_server_handler",
    "http_server_adaptor",
    "http_server_adaptor_impl",
    "register_http_server_adaptor",
)

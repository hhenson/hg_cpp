"""Tornado WebSocket server adaptor and graph handler support."""

import asyncio
import inspect
import threading
from dataclasses import dataclass
from typing import Generic, TypeVar

import tornado.websocket
from frozendict import frozendict

from hgraph import (
    CompoundScalar,
    GlobalState,
    REMOVE,
    TS,
    TSB,
    TSD,
    TimeSeriesSchema,
    adaptor,
    adaptor_impl,
    combine,
    feedback,
    from_graph,
    graph,
    map_,
    push_queue,
    register_adaptor,
    sink_node,
    to_graph,
)

from ._tornado_web import BaseHandler, TornadoWeb


STR_OR_BYTES = TypeVar("STR_OR_BYTES", bytes, str)


@dataclass(frozen=True)
class WebSocketConnectRequest(CompoundScalar):
    url: str
    url_parsed_args: tuple[str, ...] = ()
    headers: dict[str, str] = frozendict()
    cookies: dict[str, dict[str, object]] = frozendict()
    auth: object = None


class WebSocketServerRequest(TimeSeriesSchema, Generic[STR_OR_BYTES]):
    connect_request: TS[WebSocketConnectRequest]
    messages: TS[tuple[STR_OR_BYTES, ...]]


class WebSocketClientRequest(TimeSeriesSchema, Generic[STR_OR_BYTES]):
    connect_request: TS[WebSocketConnectRequest]
    message: TS[STR_OR_BYTES]


class WebSocketResponse(TimeSeriesSchema, Generic[STR_OR_BYTES]):
    connect_response: TS[bool]
    message: TS[STR_OR_BYTES]


class WebSocketAdaptorManager:
    """Bridge one message type and port between Tornado and graph queues."""

    _instances = {}
    _instances_lock = threading.Lock()

    @classmethod
    def instance(cls, port: int, message_type: type) -> "WebSocketAdaptorManager":
        key = (port, message_type)
        with cls._instances_lock:
            manager = cls._instances.get(key)
            if manager is None:
                manager = cls(port, message_type)
                cls._instances[key] = manager
            return manager

    def __init__(self, port: int, message_type: type):
        self._web = TornadoWeb.instance(port)
        self._message_type = message_type
        self._queues = {}
        self._registered_paths = set()
        self._requests = {}
        self._pending_connect = {}
        self._next_request_id = 1

    def set_queues(self, path, connect_sender, message_sender) -> None:
        self._queues[path] = (connect_sender, message_sender)
        for request_id, request in self._pending_connect.pop(path, ()):
            if request_id in self._requests:
                connect_sender({request_id: request})

    @property
    def binary(self) -> bool:
        return self._message_type is bytes

    def start(self, path: str) -> None:
        self.register(path)
        self._web.start()

    def register(self, path: str) -> None:
        if path not in self._registered_paths:
            self._web.add_handler(
                path,
                WebSocketHandler,
                {"path": path, "manager": self},
            )
            self._registered_paths.add(path)

    def stop(self, path: str) -> None:
        completed = threading.Event()

        def cleanup():
            try:
                self._queues.pop(path, None)
                for request_id, pending in tuple(self._requests.items()):
                    request_path, future, _ = pending
                    if request_path != path:
                        continue
                    if not future.done():
                        future.cancel()
                    self._requests.pop(request_id, None)
                self._pending_connect.pop(path, None)
            finally:
                completed.set()

        TornadoWeb.get_loop().add_callback(cleanup)
        if not completed.wait(timeout=5.0):
            raise RuntimeError(f"WebSocket route {path!r} did not stop")
        self._web.stop()

    def add_request(self, path, request, message_handler):
        senders = self._queues.get(path)
        request_id = self._next_request_id
        self._next_request_id += 1
        future = asyncio.get_running_loop().create_future()
        self._requests[request_id] = (path, future, message_handler)
        if senders is None:
            # A sibling route may already have started the shared server.
            # Preserve the accepted upstream behaviour by holding the open
            # request until this route's graph queues enter their start phase.
            self._pending_connect.setdefault(path, []).append((request_id, request))
        else:
            senders[0]({request_id: request})
        return request_id, future

    def add_message(self, request_id: int, message) -> None:
        pending = self._requests.get(request_id)
        if pending is None:
            return
        normalized = message
        if self._message_type is bytes and isinstance(message, str):
            normalized = message.encode()
        elif self._message_type is str and isinstance(message, bytes):
            normalized = message.decode()
        self._queues[pending[0]][1]({request_id: (normalized,)})

    def complete_request(self, request_id: int, response) -> None:
        pending = self._requests.get(request_id)
        if pending is None:
            return
        _, future, message_handler = pending
        if "connect_response" in response and not future.done():
            future.set_result(bool(response["connect_response"]))
        if "message" in response:
            TornadoWeb.get_loop().add_callback(message_handler, response["message"])

    def remove_request(self, request_id: int) -> None:
        pending = self._requests.pop(request_id, None)
        if pending is None:
            return
        path, future, _ = pending
        if not future.done():
            future.cancel()
        queued = self._pending_connect.get(path)
        if queued is not None:
            self._pending_connect[path] = [item for item in queued if item[0] != request_id]
            if not self._pending_connect[path]:
                self._pending_connect.pop(path, None)
        senders = self._queues.get(path)
        if senders is not None:
            senders[0]({request_id: REMOVE})
            senders[1]({request_id: REMOVE})


class WebSocketHandler(tornado.websocket.WebSocketHandler):
    def initialize(self, path: str, manager: WebSocketAdaptorManager) -> None:
        self._path = path
        self._manager = manager
        self._request_id = None
        self._accepted = False

    async def prepare(self) -> None:
        await BaseHandler.prepare(self)

    async def open(self, *args) -> None:
        request = WebSocketConnectRequest(
            url=self._path,
            url_parsed_args=tuple(args),
            headers=frozendict(self.request.headers.items()),
            cookies=frozendict(
                (
                    key,
                    frozendict({"value": morsel.value, **dict(morsel.items())}),
                )
                for key, morsel in self.request.cookies.items()
            ),
            auth=getattr(self, "current_user", None),
        )
        self._request_id, response = self._manager.add_request(
            self._path,
            request,
            lambda message: self.write_message(
                message,
                binary=self._manager.binary,
            ),
        )
        try:
            self._accepted = await response
        except asyncio.CancelledError:
            self.close()
            return
        if not self._accepted:
            self.close()

    def on_message(self, message) -> None:
        if self._accepted and self._request_id is not None:
            self._manager.add_message(self._request_id, message)

    def on_close(self) -> None:
        if self._request_id is not None:
            self._manager.remove_request(self._request_id)
            self._request_id = None


@adaptor
def websocket_server_adaptor(
    response: TSD[int, TSB[WebSocketResponse[STR_OR_BYTES]]],
    path: str = "websocket_server",
) -> TSD[int, TSB[WebSocketServerRequest[STR_OR_BYTES]]]:
    """Expose a typed graph request/response stream as a WebSocket route."""


_SERVER_IMPLEMENTATIONS = {}


def _server_implementation(message_type: type):
    implementation = _SERVER_IMPLEMENTATIONS.get(message_type)
    if implementation is not None:
        return implementation

    interface = websocket_server_adaptor[STR_OR_BYTES:message_type]
    request_bundle = TSB[WebSocketServerRequest[message_type]]
    response_bundle = TSB[WebSocketResponse[message_type]]
    requests_type = TSD[int, request_bundle]
    responses_type = TSD[int, response_bundle]
    message_ts = TS[tuple[message_type, ...]]

    @adaptor_impl(interfaces=(interface,))
    def websocket_server_adaptor_impl(path: str, port: int, url: str) -> None:
        connect_key = f"websocket_server_adaptor://{port}/{path}/connect_queue"
        message_key = f"websocket_server_adaptor://{port}/{path}/message_queue"
        manager = WebSocketAdaptorManager.instance(port, message_type)
        # Route discovery is a wiring concern. Register every route before
        # graph start so one adaptor cannot expose the shared port while a
        # sibling route still returns a transient 404.
        manager.register(url)

        @push_queue(TSD[int, TS[WebSocketConnectRequest]])
        def connections_from_web(sender) -> None:
            GlobalState.instance()[connect_key] = sender

        @push_queue(TSD[int, message_ts])
        def messages_from_web(sender) -> None:
            GlobalState.instance()[message_key] = sender

        @graph
        def make_request(
            connect_request: TS[WebSocketConnectRequest],
            messages: message_ts,
        ) -> request_bundle:
            return combine[request_bundle](
                connect_request=connect_request,
                messages=messages,
            )

        @sink_node
        def to_web(responses: responses_type) -> None:
            loop = TornadoWeb.get_loop()
            for request_id, response in responses.modified_items():
                loop.add_callback(
                    manager.complete_request,
                    request_id,
                    response.delta_value,
                )

        @to_web.start
        def to_web_start(_global_state: GlobalState = None) -> None:
            manager.set_queues(
                url,
                _global_state[connect_key],
                _global_state[message_key],
            )
            manager.start(url)

        @to_web.stop
        def to_web_stop(_global_state: GlobalState = None) -> None:
            try:
                manager.stop(url)
            finally:
                for key in (connect_key, message_key):
                    if key in _global_state:
                        del _global_state[key]

        connections = connections_from_web()
        messages = messages_from_web()
        requests = map_(make_request, connections, messages)
        to_web(from_graph(interface, path=path))
        to_graph(interface, requests, path=path)

    _SERVER_IMPLEMENTATIONS[message_type] = websocket_server_adaptor_impl
    return websocket_server_adaptor_impl


class _WebSocketServerHandler:
    def __init__(self, fn, url: str):
        self._fn = fn
        self.url = url
        self.__name__ = getattr(fn, "__name__", "websocket_server_handler")

        target = getattr(fn, "fn", fn)
        signature = inspect.signature(target)
        request = signature.parameters.get("request")
        if request is None:
            raise TypeError("WebSocket handler requires a 'request' time-series input")

        self._single = None
        self.message_type = None
        for message_type in (str, bytes):
            single = TSB[WebSocketServerRequest[message_type]]
            batch = TSD[int, single]
            if request.annotation == single:
                self._single = True
                self.message_type = message_type
                expected_output = TSB[WebSocketResponse[message_type]]
                break
            if request.annotation == batch:
                self._single = False
                self.message_type = message_type
                expected_output = TSD[int, TSB[WebSocketResponse[message_type]]]
                break
        if self.message_type is None:
            raise TypeError(
                "WebSocket handler request must be TSB[WebSocketServerRequest[str|bytes]] "
                "or its keyed TSD form"
            )
        if signature.return_annotation != expected_output:
            raise TypeError(
                f"WebSocket handler output must be {expected_output!r} with the same message type"
            )

        parameters = tuple(
            parameter
            for name, parameter in signature.parameters.items()
            if name != "request"
        )
        self.__signature__ = signature.replace(parameters=parameters)
        self.auto_wire = all(
            parameter.default is not inspect.Parameter.empty
            for parameter in parameters
        )

    def __call__(self, *args, **kwargs):
        bound = self.__signature__.bind(*args, **kwargs)
        bound.apply_defaults()
        response_type = TSD[int, TSB[WebSocketResponse[self.message_type]]]
        interface = websocket_server_adaptor[STR_OR_BYTES:self.message_type]
        response_feedback = feedback(response_type)
        requests = interface(response_feedback(), path=self.url)
        if self._single:
            responses = map_(self._fn, requests, *bound.args, **bound.kwargs)
        else:
            responses = self._fn(request=requests, **bound.arguments)
        response_feedback(responses)
        return responses


_WEBSOCKET_SERVER_HANDLERS = {}


def websocket_server_handler(fn=None, *, url: str):
    """Declare a typed WebSocket route handled by a graph or Python node."""
    if fn is None:
        return lambda decorated: websocket_server_handler(decorated, url=url)
    handler = _WebSocketServerHandler(fn, url)
    _WEBSOCKET_SERVER_HANDLERS[url] = handler
    return handler


def register_websocket_server_adaptor(port: int) -> None:
    """Register all declared WebSocket routes on ``port``."""
    for url, handler in tuple(_WEBSOCKET_SERVER_HANDLERS.items()):
        register_adaptor(
            url,
            _server_implementation(handler.message_type),
            port=port,
            url=url,
        )
        if handler.auto_wire:
            handler()


class _WebSocketServerAdaptorImplementation:
    """Materialize every typed route behind the generic adaptor registration."""

    def _register_adaptor(self, path, *, resolution_dict=None, port=80):
        if resolution_dict:
            raise TypeError(
                "websocket_server_adaptor_impl resolves message types from its handlers"
            )
        register_websocket_server_adaptor(port)


websocket_server_adaptor_impl = _WebSocketServerAdaptorImplementation()
# The upstream helper and implementation are two registration entry points
# for the same generic path-routed graph.
websocket_server_adaptor_helper = websocket_server_adaptor_impl


__all__ = (
    "STR_OR_BYTES",
    "WebSocketAdaptorManager",
    "WebSocketClientRequest",
    "WebSocketConnectRequest",
    "WebSocketHandler",
    "WebSocketResponse",
    "WebSocketServerRequest",
    "register_websocket_server_adaptor",
    "websocket_server_adaptor",
    "websocket_server_adaptor_helper",
    "websocket_server_adaptor_impl",
    "websocket_server_handler",
)

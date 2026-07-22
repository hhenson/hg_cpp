import asyncio
from datetime import datetime, timedelta, timezone
import socket
from threading import Thread
import time

import pytest
from tornado.httpclient import HTTPClientError
from tornado.websocket import websocket_connect

import hgraph as hg
from hgraph.adaptors.tornado import (
    WebSocketAdaptorManager,
    WebSocketClientRequest,
    WebSocketConnectRequest,
    WebSocketResponse,
    WebSocketServerRequest,
    register_websocket_server_adaptor,
    websocket_client_adaptor,
    websocket_client_adaptor_impl,
    websocket_server_handler,
    websocket_server_adaptor_impl,
)


@pytest.fixture
def free_tcp_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_feedback_supports_keyed_websocket_response_bundles():
    response_type = hg.TSD[int, hg.TSB[WebSocketResponse[bytes]]]

    @hg.graph
    def delayed(response: response_type) -> response_type:
        response_feedback = hg.feedback(response_type)
        response_feedback(response)
        return response_feedback()

    assert hg.eval_node(
        delayed,
        [{1: {"connect_response": True}}, None],
    ) == [None, {1: {"connect_response": True}}]


def test_websocket_manager_buffers_open_until_route_queues_start(free_tcp_port):
    path = f"/websocket-pending-{free_tcp_port}/(.*)"
    manager = WebSocketAdaptorManager(free_tcp_port, bytes)
    connect_events = []
    message_events = []

    async def exercise():
        request = WebSocketConnectRequest(url=path)
        request_id, accepted = manager.add_request(path, request, lambda message: None)
        assert connect_events == []
        assert not accepted.done()

        manager.set_queues(path, connect_events.append, message_events.append)
        assert connect_events == [{request_id: request}]
        manager.complete_request(request_id, {"connect_response": True})
        assert await accepted is True

        manager.remove_request(request_id)
        assert connect_events[-1] == {request_id: hg.REMOVE}
        assert message_events == [{request_id: hg.REMOVE}]

    asyncio.run(exercise())


def test_websocket_server_handler_round_trips_binary_messages(free_tcp_port):
    route = f"/websocket-{free_tcp_port}/(.*)"
    connections = []
    client_results = []
    client_errors = []
    threads = []

    @websocket_server_handler(url=route)
    @hg.compute_node
    def echo(
        request: hg.TSB[WebSocketServerRequest[bytes]],
    ) -> hg.TSB[WebSocketResponse[bytes]]:
        result = {}
        if request.connect_request.modified:
            connections.append(request.connect_request.value)
            result["connect_response"] = True
        if request.messages.modified:
            result["message"] = request.messages.value[-1]
        return result

    @hg.push_queue(hg.TS[bool])
    def drive_client(sender):
        async def communicate():
            deadline = time.monotonic() + 10.0
            while True:
                try:
                    socket = await websocket_connect(
                        f"ws://127.0.0.1:{free_tcp_port}/websocket-{free_tcp_port}/client",
                        connect_timeout=3.0,
                    )
                    break
                except (ConnectionRefusedError, OSError, HTTPClientError):
                    if time.monotonic() >= deadline:
                        raise
                    await asyncio.sleep(0.02)
            try:
                socket.write_message(b"one", binary=True)
                client_results.append(await socket.read_message())
                socket.write_message(b"two", binary=True)
                client_results.append(await socket.read_message())
            finally:
                socket.close()

        def run():
            try:
                asyncio.run(communicate())
            except BaseException as error:
                client_errors.append(error)
            finally:
                sender(True)

        thread = Thread(target=run, name="hgraph-websocket-test", daemon=True)
        threads.append(thread)
        thread.start()

    @hg.sink_node
    def stop_when_done(
        done: hg.TS[bool],
        _engine: hg.EvaluationEngineApi = None,
    ) -> None:
        _engine.request_engine_stop()

    @hg.graph
    def server_graph() -> None:
        done = drive_client()
        hg.register_adaptor(
            "websocket_server_adaptor",
            websocket_server_adaptor_impl,
            port=free_tcp_port,
        )
        stop_when_done(done)

    state = hg.GlobalState()
    with hg.GlobalContext(state):
        hg.run_graph(
            server_graph,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None)
            + timedelta(seconds=5),
            run_mode=hg.EvaluationMode.REAL_TIME,
        )
    for thread in threads:
        thread.join(timeout=5.0)

    assert client_errors == []
    assert client_results == [b"one", b"two"]
    assert len(connections) == 1
    assert isinstance(connections[0], WebSocketConnectRequest)
    assert connections[0].url_parsed_args == ("client",)
    assert not any(key.startswith("websocket_server_adaptor://") for key in state.keys())


def test_websocket_server_handler_multiplexes_batch_requests(free_tcp_port):
    route = f"/websocket-batch-{free_tcp_port}/(.*)"
    connections = []
    client_results = []
    client_errors = []

    @websocket_server_handler(url=route)
    @hg.compute_node
    def echo(
        request: hg.TSD[int, hg.TSB[WebSocketServerRequest[bytes]]],
    ) -> hg.TSD[int, hg.TSB[WebSocketResponse[bytes]]]:
        responses = {}
        for request_id, request_value in request.modified_items():
            response = {}
            if request_value.connect_request.modified:
                connections.append(request_value.connect_request.value)
                response["connect_response"] = True
            if request_value.messages.modified:
                response["message"] = request_value.messages.value[-1]
            if response:
                responses[request_id] = response
        return responses

    @hg.push_queue(hg.TS[bool])
    def drive_clients(sender):
        async def communicate(name):
            deadline = time.monotonic() + 10.0
            while True:
                try:
                    socket = await websocket_connect(
                        f"ws://127.0.0.1:{free_tcp_port}/"
                        f"websocket-batch-{free_tcp_port}/{name}",
                        connect_timeout=3.0,
                    )
                    break
                except (ConnectionRefusedError, OSError, HTTPClientError):
                    if time.monotonic() >= deadline:
                        raise
                    await asyncio.sleep(0.02)
            try:
                message = name.encode()
                socket.write_message(message, binary=True)
                return await socket.read_message()
            finally:
                socket.close()

        def run():
            async def run_all():
                client_results.extend(
                    await asyncio.gather(
                        communicate("one"),
                        communicate("two"),
                    )
                )

            try:
                asyncio.run(run_all())
            except BaseException as error:
                client_errors.append(error)
            finally:
                sender(True)

        Thread(target=run, name="hgraph-websocket-batch-test", daemon=True).start()

    @hg.sink_node
    def stop_when_done(
        done: hg.TS[bool],
        _engine: hg.EvaluationEngineApi = None,
    ) -> None:
        _engine.request_engine_stop()

    @hg.graph
    def server_graph() -> None:
        done = drive_clients()
        hg.register_adaptor(
            "websocket_server_adaptor",
            websocket_server_adaptor_impl,
            port=free_tcp_port,
        )
        # ACE explicitly wires selected handlers after registering the legacy
        # server implementation. The second call must reuse the automatic
        # wiring rather than create a duplicate adaptor client.
        echo()
        stop_when_done(done)

    state = hg.GlobalState()
    with hg.GlobalContext(state):
        hg.run_graph(
            server_graph,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None)
            + timedelta(seconds=5),
            run_mode=hg.EvaluationMode.REAL_TIME,
        )

    assert client_errors == []
    assert sorted(client_results) == [b"one", b"two"]
    assert sorted(value.url_parsed_args[0] for value in connections) == ["one", "two"]
    assert not any(key.startswith("websocket_server_adaptor://") for key in state.keys())


@pytest.mark.parametrize(
    ("message_type", "payloads"),
    [
        (bytes, (b"one", b"two")),
        (str, ("one", "two")),
    ],
)
def test_websocket_client_service_adaptor_infers_message_specialization(
    free_tcp_port,
    message_type,
    payloads,
):
    route = f"/websocket-client-{message_type.__name__}-{free_tcp_port}/(.*)"
    received = []
    threads = []
    message_ts = hg.TS[message_type]
    server_request_type = hg.TSB[WebSocketServerRequest[message_type]]
    server_response_type = hg.TSB[WebSocketResponse[message_type]]
    client_request_type = hg.TSB[WebSocketClientRequest[message_type]]

    @websocket_server_handler(url=route)
    @hg.compute_node
    def echo(
        request: server_request_type,
    ) -> server_response_type:
        result = {}
        if request.connect_request.modified:
            result["connect_response"] = True
        if request.messages.modified:
            result["message"] = request.messages.value[-1]
        return result

    @hg.push_queue(message_ts)
    def messages(sender):
        def run():
            time.sleep(0.05)
            sender(payloads[0])
            time.sleep(0.02)
            sender(payloads[1])

        thread = Thread(target=run, name="hgraph-websocket-client-test", daemon=True)
        threads.append(thread)
        thread.start()

    @hg.sink_node
    def capture(
        message: message_ts,
        _engine: hg.EvaluationEngineApi = None,
    ) -> None:
        received.append(message.value)
        if len(received) == 2:
            _engine.request_engine_stop()

    @hg.graph
    def application() -> None:
        register_websocket_server_adaptor(free_tcp_port)
        hg.register_adaptor("websocket-client-test", websocket_client_adaptor_impl)
        request = hg.combine[client_request_type](
            connect_request=hg.const(
                WebSocketConnectRequest(
                    f"ws://127.0.0.1:{free_tcp_port}/websocket-client-"
                    f"{message_type.__name__}-{free_tcp_port}/client"
                ),
                tp=hg.TS[WebSocketConnectRequest],
            ),
            message=messages(),
        )
        response = websocket_client_adaptor(
            request,
            path="websocket-client-test",
        )
        capture(response.message)

    state = hg.GlobalState()
    with hg.GlobalContext(state):
        hg.run_graph(
            application,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None)
            + timedelta(seconds=5),
            run_mode=hg.EvaluationMode.REAL_TIME,
        )
    for thread in threads:
        thread.join(timeout=5.0)

    assert received == list(payloads)
    assert not any(key.startswith("websocket_client_adaptor://") for key in state.keys())

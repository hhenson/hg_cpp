from unittest.mock import MagicMock

from frozendict import frozendict

import hgraph as hg
from hgraph.adaptors.kafka import (
    KafkaMessage,
    KafkaMessageState,
    message_publisher,
    register_kafka_adaptor,
)


def _state_with_producer(producer):
    state = KafkaMessageState.instance()
    state.configure({"producer": producer})
    return state


def test_message_publisher_uses_the_configured_producer_via_eval_node():
    producer = MagicMock()

    @message_publisher(topic="test")
    def publisher() -> hg.TS[bytes]:
        return hg.const(b"payload", tp=hg.TS[bytes])

    @hg.graph
    def app():
        register_kafka_adaptor({"producer": producer})
        publisher()

    with hg.GlobalContext(hg.GlobalState()):
        assert hg.eval_node(app) is None

    producer.send.assert_called_once_with("test", b"payload")


def test_structured_message_maps_headers_without_a_second_protocol():
    producer = MagicMock()
    message = KafkaMessage(
        payload=b"payload",
        key=b"key",
        content_type="application/json",
        headers=frozendict({"trace": b"1"}),
    )

    @message_publisher
    def publisher() -> hg.TS[KafkaMessage]:
        return hg.const(message, tp=hg.TS[KafkaMessage])

    @hg.graph
    def app():
        register_kafka_adaptor({"producer": producer})
        publisher(topic="test")

    with hg.GlobalContext(hg.GlobalState()):
        assert hg.eval_node(app) is None

    producer.send.assert_called_once_with(
        "test",
        b"payload",
        key=b"key",
        headers=[("trace", b"1"), ("content-type", b"application/json")],
    )


def test_bundle_publisher_returns_its_non_message_output():
    producer = MagicMock()

    @message_publisher(topic="test")
    def publisher() -> hg.TSB["msg" : hg.TS[bytes], "out" : hg.TS[bool]]:
        return hg.combine(
            msg=hg.const(b"payload", tp=hg.TS[bytes]),
            out=hg.const(True, tp=hg.TS[bool]),
        )

    @hg.graph
    def app() -> hg.TS[bool]:
        register_kafka_adaptor({"producer": producer})
        return publisher()

    with hg.GlobalContext(hg.GlobalState()):
        assert hg.eval_node(app) == [True]
    producer.send.assert_called_once()

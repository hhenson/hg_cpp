import threading
from dataclasses import dataclass, field

from frozendict import frozendict

from hgraph import GlobalState, TS, push_queue, sink_node

from ._api import KafkaMessage, MessageState

__all__ = (
    "KafkaMessageState",
    "register_kafka_adaptor",
    "message_source",
)

CONTENT_TYPE_HEADER = "content-type"
_STATE_KEY = ":adaptors:kafka:state"


def _record_to_kafka_message(record):
    content_type = None
    headers = {}
    for key, value in record.headers or ():
        if key == CONTENT_TYPE_HEADER:
            content_type = value.decode("utf-8") if isinstance(value, bytes) else value
        else:
            headers[key] = value
    return KafkaMessage(
        payload=record.value,
        key=record.key,
        content_type=content_type,
        headers=frozendict(headers),
    )


@dataclass
class KafkaMessageState(MessageState):
    config: dict = field(default_factory=dict)
    publishers: set[str] = field(default_factory=set)
    subscribers: set[str] = field(default_factory=set)
    _producer: object = None
    _producer_owned: bool = False
    _consumers: dict = field(default_factory=dict)

    @classmethod
    def instance(cls):
        state = GlobalState.instance()
        value = state.get(_STATE_KEY)
        if value is None:
            value = cls()
            state[_STATE_KEY] = value
        return value

    def configure(self, config):
        self.config = dict(config)
        producer = self.config.pop("producer", None)
        if producer is not None:
            self._producer = producer
            self._producer_owned = False

    def add_publisher(self, topic):
        if topic in self.publishers:
            raise ValueError(f"topic {topic!r} already has a publisher")
        self.publishers.add(topic)

    def add_subscriber(self, topic):
        self.subscribers.add(topic)

    def producer(self):
        if self._producer is not None:
            return self._producer
        factory = self.config.get("producer_factory")
        options = {
            key: value
            for key, value in self.config.items()
            if key not in {"producer_factory", "consumer_factory"}
        }
        if factory is None:
            try:
                from kafka import KafkaProducer
            except ModuleNotFoundError as error:
                raise RuntimeError("Kafka publishing requires the 'kafka' extra") from error
            factory = KafkaProducer
        self._producer = factory(**options)
        self._producer_owned = True
        return self._producer

    def publish(self, topic, message):
        producer = self.producer()
        if isinstance(message, KafkaMessage):
            headers = list(message.headers.items())
            if message.content_type is not None:
                headers.append((CONTENT_TYPE_HEADER, message.content_type.encode("utf-8")))
            producer.send(
                topic,
                message.payload,
                key=message.key,
                headers=headers or None,
            )
        else:
            producer.send(topic, message)

    def flush(self):
        if self._producer is not None:
            self._producer.flush()

    def start_consumer(self, topic, sender):
        if topic in self._consumers:
            raise ValueError(f"topic {topic!r} already has a running subscriber")
        factory = self.config.get("consumer_factory")
        options = {
            key: value
            for key, value in self.config.items()
            if key not in {"producer_factory", "consumer_factory"}
        }
        if factory is None:
            try:
                from kafka import KafkaConsumer
            except ModuleNotFoundError as error:
                raise RuntimeError("Kafka subscribing requires the 'kafka' extra") from error
            factory = KafkaConsumer
        consumer = factory(topic, **options)
        stop = threading.Event()

        def consume():
            try:
                while not stop.is_set():
                    records = consumer.poll(timeout_ms=100, max_records=1000) or {}
                    messages = [record for batch in records.values() for record in batch]
                    messages.sort(key=lambda record: (record.timestamp, record.topic, record.offset))
                    for record in messages:
                        sender(_record_to_kafka_message(record))
            finally:
                consumer.close()

        thread = threading.Thread(target=consume, name=f"hgraph-kafka-{topic}", daemon=False)
        self._consumers[topic] = (consumer, stop, thread)
        thread.start()

    def stop_consumer(self, topic):
        entry = self._consumers.pop(topic, None)
        if entry is None:
            return
        _, stop, thread = entry
        stop.set()
        thread.join()

    def close(self):
        for topic in tuple(self._consumers):
            self.stop_consumer(topic)
        if self._producer is not None:
            self._producer.flush()
            if self._producer_owned:
                self._producer.close()
            self._producer = None


def register_kafka_adaptor(config: dict):
    KafkaMessageState.instance().configure(config)


def message_source(topic, state):
    @push_queue(TS[KafkaMessage])
    def source(sender):
        state.start_consumer(topic, sender)

    @sink_node
    def lifetime(message: TS[KafkaMessage]):
        pass

    @lifetime.stop
    def stop():
        state.stop_consumer(topic)

    message = source()
    lifetime(message)
    return message

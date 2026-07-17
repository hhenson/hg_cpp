import json
import threading
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.json as pa_json

from hgraph import (
    Frame,
    REMOVED,
    TS,
    TSB,
    TSD,
    combine,
    convert,
    graph,
    push_queue,
    service_adaptor,
    service_adaptor_impl,
    sink_node,
)
from hgraph.adaptors.data_catalogue import DataEnvironment
from hgraph.adaptors.executor import adaptor_executor
from hgraph.stream import Data, Stream, StreamStatus

_RAW_STREAM = TSB[Stream[Data[Frame]]]


def _read_json_table(path: Path) -> pa.Table:
    """Read JSON lines natively and fall back to ordinary JSON documents."""
    with path.open("rb") as stream:
        first = stream.read(1)
        while first and first.isspace():
            first = stream.read(1)
    if first != b"[":
        try:
            return pa_json.read_json(path)
        except pa.ArrowInvalid:
            pass

    with path.open("r", encoding="utf-8") as stream:
        document = json.load(stream)
    if isinstance(document, list):
        return pa.Table.from_pylist(document)
    if isinstance(document, dict):
        if document and all(isinstance(value, list) for value in document.values()):
            return pa.table(document)
        return pa.Table.from_pylist([document])
    raise ValueError(f"JSON root in {path} must be an object or array")


@service_adaptor
def _json_adaptor(file: TS[str], path: str = "json") -> _RAW_STREAM:
    ...


class _JsonAdaptor:
    __name__ = "json_adaptor"

    def __init__(self, schema=None):
        self.schema = schema

    def __getitem__(self, schema):
        if isinstance(schema, slice):
            schema = schema.stop
        return _JsonAdaptor(schema)

    def __call__(self, path: str, file):
        raw = _json_adaptor(file, path=path)
        if self.schema is None:
            return raw
        output_type = TSB[Stream[Data[Frame[self.schema]]]]
        return output_type.from_ts(
            status=raw.status,
            status_msg=raw.status_msg,
            values=convert[TS[Frame[self.schema]]](raw.values),
            timestamp=raw.timestamp,
        )


json_adaptor = _JsonAdaptor()


class _RequestState:
    def __init__(self):
        self.lock = threading.Lock()
        self.generations = {}
        self.published = set()
        self.sender = None
        self.active = True

    def begin(self, request_id):
        with self.lock:
            generation = self.generations.get(request_id, 0) + 1
            self.generations[request_id] = generation
            return generation

    def cancel(self, request_id):
        with self.lock:
            self.generations[request_id] = self.generations.get(request_id, 0) + 1
            if request_id in self.published:
                self.published.remove(request_id)
                return True
            return False

    def publish(self, request_id, generation, value):
        with self.lock:
            if (
                not self.active
                or self.generations.get(request_id) != generation
                or self.sender is None
            ):
                return
            self.published.add(request_id)
            sender = self.sender
        sender({request_id: value})

    def close(self):
        with self.lock:
            self.active = False
            self.generations.clear()
            self.published.clear()
            self.sender = None


@service_adaptor_impl(interfaces=_json_adaptor)
def json_adaptor_impl(
    files: TSD[int, TS[str]],
    path: str,
) -> TSD[int, _RAW_STREAM]:
    environment = DataEnvironment.current()
    if environment is None:
        raise RuntimeError(f"no DataEnvironment is active for {path!r}")
    directory = Path(environment.get_entry(path).environment_path)
    state = _RequestState()

    @push_queue(TSD[int, _RAW_STREAM])
    def responses(sender):
        with state.lock:
            state.sender = sender

    def load(request_id, generation, file_name):
        file_path = directory / file_name
        try:
            table = _read_json_table(file_path)
            if table.num_rows == 0:
                result = {
                    "status": StreamStatus.ERROR,
                    "status_msg": f"empty JSON file {file_path}",
                    "values": None,
                    "timestamp": datetime.now(timezone.utc).replace(tzinfo=None),
                }
            else:
                result = {
                    "status": StreamStatus.OK,
                    "status_msg": "",
                    "values": table,
                    "timestamp": datetime.now(timezone.utc).replace(tzinfo=None),
                }
        except Exception as error:
            result = {
                "status": StreamStatus.ERROR,
                "status_msg": str(error),
                "values": None,
                "timestamp": datetime.now(timezone.utc).replace(tzinfo=None),
            }
        state.publish(request_id, generation, result)

    @sink_node
    def submit_requests(files: TSD[int, TS[str]], executor: TS[object]):
        for request_id in files.removed_keys():
            if state.cancel(request_id):
                with state.lock:
                    sender = state.sender
                if sender is not None:
                    sender({request_id: REMOVED})
        for request_id, file_name in files.modified_items():
            generation = state.begin(request_id)
            executor.value.submit(load, request_id, generation, file_name.value)

    @submit_requests.stop
    def stop_requests():
        state.close()

    submit_requests(files, adaptor_executor())
    return responses()


__all__ = ("json_adaptor", "json_adaptor_impl")

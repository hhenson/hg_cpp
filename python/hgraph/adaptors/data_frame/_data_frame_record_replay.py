from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from hgraph import GlobalState, operator_function

__all__ = (
    "DATA_FRAME_RECORD_REPLAY",
    "set_data_frame_record_path",
    "set_data_frame_overrides",
    "get_data_frame_record_overrides",
    "record_to_data_frame",
    "replay_from_data_frame",
    "replay_data_frame",
    "replay_const_from_data_frame",
    "WriteMode",
    "DataFrameStorage",
    "BaseDataFrameStorage",
    "FileBasedDataFrameStorage",
    "MemoryDataFrameStorage",
)


DATA_FRAME_RECORD_REPLAY = ":data_frame:__data_frame_record_replay__"
_PATH_KEY = ":data_frame:__path__"
_OVERRIDES_KEY = ":data_frame:__overrides__"
_STORAGE_KEY = ":data_frame:__storage__"


def set_data_frame_record_path(path):
    GlobalState.instance()[_PATH_KEY] = Path(path)


class _OverrideState:
    def __init__(self):
        self.data = {
            "all": {
                "track_as_of": True,
                "track_removes": False,
                "partition_keys": None,
                "remove_partition_keys": None,
            },
            "key": {},
            "recordable_id": {},
            "key_recordable_id": {},
        }


def _overrides(state=None):
    state = state or GlobalState.instance()
    value = state.get(_OVERRIDES_KEY)
    if value is None:
        value = _OverrideState()
        state[_OVERRIDES_KEY] = value
    return value.data


def set_data_frame_overrides(
    key=None,
    recordable_id=None,
    track_as_of=None,
    track_removes=None,
    partition_keys=None,
    remove_partition_keys=None,
):
    overrides = _overrides()
    if key is None and recordable_id is None:
        target = overrides["all"]
    elif key is None:
        target = overrides["recordable_id"].setdefault(recordable_id, {})
    elif recordable_id is None:
        target = overrides["key"].setdefault(key, {})
    else:
        target = overrides["key_recordable_id"].setdefault((recordable_id, key), {})
    target.update(
        track_as_of=True if track_as_of is None else track_as_of,
        track_removes=True if track_removes is None else track_removes,
        partition_keys=partition_keys,
        remove_partition_keys=remove_partition_keys,
    )


def get_data_frame_record_overrides(key, recordable_id, global_state=None):
    overrides = _overrides(global_state)
    return (
        overrides["all"]
        | overrides["recordable_id"].get(recordable_id, {})
        | overrides["key"].get(key, {})
        | overrides["key_recordable_id"].get((recordable_id, key), {})
    )


# These names are compatibility views over the C++-owned record/replay nodes.
record_to_data_frame = operator_function("record")
replay_from_data_frame = operator_function("replay")
replay_data_frame = replay_from_data_frame
replay_const_from_data_frame = operator_function("replay_const")


class WriteMode(Enum):
    EXTEND = 0
    OVERWRITE = 1
    MERGE = 2


class DataFrameStorage(ABC):
    _MISSING = object()

    def __init__(self):
        self._previous = self._MISSING

    @classmethod
    def instance(cls):
        return GlobalState.instance().get(_STORAGE_KEY)

    def set_as_instance(self):
        state = GlobalState.instance()
        self._previous = state.get(_STORAGE_KEY, self._MISSING)
        state[_STORAGE_KEY] = self

    def release_as_instance(self):
        state = GlobalState.instance()
        if self._previous is self._MISSING:
            state.pop(_STORAGE_KEY, None)
        else:
            state[_STORAGE_KEY] = self._previous
        self._previous = self._MISSING

    def __enter__(self):
        self.set_as_instance()
        return self

    def __exit__(self, *_):
        self.release_as_instance()
        return False

    @abstractmethod
    def read_frame(self, path, start_time=None, end_time=None, as_of=None):
        raise NotImplementedError

    @abstractmethod
    def write_frame(self, path, frame, mode=WriteMode.OVERWRITE, as_of=None):
        raise NotImplementedError

    @abstractmethod
    def set_schema_info(self, path, date_time_col=None, as_of_col=None):
        raise NotImplementedError


class BaseDataFrameStorage(DataFrameStorage, ABC):
    def __init__(self):
        super().__init__()
        self._schema = {}

    def read_frame(self, path, start_time=None, end_time=None, as_of=None):
        frame = self._read(path)
        date_col, as_of_col = self._schema_info(path)
        mask = None
        if start_time is not None or end_time is not None:
            date_col = date_col or "date"
            values = frame[date_col]
            if start_time is not None:
                mask = pc.greater_equal(values, pa.scalar(start_time))
            if end_time is not None:
                upper = pc.less_equal(values, pa.scalar(end_time))
                mask = upper if mask is None else pc.and_(mask, upper)
        if as_of is not None and as_of_col is not None:
            as_of_mask = pc.less_equal(frame[as_of_col], pa.scalar(as_of))
            mask = as_of_mask if mask is None else pc.and_(mask, as_of_mask)
        return frame.filter(mask) if mask is not None else frame

    def write_frame(self, path, frame, mode=WriteMode.OVERWRITE, as_of=None):
        frame = _as_arrow(frame)
        if mode is not WriteMode.OVERWRITE and self._exists(path):
            previous = self._read(path)
            frame = pa.concat_tables([previous, frame], promote_options="default")
        self._write(path, frame)
        return frame

    def set_schema_info(self, path, date_time_col=None, as_of_col=None):
        self._schema[str(path)] = (date_time_col, as_of_col)
        self._write_schema(path, date_time_col, as_of_col)

    def _schema_info(self, path):
        return self._schema.get(str(path), self._read_schema(path))

    def _write_schema(self, path, date_time_col, as_of_col):
        pass

    def _read_schema(self, path):
        return None, None

    @abstractmethod
    def _exists(self, path):
        raise NotImplementedError

    @abstractmethod
    def _read(self, path):
        raise NotImplementedError

    @abstractmethod
    def _write(self, path, frame):
        raise NotImplementedError


class FileBasedDataFrameStorage(BaseDataFrameStorage):
    def __init__(self, path):
        super().__init__()
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

    def _data_path(self, path):
        return self.path / f"{path}.parquet"

    def _schema_path(self, path):
        return self.path / f"{path}.schema"

    def _exists(self, path):
        return self._data_path(path).exists()

    def _read(self, path):
        return pq.read_table(self._data_path(path))

    def _write(self, path, frame):
        pq.write_table(frame, self._data_path(path))

    def _write_schema(self, path, date_time_col, as_of_col):
        self._schema_path(path).write_text(f"{date_time_col or ''}\n{as_of_col or ''}")

    def _read_schema(self, path):
        schema = self._schema_path(path)
        if not schema.exists():
            return None, None
        date_col, as_of_col = schema.read_text().splitlines()
        return date_col or None, as_of_col or None


class MemoryDataFrameStorage(BaseDataFrameStorage):
    def __init__(self):
        super().__init__()
        self._frames = {}

    def _exists(self, path):
        return str(path) in self._frames

    def _read(self, path):
        return self._frames[str(path)]

    def _write(self, path, frame):
        self._frames[str(path)] = frame


def _as_arrow(value):
    if isinstance(value, pa.Table):
        return value
    if isinstance(value, pa.RecordBatch):
        return pa.Table.from_batches([value])
    to_arrow = getattr(value, "to_arrow", None)
    if to_arrow is not None:
        return _as_arrow(to_arrow())
    raise TypeError(f"dataframe storage requires an Arrow-compatible frame, got {type(value)!r}")

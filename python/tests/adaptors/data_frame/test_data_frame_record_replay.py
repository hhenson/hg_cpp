from datetime import datetime

import pyarrow as pa

import hgraph as hg
from hgraph.adaptors.data_frame import (
    MemoryDataFrameStorage,
    WriteMode,
    get_data_frame_record_overrides,
    set_data_frame_overrides,
)


def test_memory_storage_and_configuration_live_in_global_state():
    first = pa.table({"time": [datetime(2026, 1, 1)], "value": [1]})
    second = pa.table({"time": [datetime(2026, 1, 2)], "value": [2]})

    with hg.GlobalContext(hg.GlobalState()):
        storage = MemoryDataFrameStorage()
        with storage:
            assert MemoryDataFrameStorage.instance() is storage
            storage.set_schema_info("values", "time", None)
            storage.write_frame("values", first)
            storage.write_frame("values", second, mode=WriteMode.EXTEND)
            assert storage.read_frame(
                "values", start_time=datetime(2026, 1, 2)
            ).equals(second)

        assert MemoryDataFrameStorage.instance() is None
        set_data_frame_overrides(key="values", track_removes=True)
        overrides = get_data_frame_record_overrides("values", "graph")
        assert overrides["track_as_of"] is True
        assert overrides["track_removes"] is True

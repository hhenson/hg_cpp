"""Arrow-native dataframe adaptors.

The public source API follows Python hgraph, but graph execution is delegated
to the native ``from_data_frame`` and conversion operators.  Polars is an
optional producer only: values are converted to Arrow at the boundary.
"""

from ._data_frame_source import *
from ._data_source_generators import *
from ._data_frame_record_replay import *
from ._data_frame_operators import *
from ._to_frame_converters import register_to_frame_converters  # noqa: F401 — import registers the frame convert targets


__all__ = [
    "join",
    "filter_frame",
    "filter_cs",
    "filter_exp",
    "filter_exp_ts",
    "filter_exp_seq",
    "group_by",
    "ungroup",
    "sorted_",
    "concat",
    "with_columns",
    "DataFrameSource",
    "ArrowDataFrameSource",
    "PolarsDataFrameSource",
    "SqlDataFrameSource",
    "DataStore",
    "DataConnectionStore",
    "schema_from_frame",
    "ts_from_data_source",
    "tsb_from_data_source",
    "tsd_k_v_from_data_source",
    "tsd_k_b_from_data_source",
    "tsd_k_tsd_from_data_source",
    "ts_of_array_from_data_source",
    "tsd_k_a_from_data_source",
    "tsl_from_data_source",
    "ts_of_matrix_from_data_source",
    "ts_of_frames_from_data_source",
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
]

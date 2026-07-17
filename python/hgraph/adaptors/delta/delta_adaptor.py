from hgraph import Frame, TS, TSB, convert
from hgraph.stream import Data, Stream

from .delta_adaptor_raw import (
    delta_query_adaptor_raw,
    delta_query_adaptor_raw_impl,
    delta_read_adaptor_raw,
    delta_read_adaptor_raw_impl,
    delta_write_adaptor_raw,
    delta_write_adaptor_raw_impl,
)

__all__ = (
    "delta_read_adaptor",
    "delta_read_adaptor_impl",
    "delta_query_adaptor",
    "delta_query_adaptor_impl",
    "delta_write_adaptor",
    "delta_write_adaptor_impl",
)


class _TypedDeltaAdaptor:
    def __init__(self, raw, schema=None):
        self.raw = raw
        self.schema = schema
        self.__name__ = raw.__name__.replace("_raw", "")

    def __getitem__(self, schema):
        return _TypedDeltaAdaptor(self.raw, schema)

    def __call__(self, *args, **kwargs):
        value = self.raw(*args, **kwargs)
        if self.schema is None:
            return value
        output_type = TSB[Stream[Data[Frame[self.schema]]]]
        return output_type.from_ts(
            status=value.status,
            status_msg=value.status_msg,
            values=convert[TS[Frame[self.schema]]](value.values),
            timestamp=value.timestamp,
        )


delta_read_adaptor = _TypedDeltaAdaptor(delta_read_adaptor_raw)
delta_read_adaptor_impl = delta_read_adaptor_raw_impl
delta_query_adaptor = _TypedDeltaAdaptor(delta_query_adaptor_raw)
delta_query_adaptor_impl = delta_query_adaptor_raw_impl
delta_write_adaptor = delta_write_adaptor_raw
delta_write_adaptor_impl = delta_write_adaptor_raw_impl

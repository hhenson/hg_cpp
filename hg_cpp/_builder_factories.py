from datetime import date, datetime, timedelta

import _hgraph
import hgraph
from hgraph._impl._builder._ts_builder import _throw


def _raise_un_implemented(value_tp: hgraph.HgTimeSeriesTypeMetaData):
    raise NotImplementedError(f"Missing builder for {value_tp}")

class HgCppFactory(hgraph.TimeSeriesBuilderFactory):

    def make_error_builder(self, value_tp: hgraph.HgTimeSeriesTypeMetaData) -> hgraph.TSOutputBuilder:
        # Error outputs are standard time-series outputs (typically TS[NodeError] or TSD[K, TS[NodeError]])
        # We can reuse the normal output builder logic here.
        return self.make_output_builder(value_tp)

    def make_input_builder(self, value_tp: hgraph.HgTimeSeriesTypeMetaData) -> hgraph.TSInputBuilder:
        # Unfortunately the approach is really all or nothing, so either we can build it or we can't
        return {
            hgraph.HgSignalMetaData: lambda: _hgraph.InputBuilder_TS_Signal(),
            hgraph.HgTSTypeMetaData: lambda: _ts_input_builder_type_for(value_tp.value_scalar_tp)(),
            hgraph.HgTSWTypeMetaData: lambda: _tsw_input_builder_type_for(value_tp.value_scalar_tp)(
                value_tp.size_tp.py_type.SIZE,
                value_tp.min_size_tp.py_type.SIZE if value_tp.min_size_tp.py_type.FIXED_SIZE else 0
            ) if getattr(value_tp.size_tp.py_type, 'FIXED_SIZE', True) else _raise_un_implemented(value_tp),
            hgraph.HgTSLTypeMetaData: lambda: _hgraph.InputBuilder_TSL(
                self.make_input_builder(value_tp.value_tp),
                value_tp.size_tp.py_type.SIZE
            ),
            hgraph.HgTSBTypeMetaData: lambda: _hgraph.InputBuilder_TSB(
                # TODO: Cache the schema
                _hgraph.TimeSeriesSchema(
                    keys=tuple(value_tp.bundle_schema_tp.meta_data_schema.keys()),
                    scalar_type=tp
                ) if (tp := value_tp.bundle_schema_tp.py_type.scalar_type()) else _hgraph.TimeSeriesSchema(
                    keys=tuple(value_tp.bundle_schema_tp.meta_data_schema.keys())
                ),
                [self.make_input_builder(tp) for tp in value_tp.bundle_schema_tp.meta_data_schema.values()]
            ),
            hgraph.HgREFTypeMetaData: lambda: _hgraph.InputBuilder_TS_Ref(
                #   value_tp=cast(HgREFTypeMetaData, value_tp).value_tp
            ),
            hgraph.HgTSSTypeMetaData: lambda: _tss_input_builder_type_for(value_tp.value_scalar_tp)(),
            hgraph.HgTSDTypeMetaData: lambda: _tsd_input_builder_type_for(value_tp.key_tp)(
                self.make_input_builder(value_tp.value_tp),
            ),
            hgraph.HgCONTEXTTypeMetaData: lambda: self.make_input_builder(value_tp.ts_type)
        }.get(type(value_tp), lambda: _throw(value_tp))()

    def make_output_builder(self, value_tp: hgraph.HgTimeSeriesTypeMetaData) -> hgraph.TSOutputBuilder:
        return {
            hgraph.HgTSTypeMetaData: lambda: _ts_output_builder_for_tp(value_tp.value_scalar_tp)(),
            hgraph.HgTSLTypeMetaData: lambda: _hgraph.OutputBuilder_TSL(
                self.make_output_builder(value_tp.value_tp),
                value_tp.size_tp.py_type.SIZE
            ),
            hgraph.HgTSBTypeMetaData: lambda: _hgraph.OutputBuilder_TSB(
                # TODO: Cache the schema
                schema=_hgraph.TimeSeriesSchema(
                    keys=tuple(value_tp.bundle_schema_tp.meta_data_schema.keys()),
                    scalar_type=tp
                ) if (tp := value_tp.bundle_schema_tp.py_type.scalar_type()) else _hgraph.TimeSeriesSchema(
                    keys=tuple(value_tp.bundle_schema_tp.meta_data_schema.keys())
                ),
                output_builders=[
                    self.make_output_builder(tp) for tp in value_tp.bundle_schema_tp.meta_data_schema.values()
                ]
            ),
            hgraph.HgREFTypeMetaData: lambda: _hgraph.OutputBuilder_TS_Ref(
                #   value_tp=cast(HgREFTypeMetaData, value_tp).value_tp
            ),
            hgraph.HgTSSTypeMetaData: lambda: _tss_output_builder_for_tp(value_tp.value_scalar_tp)(),
            hgraph.HgTSDTypeMetaData: lambda: _tsd_output_builder_for_tp(value_tp.key_tp)(
                self.make_output_builder(value_tp.value_tp),
                self.make_output_builder(value_tp.value_tp.as_reference())
            ),
            hgraph.HgTSWTypeMetaData: lambda: _tsw_output_builder_for_tp(value_tp.value_scalar_tp)(
                value_tp.size_tp.py_type.SIZE,
                value_tp.min_size_tp.py_type.SIZE if value_tp.min_size_tp.py_type.FIXED_SIZE else 0
            ) if getattr(value_tp.size_tp.py_type, 'FIXED_SIZE', True) else _raise_un_implemented(value_tp),
        }.get(type(value_tp), lambda: _throw(value_tp))()


def _ts_input_builder_type_for(scalar_type: hgraph.HgScalarTypeMetaData) -> hgraph.TimeSeriesInput:
    return {
        bool: _hgraph.InputBuilder_TS_Bool,
        int: _hgraph.InputBuilder_TS_Int,
        float: _hgraph.InputBuilder_TS_Float,
        date: _hgraph.InputBuilder_TS_Date,
        datetime: _hgraph.InputBuilder_TS_DateTime,
        timedelta: _hgraph.InputBuilder_TS_TimeDelta,
    }.get(scalar_type.py_type, _hgraph.InputBuilder_TS_Object)


def _ts_output_builder_for_tp(scalar_type: hgraph.HgScalarTypeMetaData) -> hgraph.TSInputBuilder:
    return {
        bool: _hgraph.OutputBuilder_TS_Bool,
        int: _hgraph.OutputBuilder_TS_Int,
        float: _hgraph.OutputBuilder_TS_Float,
        date: _hgraph.OutputBuilder_TS_Date,
        datetime: _hgraph.OutputBuilder_TS_DateTime,
        timedelta: _hgraph.OutputBuilder_TS_TimeDelta,
    }.get(scalar_type.py_type, _hgraph.OutputBuilder_TS_Object)


def _tss_input_builder_type_for(scalar_type: hgraph.HgScalarTypeMetaData) -> hgraph.TimeSeriesInput:
    return {
        bool: _hgraph.InputBuilder_TSS_Bool,
        int: _hgraph.InputBuilder_TSS_Int,
        float: _hgraph.InputBuilder_TSS_Float,
        date: _hgraph.InputBuilder_TSS_Date,
        datetime: _hgraph.InputBuilder_TSS_DateTime,
        timedelta: _hgraph.InputBuilder_TSS_TimeDelta,
    }.get(scalar_type.py_type, _hgraph.InputBuilder_TSS_Object)


def _tss_output_builder_for_tp(scalar_type: hgraph.HgScalarTypeMetaData) -> hgraph.TSInputBuilder:
    return {
        bool: _hgraph.OutputBuilder_TSS_Bool,
        int: _hgraph.OutputBuilder_TSS_Int,
        float: _hgraph.OutputBuilder_TSS_Float,
        date: _hgraph.OutputBuilder_TSS_Date,
        datetime: _hgraph.OutputBuilder_TSS_DateTime,
        timedelta: _hgraph.OutputBuilder_TSS_TimeDelta,
    }.get(scalar_type.py_type, _hgraph.OutputBuilder_TSS_Object)

def _tsd_input_builder_type_for(scalar_type: hgraph.HgScalarTypeMetaData) -> hgraph.TimeSeriesInput:
    return {
        bool: _hgraph.InputBuilder_TSD_Bool,
        int: _hgraph.InputBuilder_TSD_Int,
        float: _hgraph.InputBuilder_TSD_Float,
        date: _hgraph.InputBuilder_TSD_Date,
        datetime: _hgraph.InputBuilder_TSD_DateTime,
        timedelta: _hgraph.InputBuilder_TSD_TimeDelta,
    }.get(scalar_type.py_type, _hgraph.InputBuilder_TSD_Object)


def _tsd_output_builder_for_tp(scalar_type: hgraph.HgScalarTypeMetaData) -> hgraph.TSInputBuilder:
    return {
        bool: _hgraph.OutputBuilder_TSD_Bool,
        int: _hgraph.OutputBuilder_TSD_Int,
        float: _hgraph.OutputBuilder_TSD_Float,
        date: _hgraph.OutputBuilder_TSD_Date,
        datetime: _hgraph.OutputBuilder_TSD_DateTime,
        timedelta: _hgraph.OutputBuilder_TSD_TimeDelta,
    }.get(scalar_type.py_type, _hgraph.OutputBuilder_TSD_Object)


# TSW (TimeSeriesWindow) builders
# These map scalar types to corresponding C++ builders if available. If not available yet,
# we raise NotImplementedError via _raise_un_implemented to make the gap explicit during wiring.

def _tsw_input_builder_type_for(scalar_type: hgraph.HgScalarTypeMetaData):
    tp = scalar_type.py_type
    mapping = {
        bool: getattr(_hgraph, 'InputBuilder_TSW_Bool', None),
        int: getattr(_hgraph, 'InputBuilder_TSW_Int', None),
        float: getattr(_hgraph, 'InputBuilder_TSW_Float', None),
        date: getattr(_hgraph, 'InputBuilder_TSW_Date', None),
        datetime: getattr(_hgraph, 'InputBuilder_TSW_DateTime', None),
        timedelta: getattr(_hgraph, 'InputBuilder_TSW_TimeDelta', None),
    }
    builder_cls = mapping.get(tp, getattr(_hgraph, 'InputBuilder_TSW_Object', None))

    def ctor(size: int, min_size: int):
        if builder_cls is None:
            return _raise_un_implemented(f"TSW InputBuilder for type {tp}")
        return builder_cls(size, min_size)

    return ctor


def _tsw_output_builder_for_tp(scalar_type: hgraph.HgScalarTypeMetaData):
    tp = scalar_type.py_type
    mapping = {
        bool: getattr(_hgraph, 'OutputBuilder_TSW_Bool', None),
        int: getattr(_hgraph, 'OutputBuilder_TSW_Int', None),
        float: getattr(_hgraph, 'OutputBuilder_TSW_Float', None),
        date: getattr(_hgraph, 'OutputBuilder_TSW_Date', None),
        datetime: getattr(_hgraph, 'OutputBuilder_TSW_DateTime', None),
        timedelta: getattr(_hgraph, 'OutputBuilder_TSW_TimeDelta', None),
    }
    builder_cls = mapping.get(tp, getattr(_hgraph, 'OutputBuilder_TSW_Object', None))

    def ctor(size: int, min_size: int):
        if builder_cls is None:
            return _raise_un_implemented(f"TSW OutputBuilder for type {tp}")
        return builder_cls(size, min_size)

    return ctor
from datetime import date, datetime, timedelta

import _hgraph
import hgraph
from hgraph._impl._builder._ts_builder import _throw


class HgCppFactory(hgraph.TimeSeriesBuilderFactory):

    def make_input_builder(self, value_tp: hgraph.HgTimeSeriesTypeMetaData) -> hgraph.TSInputBuilder:
        # Unfortunately the approach is really all or nothing, so either we can build it or we can't
        return {
            hgraph.HgTSTypeMetaData: lambda: _ts_input_type_for(value_tp.value_scalar_type),
            hgraph.HgTSBTypeMetaData: lambda: _hgraph.InputBuilder_TSB(
                # TODO: Cache the schema
                schema=_hgraph.TimSeriesSchema(keys=value_tp.bundle_schema_tp.meta_data_schema.keys(),
                                               scalar_type=value_tp.bundle_schema_tp.py_type)
            ),
            hgraph.HgREFTypeMetaData: lambda: _hgraph.InputBuilder_TS_REF(
                #   value_tp=cast(HgREFTypeMetaData, value_tp).value_tp
            )
        }.get(type(value_tp), lambda: _throw(value_tp))()

    def make_output_builder(self, value_tp: hgraph.HgTimeSeriesTypeMetaData) -> hgraph.TSOutputBuilder:
        return {
            hgraph.HgTSTypeMetaData: lambda: _ts_output_type_for(value_tp.value_scalar_tp),
            hgraph.HgTSBTypeMetaData: lambda: _hgraph.OutputBuilder_TSB(
                # TODO: Cache the schema
                schema=_hgraph.TimSeriesSchema(keys=value_tp.bundle_schema_tp.meta_data_schema.keys(),
                                               scalar_type=value_tp.bundle_schema_tp.py_type)
            ),
            hgraph.HgREFTypeMetaData: lambda: _hgraph.OutputBuilder_TS_REF(
                #   value_tp=cast(HgREFTypeMetaData, value_tp).value_tp
            )
        }.get(type(value_tp), lambda: _throw(value_tp))()


def _ts_input_type_for(scalar_type: hgraph.HgScalarTypeMetaData) -> hgraph.TimeSeriesInput:
    return {
        bool: _hgraph.TS_Bool,
        int: _hgraph.TS_Int,
        float: _hgraph.TS_Float,
        date: _hgraph.TS_Date,
        datetime: _hgraph.TS_DateTime,
    }.get(scalar_type.py_type, _hgraph.TS_Object)


def _ts_output_type_for(scalar_type: hgraph.HgScalarTypeMetaData) -> hgraph.TSInputBuilder:
    return {
        bool: _hgraph.OutputBuilder_TS_Bool,
        int: _hgraph.OutputBuilder_TS_Int,
        float: _hgraph.OutputBuilder_TS_Float,
        date: _hgraph.OutputBuilder_TS_Date,
        datetime: _hgraph.OutputBuilder_TS_DateTime,
        timedelta: _hgraph.OutputBuilder_TS_TimeDelta,
    }.get(scalar_type.py_type, _hgraph.OutputBuilder_TS_Object)

from hgraph import PythonTimeSeriesBuilderFactory, HgTimeSeriesTypeMetaData, TSOutputBuilder, TSInputBuilder, \
    HgTSTypeMetaData
from hgraph._impl._builder._ts_builder import _throw, PythonTSInputBuilder, PythonTSOutputBuilder
from typing_extensions import cast


class HgCppFactory(PythonTimeSeriesBuilderFactory):

    def make_input_builder(self, value_tp: HgTimeSeriesTypeMetaData) -> TSInputBuilder:
        try:
            return {
                HgTSTypeMetaData: lambda: PythonTSInputBuilder(value_tp=cast(HgTSTypeMetaData, value_tp).value_scalar_tp),
            }.get(type(value_tp), lambda: _throw(value_tp))()
        except TypeError:
            return super().make_input_builder(value_tp)

    def make_output_builder(self, value_tp: HgTimeSeriesTypeMetaData) -> TSOutputBuilder:
        try:
            return {
                HgTSTypeMetaData: lambda: PythonTSOutputBuilder(value_tp=value_tp.value_scalar_tp),
            }.get(type(value_tp), lambda: _throw(value_tp))()
        except TypeError:
            return super().make_output_builder(value_tp)
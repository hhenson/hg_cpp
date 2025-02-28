import typing

from hgraph import PythonTimeSeriesBuilderFactory, HgTimeSeriesTypeMetaData, TSOutputBuilder, TSInputBuilder, \
    HgTSTypeMetaData, TimeSeriesReference, TimeSeriesInput, TimeSeriesOutput, TimeSeriesReferenceInput
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


def python_time_series_reference_builder(
        ts: typing.Optional[TimeSeriesInput | TimeSeriesOutput] = None,
        from_items: typing.Iterable[TimeSeriesOutput] = None) -> TimeSeriesReference:
    from _hgraph import TimeSeriesReference as CppTimeSeriesReference
    from _hgraph import TimeSeriesOutput as CppTimeSeriesOutput
    from _hgraph import TimeSeriesReferenceInput
    if ts is not None:
        if isinstance(ts, CppTimeSeriesOutput):
            return CppTimeSeriesReference.make(ts)
        if isinstance(ts, CppTimeSeriesReferenceInput):
            return ts.value
        if ts.has_peer:
            return CppTimeSeriesReference.make(ts.output)
        else:
            return CppTimeSeriesReference.make([python_time_series_reference_builder(i) for i in ts])
    elif from_items is not None:
        return CppTimeSeriesReference.make(from_items)
    else:
        return CppTimeSeriesReference.make()
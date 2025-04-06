
#include <hgraph/types/tsb.h>

#include <hgraph/python/pyb_wiring.h>

#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/ts.h>

void export_types(nb::module_ &m) {
    using namespace hgraph;

    TimeSeriesType::register_with_nanobind(m);
    TimeSeriesOutput::register_with_nanobind(m);
    TimeSeriesInput::register_with_nanobind(m);

    TimeSeriesReference::register_with_nanobind(m);
    TimeSeriesReferenceOutput::register_with_nanobind(m);
    TimeSeriesReferenceInput::register_with_nanobind(m);

    Traits::register_with_nanobind(m);

    register_ts_with_nanobind(m);

    TimeSeriesSchema::register_with_nanobind(m);
}

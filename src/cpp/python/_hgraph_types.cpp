#include <hgraph/types/time_series_type.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsl.h>
#include <hgraph/types/tss.h>
#include <hgraph/types/node.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/graph.h>

void export_types(nb::module_ &m) {
    using namespace hgraph;

    TimeSeriesType::register_with_nanobind(m);
    TimeSeriesOutput::register_with_nanobind(m);
    TimeSeriesInput::register_with_nanobind(m);

    TimeSeriesReference::register_with_nanobind(m);
    TimeSeriesReferenceOutput::register_with_nanobind(m);
    TimeSeriesReferenceInput::register_with_nanobind(m);

    IndexedTimeSeriesOutput::register_with_nanobind(m);
    IndexedTimeSeriesInput::register_with_nanobind(m);

    TimeSeriesListOutput::register_with_nanobind(m);
    TimeSeriesListInput::register_with_nanobind(m);

    TimeSeriesBundleInput::register_with_nanobind(m);
    TimeSeriesBundleOutput::register_with_nanobind(m);

    SetDelta::register_with_nanobind(m);
    tss_register_with_nanobind(m);

    Traits::register_with_nanobind(m);

    node_type_enum_py_register(m);
    injectable_type_enum(m);
    NodeSignature::register_with_nanobind(m);
    NodeScheduler::register_with_nanobind(m);
    Node::register_with_nanobind(m);


    Graph::register_with_nanobind(m);

    register_ts_with_nanobind(m);

    TimeSeriesSchema::register_with_nanobind(m);
}

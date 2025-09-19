
#include <hgraph/nodes/nest_graph_node.h>
#include <hgraph/nodes/nested_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/tsd_map_node.h>

void export_nodes(nb::module_ &m) {
    using namespace hgraph;

    NestedNode::register_with_nanobind(m);
    NestedGraphNode::register_with_nanobind(m);

    NestedEngineEvaluationClock::register_with_nanobind(m);
    NestedEvaluationEngine::register_with_nanobind(m);
    register_tsd_map_with_nanobind(m);
}
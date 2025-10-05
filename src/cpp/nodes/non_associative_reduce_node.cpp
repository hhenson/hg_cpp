#include <hgraph/nodes/non_associative_reduce_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>

namespace hgraph {

    TsdNonAssociativeReduceNode::TsdNonAssociativeReduceNode(
        int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
        nb::dict scalars, graph_builder_ptr nested_graph_builder,
        const std::tuple<int64_t, int64_t> &input_node_ids, int64_t output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars)),
          nested_graph_builder_(std::move(nested_graph_builder)),
          input_node_ids_(input_node_ids), output_node_id_(output_node_id) {}

    void TsdNonAssociativeReduceNode::initialise() {
        // TODO: Implement
    }

    void TsdNonAssociativeReduceNode::do_start() {
        // TODO: Implement
    }

    void TsdNonAssociativeReduceNode::do_stop() {
        // TODO: Implement
    }

    void TsdNonAssociativeReduceNode::dispose() {
        // TODO: Implement
    }

    void TsdNonAssociativeReduceNode::eval() {
        // TODO: Implement
    }

    void TsdNonAssociativeReduceNode::update_changes() {
        // TODO: Implement
    }

    void TsdNonAssociativeReduceNode::extend_nodes_to(int64_t sz) {
        // TODO: Implement
    }

    void TsdNonAssociativeReduceNode::erase_nodes_from(int64_t ndx) {
        // TODO: Implement
    }

    void TsdNonAssociativeReduceNode::bind_output() {
        // TODO: Implement
    }

    nb::object TsdNonAssociativeReduceNode::last_output_value() {
        // TODO: Implement
        return nb::none();
    }

    int64_t TsdNonAssociativeReduceNode::node_size() const {
        // TODO: Implement
        return 0;
    }

    int64_t TsdNonAssociativeReduceNode::node_count() const {
        // TODO: Implement
        return 0;
    }

    std::vector<node_ptr> TsdNonAssociativeReduceNode::get_node(int64_t ndx) {
        // TODO: Implement
        return {};
    }

    std::unordered_map<int, graph_ptr> &TsdNonAssociativeReduceNode::nested_graphs() {
        static std::unordered_map<int, graph_ptr> empty;
        // TODO: Implement
        return empty;
    }

    void register_non_associative_reduce_node_with_nanobind(nb::module_ &m) {
        nb::class_<TsdNonAssociativeReduceNode, NestedNode>(m, "TsdNonAssociativeReduceNode");
    }

}  // namespace hgraph

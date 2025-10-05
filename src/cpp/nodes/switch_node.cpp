#include <hgraph/nodes/switch_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>

namespace hgraph {

    template <typename K>
    SwitchNode<K>::SwitchNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id,
                               NodeSignature::ptr signature, nb::dict scalars,
                               const std::unordered_map<K, graph_builder_ptr> &nested_graph_builders,
                               const std::unordered_map<K, std::unordered_map<std::string, int>> &input_node_ids,
                               const std::unordered_map<K, int> &output_node_ids, bool reload_on_ticked)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars)),
          nested_graph_builders_(nested_graph_builders), input_node_ids_(input_node_ids),
          output_node_ids_(output_node_ids), reload_on_ticked_(reload_on_ticked) {}

    template <typename K>
    void SwitchNode<K>::initialise() {
        // TODO: Implement switch logic
    }

    template <typename K>
    void SwitchNode<K>::do_start() {
        // TODO: Implement
    }

    template <typename K>
    void SwitchNode<K>::do_stop() {
        // TODO: Implement
    }

    template <typename K>
    void SwitchNode<K>::dispose() {
        // TODO: Implement
    }

    template <typename K>
    void SwitchNode<K>::do_eval() {
        mark_evaluated();
        // TODO: Implement switch evaluation
    }

    template <typename K>
    std::unordered_map<int, graph_ptr> SwitchNode<K>::nested_graphs() const {
        // TODO: Implement
        return {};
    }

    // Template instantiations
    template struct SwitchNode<bool>;
    template struct SwitchNode<int64_t>;
    template struct SwitchNode<double>;
    template struct SwitchNode<std::string>;
    template struct SwitchNode<nb::object>;

    void register_switch_node_with_nanobind(nb::module_ &m) {
        nb::class_<SwitchNode<bool>, NestedNode>(m, "SwitchNode_bool")
            .def_prop_ro("nested_graphs", &SwitchNode<bool>::nested_graphs);

        nb::class_<SwitchNode<int64_t>, NestedNode>(m, "SwitchNode_int")
            .def_prop_ro("nested_graphs", &SwitchNode<int64_t>::nested_graphs);

        nb::class_<SwitchNode<double>, NestedNode>(m, "SwitchNode_float")
            .def_prop_ro("nested_graphs", &SwitchNode<double>::nested_graphs);

        nb::class_<SwitchNode<std::string>, NestedNode>(m, "SwitchNode_str")
            .def_prop_ro("nested_graphs", &SwitchNode<std::string>::nested_graphs);

        nb::class_<SwitchNode<nb::object>, NestedNode>(m, "SwitchNode_object")
            .def_prop_ro("nested_graphs", &SwitchNode<nb::object>::nested_graphs);
    }

}  // namespace hgraph

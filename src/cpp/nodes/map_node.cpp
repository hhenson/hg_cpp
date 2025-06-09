#include <hgraph/nodes/map_node.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/traits.h>

namespace hgraph
{

    template <typename K>
    TsdMapNode<K>::TsdMapNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
                              nb::dict scalars, graph_builder_ptr nested_graph_builder,
                              const std::unordered_map<std::string, int> &input_node_ids, int output_node_id,
                              const std::unordered_set<std::string> &multiplexed_args, const std::string &key_arg)
        : NestedNode(node_ndx, owning_graph_id, signature, scalars), nested_graph_builder_(nested_graph_builder),
          input_node_ids_(input_node_ids), output_node_id_(output_node_id), multiplexed_args_(multiplexed_args), key_arg_(key_arg) {
    }

    template <typename K> std::unordered_map<K, graph_ptr> &TsdMapNode<K>::nested_graphs() { return active_graphs_; }

    template <typename K> void TsdMapNode<K>::initialise() {}

    template <typename K> void TsdMapNode<K>::start() {
        auto trait{graph().traits().get_trait_or(RECORDABLE_ID_TRAIT, nb::none())};
        if (!trait.is_none()) {
            auto recordable_id{signature().record_replay_id};
            recordable_id_ = get_fq_recordable_id(graph().traits(), recordable_id.has_value() ? "map_" : recordable_id.value());
        }
    }

    template <typename K> void TsdMapNode<K>::stop() {}

    template <typename K> void TsdMapNode<K>::dispose() {}

    template <typename K> void TsdMapNode<K>::do_eval() {}

}  // namespace hgraph

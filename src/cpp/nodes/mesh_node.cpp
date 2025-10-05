#include <hgraph/nodes/mesh_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/runtime/global_state.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>

namespace hgraph {

    template <typename K>
    MeshNode<K>::MeshNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id,
                          NodeSignature::ptr signature, nb::dict scalars, graph_builder_ptr nested_graph_builder,
                          const std::unordered_map<std::string, int64_t> &input_node_ids, int64_t output_node_id,
                          const std::unordered_set<std::string> &multiplexed_args, const std::string &key_arg,
                          const std::string &context_path)
        : TsdMapNode<K>(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                       std::move(nested_graph_builder), input_node_ids, output_node_id, multiplexed_args, key_arg),
          full_context_path_(context_path) {}

    template <typename K>
    void MeshNode<K>::do_start() {
        // TODO: Implement
        TsdMapNode<K>::do_start();
    }

    template <typename K>
    void MeshNode<K>::do_stop() {
        // TODO: Implement
        TsdMapNode<K>::do_stop();
    }

    template <typename K>
    void MeshNode<K>::eval() {
        // TODO: Implement
        TsdMapNode<K>::eval();
    }

    template <typename K>
    TimeSeriesDictOutput_T<K> &MeshNode<K>::tsd_output() {
        return TsdMapNode<K>::tsd_output();
    }

    template <typename K>
    void MeshNode<K>::re_rank(const K &key, const K &depends_on, std::vector<K> re_rank_stack) {
        // TODO: Implement re-ranking logic
    }

    // Template instantiations
    template struct MeshNode<int64_t>;
    template struct MeshNode<nb::object>;

    void register_mesh_node_with_nanobind(nb::module_ &m) {
        nb::class_<MeshNode<int64_t>, TsdMapNode<int64_t>>(m, "MeshNode_int");
        nb::class_<MeshNode<nb::object>, TsdMapNode<nb::object>>(m, "MeshNode_object");
    }

}  // namespace hgraph

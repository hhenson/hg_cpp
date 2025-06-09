

#ifndef MAP_NODE_H
#define MAP_NODE_H

#include <hgraph/nodes/nested_node.h>

namespace hgraph
{
    template <typename K> struct TsdMapNode : NestedNode
    {
        TsdMapNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature, nb::dict scalars,
                graph_builder_ptr nested_graph_builder, const std::unordered_map<std::string, int> &input_node_ids,
                int output_node_id, const std::unordered_set<std::string> &multiplexed_args, const std::string &key_arg);

        std::unordered_map<K, graph_ptr> &nested_graphs();

      protected:
        void initialise() override;
        void start() override;
        void stop() override;
        void dispose() override;
        void do_eval() override;

      private:
        void          create_new_graph(const K &key);
        void          remove_graph(const K &key);
        engine_time_t evaluate_graph(const K &key);
        void          un_wire_graph(const K &key, std::shared_ptr<Graph> &graph);
        void          wire_graph(const K &key, std::shared_ptr<Graph> &graph);

        graph_builder_ptr                    nested_graph_builder_;
        std::unordered_map<std::string, int> input_node_ids_;
        size_t                               output_node_id_;
        std::unordered_set<std::string>      multiplexed_args_;
        std::string                          key_arg_;
        std::unordered_map<K, engine_time_t> scheduled_keys_;
        std::unordered_map<K, graph_ptr>     active_graphs_;
        std::set<K>                          pending_keys_;
        int64_t                              count_{1};
        std::string                          recordable_id_;
    };


}  // namespace hgraph

#endif  // MAP_NODE_H

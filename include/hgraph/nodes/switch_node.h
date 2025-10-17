
#ifndef SWITCH_NODE_H
#define SWITCH_NODE_H

#include "hgraph/types/ts.h"

#include <hgraph/nodes/nested_node.h>
#include <optional>
#include <unordered_map>

namespace hgraph
{
    void register_switch_node_with_nanobind(nb::module_ &m);

    template <typename K> struct SwitchNode;
    template <typename K> using switch_node_ptr = nb::ref<SwitchNode<K>>;

    template <typename K> struct SwitchNode : NestedNode
    {
        SwitchNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature, nb::dict scalars,
                   const std::unordered_map<K, graph_builder_ptr>                    &nested_graph_builders,
                   const std::unordered_map<K, std::unordered_map<std::string, int>> &input_node_ids,
                   const std::unordered_map<K, int> &output_node_ids, bool reload_on_ticked,
                   graph_builder_ptr default_graph_builder = nullptr);

        void                               initialise() override;
        void                               do_start() override;
        void                               do_stop() override;
        void                               dispose() override;
        void                               eval() override;
        std::unordered_map<int, graph_ptr> nested_graphs() const;

      protected:
        void do_eval() override {}
        void wire_graph(graph_ptr &graph);
        void unwire_graph(graph_ptr &graph);

        std::unordered_map<K, graph_builder_ptr>                    nested_graph_builders_;
        std::unordered_map<K, std::unordered_map<std::string, int>> input_node_ids_;
        std::unordered_map<K, int>                                  output_node_ids_;
        TimeSeriesValueInput<K>                                    *key_ts;

        bool                   reload_on_ticked_;
        graph_ptr              active_graph_{};
        std::optional<K>       active_key_{};
        int64_t                count_{0};
        time_series_output_ptr old_output_{nullptr};
        graph_builder_ptr      default_graph_builder_{nullptr};
        std::string            recordable_id_;
    };
}  // namespace hgraph

#endif  // SWITCH_NODE_H

//
// Created by Howard Henson on 05/05/2024.
//

#ifndef GRAPH_H
#define GRAPH_H

#include <hgraph/util/lifecycle.h>
#include <nanobind/intrusive/ref.h>
#include <optional>
#include <vector>

#include <hgraph/runtime/evaluation_engine.h>

namespace hgraph
{
    struct Node;
    using node_ptr = nanobind::ref<Node>;

    struct Traits;
    using traits_ptr = nanobind::ref<Traits>;

    struct HGRAPH_EXPORT Graph : ComponentLifeCycle
    {

        using ptr = nanobind::ref<Graph>;

        Graph(std::vector<int64_t> graph_id_, std::vector<node_ptr> nodes_, std::optional<node_ptr> parent_node_,
              std::optional<std::string> label_);

        [[nodiscard]] const std::vector<int64_t> &graph_id() const;

        [[nodiscard]] const std::vector<node_ptr> &nodes() const;

        [[nodiscard]] std::optional<node_ptr> parent_node() const;

        [[nodiscard]] std::optional<std::string> label() const;

        [[nodiscard]] EvaluationEngineApi &evaluation_engine_api() const;

        [[nodiscard]] EvaluationClock &evaluation_clock() const;

        [[nodiscard]] EngineEvaluationClock &evaluation_engine_clock();

        [[nodiscard]] EvaluationEngine &evaluation_engine() const;

        void set_evaluation_engine(EvaluationEngine::ptr value);

        int64_t push_source_nodes_end() const;

        void schedule_node(int64_t node_ndx, engine_time_t when);

        std::vector<engine_time_t> &schedule();

        void evaluation_graph();

        std::unique_ptr<Graph> copy_with(std::vector<node_ptr> nodes);

        Traits &traits() const;

      private:
        EvaluationEngine          *_evaluation_engine;
        int64_t                    _push_source_nodes_end;
        std::vector<int64_t>       _graph_id;
        std::vector<node_ptr>      _nodes;
        std::optional<node_ptr>    _parent_node;
        std::optional<std::string> _label;
        std::optional<traits_ptr>  _traits;
    };
}  // namespace hgraph

#endif  // GRAPH_H

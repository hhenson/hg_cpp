//
// Created by Howard Henson on 05/05/2024.
//

#ifndef GRAPH_H
#define GRAPH_H

#include <hgraph/hgraph_forward_declarations.h>

#include <hgraph/util/lifecycle.h>
#include <nanobind/intrusive/ref.h>
#include <optional>
#include <vector>

#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/util/sender_receiver_state.h>

namespace hgraph
{

    struct HGRAPH_EXPORT Graph : ComponentLifeCycle
    {

        using ptr = nanobind::ref<Graph>;

        Graph(std::vector<int64_t> graph_id_, std::vector<node_ptr> nodes_, std::optional<node_ptr> parent_node_,
              std::string label_, traits_ptr traits_);

        [[nodiscard]] const std::vector<int64_t> &graph_id() const;

        [[nodiscard]] const std::vector<node_ptr> &nodes() const;

        [[nodiscard]] std::optional<node_ptr> parent_node() const;

        [[nodiscard]] std::optional<std::string> label() const;

        [[nodiscard]] EvaluationEngineApi &evaluation_engine_api();

        [[nodiscard]] EvaluationClock &evaluation_clock();

        [[nodiscard]] const EvaluationClock &evaluation_clock() const;

        [[nodiscard]] EngineEvaluationClock &evaluation_engine_clock();

        [[nodiscard]] EvaluationEngine &evaluation_engine();

        void set_evaluation_engine(EvaluationEngine::ptr value);

        int64_t push_source_nodes_end() const;

        void schedule_node(int64_t node_ndx, engine_time_t when);

        void schedule_node(int64_t node_ndx, engine_time_t when, bool force_set);

        std::vector<engine_time_t> &schedule();

        void evaluate_graph();

        Graph::ptr copy_with(std::vector<node_ptr> nodes);

        const Traits &traits() const;

        [[nodiscard]] SenderReceiverState &receiver();

        void extend_graph(const GraphBuilder &graph_builder, bool delay_start = false);

        void reduce_graph(int64_t start_node);

        static void register_with_nanobind(nb::module_ &m);

      protected:
        void initialise_sugraph(int64_t start, int64_t end);

        void start_subgraph(int64_t start, int64_t end);
        void stop_subgraph(int64_t start, int64_t end);
        void dispose_subgraph(int64_t start, int64_t end);

        void initialise() override;
        void start() override;
        void stop() override;
        void dispose() override;

      private:
        EvaluationEngine::ptr      _evaluation_engine;
        std::vector<int64_t>       _graph_id;
        std::vector<node_ptr>      _nodes;
        std::vector<engine_time_t> _schedule;
        std::optional<node_ptr>    _parent_node;
        std::string                _label;
        traits_ptr                 _traits;
        SenderReceiverState        _receiver;
        engine_time_t              _last_evaluation_time{MIN_DT};
        mutable int64_t            _push_source_nodes_end{-1};
    };

}  // namespace hgraph

#endif  // GRAPH_H


#ifndef GRAPH_EXECUTOR_H
#define GRAPH_EXECUTOR_H

#include <hgraph/hgraph_export.h>
#include <hgraph/python/pyb.h>
#include <hgraph/util/date_time.h>
#include <nanobind/intrusive/counter.h>
#include <vector>


namespace hgraph
{
    enum class HGRAPH_EXPORT EvaluationMode { REAL_TIME = 0, SIMULATION = 1 };

    struct Graph;
    struct Node;
    struct EvaluationEngine;
    using graph_ptr = nb::ref<Graph>;

    struct EvaluationLifeCycleObserver : nb::intrusive_base
    {
        using ptr = nb::ref<EvaluationLifeCycleObserver>;

        virtual void on_before_start_graph(const Graph &) {
        };

        virtual void on_after_start_graph(const Graph &) {
        };

        virtual void on_before_start_node(const Node &) {
        };

        virtual void on_after_start_node(const Node &) {
        };

        virtual void on_before_graph_evaluation(const Graph &) {
        };

        virtual void on_after_graph_evaluation(const Graph &) {
        };

        virtual void on_before_node_evaluation(const Node &) {
        };

        virtual void on_after_node_evaluation(const Node &) {
        };

        virtual void on_before_stop_node(const Node &) {
        };

        virtual void on_after_stop_node(const Node &) {
        };

        virtual void on_before_stop_graph(const Graph &) {
        };

        virtual void on_after_stop_graph(const Graph &) {
        };
    };

    struct HGRAPH_EXPORT GraphExecutor : nb::intrusive_base
    {

        // Abstract methods.
        virtual EvaluationMode run_mode() const                                                    = 0;
        virtual const Graph   &graph() const                                                       = 0;
        virtual void           run(const engine_time_t &start_time, const engine_time_t &end_time) = 0;

        void static register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT GraphExecutorImpl : GraphExecutor
    {
        GraphExecutorImpl(graph_ptr graph, EvaluationMode run_mode, std::vector<EvaluationLifeCycleObserver::ptr> observers = {});

        EvaluationMode run_mode() const override;

        const Graph & graph() const override;

        void           run(const engine_time_t &start_time, const engine_time_t &end_time) override;

        void static register_with_nanobind(nb::module_ &m);

    protected:
        void _evaluate(EvaluationEngine& evaluationEngine);

    private:
        graph_ptr _graph;
        EvaluationMode _run_mode;
        std::vector<EvaluationLifeCycleObserver::ptr> _observers;
    };

}  // namespace hgraph
#endif  // GRAPH_EXECUTOR_H

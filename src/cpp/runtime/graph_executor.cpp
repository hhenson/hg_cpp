#include <hgraph/runtime/graph_executor.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>

namespace hgraph
{
    struct PyEvaluationLifeCycleObserver : EvaluationLifeCycleObserver
    {
        NB_TRAMPOLINE(EvaluationLifeCycleObserver, 12);

        void on_before_start_graph(const Graph &graph) override { NB_OVERRIDE(on_before_start_graph, graph); }

        void on_after_start_graph(const Graph &graph) override { NB_OVERRIDE(on_after_start_graph, graph); }

        void on_before_start_node(const Node &node) override { NB_OVERRIDE(on_before_start_node, node); }

        void on_after_start_node(const Node &node) override { NB_OVERRIDE(on_after_start_node, node); }

        void on_before_graph_evaluation(const Graph &graph) override { NB_OVERRIDE(on_before_graph_evaluation, graph); }

        void on_after_graph_evaluation(const Graph &graph) override { NB_OVERRIDE(on_after_graph_evaluation, graph); }

        void on_before_node_evaluation(const Node &node) override { NB_OVERRIDE(on_before_node_evaluation, node); }

        void on_after_node_evaluation(const Node &node) override { NB_OVERRIDE(on_after_node_evaluation, node); }

        void on_before_stop_node(const Node &node) override { NB_OVERRIDE(on_before_stop_node, node); }

        void on_after_stop_node(const Node &node) override { NB_OVERRIDE(on_after_stop_node, node); }

        void on_before_stop_graph(const Graph &graph) override { NB_OVERRIDE(on_before_stop_graph, graph); }

        void on_after_stop_graph(const Graph &graph) override { NB_OVERRIDE(on_after_stop_graph, graph); }
    };

    void GraphExecutor::register_with_nanobind(nb::module_ &m) {
        nb::class_<GraphExecutor>(m, "GraphExecutor")
            .def("run_mode", &GraphExecutor::run_mode)
            .def("graph", &GraphExecutor::graph)
            .def("run", &GraphExecutor::run);

        nb::enum_<EvaluationMode>(m, "EvaluationMode")
            .value("REAL_TIME", EvaluationMode::REAL_TIME)
            .value("SIMULATION", EvaluationMode::SIMULATION)
            .export_values();

        nb::class_<EvaluationLifeCycleObserver, PyEvaluationLifeCycleObserver>(m, "EvaluationLifeCycleObserver")
            .def("on_before_start_graph", &EvaluationLifeCycleObserver::on_before_start_graph)
            .def("on_after_start_graph", &EvaluationLifeCycleObserver::on_after_start_graph)
            .def("on_before_start_node", &EvaluationLifeCycleObserver::on_before_start_node)
            .def("on_after_start_node", &EvaluationLifeCycleObserver::on_after_start_node)
            .def("on_before_graph_evaluation", &EvaluationLifeCycleObserver::on_before_graph_evaluation)
            .def("on_after_graph_evaluation", &EvaluationLifeCycleObserver::on_after_graph_evaluation)
            .def("on_before_node_evaluation", &EvaluationLifeCycleObserver::on_before_node_evaluation)
            .def("on_after_node_evaluation", &EvaluationLifeCycleObserver::on_after_node_evaluation)
            .def("on_before_stop_node", &EvaluationLifeCycleObserver::on_before_stop_node)
            .def("on_after_stop_node", &EvaluationLifeCycleObserver::on_after_stop_node)
            .def("on_before_stop_graph", &EvaluationLifeCycleObserver::on_before_stop_graph)
            .def("on_after_stop_graph", &EvaluationLifeCycleObserver::on_after_stop_graph);
    }

    GraphExecutorImpl::GraphExecutorImpl(graph_ptr graph, EvaluationMode run_mode,
                                         std::vector<EvaluationLifeCycleObserver::ptr> observers)
        : _graph(graph), _run_mode(run_mode), _observers{std::move(observers)} {}

    EvaluationMode GraphExecutorImpl::run_mode() const { return _run_mode; }

    const Graph &GraphExecutorImpl::graph() const { return *_graph; }

    void GraphExecutorImpl::run(const engine_time_t &start_time, const engine_time_t &end_time) {
        if (end_time <= start_time) {
            if (end_time < start_time) {
                throw std::invalid_argument("End time cannot be before the start time");
            } else {
                throw std::invalid_argument("End time cannot be equal to the start time");
            }
        }

        EngineEvaluationClock::ptr clock;
        switch (_run_mode) {
            case EvaluationMode::REAL_TIME: clock = new RealTimeEvaluationClock(start_time); break;
            case EvaluationMode::SIMULATION: clock = new SimulationEvaluationClock(start_time); break;
            default: throw std::runtime_error("Unknown run mode");
        }

        nb::ref<EvaluationEngine> evaluationEngine = new EvaluationEngineImpl(clock, start_time, end_time, _run_mode);
        _graph->set_evaluation_engine(evaluationEngine);

        for (const auto &observer : _observers) { evaluationEngine->add_life_cycle_observer(observer); }

        {
            auto initialiseContext = InitialiseDisposeContext(*_graph);
            auto startStopContext  = StartStopContext(*_graph);

            while (clock->evaluation_time() < end_time) { _evaluate(*evaluationEngine); }
        }
    }

    void GraphExecutorImpl::register_with_nanobind(nb::module_ &m) {
        nb::class_<GraphExecutorImpl, GraphExecutor>(m, "GraphExecutorImpl")
            .def(nb::init<graph_ptr, EvaluationMode, std::vector<EvaluationLifeCycleObserver::ptr>>(), "graph"_a, "run_mode"_a,
                 "observers"_a = std::vector<EvaluationLifeCycleObserver::ptr>{});
    }

    void GraphExecutorImpl::_evaluate(EvaluationEngine &evaluationEngine) {
        evaluationEngine.notify_before_evaluation();
        _graph->evaluate_graph();
        evaluationEngine.notify_after_evaluation();
        evaluationEngine.advance_engine_time();
    }

}  // namespace hgraph
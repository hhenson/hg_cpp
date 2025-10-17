#include <hgraph/runtime/graph_executor.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/error_type.h>

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
        nb::class_<GraphExecutor, nb::intrusive_base>(m, "GraphExecutor")
            .def("run_mode", &GraphExecutor::run_mode)
            .def("graph", &GraphExecutor::graph)
            .def("run", &GraphExecutor::run);

        nb::enum_<EvaluationMode>(m, "EvaluationMode")
            .value("REAL_TIME", EvaluationMode::REAL_TIME)
            .value("SIMULATION", EvaluationMode::SIMULATION)
            .export_values();

        nb::class_<EvaluationLifeCycleObserver, PyEvaluationLifeCycleObserver, nb::intrusive_base>(m, "EvaluationLifeCycleObserver")
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

        try {
            {
                auto initialiseContext = InitialiseDisposeContext(*_graph);
                auto startStopContext  = StartStopContext(*_graph);

                while (clock->evaluation_time() < end_time) { _evaluate(*evaluationEngine); }
            } // RAII contexts end here
        } catch (const NodeException &e) {
            // Raise Python hgraph.NodeException constructed from C++ NodeException details
            try {
                nb::object hgraph_mod = nb::module_::import_("hgraph");
                nb::object py_node_exc_cls = hgraph_mod.attr("NodeException");
                nb::tuple args = nb::make_tuple(
                    nb::cast(e.error.signature_name),
                    nb::cast(e.error.label),
                    nb::cast(e.error.wiring_path),
                    nb::cast(e.error.error_msg),
                    nb::cast(e.error.stack_trace),
                    nb::cast(e.error.activation_back_trace),
                    nb::cast(e.error.additional_context)
                );
                PyErr_SetObject(py_node_exc_cls.ptr(), args.ptr());
            } catch (...) {
                PyErr_SetString(PyExc_RuntimeError, e.what());
            }
            throw nb::python_error();
        } catch (const nb::python_error &e) {
            throw; // Preserve Python exception raised above
        } catch (const std::exception &e) {
            // Preserve any active Python exception (e.g., hgraph.NodeException)
            if (PyErr_Occurred()) {
                throw nb::python_error();
            }
            // Provide a clear message for unexpected exceptions
            std::string msg = std::string("Graph execution failed: ") + e.what();
            throw nb::builtin_exception(nb::exception_type::runtime_error, msg.c_str());
        }
    }

    void GraphExecutorImpl::register_with_nanobind(nb::module_ &m) {
        nb::class_<GraphExecutorImpl, GraphExecutor>(m, "GraphExecutorImpl")
            .def(nb::init<graph_ptr, EvaluationMode, std::vector<EvaluationLifeCycleObserver::ptr>>(), "graph"_a, "run_mode"_a,
                 "observers"_a = std::vector<EvaluationLifeCycleObserver::ptr>{});
    }

    void GraphExecutorImpl::_evaluate(EvaluationEngine &evaluationEngine) {
        try {
            evaluationEngine.notify_before_evaluation();
        } catch (const NodeException &e) {
            // Let NodeException propagate to nanobind exception translator
            throw;
        } catch (const nb::python_error &e) {
            throw;
        } catch (const std::exception &e) {
            if (PyErr_Occurred()) { throw nb::python_error(); }
            std::string msg = std::string("Error in notify_before_evaluation: ") + e.what();
            throw nb::builtin_exception(nb::exception_type::runtime_error, msg.c_str());
        }
        try {
            _graph->evaluate_graph();
        } catch (const NodeException &e) {
            // Let NodeException propagate to nanobind exception translator
            throw;
        } catch (const nb::python_error &e) {
            throw;
        } catch (const std::exception &e) {
            if (PyErr_Occurred()) { throw nb::python_error(); }
            std::string msg = std::string("Graph evaluation failed: ") + e.what();
            throw nb::builtin_exception(nb::exception_type::runtime_error, msg.c_str());
        }
        try {
            evaluationEngine.notify_after_evaluation();
        } catch (const NodeException &e) {
            // Let NodeException propagate to nanobind exception translator
            throw;
        } catch (const std::exception &e) {
            std::string msg = std::string("Error in notify_after_evaluation: ") + e.what();
            throw nb::builtin_exception(nb::exception_type::runtime_error, msg.c_str());
        }
        evaluationEngine.advance_engine_time();
    }

}  // namespace hgraph
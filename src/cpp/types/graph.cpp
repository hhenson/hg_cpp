#include <hgraph/builders/graph_builder.h>
#include <hgraph/python/pyb_wiring.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/traits.h>
#include <fmt/chrono.h>

namespace hgraph
{

    Graph::Graph(std::vector<int64_t> graph_id_, std::vector<Node::ptr> nodes_, std::optional<Node::ptr> parent_node_,
                 std::string label_, traits_ptr traits_)
        : ComponentLifeCycle(), _graph_id{std::move(graph_id_)}, _nodes{std::move(nodes_)}, _parent_node{std::move(parent_node_)},
          _label{std::move(label_)}, _traits{std::move(traits_)} {
        auto it{std::find_if(nodes_.begin(), nodes_.end(),
                             [](const Node *v) { return v->signature().node_type != NodeTypeEnum::PUSH_SOURCE_NODE; })};
        _push_source_nodes_end = std::distance(_nodes.begin(), it);
        _schedule.resize(_nodes.size(), MIN_DT);
    }
    const std::vector<int64_t> &Graph::graph_id() const { return _graph_id; }

    const std::vector<node_ptr> &Graph::nodes() const { return _nodes; }

    std::optional<node_ptr> Graph::parent_node() const { return _parent_node; }

    std::optional<std::string> Graph::label() const { return _label; }

    EvaluationEngineApi &Graph::evaluation_engine_api() { return *_evaluation_engine; }

    EvaluationClock &Graph::evaluation_clock() { return _evaluation_engine->engine_evaluation_clock(); }

    const EvaluationClock &Graph::evaluation_clock() const { return _evaluation_engine->engine_evaluation_clock(); }

    EngineEvaluationClock &Graph::evaluation_engine_clock() { return _evaluation_engine->engine_evaluation_clock(); }

    EvaluationEngine &Graph::evaluation_engine() { return *_evaluation_engine; }

    void Graph::set_evaluation_engine(EvaluationEngine::ptr value) {
        if (_evaluation_engine.get() != nullptr && value.get() != nullptr) {
            throw std::runtime_error("Duplicate attempt to set evaluation engine");
        }
        _evaluation_engine = std::move(value);

        if (_push_source_nodes_end > 0) { _receiver.set_evaluation_clock(nb::ref(&evaluation_engine_clock())); }
    }

    int64_t Graph::push_source_nodes_end() const {
        if (_push_source_nodes_end == -1) {
            for (size_t i = 0; i < _nodes.size(); ++i) {
                if (_nodes[i]->signature().node_type != NodeTypeEnum::PUSH_SOURCE_NODE) {
                    _push_source_nodes_end = static_cast<int64_t>(i);
                    break;
                }
            }
            if (_push_source_nodes_end == -1) {
                _push_source_nodes_end =
                    static_cast<int64_t>(_nodes.size());  // In the very unlikely event that there are only push source nodes.
            }
        }
        // result is computed on demand and then cached.
        return _push_source_nodes_end;
    }

    void Graph::schedule_node(int64_t node_ndx, engine_time_t when) { schedule_node(node_ndx, when, false); }

    void Graph::schedule_node(int64_t node_ndx, engine_time_t when, bool force_set) {
        auto &clock = this->evaluation_engine_clock();
        auto  et    = clock.evaluation_time();

        if (when < et) {
            auto msg{fmt::format("Graph[{}] Trying to schedule node: {}[{}] for {:%Y-%m-%d %H:%M:%S} but current time is {:%Y-%m-%d %H:%M:%S}",
                                 this->graph_id().front(), this->nodes()[node_ndx]->signature().signature(), node_ndx, when, et)};
            throw std::runtime_error(msg);
        }

        auto &st = this->_schedule[node_ndx];
        if (force_set || st <= et || st > when) { st = when; }
        clock.update_next_scheduled_evaluation_time(when);
    }

    std::vector<engine_time_t> &Graph::schedule() { return _schedule; }

    void Graph::evaluate_graph() {
        NotifyGraphEvaluation nge{evaluation_engine(), *this};

        engine_time_t now      = evaluation_engine_clock().evaluation_time();
        auto         &clock    = evaluation_engine_clock();
        auto         &nodes    = _nodes;
        auto         &schedule = _schedule;

        _last_evaluation_time = now;

        // Handle push source nodes scheduling if necessary
        if (push_source_nodes_end() > 0 && clock.push_node_requires_scheduling()) {
            clock.reset_push_node_requires_scheduling();

            while (auto value = receiver().dequeue()) {
                auto [i, message]         = *receiver().dequeue();
                auto                &node = *nodes[i];
                NotifyNodeEvaluation nne{evaluation_engine(), node};
                bool                 success = dynamic_cast<PushQueueNode &>(node).apply_message(message);
                if (!success) {
                    receiver().enqueue_front({i, message});
                    clock.mark_push_node_requires_scheduling();
                    break;
                }
            }
        }

        for (size_t i = push_source_nodes_end(); i < nodes.size(); ++i) {
            auto  scheduled_time = schedule[i];
            auto &node           = *nodes[i];

            if (scheduled_time == now) {
                evaluation_engine().notify_before_node_evaluation(node);

                try {
                    NotifyNodeEvaluation nne{evaluation_engine(), node};
                    node.eval();
                } catch (const NodeException &e) { throw e; } catch (const std::exception &e) {
                    throw NodeException::capture_error(e, node, "During evaluation");
                } catch (...) {
                    throw NodeException::capture_error(std::current_exception(), node, "Unknown error during node evaluation");
                }
            } else if (scheduled_time > now) {
                clock.update_next_scheduled_evaluation_time(scheduled_time);
            }
        }
    }

    Graph::ptr Graph::copy_with(std::vector<Node::ptr> nodes) {
        return ptr{new Graph(std::move(_graph_id), std::move(nodes), _parent_node, _label, _traits->copy())};
    }

    const Traits &Graph::traits() const { return *_traits; }

    SenderReceiverState &Graph::receiver() { return _receiver; }

    void Graph::extend_graph(const GraphBuilder &graph_builder, bool delay_start) {
        auto first_node_index{_nodes.size()};
        auto sz{graph_builder.node_builders.size()};
        auto nodes{graph_builder.make_and_connect_nodes(_graph_id, first_node_index)};
        auto capacity{first_node_index + sz};
        _nodes.reserve(capacity);
        _schedule.reserve(capacity);
        for (auto node : nodes) {
            _nodes.emplace_back(node);
            _schedule.emplace_back(MIN_DT);
        }
        initialise_sugraph(first_node_index, capacity);
        if (!delay_start && is_started()) { start_subgraph(first_node_index, capacity); }
    }

    void Graph::reduce_graph(int64_t start_node) {
        auto end{_nodes.size()};
        if (is_started()) { stop_subgraph(start_node, end); }
        dispose_subgraph(start_node, end);

        _nodes.erase(_nodes.begin() + start_node, _nodes.end());
        _schedule.erase(_schedule.begin() + start_node, _schedule.end());
    }

    void Graph::initialise_sugraph(int64_t start, int64_t end) {
        // Need to ensure that the graph is set prior to initialising the nodes
        // In case of interaction between nodes.
        for (auto i = start; i < end; ++i) {
            auto node{_nodes[i]};
            node->set_graph(this);
        }
        for (auto i = start; i < end; ++i) {
            auto node{_nodes[i]};
            node->initialise();
        }
    }

    void Graph::start_subgraph(int64_t start, int64_t end) {
        for (auto i = start; i < end; ++i) {
            auto node{_nodes[i]};
            evaluation_engine().notify_before_start_node(*node);
            node->start();
            evaluation_engine().notify_after_start_node(*node);
        }
    }

    void Graph::stop_subgraph(int64_t start, int64_t end) {
        for (auto i = start; i < end; ++i) {
            auto node{_nodes[i]};
            evaluation_engine().notify_before_stop_node(*node);
            node->stop();
            evaluation_engine().notify_after_stop_node(*node);
        }
    }

    void Graph::dispose_subgraph(int64_t start, int64_t end) {
        for (auto i = start; i < end; ++i) {
            auto node{_nodes[i]};
            node->dispose();
        }
    }

    void Graph::register_with_nanobind(nb::module_ &m) {
        nb::class_<Graph, ComponentLifeCycle>(m, "Graph",
                                              nb::intrusive_ptr<Graph>([](Graph *o, PyObject *po) noexcept { o->set_self_py(po); }))
            .def(nb::init<std::vector<int64_t>, std::vector<node_ptr>, std::optional<node_ptr>, std::string, traits_ptr>(),
                 "graph_id"_a, "nodes"_a, "parent_node"_a, "label"_a, "traits"_a)
            .def_prop_ro("graph_id", &Graph::graph_id)
            .def_prop_ro("nodes", &Graph::nodes)
            .def_prop_ro("parent_node", &Graph::parent_node)
            .def_prop_ro("label", &Graph::label)
            .def_prop_ro("evaluation_engine_api", &Graph::evaluation_engine_api)
            .def_prop_ro("evaluation_clock", static_cast<const EvaluationClock& (Graph::*)() const>(&Graph::evaluation_clock))
            .def_prop_ro("evaluation_engine_clock", &Graph::evaluation_engine_clock)
            .def_prop_rw("evaluation_engine", &Graph::evaluation_engine, &Graph::set_evaluation_engine)
            .def_prop_ro("push_source_nodes_end", &Graph::push_source_nodes_end)
            .def("schedule_node", static_cast<void (Graph::*)(int64_t, engine_time_t, bool)>(&Graph::schedule_node), "node_ndx"_a,
                 "when"_a, "force_set"_a = false)
            .def_prop_ro("schedule", &Graph::schedule)
            .def("evaluate_graph", &Graph::evaluate_graph)
            .def("copy_with", &Graph::copy_with, "nodes"_a)
            .def_prop_ro("traits", &Graph::traits);
        ;
    }

    void Graph::initialise() {}

    void Graph::start() {}
    void Graph::stop() {}
    void Graph::dispose() {}

}  // namespace hgraph

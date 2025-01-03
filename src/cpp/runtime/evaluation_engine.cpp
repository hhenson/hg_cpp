#include <hgraph/python/pyb_wiring.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <algorithm>

namespace hgraph
{

    void BaseEvaluationClock::set_evaluation_time(engine_time_t value) {
        _evaluation_time                = value;
        _next_scheduled_evaluation_time = MAX_DT;
    }

    void EvaluationClock::register_with_nanobind(nb::module_ &m) {
        nb::class_<EvaluationClock>(
            m, "EvaluationClock",
            nb::intrusive_ptr<EvaluationClock>([](EvaluationClock *o, PyObject *po) noexcept { o->set_self_py(po); }))
            .def_prop_ro("evaluation_time", &EvaluationClock::evaluation_time)
            .def_prop_ro("now", &EvaluationClock::now)
            .def_prop_ro("next_cycle_evaluation_time", &EvaluationClock::next_cycle_evaluation_time)
            .def_prop_ro("cycle_time", &EvaluationClock::cycle_time);
    }

    void EngineEvaluationClock::register_with_nanobind(nb::module_ &m) {
        nb::class_<EngineEvaluationClock, EvaluationClock>(
            m, "EngineEvaluationClock",
            nb::intrusive_ptr<EngineEvaluationClock>([](EngineEvaluationClock *o, PyObject *po) noexcept { o->set_self_py(po); }))
            .def_prop_rw("evaluation_time", &EngineEvaluationClock::evaluation_time, &EngineEvaluationClock::set_evaluation_time)
            .def_prop_ro("next_cycle_evaluation_time", &EngineEvaluationClock::next_cycle_evaluation_time)
            .def("update_next_scheduled_evaluation_time", &EngineEvaluationClock::update_next_scheduled_evaluation_time, "et"_a)
            .def("advance_to_next_scheduled_time", &EngineEvaluationClock::advance_to_next_scheduled_time)
            .def("mark_push_node_requires_scheduling", &EngineEvaluationClock::mark_push_node_requires_scheduling)
            .def_prop_ro("push_node_requires_scheduling", &EngineEvaluationClock::push_node_requires_scheduling)
            .def("reset_push_node_requires_scheduling", &EngineEvaluationClock::reset_push_node_requires_scheduling);
    }

    EngineEvaluationClockDelegate::EngineEvaluationClockDelegate(EngineEvaluationClock::ptr clock)
        : _engine_evalaution_clock{std::move(clock)} {}

    engine_time_t EngineEvaluationClockDelegate::evaluation_time() const { return _engine_evalaution_clock->evaluation_time(); }

    engine_time_t EngineEvaluationClockDelegate::now() const { return _engine_evalaution_clock->now(); }

    engine_time_t EngineEvaluationClockDelegate::next_cycle_evaluation_time() const {
        return _engine_evalaution_clock->next_cycle_evaluation_time();
    }

    engine_time_delta_t EngineEvaluationClockDelegate::cycle_time() const { return _engine_evalaution_clock->cycle_time(); }

    void EngineEvaluationClockDelegate::set_evaluation_time(engine_time_t et) { _engine_evalaution_clock->set_evaluation_time(et); }

    engine_time_t EngineEvaluationClockDelegate::next_scheduled_evaluation_time() const {
        return _engine_evalaution_clock->next_scheduled_evaluation_time();
    }

    void EngineEvaluationClockDelegate::update_next_scheduled_evaluation_time(engine_time_t et) {
        _engine_evalaution_clock->update_next_scheduled_evaluation_time(et);
    }

    void EngineEvaluationClockDelegate::advance_to_next_scheduled_time() {
        _engine_evalaution_clock->advance_to_next_scheduled_time();
    }

    void EngineEvaluationClockDelegate::mark_push_node_requires_scheduling() {
        _engine_evalaution_clock->mark_push_node_requires_scheduling();
    }

    bool EngineEvaluationClockDelegate::push_node_requires_scheduling() const {
        return _engine_evalaution_clock->push_node_requires_scheduling();
    }

    void EngineEvaluationClockDelegate::reset_push_node_requires_scheduling() {
        _engine_evalaution_clock->reset_push_node_requires_scheduling();
    }

    void EngineEvaluationClockDelegate::register_with_nanobind(nb::module_ &m) {
        nb::class_<EngineEvaluationClockDelegate, EngineEvaluationClock>(
            m, "EngineEvaluationClockDelegate",
            nb::intrusive_ptr<EngineEvaluationClockDelegate>(
                [](EngineEvaluationClockDelegate *o, PyObject *po) noexcept { o->set_self_py(po); }))
            .def(nb::init<EngineEvaluationClock::ptr>());
    }

    void EvaluationEngineApi::register_with_nanobind(nb::module_ &m) {
        nb::class_<EvaluationEngineApi, ComponentLifeCycle>(
            m, "EvaluationEngineApi",
            nb::intrusive_ptr<EvaluationEngineApi>([](EvaluationEngineApi *o, PyObject *po) noexcept { o->set_self_py(po); }))
            .def_prop_ro("evaluation_mode", &EvaluationEngineApi::evaluation_mode)
            .def_prop_ro("start_time", &EvaluationEngineApi::start_time)
            .def_prop_ro("end_time", &EvaluationEngineApi::end_time)
            .def_prop_ro("evaluation_clock", &EvaluationEngineApi::evaluation_clock)
            .def("request_engine_stop", &EvaluationEngineApi::request_engine_stop)
            .def_prop_ro("is_stop_requested", &EvaluationEngineApi::is_stop_requested)
            .def("add_before_evaluation_notification", &EvaluationEngineApi::add_before_evaluation_notification, "fn"_a)
            .def("add_after_evaluation_notification", &EvaluationEngineApi::add_after_evaluation_notification, "fn"_a)
            .def("add_life_cycle_observer", &EvaluationEngineApi::add_life_cycle_observer, "observer"_a)
            .def("remove_life_cycle_observer", &EvaluationEngineApi::remove_life_cycle_observer, "observer"_a);
    }

    void EvaluationEngine::register_with_nanobind(nb::module_ &m) {
        nb::class_<EvaluationEngine, EvaluationEngineApi>(
            m, "EvaluationEngine",
            nb::intrusive_ptr<EvaluationEngine>([](EvaluationEngine *o, PyObject *po) noexcept { o->set_self_py(po); }))
            .def_prop_ro("engine_evaluation_clock",  static_cast<const EngineEvaluationClock& (EvaluationEngine::*)() const>(&EvaluationEngine::engine_evaluation_clock))
            .def("advance_engine_time", &EvaluationEngine::advance_engine_time)
            .def("notify_before_evaluation", &EvaluationEngine::notify_before_evaluation)
            .def("notify_after_evaluation", &EvaluationEngine::notify_after_evaluation)
            .def("notify_before_start_node", &EvaluationEngine::notify_before_start_node)
            .def("notify_after_start_node", &EvaluationEngine::notify_after_start_node)
            .def("notify_before_graph_evaluation", &EvaluationEngine::notify_before_graph_evaluation)
            .def("notify_after_graph_evaluation", &EvaluationEngine::notify_after_graph_evaluation)
            .def("notify_before_node_evaluation", &EvaluationEngine::notify_before_node_evaluation)
            .def("notify_after_node_evaluation", &EvaluationEngine::notify_after_node_evaluation)
            .def("notify_before_stop_node", &EvaluationEngine::notify_before_stop_node)
            .def("notify_after_stop_node", &EvaluationEngine::notify_after_stop_node)
            .def("notify_before_stop_graph", &EvaluationEngine::notify_before_stop_graph)
            .def("notify_after_stop_graph", &EvaluationEngine::notify_after_stop_graph);
    }

    NotifyGraphEvaluation::NotifyGraphEvaluation(EvaluationEngine &evaluation_engine, const Graph &graph)
        : _evaluation_engine{evaluation_engine}, _graph{graph} {
        _evaluation_engine.notify_before_graph_evaluation(_graph);
    }

    NotifyGraphEvaluation::~NotifyGraphEvaluation() {
        _evaluation_engine.notify_after_graph_evaluation(_graph);
    }

    NotifyNodeEvaluation::NotifyNodeEvaluation(EvaluationEngine &evaluation_engine, const Node &node)
        : _evaluation_engine{evaluation_engine}, _node{node} {
        _evaluation_engine.notify_before_node_evaluation(_node);
    }

    NotifyNodeEvaluation::~NotifyNodeEvaluation() {
        _evaluation_engine.notify_after_node_evaluation(_node);
    }

    EvaluationEngineDelegate::EvaluationEngineDelegate(ptr api) : _evaluation_engine{std::move(api)} {}

    EvaluationMode EvaluationEngineDelegate::evaluation_mode() const { return _evaluation_engine->evaluation_mode(); }

    engine_time_t EvaluationEngineDelegate::start_time() const { return _evaluation_engine->start_time(); }

    engine_time_t EvaluationEngineDelegate::end_time() const { return _evaluation_engine->end_time(); }

    EvaluationClock &EvaluationEngineDelegate::evaluation_clock() { return _evaluation_engine->evaluation_clock(); }

    EngineEvaluationClock &EvaluationEngineDelegate::engine_evaluation_clock() {
        return _evaluation_engine->engine_evaluation_clock();
    }

    const EngineEvaluationClock &EvaluationEngineDelegate::engine_evaluation_clock() const {
        return _evaluation_engine->engine_evaluation_clock();
    }

    void EvaluationEngineDelegate::request_engine_stop() { _evaluation_engine->request_engine_stop(); }

    bool EvaluationEngineDelegate::is_stop_requested() { return _evaluation_engine->is_stop_requested(); }

    void EvaluationEngineDelegate::add_before_evaluation_notification(std::function<void()> &&fn) {
        _evaluation_engine->add_before_evaluation_notification(std::forward<std::function<void()>>(fn));
    }

    void EvaluationEngineDelegate::add_after_evaluation_notification(std::function<void()> &&fn) {
        _evaluation_engine->add_after_evaluation_notification(std::forward<std::function<void()>>(fn));
    }

    void EvaluationEngineDelegate::add_life_cycle_observer(EvaluationLifeCycleObserver::ptr observer) {
        _evaluation_engine->add_life_cycle_observer(std::move(observer));
    }

    void EvaluationEngineDelegate::remove_life_cycle_observer(EvaluationLifeCycleObserver::ptr observer) {
        _evaluation_engine->remove_life_cycle_observer(std::move(observer));
    }

    void EvaluationEngineDelegate::advance_engine_time() { _evaluation_engine->advance_engine_time(); }

    void EvaluationEngineDelegate::notify_before_evaluation() { _evaluation_engine->notify_before_evaluation(); }

    void EvaluationEngineDelegate::notify_after_evaluation() { _evaluation_engine->notify_after_evaluation(); }

    void EvaluationEngineDelegate::notify_before_start_graph(const Graph &graph) {
        _evaluation_engine->notify_before_start_graph(graph);
    }

    void EvaluationEngineDelegate::notify_after_start_graph(const Graph &graph) {
        _evaluation_engine->notify_after_start_graph(graph);
    }

    void EvaluationEngineDelegate::notify_before_start_node(const Node &node) {
        _evaluation_engine->notify_before_start_node(node);
    }

    void EvaluationEngineDelegate::notify_after_start_node(const Node &node) { _evaluation_engine->notify_after_start_node(node); }

    void EvaluationEngineDelegate::notify_before_graph_evaluation(const Graph &graph) {
        _evaluation_engine->notify_before_graph_evaluation(graph);
    }

    void EvaluationEngineDelegate::notify_after_graph_evaluation(const Graph &graph) {
        _evaluation_engine->notify_after_graph_evaluation(graph);
    }

    void EvaluationEngineDelegate::notify_before_node_evaluation(const Node &node) {
        _evaluation_engine->notify_before_node_evaluation(node);
    }

    void EvaluationEngineDelegate::notify_after_node_evaluation(const Node &node) {
        _evaluation_engine->notify_after_node_evaluation(node);
    }

    void EvaluationEngineDelegate::notify_before_stop_node(const Node &node) { _evaluation_engine->notify_before_stop_node(node); }

    void EvaluationEngineDelegate::notify_after_stop_node(const Node &node) { _evaluation_engine->notify_after_stop_node(node); }

    void EvaluationEngineDelegate::notify_before_stop_graph(const Graph &graph) {
        _evaluation_engine->notify_before_stop_graph(graph);
    }

    void EvaluationEngineDelegate::notify_after_stop_graph(const Graph &graph) {
        _evaluation_engine->notify_after_stop_graph(graph);
    }

    void EvaluationEngineDelegate::register_with_nanobind(nb::module_ &m) {
        nb::class_<EvaluationEngineDelegate, EvaluationEngine>(
            m, "EvaluationEngineDelegate",
            nb::intrusive_ptr<EvaluationEngineDelegate>(
                [](EvaluationEngineDelegate *o, PyObject *po) noexcept { o->set_self_py(po); }))
            .def(nb::init<ptr>());
    }

    void EvaluationEngineDelegate::initialise() { _evaluation_engine->initialise(); }

    void EvaluationEngineDelegate::start() { _evaluation_engine->start(); }

    void EvaluationEngineDelegate::stop() { _evaluation_engine->stop(); }

    void EvaluationEngineDelegate::dispose() { _evaluation_engine->dispose(); }

    BaseEvaluationClock::BaseEvaluationClock(engine_time_t start_time)
        : _evaluation_time{start_time}, _next_scheduled_evaluation_time{MAX_DT} {}

    engine_time_t BaseEvaluationClock::evaluation_time() const { return _evaluation_time; }

    engine_time_t BaseEvaluationClock::next_cycle_evaluation_time() const { return _evaluation_time + MIN_TD; }

    engine_time_t BaseEvaluationClock::next_scheduled_evaluation_time() const { return _next_scheduled_evaluation_time; }

    void BaseEvaluationClock::update_next_scheduled_evaluation_time(engine_time_t scheduled_time) {
        if (scheduled_time == _evaluation_time) {
            return;  // This will be evaluated in the current cycle, nothing to do.
        }
        _next_scheduled_evaluation_time =
            std::max(next_cycle_evaluation_time(), std::min(_next_scheduled_evaluation_time, scheduled_time));
    }

    void BaseEvaluationClock::register_with_nanobind(nb::module_ &m) {
        nb::class_<BaseEvaluationClock, EngineEvaluationClock>(
            m, "BaseEvaluationClock",
            nb::intrusive_ptr<BaseEvaluationClock>([](BaseEvaluationClock *o, PyObject *po) noexcept { o->set_self_py(po); }));
    }

    SimulationEvaluationClock::SimulationEvaluationClock(engine_time_t current_time)
        : BaseEvaluationClock(current_time), _system_clock_at_start_of_evaluation{engine_time_t::clock::now()} {}

    void SimulationEvaluationClock::set_evaluation_time(engine_time_t value) {
        BaseEvaluationClock::set_evaluation_time(value);
        _system_clock_at_start_of_evaluation = engine_clock::now();
    }

    engine_time_t SimulationEvaluationClock::now() const { return evaluation_time() + cycle_time(); }

    engine_time_delta_t SimulationEvaluationClock::cycle_time() const {
        return engine_clock::now() - _system_clock_at_start_of_evaluation;
    }

    void SimulationEvaluationClock::advance_to_next_scheduled_time() { set_evaluation_time(next_scheduled_evaluation_time()); }

    void SimulationEvaluationClock::mark_push_node_requires_scheduling() {
        throw std::runtime_error("Simulation mode does not support push nodes.");
    }

    bool SimulationEvaluationClock::push_node_requires_scheduling() const { return false; }

    void SimulationEvaluationClock::reset_push_node_requires_scheduling() {
        throw std::runtime_error("Simulation mode does not support push nodes.");
    }

    void SimulationEvaluationClock::register_with_nanobind(nb::module_ &m) {
        nb::class_<SimulationEvaluationClock, BaseEvaluationClock>(
            m, "SimulationEvaluationClock",
            nb::intrusive_ptr<SimulationEvaluationClock>(
                [](SimulationEvaluationClock *o, PyObject *po) noexcept { o->set_self_py(po); }))
            .def(nb::init<engine_time_t>());
    }

    RealTimeEvaluationClock::RealTimeEvaluationClock(engine_time_t start_time)
        : BaseEvaluationClock(start_time), _push_node_requires_scheduling(false), _ready_to_push(false),
          _last_time_allowed_push(MIN_TD) {}

    engine_time_t RealTimeEvaluationClock::now() const {
        return std::chrono::time_point_cast<std::chrono::milliseconds>(engine_clock::now());
    }

    engine_time_delta_t RealTimeEvaluationClock::cycle_time() const { return engine_clock::now() - evaluation_time(); }
    void                RealTimeEvaluationClock::mark_push_node_requires_scheduling() {
        std::unique_lock<std::mutex> lock(_condition_mutex);
        _push_node_requires_scheduling = true;
        _push_node_requires_scheduling_condition.notify_all();
    }

    bool RealTimeEvaluationClock::push_node_requires_scheduling() const {
        if (!_ready_to_push) { return false; }
        std::unique_lock<std::mutex> lock(_condition_mutex);
        return _push_node_requires_scheduling;
    }

    void RealTimeEvaluationClock::advance_to_next_scheduled_time() {
        engine_time_t next_scheduled_time = next_scheduled_evaluation_time();
        engine_time_t now                 = engine_clock::now();

        // Process all alarms that are due and adjust the next scheduled time
        while (!_alarms.empty()) {
            const auto &next_alarm = *_alarms.begin();
            if (now >= next_alarm.first) {
                auto alarm = *_alarms.begin();
                _alarms.erase(_alarms.begin());
                next_scheduled_time = std::max(next_scheduled_time, evaluation_time() + MIN_TD);

                auto cb = _alarm_callbacks.find(alarm);
                if (cb != _alarm_callbacks.end()) {
                    cb->second(next_scheduled_time);
                    _alarm_callbacks.erase(cb);
                }
            } else if (next_scheduled_time > next_alarm.first) {
                next_scheduled_time = next_alarm.first;
                break;
            } else {
                break;
            }
        }

        _ready_to_push = false;
        if (next_scheduled_time > evaluation_time() + MIN_TD || now > _last_time_allowed_push + std::chrono::seconds(15)) {
            std::unique_lock<std::mutex> lock(_condition_mutex);
            _ready_to_push          = true;
            _last_time_allowed_push = now;

            while (now < next_scheduled_time && !_push_node_requires_scheduling) {
                auto sleep_time = (next_scheduled_time - now);
                _push_node_requires_scheduling_condition.wait_for(
                    lock, std::min(sleep_time, duration_cast<engine_time_delta_t>(std::chrono::seconds(10))));
                now = engine_clock::now();
            }
        }
        set_evaluation_time(std::min(next_scheduled_time, std::max(next_cycle_evaluation_time(), now)));

        // Process alarms again after updating evaluation_time
        while (!_alarms.empty()) {
            const auto &next_alarm = *_alarms.begin();
            if (now >= next_alarm.first) {
                auto alarm = *_alarms.begin();
                _alarms.erase(_alarms.begin());

                auto cb = _alarm_callbacks.find(alarm);
                if (cb != _alarm_callbacks.end()) {
                    cb->second(evaluation_time());
                    _alarm_callbacks.erase(cb);
                }
            } else {
                break;
            }
        }
    }

    void RealTimeEvaluationClock::reset_push_node_requires_scheduling() {
        std::unique_lock<std::mutex> lock(_condition_mutex);
        _push_node_requires_scheduling = false;
    }

    void RealTimeEvaluationClock::set_alarm(engine_time_t alarm_time, const std::string &name,
                                            std::function<void(engine_time_t)> callback) {
        if (alarm_time <= evaluation_time()) { throw std::invalid_argument("Cannot set alarm in the engine's past"); }

        _alarms.emplace(alarm_time, name);
        _alarm_callbacks[{alarm_time, name}] = std::move(callback);
    }

    void RealTimeEvaluationClock::cancel_alarm(const std::string &name) {
        for (auto it = _alarms.begin(); it != _alarms.end();) {
            if (it->second == name) {
                _alarm_callbacks.erase(*it);
                it = _alarms.erase(it);
            } else {
                ++it;
            }
        }
    }

    void RealTimeEvaluationClock::register_with_nanobind(nb::module_ &m) {
        nb::class_<RealTimeEvaluationClock, BaseEvaluationClock>(
            m, "RealTimeEvaluationClock",
            nb::intrusive_ptr<RealTimeEvaluationClock>(
                [](RealTimeEvaluationClock *o, PyObject *po) noexcept { o->set_self_py(po); }))
            .def(nb::init<engine_time_t>())
            .def("set_alarm", &RealTimeEvaluationClock::set_alarm, "alarm_time"_a, "name"_a, "callback"_a)
            .def("cancel_alarm", &RealTimeEvaluationClock::cancel_alarm, "name"_a);
    }

    EvaluationEngineImpl::EvaluationEngineImpl(EngineEvaluationClock::ptr clock, engine_time_t start_time, engine_time_t end_time,
                                               EvaluationMode run_mode)
        : _clock{std::move(clock)}, _start_time{start_time}, _end_time{end_time}, _run_mode{run_mode} {}

    void EvaluationEngineImpl::initialise() {}

    void EvaluationEngineImpl::start() {}

    void EvaluationEngineImpl::stop() {}

    void                         EvaluationEngineImpl::dispose() {}

    const EngineEvaluationClock &EvaluationEngineImpl::engine_evaluation_clock() const { return *_clock; }

    EvaluationMode EvaluationEngineImpl::evaluation_mode() const { return _run_mode; }

    engine_time_t EvaluationEngineImpl::start_time() const { return _start_time; }

    engine_time_t EvaluationEngineImpl::end_time() const { return _end_time; }

    EvaluationClock &EvaluationEngineImpl::evaluation_clock() { return *_clock; }

    void EvaluationEngineImpl::request_engine_stop() { _stop_requested = true; }

    bool EvaluationEngineImpl::is_stop_requested() { return _stop_requested; }

    void EvaluationEngineImpl::add_before_evaluation_notification(std::function<void()> &&fn) {
        _before_evaluation_notification.emplace_back(fn);
    }

    void EvaluationEngineImpl::add_after_evaluation_notification(std::function<void()> &&fn) {
        _after_evaluation_notification.emplace_back(fn);
    }

    void EvaluationEngineImpl::add_life_cycle_observer(EvaluationLifeCycleObserver::ptr observer) {
        _life_cycle_observers.emplace_back(std::move(observer));
    }

    void EvaluationEngineImpl::remove_life_cycle_observer(EvaluationLifeCycleObserver::ptr observer) {
        auto it{std::find(_life_cycle_observers.begin(), _life_cycle_observers.end(), observer)};
        if (it != _life_cycle_observers.end()) {
            // Since order is not important, we can swap the observer to remove with the last observer and then pop it.
            std::iter_swap(it, _life_cycle_observers.end() - 1);
            _life_cycle_observers.pop_back();
        }
    }

    EngineEvaluationClock &EvaluationEngineImpl::engine_evaluation_clock() { return *_clock; }

    void EvaluationEngineImpl::advance_engine_time() {

        if (_stop_requested) {
            _clock->set_evaluation_time(_end_time + MIN_TD);
            return;
        }
        // Ensure we don't run past the end time. So schedule to the end time + 1 tick.
        _clock->update_next_scheduled_evaluation_time(_end_time + MIN_TD);

        _clock->advance_to_next_scheduled_time();
    }

    void EvaluationEngineImpl::notify_before_evaluation() {
        for (auto &notification_receiver : _before_evaluation_notification) { notification_receiver(); }
        _before_evaluation_notification.clear();
    }

    void EvaluationEngineImpl::notify_after_evaluation() {
        for (auto it = _after_evaluation_notification.rbegin(); it != _after_evaluation_notification.rend(); ++it) { (*it)(); }
        _after_evaluation_notification.clear();
    }

    void EvaluationEngineImpl::notify_before_start_graph(const Graph &graph) {
        for (auto &life_cycle_observer : _life_cycle_observers) { life_cycle_observer->on_before_start_graph(graph); }
    }

    void EvaluationEngineImpl::notify_after_start_graph(const Graph &graph) {
        for (auto &life_cycle_observer : _life_cycle_observers) { life_cycle_observer->on_after_start_graph(graph); }
    }

    void EvaluationEngineImpl::notify_before_start_node(const Node &node) {
        for (auto &life_cycle_observer : _life_cycle_observers) { life_cycle_observer->on_before_start_node(node); }
    }

    void EvaluationEngineImpl::notify_after_start_node(const Node &node) {
        for (auto &life_cycle_observer : _life_cycle_observers) { life_cycle_observer->on_after_start_node(node); }
    }
    void EvaluationEngineImpl::notify_before_graph_evaluation(const Graph &graph) {
        for (auto &life_cycle_observer : _life_cycle_observers) { life_cycle_observer->on_before_graph_evaluation(graph); }
    }

    void EvaluationEngineImpl::notify_after_graph_evaluation(const Graph &graph) {
        for (auto &life_cycle_observer : _life_cycle_observers) { life_cycle_observer->on_after_graph_evaluation(graph); }
    }

    void EvaluationEngineImpl::notify_before_node_evaluation(const Node &node) {
        for (auto &life_cycle_observer : _life_cycle_observers) { life_cycle_observer->on_before_node_evaluation(node); }
    }

    void EvaluationEngineImpl::notify_after_node_evaluation(const Node &node) {
        for (auto &life_cycle_observer : _life_cycle_observers) { life_cycle_observer->on_after_node_evaluation(node); }
    }

    void EvaluationEngineImpl::notify_before_stop_node(const Node &node) {
        for (auto &life_cycle_observer : _life_cycle_observers) { life_cycle_observer->on_before_stop_node(node); }
    }

    void EvaluationEngineImpl::notify_after_stop_node(const Node &node) {
        for (auto &life_cycle_observer : _life_cycle_observers) { life_cycle_observer->on_after_stop_node(node); }
    }

    void EvaluationEngineImpl::notify_before_stop_graph(const Graph &graph) {
        for (auto &life_cycle_observer : _life_cycle_observers) { life_cycle_observer->on_before_start_graph(graph); }
    }

    void EvaluationEngineImpl::notify_after_stop_graph(const Graph &graph) {
        for (auto &life_cycle_observer : _life_cycle_observers) { life_cycle_observer->on_after_stop_graph(graph); }
    }

    void EvaluationEngineImpl::register_with_nanobind(nb::module_ &m) {
        nb::class_<EvaluationEngineImpl, EvaluationEngine>(
            m, "EvaluationEngineImpl",
            nb::intrusive_ptr<EvaluationEngineImpl>([](EvaluationEngineImpl *o, PyObject *po) noexcept { o->set_self_py(po); }))
            .def(nb::init<EngineEvaluationClock::ptr, engine_time_t, engine_time_t, EvaluationMode>());
    }

}  // namespace hgraph

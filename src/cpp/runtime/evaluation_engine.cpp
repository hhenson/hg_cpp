#include <hgraph/runtime/evaluation_engine.h>

namespace hgraph
{

    void BaseEvaluationClock::set_evaluation_time(engine_time_t value) {
        _evaluation_time                = value;
        _next_scheduled_evaluation_time = MAX_DT;
    }

    EngineEvaluationClockDelegate::EngineEvaluationClockDelegate(EngineEvaluationClock *clock) : _engine_evalaution_clock{clock} {}

    engine_time_t EngineEvaluationClockDelegate::evaluation_time() { return _engine_evalaution_clock->evaluation_time(); }

    engine_time_t EngineEvaluationClockDelegate::now() { return _engine_evalaution_clock->now(); }

    engine_time_t EngineEvaluationClockDelegate::next_cycle_evaluation_time() {
        return _engine_evalaution_clock->next_cycle_evaluation_time();
    }

    engine_time_delta_t EngineEvaluationClockDelegate::cycle_time() { return _engine_evalaution_clock->cycle_time(); }

    void EngineEvaluationClockDelegate::set_evaluation_time(engine_time_t et) { _engine_evalaution_clock->set_evaluation_time(et); }

    engine_time_t EngineEvaluationClockDelegate::next_scheduled_evaluation_time() {
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

    bool EngineEvaluationClockDelegate::push_node_requires_scheduling() {
        return _engine_evalaution_clock->push_node_requires_scheduling();
    }

    void EngineEvaluationClockDelegate::reset_push_node_requires_scheduling() {
        _engine_evalaution_clock->reset_push_node_requires_scheduling();
    }

    EvaluationEngineDelegate::EvaluationEngineDelegate(EvaluationEngine *api) : _evaluation_engine{api} {}

    EvaluationMode EvaluationEngineDelegate::evaluation_mode() const { return _evaluation_engine->evaluation_mode(); }

    engine_time_t EvaluationEngineDelegate::start_time() const { return _evaluation_engine->start_time(); }

    engine_time_t EvaluationEngineDelegate::end_time() const { return _evaluation_engine->end_time(); }

    EvaluationClock &EvaluationEngineDelegate::evaluation_clock() { return _evaluation_engine->evaluation_clock(); }

    EngineEvaluationClock &EvaluationEngineDelegate::engine_evaluation_clock() {
        return _evaluation_engine->engine_evaluation_clock();
    }

    void EvaluationEngineDelegate::request_engine_stop() { _evaluation_engine->request_engine_stop(); }

    bool EvaluationEngineDelegate::is_stop_requested() { return _evaluation_engine->is_stop_requested(); }

    void EvaluationEngineDelegate::add_before_evalaution_notification(std::function<void()> &&fn) {
        _evaluation_engine->add_before_evalaution_notification(std::forward<std::function<void()>>(fn));
    }

    void EvaluationEngineDelegate::add_after_evalaution_notification(std::function<void()> &&fn) {
        _evaluation_engine->add_after_evalaution_notification(std::forward<std::function<void()>>(fn));
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

    void EvaluationEngineDelegate::notify_before_start_graph(Graph &graph) { _evaluation_engine->notify_before_start_graph(graph); }

    void EvaluationEngineDelegate::notify_after_start_graph(Graph &graph) { _evaluation_engine->notify_after_start_graph(graph); }

    void EvaluationEngineDelegate::notify_before_start_node(Node &node) { _evaluation_engine->notify_before_start_node(node); }

    void EvaluationEngineDelegate::notify_after_start_node(Node &node) { _evaluation_engine->notify_after_start_node(node); }

    void EvaluationEngineDelegate::notify_before_graph_evaluation(Graph &graph) {
        _evaluation_engine->notify_before_graph_evaluation(graph);
    }

    void EvaluationEngineDelegate::notify_after_graph_evaluation(Graph &graph) {
        _evaluation_engine->notify_after_graph_evaluation(graph);
    }

    void EvaluationEngineDelegate::notify_before_node_evaluation(Node &node) {
        _evaluation_engine->notify_before_node_evaluation(node);
    }

    void EvaluationEngineDelegate::notify_after_node_evaluation(Node &node) {
        _evaluation_engine->notify_after_node_evaluation(node);
    }

    void EvaluationEngineDelegate::notify_before_stop_node(Node &node) { _evaluation_engine->notify_before_stop_node(node); }

    void EvaluationEngineDelegate::notify_after_stop_node(Node &node) { _evaluation_engine->notify_after_stop_node(node); }

    void EvaluationEngineDelegate::notify_before_stop_graph(Graph &graph) { _evaluation_engine->notify_before_stop_graph(graph); }

    void EvaluationEngineDelegate::notify_after_stop_graph(Graph &graph) { _evaluation_engine->notify_after_stop_graph(graph); }

    void EvaluationEngineDelegate::initialise() { _evaluation_engine->initialise(); }

    void EvaluationEngineDelegate::start() { _evaluation_engine->start(); }

    void EvaluationEngineDelegate::stop() { _evaluation_engine->stop(); }

    void EvaluationEngineDelegate::dispose() { _evaluation_engine->dispose(); }

    BaseEvaluationClock::BaseEvaluationClock(engine_time_t start_time)
        : _evaluation_time{start_time}, _next_scheduled_evaluation_time{MAX_DT} {}

    engine_time_t BaseEvaluationClock::evaluation_time() { return _evaluation_time; }

    engine_time_t BaseEvaluationClock::next_cycle_evaluation_time() { return _evaluation_time + MIN_TD; }

    engine_time_t BaseEvaluationClock::next_scheduled_evaluation_time() { return _next_scheduled_evaluation_time; }

    void BaseEvaluationClock::update_next_scheduled_evaluation_time(engine_time_t scheduled_time) {
        if (scheduled_time == _evaluation_time) {
            return;  // This will be evaluated in the current cycle, nothing to do.
        }
        _next_scheduled_evaluation_time =
            std::max(next_cycle_evaluation_time(), std::min(_next_scheduled_evaluation_time, scheduled_time));
    }

    SimulationEvaluationClock::SimulationEvaluationClock(engine_time_t current_time)
        : BaseEvaluationClock(current_time), _system_clock_at_start_of_evaluation{engine_time_t::clock::now()} {}

    void SimulationEvaluationClock::set_evaluation_time(engine_time_t value) {
        BaseEvaluationClock::set_evaluation_time(value);
        _system_clock_at_start_of_evaluation = engine_clock::now();
    }

    engine_time_t SimulationEvaluationClock::now() { return evaluation_time() + cycle_time(); }

    engine_time_delta_t SimulationEvaluationClock::cycle_time() {
        return engine_clock::now() - _system_clock_at_start_of_evaluation;
    }

    void SimulationEvaluationClock::advance_to_next_scheduled_time() { set_evaluation_time(next_scheduled_evaluation_time()); }

    void SimulationEvaluationClock::mark_push_node_requires_scheduling() {
        throw std::runtime_error("Simulation mode does not support push nodes.");
    }

    bool SimulationEvaluationClock::push_node_requires_scheduling() { return false; }

    void SimulationEvaluationClock::reset_push_node_requires_scheduling() {
        throw std::runtime_error("Simulation mode does not support push nodes.");
    }

    RealTimeEvaluationClock::RealTimeEvaluationClock(engine_time_t start_time)
        : BaseEvaluationClock(start_time), _push_node_requires_scheduling(false), _ready_to_push(false),
          _last_time_allowed_push(MIN_TD) {}

    engine_time_t RealTimeEvaluationClock::now() {
        return std::chrono::time_point_cast<std::chrono::milliseconds>(engine_clock::now());
    }

    engine_time_delta_t RealTimeEvaluationClock::cycle_time() { return engine_clock::now() - evaluation_time(); }
    void                RealTimeEvaluationClock::mark_push_node_requires_scheduling() {
        std::unique_lock<std::mutex> lock(_condition_mutex);
        _push_node_requires_scheduling = true;
        _push_node_requires_scheduling_condition.notify_all();
    }

    bool RealTimeEvaluationClock::push_node_requires_scheduling() {
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

}  // namespace hgraph

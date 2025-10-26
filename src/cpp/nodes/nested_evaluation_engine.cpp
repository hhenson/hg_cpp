#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/nested_node.h>
#include <hgraph/types/graph.h>

#include <utility>

namespace hgraph
{

    NestedEngineEvaluationClock::NestedEngineEvaluationClock(EngineEvaluationClock::ptr engine_evaluation_clock,
                                                             nested_node_ptr            nested_node)
        : EngineEvaluationClockDelegate(std::move(engine_evaluation_clock)), _nested_node(std::move(nested_node)) {}

    nested_node_ptr NestedEngineEvaluationClock::node() const { return _nested_node; }

    engine_time_t NestedEngineEvaluationClock::next_scheduled_evaluation_time() const {
        return _nested_next_scheduled_evaluation_time;
    }

    void NestedEngineEvaluationClock::reset_next_scheduled_evaluation_time() { _nested_next_scheduled_evaluation_time = MAX_DT; }

    void NestedEngineEvaluationClock::update_next_scheduled_evaluation_time(std::chrono::system_clock::time_point next_time) {
        auto let{_nested_node->last_evaluation_time()};
        //Unlike python when not set let will be MIN_DT
        // Python: if (let := self._nested_node.last_evaluation_time) and let >= next_time or self._nested_node.is_stopping:
        if (let != MIN_DT /* equivalent to falisy */ && let >= next_time || _nested_node->is_stopping()) { return; }

        // Match Python: min(next_time, max(self._nested_next_scheduled_evaluation_time, (let or MIN_DT) + MIN_TD))
        // Note let or MIN_DT is equivalent to let
        auto proposed_next_time = std::min(next_time, std::max(_nested_next_scheduled_evaluation_time, let + MIN_TD));

        if (proposed_next_time != _nested_next_scheduled_evaluation_time) {
            _nested_next_scheduled_evaluation_time = proposed_next_time;
            _nested_node->graph()->schedule_node(_nested_node->node_ndx(), proposed_next_time);
        }
    }

    void NestedEngineEvaluationClock::register_with_nanobind(nb::module_ &m) {
        nb::class_<NestedEngineEvaluationClock, EngineEvaluationClockDelegate>(m, "NestedEngineEvaluationClock")
            .def_prop_ro("node", &NestedEngineEvaluationClock::node);
    }

    NestedEvaluationEngine::NestedEvaluationEngine(EvaluationEngine::ptr engine, EngineEvaluationClock::ptr evaluation_clock)
        : EvaluationEngineDelegate(std::move(engine)), _engine_evaluation_clock(evaluation_clock),
          _nested_start_time(evaluation_clock->evaluation_time()) {}

    engine_time_t NestedEvaluationEngine::start_time() const { return _nested_start_time; }

    EvaluationClock::ptr NestedEvaluationEngine::evaluation_clock() { return _engine_evaluation_clock.get(); }

    EngineEvaluationClock::ptr NestedEvaluationEngine::engine_evaluation_clock() { return _engine_evaluation_clock; }
    void                       NestedEvaluationEngine::register_with_nanobind(nb::module_ &m) {
        nb::class_<NestedEvaluationEngine, EvaluationEngineDelegate>(m, "NestedEvaluationEngine");
    }

}  // namespace hgraph

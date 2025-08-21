//
// Created by Howard Henson on 02/08/2025.
//

#ifndef NESTED_EVALUATION_ENGINE_H
#define NESTED_EVALUATION_ENGINE_H

#include <hgraph/runtime/evaluation_engine.h>

#include <chrono>
#include <limits>
#include <utility>

namespace hgraph
{
    struct NestedNode;
    using nested_node_ptr = nb::ref<NestedNode>;

    struct NestedEngineEvaluationClock : EngineEvaluationClockDelegate
    {
        NestedEngineEvaluationClock(EngineEvaluationClock::ptr engine_evaluation_clock, nested_node_ptr nested_node);

        nested_node_ptr node() const;

        engine_time_t next_scheduled_evaluation_time() const override;

        void reset_next_scheduled_evaluation_time();

        void update_next_scheduled_evaluation_time(std::chrono::system_clock::time_point next_time) override;

        static void register_with_nanobind(nb::module_ &m);

      private:
        nested_node_ptr _nested_node;
        engine_time_t   _nested_next_scheduled_evaluation_time{MAX_DT};
    };

    struct NestedEvaluationEngine : public EvaluationEngineDelegate
    {
        NestedEvaluationEngine(EvaluationEngine::ptr engine, EngineEvaluationClock::ptr evaluation_clock);

        [[nodiscard]] engine_time_t start_time() const override;

        [[nodiscard]] EngineEvaluationClock &evaluation_clock() override;

        [[nodiscard]] EngineEvaluationClock &engine_evaluation_clock() override;

        static void register_with_nanobind(nb::module_ &m);

      private:
        NestedEngineEvaluationClock::ptr      _engine_evaluation_clock;
        std::chrono::system_clock::time_point _nested_start_time;
    };

}  // namespace hgraph

#endif  // NESTED_EVALUATION_ENGINE_H

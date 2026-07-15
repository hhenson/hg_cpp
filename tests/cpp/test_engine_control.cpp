// C++-first engine control: the node injectable is a borrowed projection over
// the native executor, and stop_engine is a native sink wired through the
// ordinary operator registry.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct EngineControlProbe
    {
        static constexpr auto name              = "engine_control_probe";
        static constexpr bool schedule_on_start = true;

        static void eval(EngineControlView engine, Out<TS<Bool>> out)
        {
            const auto clock = engine.evaluation_clock();
            out.set(engine.valid() && engine.mode() == GraphExecutorMode::Simulation &&
                    engine.start_time() == MIN_ST && engine.end_time() == MAX_ET &&
                    !engine.stop_requested() && clock.valid() && clock.evaluation_time() == MIN_ST);
        }
    };

    struct StopAfterFirstGraph
    {
        static constexpr auto name = "stop_after_first_graph";

        static Port<SIGNAL> compose(Wiring &w, Port<SIGNAL> signal)
        {
            wire<hgraph::stdlib::stop_engine>(w, signal);
            return signal;
        }
    };
}  // namespace

TEST_CASE("engine control: static nodes receive the native executor projection")
{
    CHECK_OUTPUT(eval_node<EngineControlProbe>(), {true});

    NodeBuilder builder;
    builder.implementation<EngineControlProbe>();
    CHECK(builder.type().checked_plan().find_component("engine_control") == nullptr);
}

TEST_CASE("stop_engine: the current cycle completes and later cycles do not run")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<StopAfterFirstGraph>(values<bool>(true, true, true)),
                 {true, none, none});
}

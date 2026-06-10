// Wiring-level sub-graph compilation (compile_subgraph<G> / nested_<G>).
//
// These tests drive the developer-guide *Nested Graphs* design: a sub-graph
// ``compose`` runs against boundary placeholder ports (no stub nodes), is
// compiled into a CompiledSubGraph, and is owned at runtime by the existing
// single_nested_graph_node. Sources/sinks are the standard ``replay`` /
// ``record`` testing nodes; the child bodies use the lib/std operators.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/subgraph_wiring.h>

#include <catch2/catch_test_macros.hpp>

#include <stdexcept>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    // ---------------- sub-graph definitions under test ----------------

    struct AddOneSubGraph
    {
        static constexpr auto name = "add_one_subgraph";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> in)
        {
            using namespace hgraph::stdlib::syntax;
            return (in + Int{1}).as<TS<Int>>();
        }
    };

    struct SumSubGraph
    {
        static constexpr auto name = "sum_subgraph";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            using namespace hgraph::stdlib::syntax;
            return (lhs + rhs).as<TS<Int>>();
        }
    };

    // One boundary arg consumed by two child input endpoints.
    struct FanOutSubGraph
    {
        static constexpr auto name = "fan_out_subgraph";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> in)
        {
            using namespace hgraph::stdlib::syntax;
            return (in + in).as<TS<Int>>();
        }
    };

    struct ScaledSubGraph
    {
        static constexpr auto name = "scaled_subgraph";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> in, Scalar<"factor", Int> factor)
        {
            using namespace hgraph::stdlib::syntax;
            return (in * factor.value()).as<TS<Int>>();
        }
    };

    // A sub-graph that is itself a source: its child schedule must drive the
    // outer graph through the nested node's schedule propagation (replay is a
    // self-rescheduling source).
    struct SourceOnlySubGraph
    {
        static constexpr auto name = "source_only_subgraph";
        static Port<TS<Int>>  compose(Wiring &w) { return wire<replay, TS<Int>>(w, Str{"src"}); }
    };

    // A sink sub-graph: compose returns void; the nested node becomes a sink.
    struct RecordSubGraph
    {
        static constexpr auto name = "record_subgraph";
        static void           compose(Wiring &w, Port<TS<Int>> in) { wire<record>(w, in, Str{"out"}); }
    };

    // Returning a boundary input directly is the pass-through mode
    // (alias_parent_input): the outer output aliases the outer input's source.
    struct PassThroughSubGraph
    {
        static constexpr auto name = "pass_through_subgraph";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> in) { return in; }
    };

    // A structural-initializer boundary: the outer arg is a non-peered TSL whose
    // shape is mirrored into the child compile (boundary leaves), so the child
    // consumer binds leaf-wise.
    struct TslRecordSubGraph
    {
        static constexpr auto name = "tsl_record_subgraph";
        static void           compose(Wiring &w, Port<TSL<TS<Int>, 2>> in) { wire<record>(w, in, Str{"out"}); }
    };

    struct GlobalStateSeedingSubGraph
    {
        static constexpr auto name = "global_state_seeding_subgraph";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> in)
        {
            using namespace hgraph::stdlib::syntax;
            w.global_state().set("seeded", Value{Int{1}});
            return (in + Int{1}).as<TS<Int>>();
        }
    };

    // ---------------- top-level test graphs ----------------

    struct NestedAddOneGraph
    {
        static constexpr auto name = "nested_add_one_graph";
        static void           compose(Wiring &w)
        {
            auto in = wire<replay, TS<Int>>(w, Str{"in"});
            wire<record>(w, nested_<AddOneSubGraph>(w, in), Str{"out"});
        }
    };

    struct NestedSumGraph
    {
        static constexpr auto name = "nested_sum_graph";
        static void           compose(Wiring &w)
        {
            auto a = wire<replay, TS<Int>>(w, Str{"a"});
            auto b = wire<replay, TS<Int>>(w, Str{"b"});
            wire<record>(w, nested_<SumSubGraph>(w, a, b), Str{"out"});
        }
    };

    struct NestedFanOutGraph
    {
        static constexpr auto name = "nested_fan_out_graph";
        static void           compose(Wiring &w)
        {
            auto in = wire<replay, TS<Int>>(w, Str{"in"});
            wire<record>(w, nested_<FanOutSubGraph>(w, in), Str{"out"});
        }
    };

    struct NestedSourceOnlyGraph
    {
        static constexpr auto name = "nested_source_only_graph";
        static void           compose(Wiring &w)
        {
            wire<record>(w, nested_<SourceOnlySubGraph>(w), Str{"out"});
        }
    };

    struct NestedScaledGraph
    {
        static constexpr auto name = "nested_scaled_graph";
        static void           compose(Wiring &w)
        {
            auto in = wire<replay, TS<Int>>(w, Str{"in"});

            auto a = nested_<ScaledSubGraph>(w, in, Int{2});
            auto b = nested_<ScaledSubGraph>(w, in, Int{2});
            auto c = nested_<ScaledSubGraph>(w, in, Int{3});

            // Equal scalars intern to one nested node; distinct scalars do not.
            CHECK(a.node() == b.node());
            CHECK(a.node() != c.node());

            wire<record>(w, a, Str{"doubled"});
            wire<record>(w, c, Str{"tripled"});
        }
    };

    struct NestedRecordGraph
    {
        static constexpr auto name = "nested_record_graph";
        static void           compose(Wiring &w)
        {
            auto in = wire<replay, TS<Int>>(w, Str{"in"});
            nested_<RecordSubGraph>(w, in);
        }
    };

    struct NestedPassThroughGraph
    {
        static constexpr auto name = "nested_pass_through_graph";
        static void           compose(Wiring &w)
        {
            auto in = wire<replay, TS<Int>>(w, Str{"in"});
            wire<record>(w, nested_<PassThroughSubGraph>(w, in), Str{"out"});
        }
    };

    struct NestedTslGraph
    {
        static constexpr auto name = "nested_tsl_graph";
        static void           compose(Wiring &w)
        {
            auto a = wire<replay, TS<Int>>(w, Str{"a"});
            auto b = wire<replay, TS<Int>>(w, Str{"b"});
            nested_<TslRecordSubGraph>(w, {a, b});
        }
    };

    template <typename Graph, typename Seed>
    GraphExecutorValue run_seeded(Seed seed)
    {
        GraphBuilder gb = build_graph<Graph>();
        seed(gb.global_state());
        return run_graph(std::move(gb), MIN_ST, MIN_ST + TimeDelta{10});
    }
}  // namespace

TEST_CASE("nested wiring: nested_<G> binds an outer input into the child graph and forwards its output")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    auto ex = run_seeded<NestedAddOneGraph>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "in", values<Int>(1, 2, 3));
    });
    CHECK_OUTPUT(get_recorded_values<Int>(ex.view().graph().global_state(), "out"), values<Int>(2, 3, 4));
}

TEST_CASE("nested wiring: a sub-graph with multiple boundary inputs binds each arg")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    auto ex = run_seeded<NestedSumGraph>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "a", values<Int>(1, 2, 3));
        set_replay_values<Int>(gs, "b", values<Int>(10, 20, 30));
    });
    CHECK_OUTPUT(get_recorded_values<Int>(ex.view().graph().global_state(), "out"), values<Int>(11, 22, 33));
}

TEST_CASE("nested wiring: one boundary arg feeds multiple child input endpoints")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    auto ex = run_seeded<NestedFanOutGraph>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "in", values<Int>(1, 2, 3));
    });
    CHECK_OUTPUT(get_recorded_values<Int>(ex.view().graph().global_state(), "out"), values<Int>(2, 4, 6));
}

TEST_CASE("nested wiring: a source-only sub-graph drives the outer graph via schedule propagation")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // The child replay reads its buffer through the root GlobalState (nested
    // graphs delegate state to the root) and re-schedules itself each cycle;
    // the nested node's schedule propagation must keep the outer graph running
    // with no outer source at all.
    auto ex = run_seeded<NestedSourceOnlyGraph>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "src", values<Int>(5, 6, 7));
    });
    CHECK_OUTPUT(get_recorded_values<Int>(ex.view().graph().global_state(), "out"), values<Int>(5, 6, 7));
}

TEST_CASE("nested wiring: scalar parameters configure the child graph and partition interning")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    auto ex = run_seeded<NestedScaledGraph>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "in", values<Int>(1, 2, 3));
    });
    CHECK_OUTPUT(get_recorded_values<Int>(ex.view().graph().global_state(), "doubled"), values<Int>(2, 4, 6));
    CHECK_OUTPUT(get_recorded_values<Int>(ex.view().graph().global_state(), "tripled"), values<Int>(3, 6, 9));
}

TEST_CASE("nested wiring: a sink sub-graph wires as a nested sink node")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    auto ex = run_seeded<NestedRecordGraph>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "in", values<Int>(1, 2, 3));
    });
    CHECK_OUTPUT(get_recorded_values<Int>(ex.view().graph().global_state(), "out"), values<Int>(1, 2, 3));
}

TEST_CASE("nested wiring: a pass-through sub-graph aliases the outer input (alias_parent_input)")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    auto ex = run_seeded<NestedPassThroughGraph>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "in", values<Int>(1, 2, 3));
    });
    CHECK_OUTPUT(get_recorded_values<Int>(ex.view().graph().global_state(), "out"), values<Int>(1, 2, 3));
}

TEST_CASE("nested wiring: a structural initializer boundary binds leaf-wise into the child")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    auto ex = run_seeded<NestedTslGraph>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "a", values<Int>(1, 2));
        set_replay_values<Int>(gs, "b", values<Int>(10, 20));
    });
    const auto recorded = get_recorded_deltas(ex.view().graph().global_state(), "out");
    REQUIRE(recorded.size() == 2);
    REQUIRE(recorded[0].has_value());
    REQUIRE(recorded[1].has_value());
    CHECK(recorded[0]->equals(list_delta<TS<Int>>({1, 10})));
    CHECK(recorded[1]->equals(list_delta<TS<Int>>({2, 20})));
}

TEST_CASE("nested wiring: a sub-graph compose must not seed GlobalState")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    Wiring w;
    auto   in = wire<replay, TS<Int>>(w, Str{"in"});
    CHECK_THROWS_AS((void)nested_<GlobalStateSeedingSubGraph>(w, in), std::invalid_argument);
}

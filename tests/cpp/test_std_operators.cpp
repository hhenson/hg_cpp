// Phase 3: a real lib/std operator family (`add_`, `eq_`) with several per-type
// implementations under one name, selected by the supplied argument types at wiring.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    // replay("a"), replay("b") -> Op(a, b) -> record("out"), over element type ``ElemTS``.
    template <typename Op, typename ElemTS>
    struct BinOpGraph
    {
        static constexpr auto name = "bin_op_graph";
        static void           compose(Wiring &w)
        {
            auto a = wire<testing::replay, ElemTS>(w, Str{"a"});
            auto b = wire<testing::replay, ElemTS>(w, Str{"b"});
            auto r = wire<Op>(w, a, b);
            wire<testing::record>(w, r, Str{"out"});
        }
    };

    template <typename Graph, typename Seed>
    GraphExecutorValue run_graph(Seed seed)
    {
        GraphBuilder gb = build_graph<Graph>();
        seed(gb.global_state());
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();
        return ex;
    }
}  // namespace

TEST_CASE("std operators: add_ selects the int implementation for TS<Int> operands")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    auto ex = run_graph<BinOpGraph<stdlib::add_, TS<Int>>>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "a", {1, 2, 3});
        set_replay_values<Int>(gs, "b", {10, 20, 30});
    });
    CHECK_OUTPUT(get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {11, 22, 33});
}

TEST_CASE("std operators: add_ selects the float implementation for TS<Float> operands")
{
    (void)TypeRegistry::instance().register_scalar<Float>("float");
    stdlib::register_standard_operators();

    auto ex = run_graph<BinOpGraph<stdlib::add_, TS<Float>>>([](const GlobalStateView &gs) {
        set_replay_values<Float>(gs, "a", {Float{1.5}, Float{2.25}});
        set_replay_values<Float>(gs, "b", {Float{0.5}, Float{0.75}});
    });
    CHECK_OUTPUT(get_recorded_values<Float>(ex.view().graph().global_state(), "out"), {Float{2.0}, Float{3.0}});
}

TEST_CASE("std operators: eq_ resolves its TS<Bool> output independently of the operand type")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    auto ex = run_graph<BinOpGraph<stdlib::eq_, TS<Int>>>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "a", {1, 2, 3});
        set_replay_values<Int>(gs, "b", {1, 5, 3});
    });
    CHECK_OUTPUT(get_recorded_values<Bool>(ex.view().graph().global_state(), "out"), {true, false, true});
}

TEST_CASE("std operators: eq_ works for strings (the str implementation)")
{
    (void)TypeRegistry::instance().register_scalar<Str>("str");
    stdlib::register_standard_operators();

    auto ex = run_graph<BinOpGraph<stdlib::eq_, TS<Str>>>([](const GlobalStateView &gs) {
        set_replay_values<Str>(gs, "a", {Str{"x"}, Str{"y"}});
        set_replay_values<Str>(gs, "b", {Str{"x"}, Str{"z"}});
    });
    CHECK_OUTPUT(get_recorded_values<Bool>(ex.view().graph().global_state(), "out"), {true, false});
}

TEST_CASE("std operators: a type with no registered implementation raises")
{
    (void)TypeRegistry::instance().register_scalar<Str>("str");
    stdlib::register_standard_operators();   // add_ has int / float impls only

    REQUIRE_THROWS_AS((build_graph<BinOpGraph<stdlib::add_, TS<Str>>>()), OperatorResolutionError);
}

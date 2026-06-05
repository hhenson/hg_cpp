// Phase 3: a real lib/std operator family (`add_`, `sub_`, `div_`, `eq_`). One name
// collects several per-type implementations; the most specific is selected at wiring.
//
// These exercise the "operator signature is a suggestion" principle: the operators
// declare independent type variables for lhs / rhs / result, so a single name covers
// homogeneous (int + int), mixed (int + float -> float), heterogeneous
// (datetime + timedelta -> datetime), and result-differs cases (div int / int -> float;
// datetime - datetime -> timedelta).

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/util/date_time.h>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using namespace std::chrono;

    // replay("a"), replay("b") -> Op(a, b) -> record("out") over a single element type.
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

    // Heterogeneous variant: the two operands have *different* element types.
    template <typename Op, typename LhsTS, typename RhsTS>
    struct BinOpGraph2
    {
        static constexpr auto name = "bin_op_graph2";
        static void           compose(Wiring &w)
        {
            auto a = wire<testing::replay, LhsTS>(w, Str{"a"});
            auto b = wire<testing::replay, RhsTS>(w, Str{"b"});
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

    [[nodiscard]] engine_time_t dt(std::int64_t micros) { return engine_time_t{microseconds{micros}}; }
    [[nodiscard]] engine_date_t ymd(int y, unsigned m, unsigned d)
    {
        return engine_date_t{year{y} / month{m} / day{d}};
    }

    // div_(a, b, policy): the divide-by-zero policy is a wiring-time scalar, forwarded
    // from a graph-level Scalar parameter to the operator.
    struct DivPolicyGraph
    {
        static constexpr auto name = "div_policy_graph";
        static void           compose(Wiring &w, Scalar<"policy", stdlib::DivideByZero> policy)
        {
            auto a = wire<testing::replay, TS<Int>>(w, Str{"a"});
            auto b = wire<testing::replay, TS<Int>>(w, Str{"b"});
            auto r = wire<stdlib::div_>(w, a, b, policy);
            wire<testing::record>(w, r, Str{"out"});
        }
    };

    [[nodiscard]] std::vector<std::optional<Float>> run_div_policy(stdlib::DivideByZero policy,
                                                                  const std::vector<std::optional<Int>> &a,
                                                                  const std::vector<std::optional<Int>> &b)
    {
        GraphBuilder gb = build_graph<DivPolicyGraph>(policy);
        set_replay_values<Int>(gb.global_state(), "a", a);
        set_replay_values<Int>(gb.global_state(), "b", b);
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();
        return get_recorded_values<Float>(ex.view().graph().global_state(), "out");
    }
}  // namespace

TEST_CASE("std operators: add_ selects the int implementation for TS<Int> operands")
{
    stdlib::register_standard_operators();

    auto ex = run_graph<BinOpGraph<stdlib::add_, TS<Int>>>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "a", {1, 2, 3});
        set_replay_values<Int>(gs, "b", {10, 20, 30});
    });
    CHECK_OUTPUT(get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {11, 22, 33});
}

TEST_CASE("std operators: add_ supports mixed numeric operands (int + float -> float)")
{
    stdlib::register_standard_operators();

    auto ex = run_graph<BinOpGraph2<stdlib::add_, TS<Int>, TS<Float>>>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "a", {1, 2, 3});
        set_replay_values<Float>(gs, "b", {Float{0.5}, Float{1.5}, Float{2.5}});
    });
    CHECK_OUTPUT(get_recorded_values<Float>(ex.view().graph().global_state(), "out"),
                 {Float{1.5}, Float{3.5}, Float{5.5}});
}

TEST_CASE("std operators: add_ supports datetime + timedelta -> datetime")
{
    stdlib::register_standard_operators();

    auto ex = run_graph<BinOpGraph2<stdlib::add_, TS<engine_time_t>, TS<engine_time_delta_t>>>(
        [](const GlobalStateView &gs) {
            set_replay_values<engine_time_t>(gs, "a", {dt(1'000'000), dt(2'000'000)});
            set_replay_values<engine_time_delta_t>(gs, "b", {microseconds{500'000}, microseconds{1'500'000}});
        });
    CHECK_OUTPUT(get_recorded_values<engine_time_t>(ex.view().graph().global_state(), "out"),
                 {dt(1'500'000), dt(3'500'000)});
}

TEST_CASE("std operators: add_ supports date + timedelta -> date (whole days)")
{
    stdlib::register_standard_operators();

    const engine_time_delta_t two_days  = duration_cast<engine_time_delta_t>(days{2});
    const engine_time_delta_t five_days = duration_cast<engine_time_delta_t>(days{5});

    auto ex = run_graph<BinOpGraph2<stdlib::add_, TS<engine_date_t>, TS<engine_time_delta_t>>>(
        [&](const GlobalStateView &gs) {
            set_replay_values<engine_date_t>(gs, "a", {ymd(2020, 1, 1), ymd(2020, 1, 10)});
            set_replay_values<engine_time_delta_t>(gs, "b", {two_days, five_days});
        });
    CHECK_OUTPUT(get_recorded_values<engine_date_t>(ex.view().graph().global_state(), "out"),
                 {ymd(2020, 1, 3), ymd(2020, 1, 15)});
}

TEST_CASE("std operators: div_ produces a different result type (int / int -> float)")
{
    stdlib::register_standard_operators();

    auto ex = run_graph<BinOpGraph<stdlib::div_, TS<Int>>>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "a", {7, 9});
        set_replay_values<Int>(gs, "b", {2, 3});
    });
    CHECK_OUTPUT(get_recorded_values<Float>(ex.view().graph().global_state(), "out"), {Float{3.5}, Float{3.0}});
}

TEST_CASE("std operators: sub_ of two datetimes yields a timedelta (result differs from both operands)")
{
    stdlib::register_standard_operators();

    auto ex = run_graph<BinOpGraph<stdlib::sub_, TS<engine_time_t>>>([](const GlobalStateView &gs) {
        set_replay_values<engine_time_t>(gs, "a", {dt(3'000'000), dt(5'000'000)});
        set_replay_values<engine_time_t>(gs, "b", {dt(1'000'000), dt(2'000'000)});
    });
    CHECK_OUTPUT(get_recorded_values<engine_time_delta_t>(ex.view().graph().global_state(), "out"),
                 {microseconds{2'000'000}, microseconds{3'000'000}});
}

TEST_CASE("std operators: eq_ resolves its TS<Bool> output independently of the operand type")
{
    stdlib::register_standard_operators();

    auto ex = run_graph<BinOpGraph<stdlib::eq_, TS<Int>>>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "a", {1, 2, 3});
        set_replay_values<Int>(gs, "b", {1, 5, 3});
    });
    CHECK_OUTPUT(get_recorded_values<Bool>(ex.view().graph().global_state(), "out"), {true, false, true});
}

TEST_CASE("std operators: eq_ works for strings")
{
    stdlib::register_standard_operators();

    auto ex = run_graph<BinOpGraph<stdlib::eq_, TS<Str>>>([](const GlobalStateView &gs) {
        set_replay_values<Str>(gs, "a", {Str{"x"}, Str{"y"}});
        set_replay_values<Str>(gs, "b", {Str{"x"}, Str{"z"}});
    });
    CHECK_OUTPUT(get_recorded_values<Bool>(ex.view().graph().global_state(), "out"), {true, false});
}

TEST_CASE("std operators: an operand combination with no registered implementation raises")
{
    (void)TypeRegistry::instance().register_scalar<Str>("str");
    stdlib::register_standard_operators();   // add_ has no str overload

    REQUIRE_THROWS_AS((build_graph<BinOpGraph<stdlib::add_, TS<Str>>>()), OperatorResolutionError);
}

TEST_CASE("std operators: div_ takes an optional divide-by-zero policy scalar")
{
    using DBZ = stdlib::DivideByZero;
    stdlib::register_standard_operators();

    const Float inf = std::numeric_limits<Float>::infinity();

    // Non-zero divisors are unaffected by the policy.
    CHECK_OUTPUT(run_div_policy(DBZ::Inf, {6, 9}, {2, 3}), {Float{3.0}, Float{3.0}});

    // A zero divisor takes the policy's value.
    CHECK_OUTPUT(run_div_policy(DBZ::Inf, {1, 1}, {2, 0}), {Float{0.5}, inf});
    CHECK_OUTPUT(run_div_policy(DBZ::Zero, {1, 1}, {2, 0}), {Float{0.5}, Float{0.0}});
    CHECK_OUTPUT(run_div_policy(DBZ::One, {1, 1}, {2, 0}), {Float{0.5}, Float{1.0}});

    // NoTick produces a gap (no tick) on the zero-divisor cycle.
    CHECK_OUTPUT(run_div_policy(DBZ::NoTick, {1, 1, 1}, {2, 0, 4}), {Float{0.5}, none, Float{0.25}});
}

TEST_CASE("std operators: div_ NaN policy emits NaN on a zero divisor")
{
    using DBZ = stdlib::DivideByZero;
    stdlib::register_standard_operators();

    const std::vector<std::optional<Float>> out = run_div_policy(DBZ::Nan, {1, 1}, {2, 0});
    REQUIRE(out.size() == 2);
    CHECK(out[0] == Float{0.5});
    REQUIRE(out[1].has_value());
    CHECK(std::isnan(*out[1]));
}

TEST_CASE("std operators: div_ Error policy raises on a zero divisor")
{
    using DBZ = stdlib::DivideByZero;
    stdlib::register_standard_operators();

    REQUIRE_THROWS(run_div_policy(DBZ::Error, {1}, {0}));
}

TEST_CASE("std operators: div_ without a policy defaults to Error and raises on a zero divisor")
{
    stdlib::register_standard_operators();

    using DivIntGraph = BinOpGraph<stdlib::div_, TS<Int>>;
    REQUIRE_THROWS(run_graph<DivIntGraph>([](const GlobalStateView &gs) {
        set_replay_values<Int>(gs, "a", {1});
        set_replay_values<Int>(gs, "b", {0});
    }));
}

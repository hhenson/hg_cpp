// Phase 3: a real lib/std operator family (`add_`, `sub_`, `div_`, `eq_`). One name
// collects several per-type implementations; the most specific is selected at wiring.
//
// These exercise the "operator signature is a suggestion" principle: the operators
// declare independent type variables for lhs / rhs / result, so a single name covers
// homogeneous (int + int), mixed (int + float -> float), heterogeneous
// (datetime + timedelta -> datetime), and result-differs cases (div int / int -> float;
// datetime - datetime -> timedelta).
//
// Operators are evaluated through the type-erased ``eval_node<Op>`` harness: the output
// schema is the one operator dispatch resolves at wiring time, so results come back as
// per-cycle ``Value`` deltas. The expected sequence is written with the same
// ``values<T>(...)`` helper used for the inputs; ``CHECK_OUTPUT`` boxes it and compares
// with ``Value`` equality.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
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

    [[nodiscard]] engine_time_t dt(std::int64_t micros) { return engine_time_t{microseconds{micros}}; }
    [[nodiscard]] engine_date_t ymd(int y, unsigned m, unsigned d)
    {
        return engine_date_t{year{y} / month{m} / day{d}};
    }

    // zero_ is a *source* operator (no time-series input), so it is outside eval_node's
    // scope; drive it through a small graph instead.
    template <typename Schema>
    struct ZeroGraph
    {
        static constexpr auto name = "zero_graph";
        static void           compose(Wiring &w)
        {
            auto z = wire<stdlib::zero_, Schema>(w);
            wire<testing::record>(w, z, Str{"out"});
        }
    };

    template <typename Graph>
    GraphExecutorValue run_graph()
    {
        GraphBuilder         gb = build_graph<Graph>();
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();
        return ex;
    }
}  // namespace

TEST_CASE("std operators: add_ selects the int implementation for TS<Int> operands")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::add_>(values<Int>(1, 2, 3), values<Int>(10, 20, 30)), values<Int>(11, 22, 33));
}

TEST_CASE("std operators: add_ supports mixed numeric operands (int + float -> float)")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::add_>(values<Int>(1, 2, 3), values<Float>(0.5, 1.5, 2.5)),
                 values<Float>(1.5, 3.5, 5.5));
}

TEST_CASE("std operators: add_ supports datetime + timedelta -> datetime")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::add_>(values<engine_time_t>(dt(1'000'000), dt(2'000'000)),
                                         values<engine_time_delta_t>(microseconds{500'000}, microseconds{1'500'000})),
                 values<engine_time_t>(dt(1'500'000), dt(3'500'000)));
}

TEST_CASE("std operators: add_ supports date + timedelta -> date (whole days)")
{
    stdlib::register_standard_operators();
    const engine_time_delta_t two_days  = duration_cast<engine_time_delta_t>(days{2});
    const engine_time_delta_t five_days = duration_cast<engine_time_delta_t>(days{5});

    CHECK_OUTPUT(eval_node<stdlib::add_>(values<engine_date_t>(ymd(2020, 1, 1), ymd(2020, 1, 10)),
                                         values<engine_time_delta_t>(two_days, five_days)),
                 values<engine_date_t>(ymd(2020, 1, 3), ymd(2020, 1, 15)));
}

TEST_CASE("std operators: div_ produces a different result type (int / int -> float)")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::div_>(values<Int>(7, 9), values<Int>(2, 3)), values<Float>(3.5, 3.0));
}

TEST_CASE("std operators: sub_ of two datetimes yields a timedelta (result differs from both operands)")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::sub_>(values<engine_time_t>(dt(3'000'000), dt(5'000'000)),
                                         values<engine_time_t>(dt(1'000'000), dt(2'000'000))),
                 values<engine_time_delta_t>(microseconds{2'000'000}, microseconds{3'000'000}));
}

TEST_CASE("std operators: eq_ resolves its TS<Bool> output independently of the operand type")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::eq_>(values<Int>(1, 2, 3), values<Int>(1, 5, 3)),
                 values<Bool>(true, false, true));
}

TEST_CASE("std operators: eq_ works for strings")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::eq_>(values<Str>(Str{"x"}, Str{"y"}), values<Str>(Str{"x"}, Str{"z"})),
                 values<Bool>(true, false));
}

TEST_CASE("std operators: zero_ emits the additive zero for standard scalar outputs")
{
    stdlib::register_standard_operators();

    {
        auto ex = run_graph<ZeroGraph<TS<Int>>>();
        CHECK_OUTPUT(get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {0});
    }
    {
        auto ex = run_graph<ZeroGraph<TS<Float>>>();
        CHECK_OUTPUT(get_recorded_values<Float>(ex.view().graph().global_state(), "out"), {Float{0}});
    }
    {
        auto ex = run_graph<ZeroGraph<TS<Str>>>();
        CHECK_OUTPUT(get_recorded_values<Str>(ex.view().graph().global_state(), "out"), {Str{}});
    }
}

TEST_CASE("std operators: an operand combination with no registered implementation raises")
{
    stdlib::register_standard_operators();   // add_ has no str overload
    REQUIRE_THROWS_AS(eval_node<stdlib::add_>(values<Str>(Str{"x"}), values<Str>(Str{"y"})), OperatorResolutionError);
}

TEST_CASE("std operators: div_ takes an optional divide-by-zero policy scalar")
{
    using DBZ = stdlib::DivideByZero;
    stdlib::register_standard_operators();

    const Float inf = std::numeric_limits<Float>::infinity();

    // Non-zero divisors are unaffected by the policy.
    CHECK_OUTPUT(eval_node<stdlib::div_>(values<Int>(6, 9), values<Int>(2, 3), DBZ::Inf), values<Float>(3.0, 3.0));

    // A zero divisor takes the policy's value.
    CHECK_OUTPUT(eval_node<stdlib::div_>(values<Int>(1, 1), values<Int>(2, 0), DBZ::Inf), values<Float>(0.5, inf));
    CHECK_OUTPUT(eval_node<stdlib::div_>(values<Int>(1, 1), values<Int>(2, 0), DBZ::Zero), values<Float>(0.5, 0.0));
    CHECK_OUTPUT(eval_node<stdlib::div_>(values<Int>(1, 1), values<Int>(2, 0), DBZ::One), values<Float>(0.5, 1.0));

    // NoTick produces a gap (no tick) on the zero-divisor cycle.
    CHECK_OUTPUT(eval_node<stdlib::div_>(values<Int>(1, 1, 1), values<Int>(2, 0, 4), DBZ::NoTick),
                 values<Float>(0.5, none, 0.25));
}

TEST_CASE("std operators: div_ NaN policy emits NaN on a zero divisor")
{
    using DBZ = stdlib::DivideByZero;
    stdlib::register_standard_operators();

    const std::vector<std::optional<Value>> out =
        eval_node<stdlib::div_>(values<Int>(1, 1), values<Int>(2, 0), DBZ::Nan);
    REQUIRE(out.size() == 2);
    REQUIRE(out[0].has_value());
    CHECK(out[0]->view().checked_as<Float>() == Float{0.5});
    REQUIRE(out[1].has_value());
    CHECK(std::isnan(out[1]->view().checked_as<Float>()));
}

TEST_CASE("std operators: div_ Error policy raises on a zero divisor")
{
    using DBZ = stdlib::DivideByZero;
    stdlib::register_standard_operators();

    REQUIRE_THROWS(eval_node<stdlib::div_>(values<Int>(1), values<Int>(0), DBZ::Error));
}

TEST_CASE("std operators: div_ without a policy defaults to Error and raises on a zero divisor")
{
    stdlib::register_standard_operators();
    REQUIRE_THROWS(eval_node<stdlib::div_>(values<Int>(1), values<Int>(0)));
}

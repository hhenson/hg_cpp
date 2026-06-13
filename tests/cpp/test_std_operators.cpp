// Phase 3: real lib/std operator families. One name
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

#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <numbers>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using namespace std::chrono;

    [[nodiscard]] DateTime dt(std::int64_t micros) { return DateTime{microseconds{micros}}; }
    [[nodiscard]] Date ymd(int y, unsigned m, unsigned d)
    {
        return Date{year{y} / month{m} / day{d}};
    }


    // Lightweight graphs with declared inputs/outputs, driven through eval_node.
    struct SyntaxArithmeticGraph
    {
        static constexpr auto name = "syntax_arithmetic_graph";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> a, Port<TS<Int>> b)
        {
            using namespace hgraph::stdlib::syntax;
            return (a + b * Int{2}).as<TS<Int>>();
        }
    };

    struct SyntaxComparisonGraph
    {
        static constexpr auto name = "syntax_comparison_graph";
        static Port<TS<Bool>> compose(Wiring &, Port<TS<Int>> a, Port<TS<Float>> b)
        {
            using namespace hgraph::stdlib::syntax;
            return ((a < b) || !(a == Int{0})).as<TS<Bool>>();
        }
    };

    struct SyntaxNamedHelperGraph
    {
        static constexpr auto name = "syntax_named_helper_graph";
        static Port<TS<Float>> compose(Wiring &, Port<TS<Int>> a)
        {
            using namespace hgraph::stdlib::syntax;
            return (pow(abs(-a), Int{2}) / Int{2}).as<TS<Float>>();
        }
    };

    struct SyntaxBadCastGraph
    {
        static constexpr auto name = "syntax_bad_cast_graph";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> a, Port<TS<Int>> b)
        {
            using namespace hgraph::stdlib::syntax;
            return (a / b).as<TS<Int>>();   // int / int -> float: the cast must throw
        }
    };

    struct SplitToPairGraph
    {
        static constexpr auto  name = "split_to_pair_graph";
        static Port<TSL<TS<Str>, 2>> compose(Wiring &w, Port<TS<Str>> s)
        {
            return wire<stdlib::split, TSL<TS<Str>, 2>>(w, s, Str{","});
        }
    };

    struct JoinDefaultGraph
    {
        static constexpr auto name = "join_default_graph";
        static Port<TS<Str>> compose(Wiring &w, Port<TSL<TS<Str>, 3>> strings)
        {
            return wire<stdlib::join>(w, strings, Str{","}).as<TS<Str>>();
        }
    };

    struct JoinStrictGraph
    {
        static constexpr auto name = "join_strict_graph";
        static Port<TS<Str>> compose(Wiring &w, Port<TSL<TS<Str>, 3>> strings)
        {
            return wire<stdlib::join>(w, strings, Str{","}, arg<"__strict__">(Bool{true})).as<TS<Str>>();
        }
    };

    struct FormatArgsGraph
    {
        static constexpr auto name = "format_args_graph";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> fmt, Port<TS<Int>> ts1, Port<TS<Str>> ts2)
        {
            return wire<stdlib::format_>(w, fmt, ts1, ts2).as<TS<Str>>();
        }
    };

    struct FormatNoArgsGraph
    {
        static constexpr auto name = "format_no_args_graph";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> fmt)
        {
            return wire<stdlib::format_>(w, fmt).as<TS<Str>>();
        }
    };

    struct FormatKwargsGraph
    {
        static constexpr auto name = "format_kwargs_graph";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> fmt, Port<TS<Int>> ts1, Port<TS<Str>> ts2)
        {
            return wire<stdlib::format_>(w, fmt, arg<"ts1">(ts1), arg<"ts2">(ts2)).as<TS<Str>>();
        }
    };

    struct FormatMixedGraph
    {
        static constexpr auto name = "format_mixed_graph";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> fmt, Port<TS<Float>> ts,
                                     Port<TS<Int>> ts1, Port<TS<Str>> ts2)
        {
            return wire<stdlib::format_>(w, fmt, ts, arg<"ts1">(ts1), arg<"ts2">(ts2)).as<TS<Str>>();
        }
    };

    struct FormatSampledGraph
    {
        static constexpr auto name = "format_sampled_graph";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> fmt, Port<TS<Int>> ts1, Port<TS<Str>> ts2)
        {
            return wire<stdlib::format_>(w, fmt, ts1, ts2, arg<"__sample__">(Int{3})).as<TS<Str>>();
        }
    };

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

TEST_CASE("std operators: add_ supports string concatenation")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::add_>(values<Str>(Str{"a"}, Str{"h"}), values<Str>(Str{"b"}, Str{"g"})),
                 values<Str>(Str{"ab"}, Str{"hg"}));
}

TEST_CASE("std operators: add_ supports datetime + timedelta -> datetime")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::add_>(values<DateTime>(dt(1'000'000), dt(2'000'000)),
                                         values<TimeDelta>(microseconds{500'000}, microseconds{1'500'000})),
                 values<DateTime>(dt(1'500'000), dt(3'500'000)));
}

TEST_CASE("std operators: add_ supports date + timedelta -> date (whole days)")
{
    stdlib::register_standard_operators();
    const TimeDelta two_days  = duration_cast<TimeDelta>(days{2});
    const TimeDelta five_days = duration_cast<TimeDelta>(days{5});

    CHECK_OUTPUT(eval_node<stdlib::add_>(values<Date>(ymd(2020, 1, 1), ymd(2020, 1, 10)),
                                         values<TimeDelta>(two_days, five_days)),
                 values<Date>(ymd(2020, 1, 3), ymd(2020, 1, 15)));
}

TEST_CASE("std operators: div_ produces a different result type (int / int -> float)")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::div_>(values<Int>(7, 9), values<Int>(2, 3)), values<Float>(3.5, 3.0));
}

TEST_CASE("std operators: sub_ of two datetimes yields a timedelta (result differs from both operands)")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::sub_>(values<DateTime>(dt(3'000'000), dt(5'000'000)),
                                         values<DateTime>(dt(1'000'000), dt(2'000'000))),
                 values<TimeDelta>(microseconds{2'000'000}, microseconds{3'000'000}));
}

TEST_CASE("std operators: sub_ supports mixed numeric operands")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::sub_>(values<Int>(5, 7), values<Float>(0.5, 2.25)),
                 values<Float>(4.5, 4.75));
}

TEST_CASE("std operators: mul_ supports numeric operands and string repetition")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::mul_>(values<Int>(2, 3), values<Int>(4, 5)), values<Int>(8, 15));
    CHECK_OUTPUT(eval_node<stdlib::mul_>(values<Int>(2, 3), values<Float>(0.5, 1.5)), values<Float>(1.0, 4.5));
    CHECK_OUTPUT(eval_node<stdlib::mul_>(values<Str>(Str{"a"}, Str{"bc"}), values<Int>(3, 2)),
                 values<Str>(Str{"aaa"}, Str{"bcbc"}));
}

TEST_CASE("std operators: eq_ resolves its TS<Bool> output independently of the operand type")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::eq_>(values<Int>(1, 2, 3), values<Int>(1, 5, 3)),
                 values<Bool>(true, false, true));
}

TEST_CASE("std operators: comparison operators support ordering and cmp_")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::ne_>(values<Int>(1, 2), values<Int>(1, 3)), values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<stdlib::lt_>(values<Int>(1, 5), values<Float>(2.0, 4.0)), values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<stdlib::ge_>(values<Str>(Str{"b"}, Str{"a"}), values<Str>(Str{"a"}, Str{"a"})),
                 values<Bool>(true, true));
    CHECK_OUTPUT(eval_node<stdlib::cmp_>(values<Int>(1, 2, 3), values<Int>(2, 2, 1)),
                 values<stdlib::CmpResult>(stdlib::CmpResult::LT, stdlib::CmpResult::EQ, stdlib::CmpResult::GT));
}

TEST_CASE("std operators: min_ and max_ support binary scalar operands")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::min_>(values<Int>(3, 1), values<Int>(2, 5)), values<Int>(2, 1));
    CHECK_OUTPUT(eval_node<stdlib::max_>(values<Int>(3, 1), values<Float>(2.5, 5.5)), values<Float>(3.0, 5.5));
    CHECK_OUTPUT(eval_node<stdlib::min_>(values<Str>(Str{"b"}, Str{"a"}), values<Str>(Str{"a"}, Str{"c"})),
                 values<Str>(Str{"a"}, Str{"a"}));
    CHECK_OUTPUT(eval_node<stdlib::max_>(values<Date>(ymd(2020, 1, 1), ymd(2020, 1, 10)),
                                         values<Date>(ymd(2020, 1, 3), ymd(2020, 1, 5))),
                 values<Date>(ymd(2020, 1, 3), ymd(2020, 1, 10)));
}

TEST_CASE("std operators: eq_ works for strings")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::eq_>(values<Str>(Str{"x"}, Str{"y"}), values<Str>(Str{"x"}, Str{"z"})),
                 values<Bool>(true, false));
}

TEST_CASE("std operators: zero_ emits the op-aware zero for standard scalar outputs")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::zero_, TS<Int>>(fn<stdlib::add_>())), values<Int>(0));
    CHECK_OUTPUT((eval_node<stdlib::zero_, TS<Int>>(fn<stdlib::mul_>())), values<Int>(1));
    CHECK_OUTPUT((eval_node<stdlib::zero_, TS<Int>>(fn<stdlib::min_>())),
                 values<Int>(std::numeric_limits<Int>::max()));
    CHECK_OUTPUT((eval_node<stdlib::zero_, TS<Float>>(fn<stdlib::add_>())), values<Float>(Float{0}));
    CHECK_OUTPUT((eval_node<stdlib::zero_, TS<Float>>(fn<stdlib::max_>())),
                 values<Float>(-std::numeric_limits<Float>::infinity()));
    CHECK_OUTPUT((eval_node<stdlib::zero_, TS<Str>>(fn<stdlib::add_>())), values<Str>(Str{}));
}

TEST_CASE("std operators: default_ substitutes the default until ts first ticks")
{
    stdlib::register_standard_operators();

    // ts invalid for two cycles: the default (9) holds, then ts takes over.
    CHECK_OUTPUT(eval_node<stdlib::default_>(values<Int>(none, none, 3, 4), values<Int>(9)),
                 values<Int>(9, none, 3, 4));

    // ts valid from the first cycle: the default never shows.
    CHECK_OUTPUT(eval_node<stdlib::default_>(values<Int>(1, 2), values<Int>(9)), values<Int>(1, 2));
}

TEST_CASE("std operators: syntax sugar wires arithmetic expressions through standard overloads")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<SyntaxArithmeticGraph>(values<Int>(1, 2, 3), values<Int>(10, 20, 30)),
                 values<Int>(21, 42, 63));
}

TEST_CASE("std operators: syntax sugar composes comparisons and logical operators")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<SyntaxComparisonGraph>(values<Int>(1, 0, 5), values<Float>(2.0, -1.0, 4.0)),
                 values<Bool>(true, false, true));
}

TEST_CASE("std operators: syntax helpers cover non-overloadable arithmetic operators")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<SyntaxNamedHelperGraph>(values<Int>(-2, 3)), values<Float>(2.0, 4.5));
}

TEST_CASE("std operators: syntax port cast validates the resolved runtime schema")
{
    stdlib::register_standard_operators();
    REQUIRE_THROWS_AS(eval_node<SyntaxBadCastGraph>(values<Int>(7), values<Int>(2)), std::logic_error);
}

TEST_CASE("std operators: floordiv_ and mod_ use floor semantics")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::floordiv_>(values<Int>(7, -7), values<Int>(3, 3)), values<Int>(2, -3));
    CHECK_OUTPUT(eval_node<stdlib::mod_>(values<Int>(7, -7), values<Int>(3, 3)), values<Int>(1, 2));
    CHECK_OUTPUT(eval_node<stdlib::floordiv_>(values<Float>(7.5, -7.5), values<Int>(2, 2)), values<Float>(3.0, -4.0));
}

TEST_CASE("std operators: divmod_ returns quotient and remainder as a two-element list")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::divmod_>(values<Int>(5, -7), values<Int>(2, 3)),
                 values<Value>(list_delta<TS<Int>>({{0, 2}, {1, 1}}),
                               list_delta<TS<Int>>({{0, -3}, {1, 2}})));
    CHECK_OUTPUT(eval_node<stdlib::divmod_>(values<Float>(5.0), values<Int>(2)),
                 values<Value>(list_delta<TS<Float>>({{0, 2.0}, {1, 1.0}})));
    CHECK_OUTPUT(eval_node<stdlib::divmod_>(values<Int>(5), values<Float>(2.0)),
                 values<Value>(list_delta<TS<Float>>({{0, 2.0}, {1, 1.0}})));
}

TEST_CASE("std operators: pow_ is Float-valued for numeric operands")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::pow_>(values<Int>(2, 9), values<Int>(3, 2)), values<Float>(8.0, 81.0));
    CHECK_OUTPUT(eval_node<stdlib::pow_>(values<Float>(4.0, 9.0), values<Float>(0.5, 0.5)), values<Float>(2.0, 3.0));
}

TEST_CASE("std operators: unary numeric operators support neg pos abs sign and ln")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::neg_>(values<Int>(1, -2)), values<Int>(-1, 2));
    CHECK_OUTPUT(eval_node<stdlib::pos_>(values<Float>(-1.5, 2.5)), values<Float>(-1.5, 2.5));
    CHECK_OUTPUT(eval_node<stdlib::abs_>(values<Int>(-3, 4)), values<Int>(3, 4));
    CHECK_OUTPUT(eval_node<stdlib::sign>(values<Int>(-3, 0, 4)), values<Int>(-1, 0, 1));
    CHECK_OUTPUT(eval_node<stdlib::ln>(values<Float>(1.0, std::numbers::e)), values<Float>(0.0, 1.0));
}

TEST_CASE("std operators: logical and bitwise operators support standard scalars")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::not_>(values<Bool>(true, false)), values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<stdlib::and_>(values<Int>(1, 0), values<Int>(2, 3)), values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<stdlib::or_>(values<Str>(Str{}, Str{"x"}), values<Str>(Str{}, Str{})),
                 values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<stdlib::bit_and>(values<Int>(6, 5), values<Int>(3, 1)), values<Int>(2, 1));
    CHECK_OUTPUT(eval_node<stdlib::bit_or>(values<Bool>(true, false), values<Bool>(false, false)),
                 values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<stdlib::invert_>(values<Int>(0, 1)), values<Int>(~Int{0}, ~Int{1}));
    CHECK_OUTPUT(eval_node<stdlib::lshift_>(values<Int>(1, 2), values<Int>(3, 2)), values<Int>(8, 8));
    CHECK_OUTPUT(eval_node<stdlib::rshift_>(values<Int>(8, 9), values<Int>(1, 2)), values<Int>(4, 2));
}

TEST_CASE("std operators: string operators support replace substr and container basics")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::match_>(values<Str>(Str{"a"}), values<Str>(Str{"a"})),
                 values<Value>(tsb_delta<stdlib::MatchResult>(Bool{true}, std::nullopt)));
    CHECK_OUTPUT(eval_node<stdlib::match_>(values<Str>(Str{"(a)"}), values<Str>(Str{"aa"})),
                 values<Value>(tsb_delta<stdlib::MatchResult>(Bool{true},
                                                              list_delta<TS<Str>>({{0, Str{"a"}}}))));
    CHECK_OUTPUT(eval_node<stdlib::match_>(values<Str>(Str{"a"}), values<Str>(Str{"b"})),
                 values<Value>(tsb_delta<stdlib::MatchResult>(Bool{false}, std::nullopt)));
    CHECK_OUTPUT(eval_node<stdlib::replace>(values<Str>(Str{"a"}, Str{"^a"}),
                                            values<Str>(Str{"z"}, Str{"z"}),
                                            values<Str>(Str{"abcabcabc"}, Str{"abcabcabc"})),
                 values<Str>(Str{"zbczbczbc"}, Str{"zbcabcabc"}));
    CHECK_OUTPUT(eval_node<stdlib::substr>(values<Str>(Str{"abcdef"}, Str{"abcdef"}, Str{"abcdef"}),
                                           values<Int>(0, 2, 1),
                                           values<Int>(3, 4, 5)),
                 values<Str>(Str{"abc"}, Str{"cd"}, Str{"bcde"}));
    CHECK_OUTPUT(eval_node<stdlib::contains_>(values<Str>(Str{"abc"}, none, Str{}),
                                              values<Str>(Str{"z"}, Str{"bc"}, Str{})),
                 values<Bool>(false, true, true));
    CHECK_OUTPUT(eval_node<stdlib::len_>(values<Str>(Str{}, Str{"abc"})), values<Int>(0, 3));
    CHECK_OUTPUT(eval_node<stdlib::is_empty>(values<Str>(Str{}, Str{"abc"})), values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<stdlib::getitem_>(values<Str>(Str{"abc"}, Str{"abc"}), values<Int>(1, -1)),
                 values<Str>(Str{"b"}, Str{"c"}));

    CHECK_OUTPUT(eval_node<SplitToPairGraph>(values<Str>(Str{"a,b,c"})),
                 values<Value>(list_delta<TS<Str>>({{0, Str{"a"}}, {1, Str{"b,c"}}})));

    WiringArg split_source;
    split_source.kind        = WiringArg::Kind::TimeSeries;
    split_source.port.schema = ts_type<TS<Str>>();

    WiringArg split_separator;
    split_separator.kind         = WiringArg::Kind::Scalar;
    split_separator.scalar_value = Value{Str{","}};
    split_separator.scalar_meta  = split_separator.scalar_value.schema();

    std::array<WiringArg, 2> unresolved_split_args{split_source, split_separator};
    CHECK_THROWS_AS(OperatorRegistry::instance().resolve(
                        "split", std::span<const WiringArg>{unresolved_split_args.data(), unresolved_split_args.size()},
                        true),
                    OperatorResolutionError);

    ResolvedOperatorCall resolved_split = OperatorRegistry::instance().resolve(
        "split", std::span<const WiringArg>{unresolved_split_args.data(), unresolved_split_args.size()}, true,
        ts_type<TSL<TS<Str>, 2>>());
    REQUIRE(resolved_split.map.find_size("N").has_value());
    CHECK(*resolved_split.map.find_size("N") == 2);
    CHECK(ts_pattern_resolve(resolved_split.impl->output, resolved_split.map) == ts_type<TSL<TS<Str>, 2>>());

    CHECK_OUTPUT(eval_node<JoinDefaultGraph>(values<Value>(list_delta<TS<Str>>({{0, Str{"a"}}, {2, Str{"c"}}}),
                                                           list_delta<TS<Str>>({{1, Str{"b"}}}))),
                 values<Str>(Str{"a,c"}, Str{"a,b,c"}));
    CHECK_OUTPUT(eval_node<JoinStrictGraph>(values<Value>(list_delta<TS<Str>>({{0, Str{"a"}}, {2, Str{"c"}}}),
                                                          list_delta<TS<Str>>({{1, Str{"b"}}}))),
                 values<Str>(none, Str{"a,b,c"}));

    CHECK_OUTPUT(eval_node<FormatArgsGraph>(values<Str>(Str{"{} is a test {}"}, none),
                                            values<Int>(1, 2),
                                            values<Str>(Str{"a"}, Str{"b"})),
                 values<Str>(Str{"1 is a test a"}, Str{"2 is a test b"}));
    CHECK_OUTPUT(eval_node<FormatNoArgsGraph>(values<Str>(Str{"plain"}, Str{"escaped {{brace}}"})),
                 values<Str>(Str{"plain"}, Str{"escaped {brace}"}));
    CHECK_OUTPUT(eval_node<FormatKwargsGraph>(values<Str>(Str{"{ts1} is a test {ts2}"}, none),
                                              values<Int>(1, 2),
                                              values<Str>(Str{"a"}, Str{"b"})),
                 values<Str>(Str{"1 is a test a"}, Str{"2 is a test b"}));
    CHECK_OUTPUT(eval_node<FormatMixedGraph>(values<Str>(Str{"{ts1} is a test {ts2}"}, none),
                                             values<Float>(1.1, 1.2),
                                             values<Int>(1, 2),
                                             values<Str>(Str{"a"}, Str{"b"})),
                 values<Str>(Str{"1 is a test a"}, Str{"2 is a test b"}));
    CHECK_OUTPUT(eval_node<FormatSampledGraph>(values<Str>(Str{"{} is a test {}"}, none, none, none),
                                               values<Int>(1, 2, 3, 4),
                                               values<Str>(Str{"a"}, Str{"b"}, Str{"c"}, Str{"d"})),
                 values<Str>(none, none, Str{"3 is a test c"}, none));
}

TEST_CASE("std operators: collection container operators support TSS TSD and fixed TSL")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::len_, TSS<Int>>(values<Value>(set_delta<Int>({}, {}),
                                                                  set_delta<Int>({1}, {}),
                                                                  set_delta<Int>({2, 3}, {}),
                                                                  set_delta<Int>({}, {1})))),
                 values<Int>(0, 1, 3, 2));
    CHECK_OUTPUT((eval_node<stdlib::is_empty, TSS<Int>>(values<Value>(none,
                                                                      set_delta<Int>({1}, {}),
                                                                      set_delta<Int>({2}, {}),
                                                                      set_delta<Int>({}, {1}),
                                                                      set_delta<Int>({}, {2})))),
                 values<Bool>(true, false, none, none, true));
    CHECK_OUTPUT((eval_node<stdlib::contains_, TSS<Int>>(values<Value>(set_delta<Int>({1, 2, 3}, {})),
                                                         values<Int>(1, 4))),
                 values<Bool>(true, false));
    CHECK_OUTPUT((eval_node<stdlib::contains_, TSS<Int>, TSS<Int>>(
                     values<Value>(set_delta<Int>({1, 2, 3}, {})),
                     values<Value>(set_delta<Int>({1, 2}, {}), set_delta<Int>({4}, {1, 2})))),
                 values<Bool>(true, false));

    CHECK_OUTPUT((eval_node<stdlib::len_, TSD<Int, TS<Int>>>(values<Value>(dict_delta<Int, TS<Int>>({}),
                                                                           dict_delta<Int, TS<Int>>({{0, 1}}),
                                                                           dict_delta<Int, TS<Int>>({}, {0})))),
                 values<Int>(0, 1, 0));
    CHECK_OUTPUT((eval_node<stdlib::is_empty, TSD<Int, TS<Int>>>(values<Value>(none,
                                                                               dict_delta<Int, TS<Int>>({{1, 1}}),
                                                                               dict_delta<Int, TS<Int>>({{2, 2}}),
                                                                               dict_delta<Int, TS<Int>>({}, {1}),
                                                                               dict_delta<Int, TS<Int>>({}, {2})))),
                 values<Bool>(true, false, none, none, true));
    CHECK_OUTPUT((eval_node<stdlib::contains_, TSD<Int, TS<Int>>>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 10}, {2, 20}})), values<Int>(1, 3))),
                 values<Bool>(true, false));

    CHECK_OUTPUT((eval_node<stdlib::len_, TSL<TS<Int>, 2>>(values<Value>(list_delta<TS<Int>>({}),
                                                                         list_delta<TS<Int>>({{0, 1}}),
                                                                         list_delta<TS<Int>>({{1, 2}})))),
                 values<Int>(2, none, none));
    CHECK_OUTPUT((eval_node<stdlib::len_, TSL<TS<Int>, 4>>(values<Value>(list_delta<TS<Int>>({1, 2, 3, 4})))),
                 values<Int>(4));
    CHECK_OUTPUT((eval_node<stdlib::getitem_, TSL<TS<Int>, 2>>(values<Value>(list_delta<TS<Int>>({1, 2}),
                                                                             list_delta<TS<Int>>({2, 3}),
                                                                             list_delta<TS<Int>>({4, 5})),
                                                               values<Int>(0))),
                 values<Int>(1, 2, 4));
    CHECK_OUTPUT((eval_node<stdlib::index_of, TSL<TS<Int>, 3>>(
                     values<Value>(list_delta<TS<Int>>({1, 2, 3}),
                                   none,
                                   list_delta<TS<Int>>({2, 3, 4}),
                                   list_delta<TS<Int>>({-1, 0, 1})),
                     values<Int>(2, 1))),
                 values<Int>(1, 0, -1, 2));
}

TEST_CASE("std operators: str_ converts scalar time-series values to strings")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::str_>(values<Int>(3, -2)), values<Str>(Str{"3"}, Str{"-2"}));
    CHECK_OUTPUT(eval_node<stdlib::str_>(values<Bool>(true, false)), values<Str>(Str{"true"}, Str{"false"}));
}

TEST_CASE("std operators: stream operators cover sampling filtering slicing and scalar analytics")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::sample>(values<Bool>(none, true, none, true),
                                           values<Int>(1, 2, 3, 4, 5)),
                 values<Int>(none, 2, none, 4, none));

    CHECK_OUTPUT(eval_node<stdlib::filter_>(values<Bool>(true, false, false, true, true, none),
                                            values<Int>(1, 2, 3, none, none, 4)),
                 values<Int>(1, none, none, 3, none, 4));
    CHECK_OUTPUT((eval_node<stdlib::filter_, TSS<Int>>(
                     values<Bool>(true, false, none, true),
                     values<Value>(set_delta<Int>({1}, {}),
                                   set_delta<Int>({2}, {}),
                                   set_delta<Int>({}, {1}),
                                   set_delta<Int>({3}, {})))),
                 values<Value>(set_delta<Int>({1}, {}),
                               none,
                               none,
                               set_delta<Int>({2, 3}, {1})));
    CHECK_OUTPUT((eval_node<stdlib::filter_, TSD<Int, TS<Int>>>(
                     values<Bool>(true, false, none, true),
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 1}}),
                                   dict_delta<Int, TS<Int>>({{1, 2}, {2, 2}}),
                                   dict_delta<Int, TS<Int>>({}, {1}),
                                   dict_delta<Int, TS<Int>>({{3, 3}})))),
                 values<Value>(dict_delta<Int, TS<Int>>({{1, 1}}),
                               none,
                               none,
                               dict_delta<Int, TS<Int>>({{2, 2}, {3, 3}}, {1})));

    CHECK_OUTPUT(eval_node<stdlib::take>(values<Int>(1, 2, 3, 4, 5), Int{3}),
                 values<Int>(1, 2, 3, none, none));
    CHECK_OUTPUT(eval_node<stdlib::drop>(values<Int>(1, 2, 3, 4, 5), Int{3}),
                 values<Int>(none, none, none, 4, 5));
    CHECK_OUTPUT(eval_node<stdlib::step>(values<Int>(1, 2, 3, 4, 5, 6, 7, 8), Int{2}),
                 values<Int>(1, none, 3, none, 5, none, 7, none));
    CHECK_OUTPUT(eval_node<stdlib::slice_>(values<Int>(0, 1, 2, 3, 4, 5, 6, 7, 8), Int{2}, Int{-1}, Int{2}),
                 values<Int>(none, none, 2, none, 4, none, 6, none, 8));
    CHECK_OUTPUT(eval_node<stdlib::slice_>(values<Int>(0, 1, 2, 3), Int{-1}, Int{-1}, Int{2}),
                 values<Int>(none, none, none, none));

    CHECK_OUTPUT(eval_node<stdlib::count>(values<Int>(3, none, 2, 1)), values<Int>(1, none, 2, 3));
    CHECK_OUTPUT(eval_node<stdlib::dedup>(values<Int>(1, 2, 2, 3, 3, 3, 4)),
                 values<Int>(1, 2, none, 3, none, none, 4));
    CHECK_OUTPUT(eval_node<stdlib::diff>(values<Int>(1, 2, 4, 7)), values<Int>(none, 1, 2, 3));
    CHECK_OUTPUT(eval_node<stdlib::diff>(values<Float>(1.0, 1.5, 3.0)), values<Float>(none, 0.5, 1.5));
    CHECK_OUTPUT(eval_node<stdlib::clip>(values<Float>(-1.0, 0.5, 2.0), Float{0.0}, Float{1.0}),
                 values<Float>(0.0, 0.5, 1.0));
    CHECK_OUTPUT(eval_node<stdlib::ewma>(values<Float>(1.0, 2.0, 3.0, 4.0), Float{0.5}),
                 values<Float>(1.0, 1.5, 2.25, 3.125));
}

TEST_CASE("std operators: control operators cover variadic booleans merge and selection")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::all_>(values<Bool>(true, true, true),
                                         values<Bool>(true, false, true),
                                         values<Bool>(true, true, none)),
                 values<Bool>(true, false, true));
    CHECK_OUTPUT(eval_node<stdlib::any_>(values<Bool>(false, false, none),
                                         values<Bool>(false, true, false),
                                         values<Bool>(false, false, none)),
                 values<Bool>(false, true, false));
    CHECK_OUTPUT((eval_node<stdlib::all_, TSD<Int, TS<Bool>>>(
                     values<Value>(dict_delta<Int, TS<Bool>>({}),
                                   dict_delta<Int, TS<Bool>>({{1, false}}),
                                   dict_delta<Int, TS<Bool>>({{2, true}}),
                                   dict_delta<Int, TS<Bool>>({{1, true}}),
                                   dict_delta<Int, TS<Bool>>({{2, false}}),
                                   dict_delta<Int, TS<Bool>>({}, {2}),
                                   dict_delta<Int, TS<Bool>>({}, {1})))),
                 values<Bool>(true, false, false, true, false, true, true));
    CHECK_OUTPUT((eval_node<stdlib::any_, TSD<Int, TS<Bool>>>(
                     values<Value>(dict_delta<Int, TS<Bool>>({}),
                                   dict_delta<Int, TS<Bool>>({{1, false}}),
                                   dict_delta<Int, TS<Bool>>({{2, false}}),
                                   dict_delta<Int, TS<Bool>>({{1, true}}),
                                   dict_delta<Int, TS<Bool>>({{2, true}}),
                                   dict_delta<Int, TS<Bool>>({{2, false}}),
                                   dict_delta<Int, TS<Bool>>({}, {1})))),
                 values<Bool>(false, false, false, true, true, true, false));

    CHECK_OUTPUT(eval_node<stdlib::merge>(values<Int>(none, 2, none, none, 6),
                                          values<Int>(1, none, 4, none, none),
                                          values<Int>(none, 3, 5, none, none)),
                 values<Int>(1, 2, 4, none, 6));

    CHECK_OUTPUT(eval_node<stdlib::if_true>(values<Bool>(true, false, true)),
                 values<Bool>(true, none, true));
    CHECK_OUTPUT(eval_node<stdlib::if_true>(values<Bool>(true, false, true), Bool{true}),
                 values<Bool>(true, none, none));
    CHECK_OUTPUT(eval_node<stdlib::if_then_else>(values<Bool>(true, false, true),
                                                 values<Int>(1, 2, 3),
                                                 values<Int>(4, 5, 6)),
                 values<Int>(1, 5, 3));
    CHECK_OUTPUT(eval_node<stdlib::if_cmp>(values<stdlib::CmpResult>(stdlib::CmpResult::LT,
                                                                     stdlib::CmpResult::EQ,
                                                                     stdlib::CmpResult::GT),
                                           values<Int>(1, 2, 3),
                                           values<Int>(10, 20, 30),
                                           values<Int>(100, 200, 300)),
                 values<Int>(1, 20, 300));
}

TEST_CASE("std operators: date component operators extract day month year and explode")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::day_of_month>(values<Date>(ymd(2020, 1, 3), ymd(2021, 12, 31))),
                 values<Int>(3, 31));
    CHECK_OUTPUT(eval_node<stdlib::month_of_year>(values<Date>(ymd(2020, 1, 3), ymd(2021, 12, 31))),
                 values<Int>(1, 12));
    CHECK_OUTPUT(eval_node<stdlib::year>(values<Date>(ymd(2020, 1, 3), ymd(2021, 12, 31))), values<Int>(2020, 2021));
    CHECK_OUTPUT(eval_node<stdlib::explode>(values<Date>(ymd(2024, 1, 1), ymd(2024, 1, 2),
                                                          ymd(2024, 2, 2), ymd(2025, 2, 2))),
                 values<Value>(list_delta<TS<Int>>({{0, 2024}, {1, 1}, {2, 1}}),
                               list_delta<TS<Int>>({{2, 2}}),
                               list_delta<TS<Int>>({{1, 2}}),
                               list_delta<TS<Int>>({{0, 2025}})));
}

TEST_CASE("std operators: time-series property operators report valid modified and last-modified")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::valid>(values<Int>(none, 1)), values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<stdlib::modified>(values<Int>(none, 1, none, none, 2, none)),
                 values<Bool>(false, true, false, none, true, false));
    CHECK_OUTPUT(eval_node<stdlib::last_modified_time>(values<Int>(1, none, 2)),
                 values<DateTime>(MIN_ST, none, MIN_ST + 2 * MIN_TD));

    const Date start_date{std::chrono::floor<std::chrono::days>(MIN_ST)};
    CHECK_OUTPUT(eval_node<stdlib::last_modified_date>(values<Int>(1, none, 2)),
                 values<Date>(start_date, none, start_date));

    const std::vector<std::optional<Value>> wall_clock =
        eval_node<stdlib::last_modified_wall_clock_time>(values<Int>(1, none, 2));
    REQUIRE(wall_clock.size() == 3);
    REQUIRE(wall_clock[0].has_value());
    CHECK(wall_clock[0]->view().checked_as<DateTime>() != MIN_DT);
    CHECK_FALSE(wall_clock[1].has_value());
    REQUIRE(wall_clock[2].has_value());
    CHECK(wall_clock[2]->view().checked_as<DateTime>() != MIN_DT);
}

TEST_CASE("std operators: an operand combination with no registered implementation raises")
{
    stdlib::register_standard_operators();   // bool arithmetic is deliberately not registered
    REQUIRE_THROWS_AS(eval_node<stdlib::add_>(values<Bool>(true), values<Bool>(false)), OperatorResolutionError);
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

    CHECK_OUTPUT(eval_node<stdlib::floordiv_>(values<Int>(5, 5, 5), values<Int>(2, 0, 4), DBZ::NoTick),
                 values<Int>(2, none, 1));
    CHECK_OUTPUT(eval_node<stdlib::mod_>(values<Int>(5, 5, 5), values<Int>(2, 0, 4), DBZ::NoTick),
                 values<Int>(1, none, 1));
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

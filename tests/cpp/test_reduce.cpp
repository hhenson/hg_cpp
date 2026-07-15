// The ``reduce`` higher-order OPERATOR (lib/std/operators/higher_order.h).
//
// reduce is an Operator marker whose default overload (impl/higher_order_impl.h)
// covers a fixed-size TSL by laying the reduction out statically at wiring time
// (linear chain for small sizes, balanced binary tree with odd-element carry —
// mirroring Python _reduce_tsl). The combiner is a WiredFn scalar (fn<X>()), so
// user specialisations register as ordinary overloads and are selected by the
// standard best-match machinery — including requires_ gating on the function's
// identity, mirroring ext/main's map_ overload test.
//
// Tests run through the eval_node operator harness; the TSL input is the usual
// canonical-delta values<Value>(...) sequence, with its time-series schema
// supplied as an explicit template argument (the same convention as
// eval_node<const_, TSL<...>>(...)).

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/wired_fn.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <limits>
#include <string>
#include <string_view>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct ReduceLiftedAddNoIdentity
    {
        static constexpr const char *name = "reduce_lifted_add_no_identity";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};
        static constexpr bool associative = true;
        static constexpr bool commutative = true;

        [[nodiscard]] static Int apply(Int lhs, Int rhs) { return lhs + rhs; }
    };

    // Sub-graph combiner: flattens through wire<G> at every reduction node.
    struct SumCombiner
    {
        static constexpr auto name = "sum_combiner";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            using namespace hgraph::stdlib::syntax;
            return (lhs + rhs).as<TS<Int>>();
        }
    };

    // reduce over a sub-graph boundary TSL: the boundary elements are projected
    // by the operator's wiring exactly like peered/structural ones. This is the
    // one scenario the eval_node harness cannot express (a nested boundary).
    struct BoundaryReduceSubGraph
    {
        static constexpr auto name = "boundary_reduce_subgraph";
        static Port<TS<Int>>  compose(Wiring &w, Port<TSL<TS<Int>, 2>> in)
        {
            return wire<stdlib::reduce_>(w, fn<stdlib::add_>(), in).as<TS<Int>>();
        }
    };

    struct NestedBoundaryReduceGraph
    {
        static constexpr auto name = "nested_boundary_reduce_graph";
        static void           compose(Wiring &w)
        {
            auto a = wire<replay, TS<Int>>(w, Str{"a"});
            auto b = wire<replay, TS<Int>>(w, Str{"b"});
            wire<record>(w, nested_<BoundaryReduceSubGraph>(w, {a, b}), Str{"out"});
        }
    };

    struct VariadicRawReduce : Operator<"test_variadic_raw_reduce", VarIn<"ts", TS<Int>>, Out<TS<Int>>>
    {
    };

    struct VariadicRawReduceImpl
    {
        static constexpr auto name = "variadic_raw_reduce_impl";

        static Port<TS<Int>> compose(Wiring &w, VarIn<"ts", TS<Int>> ts)
        {
            return wire<stdlib::reduce_>(w, fn<SumCombiner>(), ts).as<TS<Int>>();
        }
    };

    // ---- user specialisation, mirroring ext/main's test_map_overload ----

    // A combiner used purely as a selection marker for the specialised overload.
    struct FirstCombiner
    {
        static constexpr auto name = "first_combiner";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> lhs, Port<TS<Int>>) { return lhs; }
    };

    struct ConstLeftReduceDict
    {
        static constexpr auto          name = "const_left_reduce_dict";
        static Port<TSD<Str, TS<Int>>> compose(Wiring &w)
        {
            return wire<stdlib::const_, TSD<Str, TS<Int>>>(
                w, stdlib::make_map<Str, Int>({{Str{"a"}, Int{1}}, {Str{"b"}, Int{2}}}));
        }
    };

    struct ConstRightReduceDict
    {
        static constexpr auto          name = "const_right_reduce_dict";
        static Port<TSD<Str, TS<Int>>> compose(Wiring &w)
        {
            return wire<stdlib::const_, TSD<Str, TS<Int>>>(
                w, stdlib::make_map<Str, Int>({{Str{"a"}, Int{10}}, {Str{"b"}, Int{20}}}));
        }
    };

    struct NeverReduceDictNode
    {
        static constexpr auto name = "never_reduce_dict";
        static void eval(Out<TSD<Str, TS<Int>>>) {}
    };

    struct NoReduceDict
    {
        static constexpr auto          name = "no_reduce_dict";
        static Port<TSD<Str, TS<Int>>> compose(Wiring &w) { return wire<NeverReduceDictNode>(w); }
    };

    struct ReduceSwitchedDictGraph
    {
        static constexpr auto name = "reduce_switched_dict_graph";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Str>> select)
        {
            auto source = wire<stdlib::switch_>(
                              w, select,
                              stdlib::switch_cases({{Value{Str{"left"}}, fn<ConstLeftReduceDict>()},
                                                     {Value{Str{"right"}}, fn<ConstRightReduceDict>()},
                                                     {Value{Str{"none"}}, fn<NoReduceDict>()}}))
                              .as<TSD<Str, TS<Int>>>();
            return wire<stdlib::reduce_>(w, fn<stdlib::add_>(), source, Int{100}).as<TS<Int>>();
        }
    };

    struct ReduceLiveZeroGraph
    {
        static constexpr auto name = "reduce_live_zero_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TSD<Str, TS<Int>>> ts, Port<TS<Int>> zero)
        {
            return wire<stdlib::reduce_>(w, fn<stdlib::add_>(), ts, zero).as<TS<Int>>();
        }
    };

    using ReduceBundle = UnNamedTSB<Field<"a", TS<Int>>, Field<"b", TS<Int>>>;

    struct SumBundleCombiner
    {
        static constexpr auto name = "sum_bundle_combiner";

        static Port<ReduceBundle> compose(Wiring &w, Port<ReduceBundle> lhs, Port<ReduceBundle> rhs)
        {
            auto lhs_a = wire<stdlib::getitem_>(w, lhs, Str{"a"}).as<TS<Int>>();
            auto lhs_b = wire<stdlib::getitem_>(w, lhs, Str{"b"}).as<TS<Int>>();
            auto rhs_a = wire<stdlib::getitem_>(w, rhs, Str{"a"}).as<TS<Int>>();
            auto rhs_b = wire<stdlib::getitem_>(w, rhs, Str{"b"}).as<TS<Int>>();
            auto result = stdlib::to_tsb<ReduceBundle>(
                w, wire<stdlib::add_>(w, lhs_a, rhs_a), wire<stdlib::add_>(w, lhs_b, rhs_b));
            return wire<stdlib::pass_through_node>(w, result).as<ReduceBundle>();
        }
    };

    struct ReduceBundleGraph
    {
        static constexpr auto name = "reduce_bundle_graph";

        static Port<ReduceBundle> compose(Wiring &w, Port<TSD<Str, ReduceBundle>> ts)
        {
            auto zero = stdlib::to_tsb<ReduceBundle>(
                w, wire<stdlib::const_, TS<Int>>(w, Int{0}),
                wire<stdlib::const_, TS<Int>>(w, Int{0}));
            return wire<stdlib::reduce_>(w, fn<SumBundleCombiner>(), ts, zero).as<ReduceBundle>();
        }
    };

    // Specialised reduce overload: concrete TSL<TS<Int>, 2>, gated on the wired
    // function's identity — selected over the generic default by requires_.
    struct ReduceFirstTimes100
    {
        static constexpr auto name = "reduce_first_times_100";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const WiredFn *func = context.scalar_as<WiredFn>("func");
            return func != nullptr && *func == fn<FirstCombiner>();
        }

        static void eval(Scalar<"func", WiredFn>, In<"ts", TSL<TS<Int>, 2>> ts, Out<TS<Int>> out)
        {
            out.set(ts[0].value() * 100);
        }
    };

    struct LiftedReduceConstGraph
    {
        static constexpr auto name = "lifted_reduce_const_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            auto ts = wire<stdlib::const_, TSL<TS<Int>, 3>>(
                w, stdlib::make_list<Int>({Int{1}, Int{2}, Int{3}}));
            return wire<stdlib::reduce_>(w, lift<stdlib::scalar_add<Int>>(), ts).as<TS<Int>>();
        }
    };

    struct OperatorFnReduceConstGraph
    {
        static constexpr auto name = "operator_fn_reduce_const_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            auto ts = wire<stdlib::const_, TSL<TS<Int>, 3>>(
                w, stdlib::make_list<Int>({Int{1}, Int{2}, Int{3}}));
            return wire<stdlib::reduce_>(w, fn<stdlib::add_>(), ts).as<TS<Int>>();
        }
    };

    struct LiftedReduceExplicitIdentityConstGraph
    {
        static constexpr auto name = "lifted_reduce_explicit_identity_const_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            auto ts = wire<stdlib::const_, TSL<TS<Int>, 3>>(
                w, stdlib::make_list<Int>({Int{1}, Int{2}, Int{3}}));
            return wire<stdlib::reduce_>(w, lift<ReduceLiftedAddNoIdentity, Int{0}>(), ts).as<TS<Int>>();
        }
    };

    struct LiftedReduceMinConstGraph
    {
        static constexpr auto name = "lifted_reduce_min_const_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            auto ts = wire<stdlib::const_, TSL<TS<Int>, 4>>(
                w, stdlib::make_list<Int>({Int{9}, Int{4}, Int{7}, Int{2}}));
            return wire<stdlib::reduce_>(
                       w, lift<stdlib::scalar_min<Int>, std::numeric_limits<Int>::max()>(), ts)
                .as<TS<Int>>();
        }
    };

    struct OperatorFnMinReduceConstGraph
    {
        static constexpr auto name = "operator_fn_min_reduce_const_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            auto ts = wire<stdlib::const_, TSL<TS<Int>, 4>>(
                w, stdlib::make_list<Int>({Int{9}, Int{4}, Int{7}, Int{2}}));
            return wire<stdlib::reduce_>(w, fn<stdlib::min_>(), ts).as<TS<Int>>();
        }
    };

    struct OrderedSubtractTslGraph
    {
        static constexpr auto name = "ordered_subtract_tsl_graph";

        static Port<TS<Int>> compose(
            Wiring &w,
            Port<TSL<TS<Int>, 4>> values,
            Port<TS<Int>> zero)
        {
            return wire<stdlib::reduce_>(
                       w, fn<stdlib::sub_>(), values, zero, Bool{false})
                .as<TS<Int>>();
        }
    };

    struct AppendIntCombiner
    {
        static constexpr auto name = "append_int_combiner";

        static void eval(In<"lhs", TS<Str>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Str>> out)
        {
            out.set(lhs.value() + ", " + std::to_string(rhs.value()));
        }
    };

    struct OrderedAppendTsdGraph
    {
        static constexpr auto name = "ordered_append_tsd_graph";

        static Port<TS<Str>> compose(
            Wiring &w,
            Port<TSD<Int, TS<Int>>> values,
            Port<TS<Str>> zero)
        {
            return wire<stdlib::reduce_>(
                       w, fn<AppendIntCombiner>(), values, zero, Bool{false})
                .as<TS<Str>>();
        }
    };
}  // namespace

TEST_CASE("reduce: a five-element TSL reduces through a binary tree with carry")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSL<TS<Int>, 5>>(
                     fn<stdlib::add_>(),
                     values<Value>(list_delta<TS<Int>>({1, 2, 3, 4, 5}), list_delta<TS<Int>>({{0, 10}})))),
                 values<Int>(15, 24));
}

TEST_CASE("reduce: a lifted scalar add reduces a fixed TSL in one specialised node")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSL<TS<Int>, 5>>(
                     lift<stdlib::scalar_add<Int>>(),
                     values<Value>(list_delta<TS<Int>>({1, 2, 3, 4, 5}),
                                   list_delta<TS<Int>>({{0, 10}})))),
                 values<Int>(15, 24));

    GraphBuilder gb = build_graph<LiftedReduceConstGraph>();
    CHECK(gb.node_count() == 2);   // const source + lifted reduce node

    GraphBuilder operator_fn_gb = build_graph<OperatorFnReduceConstGraph>();
    CHECK(operator_fn_gb.node_count() == 2);   // const source + lifted reduce node resolved through fn<add_>
}

TEST_CASE("reduce: a lifted scalar function can take its identity from lift")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSL<TS<Int>, 5>>(
                     lift<ReduceLiftedAddNoIdentity, Int{0}>(),
                     values<Value>(list_delta<TS<Int>>({1, 2, 3, 4, 5}),
                                   list_delta<TS<Int>>({{0, 10}})))),
                 values<Int>(15, 24));

    GraphBuilder gb = build_graph<LiftedReduceExplicitIdentityConstGraph>();
    CHECK(gb.node_count() == 2);   // const source + lifted reduce node
}

TEST_CASE("reduce: lifted standard kernels use built-in and explicit identities")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSL<TS<Int>, 5>>(
                     lift<stdlib::scalar_mul<Int>>(),
                     values<Value>(list_delta<TS<Int>>({1, 2, 3, 4, 5}),
                                   list_delta<TS<Int>>({{0, 2}})))),
                 values<Int>(120, 240));

    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSL<TS<Int>, 3>>(
                     lift<stdlib::scalar_min<Int>, std::numeric_limits<Int>::max()>(),
                     values<Value>(list_delta<TS<Int>>({5, 2, 9}),
                                   list_delta<TS<Int>>({{1, 7}})))),
                 values<Int>(2, 5));

    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSL<TS<Int>, 3>>(
                     fn<stdlib::min_>(),
                     values<Value>(list_delta<TS<Int>>({5, 2, 9}),
                                   list_delta<TS<Int>>({{1, 7}})))),
                 values<Int>(2, 5));

    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSL<TS<Int>, 3>>(
                     lift<stdlib::scalar_max<Int>, std::numeric_limits<Int>::lowest()>(),
                     values<Value>(list_delta<TS<Int>>({5, 2, 9}),
                                   list_delta<TS<Int>>({{1, 10}})))),
                 values<Int>(9, 10));

    GraphBuilder gb = build_graph<LiftedReduceMinConstGraph>();
    CHECK(gb.node_count() == 2);   // const source + lifted reduce node

    GraphBuilder operator_min_gb = build_graph<OperatorFnMinReduceConstGraph>();
    CHECK(operator_min_gb.node_count() == 2);   // const source + lifted reduce node via fn<min_>
}

TEST_CASE("reduce: not-yet-valid elements count as the operation's zero")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // Python parity: every leaf is default(ts[i], zero), so a partially-valid
    // TSL reduces immediately with zeros for the absent elements.
    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSL<TS<Int>, 5>>(
                     fn<stdlib::add_>(),
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}}),              // partial: 1+2+0+0+0
                                   list_delta<TS<Int>>({{2, 3}, {3, 4}, {4, 5}})))),   // completes: full sum
                 values<Int>(3, 15));
}

TEST_CASE("reduce: small sizes lay out as a linear chain")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSL<TS<Int>, 3>>(
                     fn<stdlib::add_>(),
                     values<Value>(list_delta<TS<Int>>({1, 2, 3}), list_delta<TS<Int>>({10, 20, 30})))),
                 values<Int>(6, 60));
}

TEST_CASE("reduce: a single-element TSL reduces to the element itself")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSL<TS<Int>, 1>>(
                     fn<stdlib::add_>(),
                     values<Value>(list_delta<TS<Int>>({7}), list_delta<TS<Int>>({9})))),
                 values<Int>(7, 9));
}

TEST_CASE("reduce: the combiner may be a sub-graph, with an explicit zero value")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // A custom combiner has no registered zero overload (Python KeyError
    // parity), so the explicit-zero arity supplies it: reduce(func, ts, 0).
    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSL<TS<Int>, 4>>(
                     fn<SumCombiner>(),
                     values<Value>(list_delta<TS<Int>>({1, 2, 3, 4}), list_delta<TS<Int>>({{3, 40}})),
                     Int{0})),
                 values<Int>(10, 46));
}

TEST_CASE("reduce: a supplied zero value substitutes for not-yet-valid elements")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // Only element 0 ticks on the first cycle: the explicit zero (10) pads the
    // other three leaves (1 + 10 + 10 + 10), then the full sum once all tick.
    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSL<TS<Int>, 4>>(
                     fn<stdlib::add_>(),
                     values<Value>(list_delta<TS<Int>>({{0, 1}}),
                                   list_delta<TS<Int>>({{1, 2}, {2, 3}, {3, 4}})),
                     Int{10})),
                 values<Int>(31, 10));
}

TEST_CASE("reduce: works over a sub-graph boundary TSL via boundary element projection")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphBuilder gb = build_graph<NestedBoundaryReduceGraph>();
    set_replay_values<Int>(gb.global_state(), "a", values<Int>(1, 10));
    set_replay_values<Int>(gb.global_state(), "b", values<Int>(2, 20));
    auto ex = run_graph(std::move(gb), MIN_ST, MIN_ST + TimeDelta{10});

    CHECK_OUTPUT(get_recorded_values<Int>(ex.view().graph().global_state(), "out"), values<Int>(3, 30));
}

TEST_CASE("reduce: a VarIn tail without zero folds raw inputs")
{
    using namespace hgraph;
    stdlib::register_standard_operators();
    register_graph_overload<VariadicRawReduce, VariadicRawReduceImpl>();

    // SumCombiner has no zero overload. Passing a VarIn tail to reduce_ must
    // therefore select the raw variadic reduction path. If it used ordinary
    // fixed-TSL reduce semantics, wiring would require a zero and fail.
    CHECK_OUTPUT(eval_node<VariadicRawReduce>(values<Int>(1, none, 4),
                                              values<Int>(2, 3, none),
                                              values<Int>(none, 5, 6)),
                 values<Int>(none, 9, 13));
}

TEST_CASE("reduce: a user overload gated on the wired function's identity wins selection")
{
    using namespace hgraph;
    stdlib::register_standard_operators();
    register_overload<stdlib::reduce_, ReduceFirstTimes100>();

    // fn<FirstCombiner>() selects the specialised node (ts[0] * 100); any other
    // function falls through to the generic TSL default (the sum).
    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSL<TS<Int>, 2>>(
                     fn<FirstCombiner>(), values<Value>(list_delta<TS<Int>>({3, 9})))),
                 values<Int>(300));
    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSL<TS<Int>, 2>>(
                     fn<stdlib::add_>(), values<Value>(list_delta<TS<Int>>({3, 9})))),
                 values<Int>(12));
}

TEST_CASE("reduce: a non-associative fixed TSL uses an ordered left fold")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // A balanced layout would produce (1 - 2) - (3 - 4) == 0. The explicit
    // ordered path is ((1 - 2) - 3) - 4 == -8. While positions are invalid,
    // their live zero/default input participates in the same left-to-right
    // layout and a later zero tick propagates through those default leaves.
    CHECK_OUTPUT(
        eval_node<OrderedSubtractTslGraph>(
            values<Value>(list_delta<TS<Int>>({{0, 1}}),
                          list_delta<TS<Int>>({{1, 2}, {2, 3}, {3, 4}}),
                          none,
                          list_delta<TS<Int>>({{0, 10}})),
            values<Int>(100, none, 7, none)),
        values<Int>(-299, -8, none, 1));
}

TEST_CASE("reduce: ordered TSD uses a live seed and contiguous integer order")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(
        eval_node<OrderedAppendTsdGraph>(
            values<Value>(dict_delta<Int, TS<Int>>({}),
                          dict_delta<Int, TS<Int>>({{0, 1}, {1, 2}}),
                          dict_delta<Int, TS<Int>>({{1, 3}}),
                          dict_delta<Int, TS<Int>>({{2, 4}}),
                          none,
                          dict_delta<Int, TS<Int>>({}, {2})),
            values<Str>(Str{"a"}, none, none, none, Str{"b"}, none)),
        values<Str>(Str{"a"}, Str{"a, 1, 2"}, Str{"a, 1, 3"},
                    Str{"a, 1, 3, 4"}, Str{"b, 1, 3, 4"}, Str{"b, 1, 3"}));
}

TEST_CASE("reduce: ordered TSD rejects holes in its integer key sequence")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    REQUIRE_THROWS((eval_node<OrderedAppendTsdGraph>(
        values<Value>(dict_delta<Int, TS<Int>>({{0, 1}, {2, 3}})),
        values<Str>(Str{"seed"}))));
}

// ---------------------------------------------------------------------------
// Dynamic TSD reduce (the runtime kernel — Nested Graphs > reduce over
// dynamic TSD): a balanced tree of combiner child graphs over the LIVE keys.
// Leaves alias source elements (no leaf child graphs); zero is the
// empty-collection result only — never odd-branch padding.
// ---------------------------------------------------------------------------

TEST_CASE("reduce over TSD: sums the live values, follows updates and removals")
{
    using namespace hgraph;
    using namespace std::string_literals;
    stdlib::register_standard_operators();

    // t0: {a:1, b:2, c:3} -> 6; t1: a -> 10 ticks through standing bindings
    // -> 15; t2: c removed -> the tree re-shapes and re-publishes -> 12.
    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSD<Str, TS<Int>>>(
                     fn<stdlib::add_>(),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}, {"c"s, 3}}),
                                   dict_delta<Str, TS<Int>>({{"a"s, 10}}),
                                   dict_delta<Str, TS<Int>>({}, {"c"s})))),
                 values<Int>(6, 15, 12));
}

TEST_CASE("reduce over TSD: an empty collection publishes the derived zero")
{
    using namespace hgraph;
    using namespace std::string_literals;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSD<Str, TS<Int>>>(
                     fn<stdlib::add_>(),
                     values<Value>(dict_delta<Str, TS<Int>>({})))),
                 values<Int>(0));
}

TEST_CASE("reduce over TSD: a single live key aliases the element — no zero padding")
{
    using namespace hgraph;
    using namespace std::string_literals;
    stdlib::register_standard_operators();

    // The 2603 no-spurious-zero regression: with an explicit zero of 100, one
    // live key must publish the element itself (1), NOT element + zero (101);
    // emptying the collection falls back to the zero (100).
    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSD<Str, TS<Int>>>(
                     fn<stdlib::add_>(),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}}),
                                   dict_delta<Str, TS<Int>>({{"a"s, 7}}),
                                   dict_delta<Str, TS<Int>>({}, {"a"s})),
                     Int{100})),
                 values<Int>(1, 7, 100));
}

TEST_CASE("reduce over TSD: a live time-series zero drives the empty result")
{
    using namespace hgraph;
    using namespace std::string_literals;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<ReduceLiveZeroGraph>(
                     values<Value>(dict_delta<Str, TS<Int>>({}),
                                   none,
                                   dict_delta<Str, TS<Int>>({{"a"s, 5}}),
                                   dict_delta<Str, TS<Int>>({}, {"a"s})),
                     values<Int>(10, 20, none, 30)),
                 values<Int>(10, 20, 5, 30));
}

TEST_CASE("reduce over TSD: fixed composite results remain projectable and follow structural changes")
{
    using namespace hgraph;
    using namespace std::string_literals;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(
        eval_node<ReduceBundleGraph>(
            values<Value>(dict_delta<Str, ReduceBundle>({}),
                          dict_delta<Str, ReduceBundle>({{"one"s, tsb_delta<ReduceBundle>(Int{1}, Int{2})}}),
                          dict_delta<Str, ReduceBundle>({{"two"s, tsb_delta<ReduceBundle>(Int{3}, Int{4})}}),
                          dict_delta<Str, ReduceBundle>({}, {"one"s}))),
        values<Value>(tsb_delta<ReduceBundle>(Int{0}, Int{0}),
                      tsb_delta<ReduceBundle>(Int{1}, Int{2}),
                      tsb_delta<ReduceBundle>(Int{4}, Int{6}),
                      tsb_delta<ReduceBundle>(Int{3}, Int{4})));
}

TEST_CASE("reduce over TSD: keys added over time grow the tree")
{
    using namespace hgraph;
    using namespace std::string_literals;
    stdlib::register_standard_operators();

    // 1 key (alias) -> 2 keys (one combiner) -> 5 keys (capacity growth
    // rebuild) — values stay exact sums throughout.
    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSD<Str, TS<Int>>>(
                     fn<stdlib::add_>(),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}}),
                                   dict_delta<Str, TS<Int>>({{"b"s, 2}}),
                                   dict_delta<Str, TS<Int>>(
                                       {{"c"s, 3}, {"d"s, 4}, {"e"s, 5}})))),
                 values<Int>(1, 3, 15));
}

TEST_CASE("reduce over TSD: a sub-graph combiner with an explicit zero")
{
    using namespace hgraph;
    using namespace std::string_literals;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::reduce_, TSD<Str, TS<Int>>>(
                     fn<SumCombiner>(),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 10}, {"b"s, 20}, {"c"s, 30}}),
                                   dict_delta<Str, TS<Int>>({{"b"s, 200}})),
                     Int{0})),
                 values<Int>(60, 240));
}

TEST_CASE("reduce over TSD: source retarget refreshes bindings and invalid source publishes zero")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<ReduceSwitchedDictGraph>(
                     values<Str>(Str{"left"}, Str{"right"}, Str{"none"})),
                 values<Int>(3, 30, 100));
}

TEST_CASE("reduce over TSD: a pass-through combiner is rejected")
{
    using namespace hgraph;
    using namespace std::string_literals;
    stdlib::register_standard_operators();

    REQUIRE_THROWS((eval_node<stdlib::reduce_, TSD<Str, TS<Int>>>(
        fn<FirstCombiner>(),
        values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}})),
        Int{0})));
}

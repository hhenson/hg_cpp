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

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    // Sub-graph combiner: flattens through wire<G> at every reduction node.
    struct SumCombiner
    {
        static constexpr auto name = "sum_combiner";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
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

    // ---- user specialisation, mirroring ext/main's test_map_overload ----

    // A combiner used purely as a selection marker for the specialised overload.
    struct FirstCombiner
    {
        static constexpr auto name = "first_combiner";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> lhs, Port<TS<Int>>) { return lhs; }
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

// The ``switch_`` higher-order OPERATOR (lib/std/operators/higher_order.h).
//
// switch_ routes through ONE child graph at a time, selected by its key input:
// on a key change the active branch is stopped and destroyed, the new branch is
// compiled-in (built/bound/started) and the forwarding output re-points —
// sampling the new branch at the switch time (the sampled-runtime contract; a
// deliberate divergence from Python's value=None reset). Branches are WiredFn
// values (graphs, nodes, or operators) and may take the key as their first
// argument (by arity). See the developer guide *Nested Graphs*.

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

#include <stdexcept>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct Doubler
    {
        static constexpr auto name = "doubler";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts)
        {
            using namespace hgraph::stdlib::syntax;
            return (ts * Int{2}).as<TS<Int>>();
        }
    };

    struct Negator
    {
        static constexpr auto name = "negator";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts)
        {
            using namespace hgraph::stdlib::syntax;
            return (ts * Int{-1}).as<TS<Int>>();
        }
    };

    // A key-consuming branch: arity == ts args + 1, the key first.
    struct AddKey
    {
        static constexpr auto name = "add_key";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> key, Port<TS<Int>> ts)
        {
            using namespace hgraph::stdlib::syntax;
            return (key + ts).as<TS<Int>>();
        }
    };

    // Source-style branches (arity zero, no ts args).
    struct ConstOne
    {
        static constexpr auto name = "const_one";
        static Port<TS<Int>>  compose(Wiring &w) { return wire<stdlib::const_, TS<Int>>(w, Int{1}); }
    };

    struct ConstTwo
    {
        static constexpr auto name = "const_two";
        static Port<TS<Int>>  compose(Wiring &w) { return wire<stdlib::const_, TS<Int>>(w, Int{2}); }
    };

    // A stateful NODE branch (exercises node-as-branch through the WiredFn
    // compile thunk): counts its input ticks.
    struct CounterNode
    {
        static constexpr auto name = "tick_counter";
        static void eval(In<"ts", TS<Int>>, State<Int> count, Out<TS<Int>> out)
        {
            count.set(count.get() + 1);
            out.set(count.get());
        }
    };
}  // namespace

TEST_CASE("switch_: the key selects the branch and a swap samples the held input")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // Cycle 2 switches branch while ts holds 4: the new branch evaluates with
    // the sampled value (-4) rather than waiting for the next ts tick.
    CHECK_OUTPUT(eval_node<stdlib::switch_>(
                     values<Str>(Str{"a"}, none, Str{"b"}, none),
                     stdlib::switch_cases({{Value{Str{"a"}}, fn<Doubler>()}, {Value{Str{"b"}}, fn<Negator>()}}),
                     values<Int>(3, 4, none, 5)),
                 values<Int>(6, 8, -4, -5));
}

TEST_CASE("switch_: an unmatched key falls through to the default branch")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::switch_>(
                     values<Str>(Str{"a"}, Str{"zzz"}),
                     stdlib::switch_cases({{Value{Str{"a"}}, fn<Doubler>()}}, fn<Negator>()),
                     values<Int>(3, none)),
                 values<Int>(6, -3));
}

TEST_CASE("switch_: a branch may consume the key as its first argument (mixed arities)")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::switch_>(
                     values<Int>(1, none, 2),
                     stdlib::switch_cases({{Value{Int{1}}, fn<AddKey>()}, {Value{Int{2}}, fn<Doubler>()}}),
                     values<Int>(10, 20, none)),
                 values<Int>(11, 21, 40));
}

TEST_CASE("switch_: source-style branches need no time-series arguments")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // A re-tick of the same key is not a switch: the active branch stays and
    // its const source does not re-emit.
    CHECK_OUTPUT(eval_node<stdlib::switch_>(
                     values<Str>(Str{"x"}, Str{"x"}, Str{"y"}),
                     stdlib::switch_cases({{Value{Str{"x"}}, fn<ConstOne>()}, {Value{Str{"y"}}, fn<ConstTwo>()}})),
                 values<Int>(1, none, 2));
}

TEST_CASE("switch_: an unmatched key with no default branch is a runtime error")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    REQUIRE_THROWS_AS(eval_node<stdlib::switch_>(
                          values<Str>(Str{"zzz"}),
                          stdlib::switch_cases({{Value{Str{"a"}}, fn<Doubler>()}}),
                          values<Int>(1)),
                      std::runtime_error);
}

TEST_CASE("switch_: switching away destroys the branch; switching back rebuilds it fresh")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // The counter branch accumulates per-branch state; returning to it after a
    // swap must observe a FRESH child graph (count restarts at 1).
    CHECK_OUTPUT(eval_node<stdlib::switch_>(
                     values<Str>(Str{"c"}, none, Str{"d"}, Str{"c"}),
                     stdlib::switch_cases({{Value{Str{"c"}}, fn<CounterNode>()}, {Value{Str{"d"}}, fn<Doubler>()}}),
                     values<Int>(5, 6, 7, 8)),
                 values<Int>(1, 2, 14, 1));
}

TEST_CASE("switch_: reload_on_ticked rebuilds the branch on every key tick")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // Without the flag a same-key re-tick keeps the branch (counter keeps
    // counting); with reload the branch is rebuilt fresh each key tick.
    CHECK_OUTPUT(eval_node<stdlib::switch_>(
                     values<Str>(Str{"c"}, Str{"c"}, none),
                     stdlib::switch_cases({{Value{Str{"c"}}, fn<CounterNode>()}}),
                     values<Int>(5, 6, 7)),
                 values<Int>(1, 2, 3));

    CHECK_OUTPUT(eval_node<stdlib::switch_>(
                     values<Str>(Str{"c"}, Str{"c"}, none),
                     stdlib::switch_cases({{Value{Str{"c"}}, fn<CounterNode>()}}).reload(),
                     values<Int>(5, 6, 7)),
                 values<Int>(1, 1, 2));
}

namespace
{
    struct AddBoth
    {
        static constexpr auto name = "add_both";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> a, Port<TS<Int>> b)
        {
            using namespace hgraph::stdlib::syntax;
            return (a + b).as<TS<Int>>();
        }
    };

    struct SubBoth
    {
        static constexpr auto name = "sub_both";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> a, Port<TS<Int>> b)
        {
            using namespace hgraph::stdlib::syntax;
            return (a - b).as<TS<Int>>();
        }
    };

    // Key-consuming over two ts args: arity = ts count + 1.
    struct KeySumBoth
    {
        static constexpr auto name = "key_sum_both";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> key, Port<TS<Int>> a, Port<TS<Int>> b)
        {
            using namespace hgraph::stdlib::syntax;
            return (key + (a + b).as<TS<Int>>()).as<TS<Int>>();
        }
    };
}  // namespace

TEST_CASE("switch_: variadic time-series arguments feed the branches, mixed arities")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // Two ts args; branch 1 adds them, branch 2 subtracts, branch 3 consumes
    // the key too (arity 3 = key first). Keys 1 -> add, 2 -> sub, 3 -> key+sum.
    CHECK_OUTPUT(eval_node<stdlib::switch_>(
                     values<Int>(1, none, 2, 3),
                     stdlib::switch_cases({{Value{Int{1}}, fn<AddBoth>()},
                                           {Value{Int{2}}, fn<SubBoth>()},
                                           {Value{Int{3}}, fn<KeySumBoth>()}}),
                     values<Int>(10, 20, none, none),
                     values<Int>(4, none, 6, none)),
                 values<Int>(14, 24, 14, 29));
}

// ---------------------------------------------------------------------------
// Keyword arguments resolve PER BRANCH against each branch's own parameter
// names (Python parity) — branches may declare the same names in different
// parameter orders.
// ---------------------------------------------------------------------------

namespace
{
    struct DiffLhsFirst
    {
        static constexpr auto name = "diff_lhs_first";
        static Port<TS<Int>>  compose(Wiring &w, NamedPort<"lhs", TS<Int>> lhs, NamedPort<"rhs", TS<Int>> rhs)
        {
            using namespace hgraph::stdlib::syntax;
            return (lhs - rhs).as<TS<Int>>();
        }
    };

    // Parameter ORDER reversed; the names still bind lhs=lhs, rhs=rhs — this
    // branch computes rhs - lhs to make the per-branch resolution observable.
    struct DiffRhsFirst
    {
        static constexpr auto name = "diff_rhs_first";
        static Port<TS<Int>>  compose(Wiring &w, NamedPort<"rhs", TS<Int>> rhs, NamedPort<"lhs", TS<Int>> lhs)
        {
            using namespace hgraph::stdlib::syntax;
            return (rhs - lhs).as<TS<Int>>();
        }
    };

    struct SwitchKwargsGraph
    {
        static constexpr auto name = "switch_kwargs_graph";
        static void           compose(Wiring &w)
        {
            auto key = wire<testing::replay, TS<Str>>(w, Str{"key"});
            auto a   = wire<testing::replay, TS<Int>>(w, Str{"a"});
            auto b   = wire<testing::replay, TS<Int>>(w, Str{"b"});
            wire<testing::record>(
                w,
                wire<stdlib::switch_>(w, key,
                                      stdlib::switch_cases({{Value{Str{"fwd"}}, fn<DiffLhsFirst>()},
                                                            {Value{Str{"rev"}}, fn<DiffRhsFirst>()}}),
                                      arg<"lhs">(a), arg<"rhs">(b)),
                Str{"out"});
        }
    };
}  // namespace

TEST_CASE("switch_: keyword arguments bind per branch by parameter name")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphBuilder gb = build_graph<SwitchKwargsGraph>();
    set_replay_values<Str>(gb.global_state(), "key", values<Str>(Str{"fwd"}, Str{"rev"}));
    set_replay_values<Int>(gb.global_state(), "a", values<Int>(10, none));
    set_replay_values<Int>(gb.global_state(), "b", values<Int>(3, none));

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    // fwd: lhs - rhs = 7; rev (params declared rhs-first): rhs - lhs = -7.
    CHECK_OUTPUT(get_recorded_values<Int>(ex.view().graph().global_state(), "out"), values<Int>(7, -7));
}

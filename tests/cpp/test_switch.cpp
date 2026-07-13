// The ``switch_`` higher-order OPERATOR (lib/std/operators/higher_order.h).
//
// switch_ routes through ONE active child graph at a time, selected by its key
// input. Two fixed graph-storage slots alternate: on a key change the inactive
// slot is reused for the new branch and the old active branch is stopped, then
// retained until its slot is reused by the following switch. The output samples
// the new branch at switch time (the sampled-runtime contract; a deliberate
// divergence from Python's value=None reset). Branches are WiredFn values
// (graphs, nodes, or operators) and may take the key when their first parameter
// is named "key". See the developer guide *Nested Graphs*.

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

#include <array>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct Doubler
    {
        static constexpr auto name = "doubler";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> ts)
        {
            using namespace hgraph::stdlib::syntax;
            return (ts * Int{2}).as<TS<Int>>();
        }
    };

    struct Negator
    {
        static constexpr auto name = "negator";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> ts)
        {
            using namespace hgraph::stdlib::syntax;
            return (ts * Int{-1}).as<TS<Int>>();
        }
    };

    struct PassThrough
    {
        static constexpr auto name = "pass_through";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> ts) { return ts; }
    };

    // A key-consuming branch: the first parameter is named "key".
    struct AddKey
    {
        static constexpr auto name = "add_key";
        static Port<TS<Int>>  compose(Wiring &, NamedPort<"key", TS<Int>> key, Port<TS<Int>> ts)
        {
            using namespace hgraph::stdlib::syntax;
            return (key + ts).as<TS<Int>>();
        }
    };

    struct AddUnnamedKey
    {
        static constexpr auto name = "add_unnamed_key";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> key, Port<TS<Int>> ts)
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

    struct SwitchStorageRecorderTag
    {
    };

    void wire_switch_storage_recorder(Wiring &w,
                                      const WiringPortRef &switch_output,
                                      const WiringPortRef &key,
                                      std::vector<std::size_t> &stored_counts,
                                      std::vector<std::uintptr_t> &active_addresses)
    {
        const auto *input_schema = TypeRegistry::instance().un_named_tsb(
            {{"switch", switch_output.schema}, {"key", key.schema}});
        std::vector<TSEndpointSchema> endpoints{
            TSEndpointSchema::peered(switch_output.schema),
            TSEndpointSchema::peered(key.schema),
        };

        NodeTypeMetaData meta;
        meta.display_name = "switch_storage_recorder";
        meta.input_schema = input_schema;
        meta.node_kind    = NodeKind::Sink;
        meta.valid_inputs = std::vector<std::size_t>{};

        NodeCallbacks callbacks;
        callbacks.evaluate = [&stored_counts, &active_addresses](const NodeView &view, DateTime) {
            auto graph = view.graph();
            for (std::size_t i = 0; i < graph.node_count(); ++i)
            {
                try
                {
                    auto switch_view = graph.node_at(i).as<SwitchNodeView>();
                    stored_counts.push_back(switch_view.stored_graph_count());
                    active_addresses.push_back(reinterpret_cast<std::uintptr_t>(
                        switch_view.active_graph_value().view().data()));
                    return;
                }
                catch (const std::invalid_argument &)
                {
                }
            }
            throw std::logic_error("switch_storage_recorder could not find a switch node");
        };

        NodeBuilder builder = NodeBuilder::native(
            std::move(meta), std::move(callbacks),
            TSEndpointSchema::non_peered(input_schema, std::move(endpoints)));
        const std::array<WiringPortRef, 2> inputs{switch_output, key};
        static_cast<void>(w.add_node(std::type_index(typeid(SwitchStorageRecorderTag)),
                                     std::move(builder), inputs, Value{}));
    }
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

TEST_CASE("switch_: a branch may return a parent input directly")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::switch_>(
                     values<Str>(Str{"id"}, none, Str{"double"}, Str{"id"}, none),
                     stdlib::switch_cases({{Value{Str{"id"}}, fn<PassThrough>()},
                                           {Value{Str{"double"}}, fn<Doubler>()}}),
                     values<Int>(3, 4, none, 5, 6)),
                 values<Int>(3, 4, 8, 5, 6));
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

TEST_CASE("switch_: unnamed arity-plus-one branch does not consume the key")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    REQUIRE_THROWS_AS((eval_node<stdlib::switch_>(
                          values<Int>(1),
                          stdlib::switch_cases({{Value{Int{1}}, fn<AddUnnamedKey>()}}),
                          values<Int>(10))),
                      OperatorResolutionError);
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

TEST_CASE("switch_: switching back reuses the old slot with a fresh branch graph")
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

TEST_CASE("switch_: alternating branches reuse two fixed graph addresses")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    std::vector<std::size_t>    stored_counts;
    std::vector<std::uintptr_t> active_addresses;
    Wiring                      w;
    auto key = wire<testing::replay, TS<Str>>(w, Str{"key"});
    auto source = wire<testing::replay, TS<Int>>(w, Str{"source"});
    auto switched = wire<stdlib::switch_>(
                        w, key,
                        stdlib::switch_cases({{Value{Str{"a"}}, fn<Doubler>()},
                                              {Value{Str{"b"}}, fn<Negator>()}}),
                        source)
                        .as<TS<Int>>();
    wire_switch_storage_recorder(w, switched.erased(), key.erased(), stored_counts,
                                 active_addresses);

    GraphBuilder gb = std::move(w).finish();
    set_replay_values(gb.global_state(), "key",
                      values<Str>(Str{"a"}, Str{"b"}, Str{"a"}));
    set_replay_values(gb.global_state(), "source", values<Int>(1, 2, 3));

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    REQUIRE(stored_counts == std::vector<std::size_t>{1, 2, 2});
    REQUIRE(active_addresses.size() == 3);
    CHECK(active_addresses[0] != active_addresses[1]);
    CHECK(active_addresses[0] == active_addresses[2]);
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
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> a, Port<TS<Int>> b)
        {
            using namespace hgraph::stdlib::syntax;
            return (a + b).as<TS<Int>>();
        }
    };

    struct SubBoth
    {
        static constexpr auto name = "sub_both";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> a, Port<TS<Int>> b)
        {
            using namespace hgraph::stdlib::syntax;
            return (a - b).as<TS<Int>>();
        }
    };

    // Key-consuming over two ts args: arity = ts count + 1.
    struct KeySumBoth
    {
        static constexpr auto name = "key_sum_both";
        static Port<TS<Int>>  compose(Wiring &, NamedPort<"key", TS<Int>> key, Port<TS<Int>> a, Port<TS<Int>> b)
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
    // the key too because its first parameter is named "key".
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
        static Port<TS<Int>>  compose(Wiring &, NamedPort<"lhs", TS<Int>> lhs, NamedPort<"rhs", TS<Int>> rhs)
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
        static Port<TS<Int>>  compose(Wiring &, NamedPort<"rhs", TS<Int>> rhs, NamedPort<"lhs", TS<Int>> lhs)
        {
            using namespace hgraph::stdlib::syntax;
            return (rhs - lhs).as<TS<Int>>();
        }
    };

}  // namespace

TEST_CASE("switch_: keyword arguments bind per branch by parameter name")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // fwd: lhs - rhs = 7; rev (params declared rhs-first): rhs - lhs = -7 —
    // the names bind per branch despite the reversed parameter order.
    CHECK_OUTPUT(eval_node<stdlib::switch_>(
                     values<Str>(Str{"fwd"}, Str{"rev"}),
                     stdlib::switch_cases({{Value{Str{"fwd"}}, fn<DiffLhsFirst>()},
                                           {Value{Str{"rev"}}, fn<DiffRhsFirst>()}}),
                     arg<"lhs">(values<Int>(10, none)), arg<"rhs">(values<Int>(3, none))),
                 values<Int>(7, -7));
}

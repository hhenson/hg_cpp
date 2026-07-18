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
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/wired_fn.h>

#include "nested_lifecycle_test_support.h"

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

    struct ConstOneString
    {
        static constexpr auto name = "const_one_string";
        static Port<TS<Str>> compose(Wiring &w) { return wire<stdlib::const_, TS<Str>>(w, Str{"one"}); }
    };

    struct ConstTwoString
    {
        static constexpr auto name = "const_two_string";
        static Port<TS<Str>> compose(Wiring &w) { return wire<stdlib::const_, TS<Str>>(w, Str{"two"}); }
    };

    struct SwitchRefKeyGraph
    {
        static constexpr auto name = "switch_ref_key_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> key)
        {
            auto fallback = wire<stdlib::const_, TS<Str>>(w, Str{"two"});
            auto selected = wire<stdlib::default_>(w, key, fallback);
            return wire<stdlib::switch_>(
                       w, selected,
                       stdlib::switch_cases({{Value{Str{"one"}}, fn<ConstOneString>()},
                                             {Value{Str{"two"}}, fn<ConstTwoString>()}}))
                .as<TS<Str>>();
        }
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

    struct PassiveEcho
    {
        static constexpr auto name = "passive_echo";

        static void eval(In<"ts", TS<Int>, InputActivity::Passive> ts, Out<TS<Int>> out)
        {
            out.set(ts.value());
        }
    };

    inline std::vector<Str> switch_sink_values;

    struct SwitchSinkBranch
    {
        static constexpr auto name = "switch_sink_branch";

        static void eval(In<"key", TS<Str>> key)
        {
            switch_sink_values.push_back(key.value());
        }
    };

    struct SwitchSinkGraph
    {
        static constexpr auto name = "switch_sink_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> key)
        {
            wire<stdlib::switch_sink_>(
                w, key,
                stdlib::switch_cases({{Value{Str{"one"}}, fn<SwitchSinkBranch>()},
                                      {Value{Str{"two"}}, fn<SwitchSinkBranch>()}}));
            return key;
        }
    };

    using SwitchSignalBundle = UnNamedTSB<Field<"p1", TS<Int>>, Field<"p2", TS<Str>>>;
    using SwitchIntList = TSL<TS<Int>, 2>;

    struct PeeredBundleBranch
    {
        static constexpr auto name = "peered_bundle_branch";

        static void eval(In<"p1", TS<Int>, InputValidity::Unchecked> p1,
                         In<"p2", TS<Str>, InputValidity::Unchecked> p2,
                         Out<SwitchSignalBundle> out)
        {
            if (p1.valid()) { out.field<"p1">().set(p1.value()); }
            if (p2.valid()) { out.field<"p2">().set(p2.value()); }
        }
    };

    struct StructuralBundleBranch
    {
        static constexpr auto name = "structural_bundle_branch";

        static Port<SwitchSignalBundle> compose(Wiring &w, Port<TS<Int>> p1, Port<TS<Str>> p2)
        {
            auto structural = stdlib::to_tsb<SwitchSignalBundle>(w, p1, p2);
            return wire<stdlib::pass_through_node>(w, structural).as<SwitchSignalBundle>();
        }
    };

    struct DirectBundleBranch
    {
        static constexpr auto name = "direct_bundle_branch";

        static Port<SwitchSignalBundle> compose(Wiring &, Port<SwitchSignalBundle> bundle)
        {
            return bundle;
        }
    };

    struct ConstantBundleBranch
    {
        static constexpr auto name = "constant_bundle_branch";

        static Port<SwitchSignalBundle> compose(Wiring &w, Port<SwitchSignalBundle>)
        {
            auto p1 = wire<stdlib::const_, TS<Int>>(w, Int{1});
            auto p2 = wire<stdlib::const_, TS<Str>>(w, Str{"fixed"});
            auto bundle = stdlib::to_tsb<SwitchSignalBundle>(w, p1, p2);
            return wire<stdlib::pass_through_node>(w, bundle).as<SwitchSignalBundle>();
        }
    };

    struct DirectListBranch
    {
        static constexpr auto name = "direct_list_branch";

        static Port<SwitchIntList> compose(Wiring &, Port<SwitchIntList> list)
        {
            return list;
        }
    };

    struct ConstantListBranch
    {
        static constexpr auto name = "constant_list_branch";

        static Port<SwitchIntList> compose(Wiring &w, Port<SwitchIntList>)
        {
            auto first = wire<stdlib::const_, TS<Int>>(w, Int{1});
            auto second = wire<stdlib::const_, TS<Int>>(w, Int{2});
            auto list = stdlib::to_tsl<SwitchIntList>(w, first, second);
            return wire<stdlib::pass_through_node>(w, list).as<SwitchIntList>();
        }
    };

    struct SignalTick
    {
        static constexpr auto name = "signal_tick";

        static void eval(In<"signal", SIGNAL>, Out<TS<Bool>> out)
        {
            out.set(true);
        }
    };

    struct SwitchBundleSignalGraph
    {
        static constexpr auto name = "switch_bundle_signal_graph";

        static Port<TS<Bool>> compose(Wiring &w,
                                      Port<TS<Bool>> structural,
                                      Port<TS<Int>> p1,
                                      Port<TS<Str>> p2)
        {
            auto bundle = wire<stdlib::switch_>(
                w, structural,
                stdlib::switch_cases({{Value{false}, fn<PeeredBundleBranch>()},
                                      {Value{true}, fn<StructuralBundleBranch>()}}),
                p1, p2);
            return wire<SignalTick>(w, bundle);
        }
    };

    struct SwitchBundleValueGraph
    {
        static constexpr auto name = "switch_bundle_value_graph";

        static Port<SwitchSignalBundle> compose(Wiring &w,
                                                Port<TS<Bool>> direct,
                                                Port<TS<Int>> p1,
                                                Port<TS<Str>> p2)
        {
            auto bundle = stdlib::to_tsb<SwitchSignalBundle>(w, p1, p2);
            return wire<stdlib::switch_>(
                       w, direct,
                       stdlib::switch_cases({{Value{false}, fn<ConstantBundleBranch>()},
                                             {Value{true}, fn<DirectBundleBranch>()}}),
                       bundle)
                .as<SwitchSignalBundle>();
        }
    };

    struct SwitchListValueGraph
    {
        static constexpr auto name = "switch_list_value_graph";

        static Port<SwitchIntList> compose(Wiring &w,
                                          Port<TS<Bool>> direct,
                                          Port<TS<Int>> first,
                                          Port<TS<Int>> second)
        {
            auto list = stdlib::to_tsl<SwitchIntList>(w, first, second);
            return wire<stdlib::switch_>(
                       w, direct,
                       stdlib::switch_cases({{Value{false}, fn<ConstantListBranch>()},
                                             {Value{true}, fn<DirectListBranch>()}}),
                       list)
                .as<SwitchIntList>();
        }
    };

    struct SwitchStorageRecorderTag
    {
    };

    void wire_switch_storage_recorder(Wiring &w,
                                      const WiringPortRef &switch_output,
                                      const WiringPortRef &key,
                                      std::vector<std::size_t> &stored_counts,
                                      std::vector<std::uintptr_t> &active_addresses,
                                      std::vector<NestedLifecycleSnapshot> *lifecycle = nullptr)
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
        callbacks.evaluate = [&stored_counts, &active_addresses, lifecycle](const NodeView &view, DateTime) {
            auto graph = view.graph();
            for (std::size_t i = 0; i < graph.node_count(); ++i)
            {
                auto node = graph.node_at(i);
                if (node.is<SwitchNodeView>())
                {
                    auto switch_view = node.as<SwitchNodeView>();
                    stored_counts.push_back(switch_view.stored_graph_count());
                    active_addresses.push_back(reinterpret_cast<std::uintptr_t>(
                        switch_view.active_graph_value().view().data()));
                    if (lifecycle != nullptr)
                    {
                        lifecycle->push_back(NestedLifecycleCounters::snapshot());
                    }
                    return;
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

TEST_CASE("switch_: sink branches use the native outputless wiring contract")
{
    using namespace hgraph;
    stdlib::register_standard_operators();
    switch_sink_values.clear();

    CHECK_OUTPUT(eval_node<SwitchSinkGraph>(values<Str>(Str{"one"}, Str{"two"})),
                 values<Str>(Str{"one"}, Str{"two"}));
    CHECK(switch_sink_values == std::vector<Str>{Str{"one"}, Str{"two"}});
}

TEST_CASE("switch_: a REF-shaped key is selected by its dereferenced value")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<SwitchRefKeyGraph>(values<Str>(Str{"one"}, Str{"two"})),
                 values<Str>(Str{"one"}, Str{"two"}));
}

TEST_CASE("switch_: selecting a branch does not sample a passive held input")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<stdlib::switch_>(
                     values<Str>(Str{"active"}, none, Str{"passive"}, none),
                     stdlib::switch_cases({{Value{Str{"active"}}, fn<Doubler>()},
                                           {Value{Str{"passive"}}, fn<PassiveEcho>()}}),
                     values<Int>(3, 4, none, 5)),
                 values<Int>(6, 8, none, none));
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

TEST_CASE("switch_: peered and structural TSB branches notify SIGNAL on every retarget")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<SwitchBundleSignalGraph>(values<Bool>(none, false, true, false),
                                                     values<Int>(none, 1, none, none),
                                                     values<Str>(none, none, Str{"b"})),
                 values<Bool>(none, true, true, true));
}

TEST_CASE("switch_: a direct structural branch samples held bundle values on activation")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<SwitchBundleValueGraph>(values<Bool>(false, true, false, true),
                                                    values<Int>(10, 20, 30, 40),
                                                    values<Str>(Str{"a"}, Str{"b"}, Str{"c"}, Str{"d"})),
                 values<Value>(tsb_delta<SwitchSignalBundle>(Int{1}, Str{"fixed"}),
                               tsb_delta<SwitchSignalBundle>(Int{20}, Str{"b"}),
                               tsb_delta<SwitchSignalBundle>(Int{1}, Str{"fixed"}),
                               tsb_delta<SwitchSignalBundle>(Int{40}, Str{"d"})));
}

TEST_CASE("switch_: a direct structural branch samples held list values on activation")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<SwitchListValueGraph>(values<Bool>(false, true, false, true),
                                                 values<Int>(10, 20, 30, 40),
                                                 values<Int>(-10, -20, -30, -40)),
                 values<Value>(list_delta<TS<Int>>({1, 2}),
                               list_delta<TS<Int>>({20, -20}),
                               list_delta<TS<Int>>({1, 2}),
                               list_delta<TS<Int>>({40, -40})));
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
    NestedLifecycleCounters::reset();

    std::vector<std::size_t>    stored_counts;
    std::vector<std::uintptr_t> active_addresses;
    std::vector<NestedLifecycleSnapshot> lifecycle;
    Wiring                      w;
    auto key = wire<stdlib::replay_impl, TS<Str>>(w, Str{"key"});
    auto source = wire<stdlib::replay_impl, TS<Int>>(w, Str{"source"});
    auto switched = wire<stdlib::switch_>(
                        w, key,
                        stdlib::switch_cases({{Value{Str{"a"}}, fn<NestedLifecycleNode>()},
                                              {Value{Str{"b"}}, fn<NestedLifecycleNode>()}}),
                        source)
                        .as<TS<Int>>();
    wire_switch_storage_recorder(w, switched.erased(), key.erased(), stored_counts,
                                 active_addresses, &lifecycle);

    GraphBuilder gb = std::move(w).finish();
    set_replay_values(gb.global_state(), "key",
                      values<Str>(Str{"a"}, Str{"b"}, Str{"a"}));
    set_replay_values(gb.global_state(), "source", values<Int>(1, 2, 3));

    {
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();

        CHECK(lifecycle == std::vector<NestedLifecycleSnapshot>{
                               {1, 1, 1, 0, 0},
                               {2, 2, 2, 1, 0},
                               {3, 2, 3, 2, 1},
                           });
    }

    REQUIRE(stored_counts == std::vector<std::size_t>{1, 2, 2});
    REQUIRE(active_addresses.size() == 3);
    CHECK(active_addresses[0] != active_addresses[1]);
    CHECK(active_addresses[0] == active_addresses[2]);
    CHECK(NestedLifecycleCounters::snapshot() == NestedLifecycleSnapshot{3, 0, 3, 3, 3});
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

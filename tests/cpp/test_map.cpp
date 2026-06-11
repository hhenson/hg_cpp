// The ``map_`` higher-order OPERATOR (lib/std/operators/higher_order.h).
//
// map_ owns one child graph instance per key of its multiplexed TSD input:
// added keys instantiate an element in the owned TSD<K, OUT> output and
// build/bind/start a fresh child whose terminal forwarding output writes that
// element directly (no copy); removed keys destroy the child and remove the
// element. func is a WiredFn (graph, node, or operator)
// and may take the key as its first argument (by arity); further time-series
// arguments broadcast whole to every child. See *Nested Graphs*.

#include <hgraph/lib/std/std_operators.h>
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

#include <stdexcept>
#include <string>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using namespace std::string_literals;

    struct AddOneG
    {
        static constexpr auto name = "add_one_g";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts)
        {
            using namespace hgraph::stdlib::syntax;
            return (ts + Int{1}).as<TS<Int>>();
        }
    };

    // Key-consuming function: arity = element + 1, the key first.
    struct AddKeyG
    {
        static constexpr auto name = "add_key_g";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> key, Port<TS<Int>> ts)
        {
            using namespace hgraph::stdlib::syntax;
            return (key + ts).as<TS<Int>>();
        }
    };

    // Broadcast function: (element, offset) — the offset binds whole per child.
    struct AddOffsetG
    {
        static constexpr auto name = "add_offset_g";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts, Port<TS<Int>> offset)
        {
            using namespace hgraph::stdlib::syntax;
            return (ts + offset).as<TS<Int>>();
        }
    };

    struct IdentityG
    {
        static constexpr auto name = "identity_g";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> ts) { return ts; }
    };

    struct ConstLeftDict
    {
        static constexpr auto             name = "const_left_dict";
        static Port<TSD<Str, TS<Int>>> compose(Wiring &w)
        {
            return wire<stdlib::const_, TSD<Str, TS<Int>>>(
                w, stdlib::make_map<Str, Int>({{Str{"a"}, Int{1}}, {Str{"b"}, Int{2}}}));
        }
    };

    struct ConstRightDict
    {
        static constexpr auto             name = "const_right_dict";
        static Port<TSD<Str, TS<Int>>> compose(Wiring &w)
        {
            return wire<stdlib::const_, TSD<Str, TS<Int>>>(
                w, stdlib::make_map<Str, Int>({{Str{"b"}, Int{20}}, {Str{"c"}, Int{30}}}));
        }
    };

    struct NeverDictNode
    {
        static constexpr auto name = "never_dict";
        static void eval(Out<TSD<Str, TS<Int>>>) {}
    };

    struct NoDict
    {
        static constexpr auto             name = "no_dict";
        static Port<TSD<Str, TS<Int>>> compose(Wiring &w) { return wire<NeverDictNode>(w); }
    };

    struct MapSwitchedDictGraph
    {
        static constexpr auto             name = "map_switched_dict_graph";
        static Port<TSD<Str, TS<Int>>> compose(Wiring &w, Port<TS<Str>> select)
        {
            auto source = wire<stdlib::switch_>(
                              w, select,
                              stdlib::switch_cases({{Value{Str{"left"}}, fn<ConstLeftDict>()},
                                                     {Value{Str{"right"}}, fn<ConstRightDict>()},
                                                     {Value{Str{"none"}}, fn<NoDict>()}}))
                              .as<TSD<Str, TS<Int>>>();
            return wire<stdlib::map_>(w, fn<AddOneG>(), source).as<TSD<Str, TS<Int>>>();
        }
    };

    // A stateful NODE function: counts its element's ticks (per-key isolation
    // and fresh-rebuild checks).
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

TEST_CASE("map_: keys add, update, and remove drive per-key children and the TSD output")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::map_, TSD<Str, TS<Int>>>(
                     fn<AddOneG>(),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
                                   dict_delta<Str, TS<Int>>({{"a"s, 10}}),
                                   dict_delta<Str, TS<Int>>({}, {"b"s})))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 2}, {"b"s, 3}}),
                               dict_delta<Str, TS<Int>>({{"a"s, 11}}),
                               dict_delta<Str, TS<Int>>({}, {"b"s})));
}

TEST_CASE("map_: each key owns an isolated child instance (independent state)")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // a ticks twice, b once: the counters do not share state.
    CHECK_OUTPUT((eval_node<stdlib::map_, TSD<Str, TS<Int>>>(
                     fn<CounterNode>(),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 5}}),
                                   dict_delta<Str, TS<Int>>({{"a"s, 2}})))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 1}}),
                               dict_delta<Str, TS<Int>>({{"a"s, 2}})));
}

TEST_CASE("map_: the function may consume the key as its first argument")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::map_, TSD<Int, TS<Int>>>(
                     fn<AddKeyG>(),
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 10}, {2, 20}}),
                                   dict_delta<Int, TS<Int>>({{2, 200}})))),
                 values<Value>(dict_delta<Int, TS<Int>>({{1, 11}, {2, 22}}),
                               dict_delta<Int, TS<Int>>({{2, 202}})));
}

TEST_CASE("map_: a broadcast argument binds whole to every child")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // The offset re-tick re-evaluates every child with its held element.
    CHECK_OUTPUT((eval_node<stdlib::map_, TSD<Str, TS<Int>>>(
                     fn<AddOffsetG>(),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
                                   dict_delta<Str, TS<Int>>({{"a"s, 5}}),
                                   none),
                     values<Int>(100, none, 200))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 101}, {"b"s, 102}}),
                               dict_delta<Str, TS<Int>>({{"a"s, 105}}),
                               dict_delta<Str, TS<Int>>({{"a"s, 205}, {"b"s, 202}})));
}

TEST_CASE("map_: a removed key re-added later gets a fresh child instance")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::map_, TSD<Str, TS<Int>>>(
                     fn<CounterNode>(),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}}),
                                   dict_delta<Str, TS<Int>>({{"a"s, 2}}),
                                   dict_delta<Str, TS<Int>>({}, {"a"s}),
                                   dict_delta<Str, TS<Int>>({{"a"s, 3}})))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}}),
                               dict_delta<Str, TS<Int>>({{"a"s, 2}}),
                               dict_delta<Str, TS<Int>>({}, {"a"s}),
                               dict_delta<Str, TS<Int>>({{"a"s, 1}})));
}

TEST_CASE("map_: a mapped source retarget reconciles keys and clears when the input goes invalid")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<MapSwitchedDictGraph>(
                     values<Str>(Str{"left"}, Str{"right"}, Str{"none"})),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 2}, {"b"s, 3}}),
                               dict_delta<Str, TS<Int>>({{"b"s, 21}, {"c"s, 31}}, {"a"s}),
                               dict_delta<Str, TS<Int>>({}, {"b"s, "c"s})));
}

TEST_CASE("map_: a function whose signature does not fit the inputs is rejected at wiring time")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // Arity 2 with no broadcast arg reads as key-consuming, but AddOffsetG's
    // first port is TS<Int> while the keys are Str — the output resolver leaves
    // ``O`` unresolved and dispatch rejects the call.
    REQUIRE_THROWS_AS((eval_node<stdlib::map_, TSD<Str, TS<Int>>>(
                          fn<AddOffsetG>(),
                          values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}})))),
                      OperatorResolutionError);
}

TEST_CASE("map_: a pass-through child output is rejected at wiring time")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    REQUIRE_THROWS((eval_node<stdlib::map_, TSD<Str, TS<Int>>>(
        fn<IdentityG>(),
        values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}})))));
}

// ---------------------------------------------------------------------------
// map_ over a fixed-size TSL: a wiring-time expansion (Python _map_no_index)
// — one inline application of func per index, key = the Int index, output a
// structural TSL. No runtime node.
// ---------------------------------------------------------------------------

TEST_CASE("map_ over TSL: applies func per index, partial ticks stay element-wise")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::map_, TSL<TS<Int>, 3>>(
                     fn<AddOneG>(),
                     values<Value>(list_delta<TS<Int>>({1, 2, 3}),
                                   list_delta<TS<Int>>({none, 20, none})))),
                 values<Value>(list_delta<TS<Int>>({2, 3, 4}),
                               list_delta<TS<Int>>({none, 21, none})));
}

TEST_CASE("map_ over TSL: the function may consume the Int index as its first argument")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::map_, TSL<TS<Int>, 3>>(
                     fn<AddKeyG>(),
                     values<Value>(list_delta<TS<Int>>({10, 20, 30})))),
                 values<Value>(list_delta<TS<Int>>({10, 21, 32})));
}

TEST_CASE("map_ over TSL: a broadcast argument feeds every index")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::map_, TSL<TS<Int>, 2>>(
                     fn<AddOffsetG>(),
                     values<Value>(list_delta<TS<Int>>({1, 2}), none),
                     values<Int>(100, 200))),
                 values<Value>(list_delta<TS<Int>>({101, 102}),
                               list_delta<TS<Int>>({201, 202})));
}

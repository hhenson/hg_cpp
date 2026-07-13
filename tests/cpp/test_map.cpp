// The ``map_`` higher-order OPERATOR (lib/std/operators/higher_order.h).
//
// map_ owns one child graph instance per key of its multiplexed TSD inputs:
// added keys instantiate an element in the owned TSD<K, OUT> output and
// build/bind/start a fresh child whose terminal forwarding output writes that
// element directly (no copy); removed keys destroy the child and remove the
// element. func is a WiredFn (graph, node, or operator) and may take the key
// when its first parameter is named "key" (or the configured __key_arg__). TSD
// inputs multiplex; other time-series inputs broadcast whole. See *Nested
// Graphs*.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/map_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/debug_descriptor.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/wired_fn.h>

#include "../../src/hgraph/runtime/mapped_key_source.h"

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <typeindex>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using namespace std::string_literals;

    struct MappedLifetimeNotifier final : Notifiable
    {
        void notify(DateTime modified_time) override { notifications.push_back(modified_time); }
        std::vector<DateTime> notifications{};
    };

    struct AddOneG
    {
        static constexpr auto name = "add_one_g";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> ts)
        {
            using namespace hgraph::stdlib::syntax;
            return (ts + Int{1}).as<TS<Int>>();
        }
    };

    // Key-consuming functions are name-detected (the Python rule): the first
    // parameter must be named "key" (TSD maps / switch_) or "ndx" (TSL maps).
    struct AddKeyG
    {
        static constexpr auto name = "add_key_g";
        static Port<TS<Int>>  compose(Wiring &, NamedPort<"key", TS<Int>> key, Port<TS<Int>> ts)
        {
            using namespace hgraph::stdlib::syntax;
            return (key + ts).as<TS<Int>>();
        }
    };

    struct AddNdxG
    {
        static constexpr auto name = "add_ndx_g";
        static Port<TS<Int>>  compose(Wiring &, NamedPort<"ndx", TS<Int>> ndx, Port<TS<Int>> ts)
        {
            using namespace hgraph::stdlib::syntax;
            return (ndx + ts).as<TS<Int>>();
        }
    };

    // Broadcast function: (element, offset) — the offset binds whole per child.
    struct AddOffsetG
    {
        static constexpr auto name = "add_offset_g";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> ts, Port<TS<Int>> offset)
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

    struct ConstTenOffset
    {
        static constexpr auto name = "const_ten_offset";
        static Port<TS<Int>>  compose(Wiring &w) { return wire<stdlib::const_, TS<Int>>(w, Int{10}); }
    };

    struct ConstHundredOffset
    {
        static constexpr auto name = "const_hundred_offset";
        static Port<TS<Int>>  compose(Wiring &w) { return wire<stdlib::const_, TS<Int>>(w, Int{100}); }
    };

    struct MapBroadcastRepointGraph
    {
        static constexpr auto             name = "map_broadcast_repoint_graph";
        static Port<TSD<Str, TS<Int>>> compose(Wiring &w,
                                               Port<TS<Str>> select,
                                               Port<TSD<Str, TS<Int>>> source)
        {
            auto offset = wire<stdlib::switch_>(
                              w, select,
                              stdlib::switch_cases({{Value{Str{"ten"}}, fn<ConstTenOffset>()},
                                                     {Value{Str{"hundred"}}, fn<ConstHundredOffset>()}}))
                              .as<TS<Int>>();
            return wire<stdlib::map_>(w, fn<AddOffsetG>(), source, offset)
                .as<TSD<Str, TS<Int>>>();
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

    struct MapActiveCountRecorderTag
    {
    };

    void wire_map_active_count_recorder(Wiring &w,
                                        const WiringPortRef &map_output,
                                        std::span<const WiringPortRef> triggers,
                                        std::vector<std::size_t> &counts)
    {
        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
        fields.reserve(1 + triggers.size());
        fields.emplace_back("map", map_output.schema);
        for (std::size_t i = 0; i < triggers.size(); ++i)
        {
            fields.emplace_back("trigger" + std::to_string(i), triggers[i].schema);
        }
        const auto *input_schema = TypeRegistry::instance().un_named_tsb(fields);

        std::vector<TSEndpointSchema> endpoint_fields;
        endpoint_fields.reserve(fields.size());
        endpoint_fields.push_back(TSEndpointSchema::peered(map_output.schema));
        for (const WiringPortRef &trigger : triggers)
        {
            endpoint_fields.push_back(TSEndpointSchema::peered(trigger.schema));
        }

        NodeTypeMetaData meta;
        meta.display_name = "map_active_count_recorder";
        meta.input_schema = input_schema;
        meta.node_kind    = NodeKind::Sink;
        meta.valid_inputs = std::vector<std::size_t>{};

        NodeCallbacks callbacks;
        callbacks.evaluate = [&counts](const NodeView &view, DateTime) {
            auto graph = view.graph();
            for (std::size_t i = 0; i < graph.node_count(); ++i)
            {
                try
                {
                    counts.push_back(graph.node_at(i).as<MapNodeView>().active_count());
                    return;
                }
                catch (const std::invalid_argument &)
                {
                }
            }
            throw std::logic_error("map_active_count_recorder could not find a map node");
        };

        NodeBuilder builder = NodeBuilder::native(
            std::move(meta), std::move(callbacks),
            TSEndpointSchema::non_peered(input_schema, std::move(endpoint_fields)));

        std::vector<WiringPortRef> inputs;
        inputs.reserve(1 + triggers.size());
        inputs.push_back(map_output);
        inputs.insert(inputs.end(), triggers.begin(), triggers.end());
        static_cast<void>(w.add_node(std::type_index(typeid(MapActiveCountRecorderTag)), std::move(builder),
                                     std::span<const WiringPortRef>{inputs.data(), inputs.size()}, Value{}));
    }
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

TEST_CASE("mapped key sources expose their exact Output role record")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *key_schema = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_key = registry.ts(key_schema);
    Value key{std::int32_t{17}};
    const auto time = MIN_ST + TimeDelta{17};

    runtime_detail::MappedKeySource source;
    source.bind(*ts_key, key, time);
    auto view = source.view(time);
    const auto type = view.type_ref();

    REQUIRE(view.bound());
    REQUIRE(view.binding() == nullptr);
    REQUIRE(type);
    REQUIRE(type.valid());
    REQUIRE(type.record()->role == TypeRole::Output);
    REQUIRE(type.schema() == ts_key);
    REQUIRE(type.plan() == &MemoryUtils::plan_for<runtime_detail::MappedKeySourceStorage>());
    REQUIRE(type.ops() == view.storage_type().ops());
    REQUIRE(view.handle().type_ref() == type);
    REQUIRE(view.valid());
    REQUIRE(view.modified());
    REQUIRE(view.value().checked_as<std::int32_t>() == 17);
    REQUIRE(view.delta_value().checked_as<std::int32_t>() == 17);
}

TEST_CASE("mapped key source teardown invalidates passive and active direct inputs")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *ts_key = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    Value key{std::int32_t{17}};
    const auto time = MIN_ST + TimeDelta{17};

    for (const bool active : {false, true})
    {
        CAPTURE(active);
        TSInput input{
            TSInputBuilderFactory::checked_builder_for(*ts_key, TSEndpointSchema::peered(ts_key))};
        MappedLifetimeNotifier scheduling;
        auto in = input.view(&scheduling, time);
        std::optional<runtime_detail::MappedKeySource> source;
        source.emplace();
        source->bind(*ts_key, key, time);
        in.bind_output(source->view(time));
        if (active) { in.make_active(); }

        REQUIRE(in.bound());
        REQUIRE(in.active() == active);
        REQUIRE(source->view(time).data_view().observer_count() == (active ? 2 : 1));
        scheduling.notifications.clear();

        source.reset();
        REQUIRE_FALSE(in.bound());
        REQUIRE(in.active() == active);
        REQUIRE(scheduling.notifications.empty());
    }
}

TEST_CASE("mapped key source has no observers after input-first teardown")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *ts_key = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    Value key{std::int32_t{23}};
    const auto time = MIN_ST + TimeDelta{23};
    runtime_detail::MappedKeySource source;
    source.bind(*ts_key, key, time);

    for (const bool active : {false, true})
    {
        CAPTURE(active);
        {
            TSInput input{
                TSInputBuilderFactory::checked_builder_for(*ts_key, TSEndpointSchema::peered(ts_key))};
            MappedLifetimeNotifier scheduling;
            auto in = input.view(&scheduling, time);
            in.bind_output(source.view(time));
            if (active) { in.make_active(); }
            REQUIRE(source.view(time).data_view().observer_count() == (active ? 2 : 1));
        }
        REQUIRE(source.view(time).data_view().observer_count() == 0);
    }
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

TEST_CASE("map_: a broadcast source re-point refreshes existing child bindings")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<MapBroadcastRepointGraph>(
                     values<Str>(Str{"ten"}, Str{"hundred"}),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}), none)),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 11}, {"b"s, 12}}),
                               dict_delta<Str, TS<Int>>({{"a"s, 101}, {"b"s, 102}})));
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

TEST_CASE("map_: unnamed arity-plus-one function does not consume the key")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // AddOffsetG has two ordinary TS inputs. Its first parameter is not named
    // "key", so one supplied TSD leaves a func parameter missing instead of
    // treating the Int map key as input 0.
    REQUIRE_THROWS_AS((eval_node<stdlib::map_, TSD<Int, TS<Int>>>(
                          fn<AddOffsetG>(),
                          values<Value>(dict_delta<Int, TS<Int>>({{1, 10}})))),
                      OperatorResolutionError);
}

TEST_CASE("map_: a pass-through child output forwards the mapped element")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::map_, TSD<Str, TS<Int>>>(
                     fn<IdentityG>(),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
                                   dict_delta<Str, TS<Int>>({{"a"s, 10}}),
                                   dict_delta<Str, TS<Int>>({}, {"b"s})))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
                               dict_delta<Str, TS<Int>>({{"a"s, 10}}),
                               dict_delta<Str, TS<Int>>({}, {"b"s})));
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
                     fn<AddNdxG>(),
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

namespace
{
    // Two broadcast args: (element, offset1, offset2).
    struct AddTwoOffsetsG
    {
        static constexpr auto name = "add_two_offsets_g";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> ts, Port<TS<Int>> o1, Port<TS<Int>> o2)
        {
            using namespace hgraph::stdlib::syntax;
            return ((ts + o1).as<TS<Int>>() + o2).as<TS<Int>>();
        }
    };

    // TSL key + element + broadcast: the first parameter is named "ndx".
    struct KeyOffsetG
    {
        static constexpr auto name = "key_offset_g";
        static Port<TS<Int>>  compose(Wiring &, NamedPort<"ndx", TS<Int>> key, Port<TS<Int>> ts, Port<TS<Int>> offset)
        {
            using namespace hgraph::stdlib::syntax;
            return ((key + ts).as<TS<Int>>() + offset).as<TS<Int>>();
        }
    };
}  // namespace

TEST_CASE("map_ over TSD: variadic broadcast arguments feed every child")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::map_, TSD<Str, TS<Int>>>(
                     fn<AddTwoOffsetsG>(),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}), none),
                     values<Int>(10, none),
                     values<Int>(100, 200))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 111}, {"b"s, 112}}),
                               dict_delta<Str, TS<Int>>({{"a"s, 211}, {"b"s, 212}})));
}

TEST_CASE("merge over TSDs is per-key (map_ with a lifted variadic operator)")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // hgraph's merge_tsd: merge(tsd1, tsd2) == map_(merge, tsd1, tsd2) - the
    // union key set, per-key leftmost-modified value.
    CHECK_OUTPUT((eval_node<stdlib::merge, TSD<Str, TS<Int>>, TSD<Str, TS<Int>>>(
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}}), none),
                     values<Value>(dict_delta<Str, TS<Int>>({{"b"s, 6}}), dict_delta<Str, TS<Int>>({{"a"s, 9}})))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 6}}),
                               dict_delta<Str, TS<Int>>({{"a"s, 9}})));   // leftmost MODIFIED wins
}

TEST_CASE("variadic WiredFn compilation does not copy an active global-state seed")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GlobalState seed;
    seed.view().set("configured", Value{Bool{true}});
    GlobalContext context{seed};

    const auto *element = schema_descriptor<TS<Int>>::ts_meta();
    std::array<const TSValueTypeMetaData *, 2> inputs{element, element};
    CompiledSubGraph compiled = fn<stdlib::merge>().compile(inputs);

    CHECK(compiled.output_schema == element);
    CHECK(compiled.graph_builder.global_state().size() == 0);
}

TEST_CASE("merge over NESTED TSDs recurses per key (embedding)")
{
    using namespace hgraph;
    stdlib::register_standard_operators();
    using Inner = TSD<Int, TS<Int>>;

    CHECK_OUTPUT((eval_node<stdlib::merge, TSD<Str, Inner>, TSD<Str, Inner>>(
                     values<Value>(dict_delta<Str, Inner>({{"a"s, dict_delta<Int, TS<Int>>({{1, 1}})}}), none),
                     values<Value>(dict_delta<Str, Inner>({{"a"s, dict_delta<Int, TS<Int>>({{2, 6}})}}), none))),
                 values<Value>(dict_delta<Str, Inner>({{"a"s, dict_delta<Int, TS<Int>>({{1, 1}, {2, 6}})}}), none));
}

TEST_CASE("map_ over TSL: ndx key plus broadcast")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::map_, TSL<TS<Int>, 3>>(
                     fn<KeyOffsetG>(),
                     values<Value>(list_delta<TS<Int>>({10, 20, 30})),
                     values<Int>(100))),
                 values<Value>(list_delta<TS<Int>>({110, 121, 132})));
}

// ---------------------------------------------------------------------------
// Multi-multiplexed inputs (Python parity): every TSD in *args demultiplexes
// by key — the live key set is the UNION; a key absent from one TSD leaves
// that child input invalid until it appears. Same-size TSLs multiplex per
// index in the TSL form.
// ---------------------------------------------------------------------------

namespace
{
    // Two multiplexed elements (both must be valid before it emits).
    struct AddPairG
    {
        static constexpr auto name = "add_pair_g";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            using namespace hgraph::stdlib::syntax;
            return (lhs + rhs).as<TS<Int>>();
        }
    };

    // Two multiplexed elements plus a broadcast offset.
    struct AddPairOffsetG
    {
        static constexpr auto name = "add_pair_offset_g";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> lhs, Port<TS<Int>> rhs, Port<TS<Int>> offset)
        {
            using namespace hgraph::stdlib::syntax;
            return ((lhs + rhs).as<TS<Int>>() + offset).as<TS<Int>>();
        }
    };

    // Emits from the second mux and treats the first mux as optional. This
    // exposes lifecycle bugs where an invalidating mux must remove a key that
    // existed only in that mux while another mux remains valid.
    struct OptionalLeftRightNode
    {
        static constexpr auto name = "optional_left_right";

        static void eval(In<"lhs", TS<Int>, InputValidity::Unchecked> lhs,
                         In<"rhs", TS<Int>, InputValidity::Unchecked> rhs,
                         Out<TS<Int>> out)
        {
            if (!rhs.valid()) { return; }
            out.set((lhs.valid() ? lhs.value() : Int{0}) + rhs.value());
        }
    };

    struct OptionalLeftRightG
    {
        static constexpr auto name = "optional_left_right_g";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            return wire<OptionalLeftRightNode>(w, lhs, rhs);
        }
    };

    struct ConstLeftOnlyDict
    {
        [[maybe_unused]] static constexpr auto name = "const_left_only_dict";
        static Port<TSD<Str, TS<Int>>> compose(Wiring &w)
        {
            return wire<stdlib::const_, TSD<Str, TS<Int>>>(
                w, stdlib::make_map<Str, Int>({{Str{"left"}, Int{1}}}));
        }
    };

    struct ConstRightOnlyDict
    {
        static constexpr auto             name = "const_right_only_dict";
        static Port<TSD<Str, TS<Int>>> compose(Wiring &w)
        {
            return wire<stdlib::const_, TSD<Str, TS<Int>>>(
                w, stdlib::make_map<Str, Int>({{Str{"right"}, Int{7}}}));
        }
    };

    struct MapSecondMuxInvalidGraph
    {
        static constexpr auto             name = "map_second_mux_invalid_graph";
        static Port<TSD<Str, TS<Int>>> compose(Wiring &w, Port<TS<Str>> select)
        {
            auto left = wire<ConstLeftOnlyDict>(w);
            auto right = wire<stdlib::switch_>(
                             w, select,
                             stdlib::switch_cases({{Value{Str{"right"}}, fn<ConstRightOnlyDict>()},
                                                    {Value{Str{"none"}}, fn<NoDict>()}}))
                             .as<TSD<Str, TS<Int>>>();
            return wire<stdlib::map_>(w, fn<OptionalLeftRightG>(), left, right)
                .as<TSD<Str, TS<Int>>>();
        }
    };
}  // namespace

TEST_CASE("map_ over two TSDs: union key set, per-key pairing, absent keys stay pending")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // t0: union {a, b}; only a is in both -> {a: 11}; b waits for tsd2.
    // t1: b appears in tsd2 -> {b: 22}.
    // t2: a leaves tsd1 only -> a stays live in the union, nothing emits.
    // t3: a leaves tsd2 too -> the union drops a: child destroyed, key removed.
    CHECK_OUTPUT((eval_node<stdlib::map_, TSD<Str, TS<Int>>, TSD<Str, TS<Int>>>(
                     fn<AddPairG>(),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
                                   none,
                                   dict_delta<Str, TS<Int>>({}, {"a"s}),
                                   none),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 10}}),
                                   dict_delta<Str, TS<Int>>({{"b"s, 20}}),
                                   none,
                                   dict_delta<Str, TS<Int>>({}, {"a"s})))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 11}}),
                               dict_delta<Str, TS<Int>>({{"b"s, 22}}),
                               none,
                               dict_delta<Str, TS<Int>>({}, {"a"s})));
}

TEST_CASE("map_ over two TSDs plus a broadcast: positions map straight onto func parameters")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::map_, TSD<Str, TS<Int>>, TSD<Str, TS<Int>>>(
                     fn<AddPairOffsetG>(),
                     values<Value>(dict_delta<Str, TS<Int>>({{"x"s, 1}})),
                     values<Value>(dict_delta<Str, TS<Int>>({{"x"s, 2}})),
                     values<Int>(100))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"x"s, 103}})));
}

TEST_CASE("map_ removes keys from an invalidated mux while another mux remains valid")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<MapSecondMuxInvalidGraph>(values<Str>(Str{"right"}, Str{"none"})),
                 values<Value>(dict_delta<Str, TS<Int>>({{"right"s, 7}}),
                               dict_delta<Str, TS<Int>>({}, {"right"s})));
}

TEST_CASE("map_ over two same-size TSLs: pairs multiplex per index")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::map_, TSL<TS<Int>, 3>, TSL<TS<Int>, 3>>(
                     fn<AddPairG>(),
                     values<Value>(list_delta<TS<Int>>({1, 2, 3})),
                     values<Value>(list_delta<TS<Int>>({10, 20, 30})))),
                 values<Value>(list_delta<TS<Int>>({11, 22, 33})));
}

// ---------------------------------------------------------------------------
// Keyword arguments forward onto the mapped FUNCTION's parameters (Python's
// map_(func, **kwargs)): the function names its ports with NamedPort and the
// kwargs resolve by name, regardless of call order.
// ---------------------------------------------------------------------------

namespace
{
    struct NamedPairFn
    {
        static constexpr auto name = "named_pair_fn";
        static Port<TS<Int>>  compose(Wiring &, NamedPort<"lhs", TS<Int>> lhs, NamedPort<"rhs", TS<Int>> rhs)
        {
            using namespace hgraph::stdlib::syntax;
            return (lhs - rhs).as<TS<Int>>();
        }
    };

    struct NamedTsdThenTslFn
    {
        static constexpr auto name = "named_tsd_then_tsl_fn";
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TSL<TS<Int>, 2>> rhs, Out<TS<Int>> out)
        {
            if (lhs.valid() && rhs[0].valid() && rhs[1].valid())
            {
                out.set(lhs.value() + rhs[0].value() + rhs[1].value());
            }
        }
    };

    struct MapKwargsMixedCollectionGraph
    {
        static constexpr auto name = "map_kwargs_mixed_collection_graph";
        static void           compose(Wiring &w)
        {
            auto lhs = wire<testing::replay, TSD<Str, TS<Int>>>(w, Str{"lhs"});
            auto rhs = wire<testing::replay, TSL<TS<Int>, 2>>(w, Str{"rhs"});
            // Raw call order sees the TSL first, but func parameter order sees
            // the TSD first. The TSD kernel must win.
            wire<testing::record>(w, wire<stdlib::map_>(w, fn<NamedTsdThenTslFn>(),
                                                        arg<"rhs">(rhs), arg<"lhs">(lhs)),
                                  Str{"out"});
        }
    };
}  // namespace

TEST_CASE("map_: keyword arguments resolve onto the function's named ports")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // All-keyword call, deliberately rhs-first: names must win — lhs=a,
    // rhs=b, so x -> 10 - 3 = 7; y waits for b. (Schema template args follow
    // the sequences in CALL order: rhs first.)
    CHECK_OUTPUT((eval_node<stdlib::map_, TSD<Str, TS<Int>>, TSD<Str, TS<Int>>>(
                     fn<NamedPairFn>(),
                     arg<"rhs">(values<Value>(dict_delta<Str, TS<Int>>({{"x"s, 3}}))),
                     arg<"lhs">(values<Value>(dict_delta<Str, TS<Int>>({{"x"s, 10}, {"y"s, 100}}))))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"x"s, 7}})));
}

TEST_CASE("map_: kwargs select the kernel from function parameter order")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphBuilder gb = build_graph<MapKwargsMixedCollectionGraph>();
    set_replay_deltas(gb.global_state(), "lhs",
                      values<Value>(dict_delta<Str, TS<Int>>({{"x"s, 10}})));
    set_replay_deltas(gb.global_state(), "rhs",
                      values<Value>(list_delta<TS<Int>>({1, 2})));

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(get_recorded_deltas(ex.view().graph().global_state(), "out"),
                 values<Value>(dict_delta<Str, TS<Int>>({{"x"s, 13}})));
}

// ---------------------------------------------------------------------------
// __keys__: an explicit TSS[K] key set drives the child lifecycle (Python's
// map_(func, ..., __keys__=tss)) — the multiplexed dicts only feed elements.
// ---------------------------------------------------------------------------

TEST_CASE("map_: __keys__ restricts the children to the explicit key set")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // tsd holds a, b, c but only a and c are in __keys__: b never maps.
    CHECK_OUTPUT((eval_node<stdlib::map_, TSD<Str, TS<Int>>, TSS<Str>>(
                     fn<AddOneG>(),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}, {"c"s, 3}})),
                     arg<"__keys__">(values<Value>(set_delta<Str>({"a"s, "c"s}, {}))))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 2}, {"c"s, 4}})));
}

TEST_CASE("map_: __keys__ additions and removals drive the lifecycle, not the dict")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // t0: keys {a} -> only a maps. t1: keys add b (the dict held b all along:
    // the fresh child samples it). t2: keys remove a while the dict still has
    // it -> the output drops a.
    CHECK_OUTPUT((eval_node<stdlib::map_, TSD<Str, TS<Int>>, TSS<Str>>(
                     fn<AddOneG>(),
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}), none, none),
                     arg<"__keys__">(values<Value>(set_delta<Str>({"a"s}, {}),
                                                   set_delta<Str>({"b"s}, {}),
                                                   set_delta<Str>({}, {"a"s}))))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 2}}),
                               dict_delta<Str, TS<Int>>({{"b"s, 3}}),
                               dict_delta<Str, TS<Int>>({}, {"a"s})));
}

TEST_CASE("map_: __keys__ creates children for keys missing from the multiplexed dict")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    std::vector<std::size_t> counts;
    Wiring                   w;
    auto source = wire<testing::replay, TSD<Str, TS<Int>>>(w, Str{"source"});
    auto keys   = wire<testing::replay, TSS<Str>>(w, Str{"keys"});
    auto mapped = wire<stdlib::map_>(w, fn<AddOneG>(), source, arg<"__keys__">(keys))
                      .as<TSD<Str, TS<Int>>>();

    const std::array<WiringPortRef, 2> triggers{keys.erased(), source.erased()};
    wire_map_active_count_recorder(w, mapped.erased(), triggers, counts);

    GraphBuilder gb = std::move(w).finish();
    set_replay_deltas(gb.global_state(), "source", values<Value>(none));
    set_replay_deltas(gb.global_state(), "keys",
                      values<Value>(set_delta<Str>({"ghost"s}, {})));

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK(counts == std::vector<std::size_t>{1});
}

TEST_CASE("map_: multiplexed dict add and remove rebind existing explicit-key children")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    std::vector<std::size_t> counts;
    Wiring                   w;
    auto source = wire<testing::replay, TSD<Str, TS<Int>>>(w, Str{"source"});
    auto keys   = wire<testing::replay, TSS<Str>>(w, Str{"keys"});
    auto mapped = wire<stdlib::map_>(w, fn<AddOneG>(), source, arg<"__keys__">(keys))
                      .as<TSD<Str, TS<Int>>>();
    wire<testing::record>(w, mapped, Str{"out"});

    const std::array<WiringPortRef, 2> triggers{keys.erased(), source.erased()};
    wire_map_active_count_recorder(w, mapped.erased(), triggers, counts);

    GraphBuilder gb = std::move(w).finish();
    set_replay_deltas(gb.global_state(), "source",
                      values<Value>(none,
                                    dict_delta<Str, TS<Int>>({{"x"s, 10}}),
                                    dict_delta<Str, TS<Int>>({}, {"x"s})));
    set_replay_deltas(gb.global_state(), "keys",
                      values<Value>(set_delta<Str>({"x"s}, {}), none, none));

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK(counts == std::vector<std::size_t>{1, 1, 1});
    const auto out = get_recorded_deltas(ex.view().graph().global_state(), "out");
    REQUIRE(out.size() >= 2);
    REQUIRE(out[1].has_value());
    CHECK(out[1]->equals(dict_delta<Str, TS<Int>>({{"x"s, 11}})));
}

namespace
{
    struct ConstABKeys
    {
        static constexpr auto name = "const_ab_keys";
        static Port<TSS<Str>> compose(Wiring &w)
        {
            return wire<stdlib::const_, TSS<Str>>(w,
                                                  stdlib::make_set<Str>({Str{"a"}, Str{"b"}}));
        }
    };

    struct ConstCKeys
    {
        static constexpr auto name = "const_c_keys";
        static Port<TSS<Str>> compose(Wiring &w)
        {
            return wire<stdlib::const_, TSS<Str>>(w, stdlib::make_set<Str>({Str{"c"}}));
        }
    };

    struct NeverKeysNode
    {
        static constexpr auto name = "never_keys";
        static void eval(Out<TSS<Str>>) {}
    };

    struct NoKeys
    {
        static constexpr auto name = "no_keys";
        static Port<TSS<Str>> compose(Wiring &w) { return wire<NeverKeysNode>(w); }
    };
}  // namespace

TEST_CASE("map_: __keys__ source repoint rebuilds children from the new slot layout")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    std::vector<std::size_t> counts;
    Wiring                   w;
    auto source = wire<testing::replay, TSD<Str, TS<Int>>>(w, Str{"source"});
    auto select = wire<testing::replay, TS<Str>>(w, Str{"select"});
    auto keys = wire<stdlib::switch_>(
                    w, select,
                    stdlib::switch_cases({{Value{Str{"ab"}}, fn<ConstABKeys>()},
                                           {Value{Str{"c"}}, fn<ConstCKeys>()}}))
                    .as<TSS<Str>>();
    auto mapped = wire<stdlib::map_>(w, fn<AddOneG>(), source, arg<"__keys__">(keys))
                      .as<TSD<Str, TS<Int>>>();
    wire<testing::record>(w, mapped, Str{"out"});

    const std::array<WiringPortRef, 2> triggers{keys.erased(), source.erased()};
    wire_map_active_count_recorder(w, mapped.erased(), triggers, counts);

    GraphBuilder gb = std::move(w).finish();
    set_replay_deltas(gb.global_state(), "source",
                      values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}, {"c"s, 3}}),
                                    none));
    set_replay_values(gb.global_state(), "select",
                      values<Str>(Str{"ab"}, Str{"c"}));

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(get_recorded_deltas(ex.view().graph().global_state(), "out"),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 2}, {"b"s, 3}}),
                               dict_delta<Str, TS<Int>>({{"c"s, 4}}, {"a"s, "b"s})));
    CHECK(counts == std::vector<std::size_t>{2, 1});
}

TEST_CASE("map_: invalid __keys__ source clears children and output")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    std::vector<std::size_t> counts;
    Wiring                   w;
    auto source = wire<testing::replay, TSD<Str, TS<Int>>>(w, Str{"source"});
    auto select = wire<testing::replay, TS<Str>>(w, Str{"select"});
    auto keys = wire<stdlib::switch_>(
                    w, select,
                    stdlib::switch_cases({{Value{Str{"ab"}}, fn<ConstABKeys>()},
                                           {Value{Str{"none"}}, fn<NoKeys>()}}))
                    .as<TSS<Str>>();
    auto mapped = wire<stdlib::map_>(w, fn<AddOneG>(), source, arg<"__keys__">(keys))
                      .as<TSD<Str, TS<Int>>>();
    wire<testing::record>(w, mapped, Str{"out"});

    const std::array<WiringPortRef, 2> triggers{keys.erased(), source.erased()};
    wire_map_active_count_recorder(w, mapped.erased(), triggers, counts);

    GraphBuilder gb = std::move(w).finish();
    set_replay_deltas(gb.global_state(), "source",
                      values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}), none));
    set_replay_values(gb.global_state(), "select",
                      values<Str>(Str{"ab"}, Str{"none"}));

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(get_recorded_deltas(ex.view().graph().global_state(), "out"),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 2}, {"b"s, 3}}),
                               dict_delta<Str, TS<Int>>({}, {"a"s, "b"s})));
    CHECK(counts == std::vector<std::size_t>{2, 0});
}

TEST_CASE("map_: __keys__ on a TSL map is rejected")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    REQUIRE_THROWS_AS((eval_node<stdlib::map_, TSL<TS<Int>, 2>, TSS<Str>>(
                          fn<AddOneG>(),
                          values<Value>(list_delta<TS<Int>>({1, 2})),
                          arg<"__keys__">(values<Value>(set_delta<Str>({"a"s}, {}))))),
                      std::invalid_argument);
}

// ---------------------------------------------------------------------------
// Name-based key detection (the Python rule): the first parameter named
// "key" (TSD) / "ndx" (TSL) consumes the operator key; __key_arg__ renames,
// "" disables. Keyword-only: __key_arg__ can only be passed by name.
// ---------------------------------------------------------------------------

namespace
{
    struct AddInstrumentG
    {
        static constexpr auto name = "add_instrument_g";
        static Port<TS<Int>>  compose(Wiring &, NamedPort<"instrument", TS<Int>> instrument, Port<TS<Int>> ts)
        {
            using namespace hgraph::stdlib::syntax;
            return (instrument + ts).as<TS<Int>>();
        }
    };
}  // namespace

TEST_CASE("map_: __key_arg__ renames the key parameter")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::map_, TSD<Int, TS<Int>>>(
                     fn<AddInstrumentG>(),
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 10}, {2, 20}})),
                     arg<"__key_arg__">(Str{"instrument"}))),
                 values<Value>(dict_delta<Int, TS<Int>>({{1, 11}, {2, 22}})));
}

TEST_CASE("map_: an empty __key_arg__ disables key consumption by name")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // AddKeyG's first parameter is named "key", but with __key_arg__="" it is
    // an ordinary input: two multiplexed dicts pair per key.
    CHECK_OUTPUT((eval_node<stdlib::map_, TSD<Str, TS<Int>>, TSD<Str, TS<Int>>>(
                     fn<AddKeyG>(),
                     values<Value>(dict_delta<Str, TS<Int>>({{"x"s, 1}})),
                     values<Value>(dict_delta<Str, TS<Int>>({{"x"s, 2}})),
                     arg<"__key_arg__">(Str{""}))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"x"s, 3}})));
}

TEST_CASE("map_: __key_arg__ is keyword-only")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    REQUIRE_THROWS_AS((eval_node<stdlib::map_, TSD<Int, TS<Int>>>(
                          fn<AddInstrumentG>(),
                          values<Value>(dict_delta<Int, TS<Int>>({{1, 10}})),
                          Str{"instrument"})),
                      OperatorResolutionError);
}

// ---------------------------------------------------------------------------
// pass_through / no_key (Python's wrappers): wiring-time argument tags.
// pass_through(x) -> x binds whole (broadcast) whatever its kind;
// no_key(x) -> x demultiplexes as usual but is excluded from key inference.
// The tags adorn Ports, so each test wraps the map_ call in a lightweight
// graph that applies the tag inside compose — eval_node drives the rest.
// ---------------------------------------------------------------------------

namespace
{
    // Reads a whole TSD: only reachable through pass_through (an untagged TSD
    // argument is always multiplexed).
    struct ElemPlusDictSizeNode
    {
        static constexpr auto name = "elem_plus_dict_size";
        static void eval(In<"ts", TS<Int>> ts, In<"d", TSD<Str, TS<Int>>> d, Out<TS<Int>> out)
        {
            out.set(ts.value() + static_cast<Int>(d.size()));
        }
    };

    struct ElemPlusDictSizeG
    {
        static constexpr auto name = "elem_plus_dict_size_g";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts, Port<TSD<Str, TS<Int>>> d)
        {
            return wire<ElemPlusDictSizeNode>(w, ts, d);
        }
    };

    struct MapPassThroughDictG
    {
        static constexpr auto             name = "map_pass_through_dict_g";
        static Port<TSD<Str, TS<Int>>> compose(Wiring &w, Port<TSD<Str, TS<Int>>> mux,
                                               Port<TSD<Str, TS<Int>>> whole)
        {
            return wire<stdlib::map_>(w, fn<ElemPlusDictSizeG>(), mux,
                                      stdlib::pass_through(whole))
                .as<TSD<Str, TS<Int>>>();
        }
    };

    struct MapNoKeyG
    {
        static constexpr auto             name = "map_no_key_g";
        static Port<TSD<Str, TS<Int>>> compose(Wiring &w, Port<TSD<Str, TS<Int>>> lhs,
                                               Port<TSD<Str, TS<Int>>> rhs)
        {
            return wire<stdlib::map_>(w, fn<AddPairG>(), lhs, stdlib::no_key(rhs))
                .as<TSD<Str, TS<Int>>>();
        }
    };

    struct MapAllNoKeyG
    {
        static constexpr auto             name = "map_all_no_key_g";
        static Port<TSD<Str, TS<Int>>> compose(Wiring &w, Port<TSD<Str, TS<Int>>> mux)
        {
            return wire<stdlib::map_>(w, fn<AddOneG>(), stdlib::no_key(mux))
                .as<TSD<Str, TS<Int>>>();
        }
    };

    struct MapAllNoKeyExplicitKeysG
    {
        static constexpr auto             name = "map_all_no_key_explicit_keys_g";
        static Port<TSD<Str, TS<Int>>> compose(Wiring &w, Port<TSD<Str, TS<Int>>> mux,
                                               Port<TSS<Str>> keys)
        {
            return wire<stdlib::map_>(w, fn<AddOneG>(), stdlib::no_key(mux),
                                      arg<"__keys__">(keys))
                .as<TSD<Str, TS<Int>>>();
        }
    };

    // Reads a whole fixed TSL: in the TSL form a same-size TSL would
    // otherwise multiplex per index — pass_through keeps it whole.
    struct ElemPlusListSumNode
    {
        static constexpr auto name = "elem_plus_list_sum";
        static void eval(In<"ts", TS<Int>> ts, In<"l", TSL<TS<Int>, 3>> l, Out<TS<Int>> out)
        {
            Int total = ts.value();
            for (std::size_t i = 0; i < 3; ++i) { total += l[i].value(); }
            out.set(total);
        }
    };

    struct ElemPlusListSumG
    {
        static constexpr auto name = "elem_plus_list_sum_g";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts, Port<TSL<TS<Int>, 3>> l)
        {
            return wire<ElemPlusListSumNode>(w, ts, l);
        }
    };

    struct MapTslPassThroughG
    {
        static constexpr auto              name = "map_tsl_pass_through_g";
        static Port<TSL<TS<Int>, 3>> compose(Wiring &w, Port<TSL<TS<Int>, 3>> mux,
                                             Port<TSL<TS<Int>, 3>> whole)
        {
            return wire<stdlib::map_>(w, fn<ElemPlusListSumG>(), mux,
                                      stdlib::pass_through(whole))
                .as<TSL<TS<Int>, 3>>();
        }
    };

    struct MapTslNoKeyG
    {
        static constexpr auto              name = "map_tsl_no_key_g";
        static Port<TSL<TS<Int>, 3>> compose(Wiring &w, Port<TSL<TS<Int>, 3>> mux)
        {
            return wire<stdlib::map_>(w, fn<AddOneG>(), stdlib::no_key(mux))
                .as<TSL<TS<Int>, 3>>();
        }
    };

    struct MapLiftedAddTslG
    {
        static constexpr auto name = "map_lifted_add_tsl_g";

        static Port<TSL<TS<Int>, 3>> compose(Wiring &w,
                                             Port<TSL<TS<Int>, 3>> lhs,
                                             Port<TSL<TS<Int>, 3>> rhs)
        {
            return wire<stdlib::map_>(w, lift<stdlib::scalar_add<Int>>(), lhs, rhs)
                .as<TSL<TS<Int>, 3>>();
        }
    };

    struct MapLiftedAddBroadcastG
    {
        static constexpr auto name = "map_lifted_add_broadcast_g";

        static Port<TSL<TS<Int>, 3>> compose(Wiring &w, Port<TSL<TS<Int>, 3>> lhs, Port<TS<Int>> rhs)
        {
            return wire<stdlib::map_>(w, lift<stdlib::scalar_add<Int>>(), lhs, rhs)
                .as<TSL<TS<Int>, 3>>();
        }
    };

    struct MapLiftedSubTslG
    {
        static constexpr auto name = "map_lifted_sub_tsl_g";

        static Port<TSL<TS<Int>, 3>> compose(Wiring &w,
                                             Port<TSL<TS<Int>, 3>> lhs,
                                             Port<TSL<TS<Int>, 3>> rhs)
        {
            return wire<stdlib::map_>(w, lift<stdlib::scalar_sub<Int>>(), lhs, rhs)
                .as<TSL<TS<Int>, 3>>();
        }
    };

    struct MapLiftedDivTslG
    {
        static constexpr auto name = "map_lifted_div_tsl_g";

        static Port<TSL<TS<Float>, 3>> compose(Wiring &w,
                                               Port<TSL<TS<Int>, 3>> lhs,
                                               Port<TSL<TS<Int>, 3>> rhs)
        {
            return wire<stdlib::map_>(w, lift<stdlib::scalar_div<Int>>(), lhs, rhs)
                .as<TSL<TS<Float>, 3>>();
        }
    };

    struct MapOperatorDivTslG
    {
        static constexpr auto name = "map_operator_div_tsl_g";

        static Port<TSL<TS<Float>, 3>> compose(Wiring &w,
                                               Port<TSL<TS<Int>, 3>> lhs,
                                               Port<TSL<TS<Int>, 3>> rhs)
        {
            return wire<stdlib::map_>(w, fn<stdlib::div_>(), lhs, rhs)
                .as<TSL<TS<Float>, 3>>();
        }
    };

    struct MapLiftedMinBroadcastG
    {
        static constexpr auto name = "map_lifted_min_broadcast_g";

        static Port<TSL<TS<Int>, 3>> compose(Wiring &w, Port<TSL<TS<Int>, 3>> lhs, Port<TS<Int>> rhs)
        {
            return wire<stdlib::map_>(w, lift<stdlib::scalar_min<Int>>(), lhs, rhs)
                .as<TSL<TS<Int>, 3>>();
        }
    };

    struct MapLiftedAddConstG
    {
        static constexpr auto name = "map_lifted_add_const_g";

        static Port<TSL<TS<Int>, 3>> compose(Wiring &w)
        {
            auto lhs = wire<stdlib::const_, TSL<TS<Int>, 3>>(
                w, stdlib::make_list<Int>({Int{1}, Int{2}, Int{3}}));
            auto rhs = wire<stdlib::const_, TSL<TS<Int>, 3>>(
                w, stdlib::make_list<Int>({Int{10}, Int{20}, Int{30}}));
            return wire<stdlib::map_>(w, lift<stdlib::scalar_add<Int>>(), lhs, rhs)
                .as<TSL<TS<Int>, 3>>();
        }
    };

    struct MapOperatorSubConstG
    {
        static constexpr auto name = "map_operator_sub_const_g";

        static Port<TSL<TS<Int>, 3>> compose(Wiring &w)
        {
            auto lhs = wire<stdlib::const_, TSL<TS<Int>, 3>>(
                w, stdlib::make_list<Int>({Int{10}, Int{20}, Int{30}}));
            auto rhs = wire<stdlib::const_, TSL<TS<Int>, 3>>(
                w, stdlib::make_list<Int>({Int{1}, Int{2}, Int{3}}));
            return wire<stdlib::map_>(w, fn<stdlib::sub_>(), lhs, rhs)
                .as<TSL<TS<Int>, 3>>();
        }
    };
}  // namespace

TEST_CASE("map_: pass_through binds a TSD whole to every child")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // Each child sees the WHOLE second dict (size 3); its keys {p,q,r} do not
    // join the lifecycle key set — only {a,b} from the multiplexed dict map.
    CHECK_OUTPUT((eval_node<MapPassThroughDictG>(
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}})),
                     values<Value>(dict_delta<Str, TS<Int>>({{"p"s, 10}, {"q"s, 20}, {"r"s, 30}})))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 4}, {"b"s, 5}})));
}

TEST_CASE("map_: no_key demultiplexes but is excluded from key inference")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // Keys come from lhs only: {x, y}. x pairs (1 + 10); y's rhs element is
    // absent so its child stays pending; rhs's z never creates a child.
    CHECK_OUTPUT((eval_node<MapNoKeyG>(
                     values<Value>(dict_delta<Str, TS<Int>>({{"x"s, 1}, {"y"s, 2}})),
                     values<Value>(dict_delta<Str, TS<Int>>({{"x"s, 10}, {"z"s, 30}})))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"x"s, 11}})));
}

TEST_CASE("map_: every multiplexed input no_key requires an explicit __keys__")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    REQUIRE_THROWS_AS((eval_node<MapAllNoKeyG>(
                          values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}})))),
                      std::invalid_argument);
}

TEST_CASE("map_: no_key with an explicit __keys__ drives the lifecycle from the key set")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<MapAllNoKeyExplicitKeysG>(
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}, {"c"s, 3}})),
                     values<Value>(set_delta<Str>({"a"s, "c"s}, {})))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 2}, {"c"s, 4}})));
}

TEST_CASE("map_ over TSL: pass_through keeps a same-size TSL whole")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    // Without the tag the second TSL would pair per index (11, 22, 33); with
    // pass_through every index adds the whole list's sum (60).
    CHECK_OUTPUT((eval_node<MapTslPassThroughG>(
                     values<Value>(list_delta<TS<Int>>({1, 2, 3})),
                     values<Value>(list_delta<TS<Int>>({10, 20, 30})))),
                 values<Value>(list_delta<TS<Int>>({61, 62, 63})));
}

TEST_CASE("map_ over TSL: lifted scalar add uses one specialised vector node")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<MapLiftedAddTslG>(
                     values<Value>(list_delta<TS<Int>>({1, 2, 3})),
                     values<Value>(list_delta<TS<Int>>({10, 20, 30})))),
                 values<Value>(list_delta<TS<Int>>({11, 22, 33})));

    CHECK_OUTPUT((eval_node<MapLiftedAddBroadcastG>(
                     values<Value>(list_delta<TS<Int>>({1, 2, 3})),
                     values<Int>(10))),
                 values<Value>(list_delta<TS<Int>>({11, 12, 13})));

    GraphBuilder gb = build_graph<MapLiftedAddConstG>();
    CHECK(gb.node_count() == 3);   // two const sources + one lifted TSL map node

    GraphBuilder operator_fn_gb = build_graph<MapOperatorSubConstG>();
    CHECK(operator_fn_gb.node_count() == 3);   // two const sources + one lifted TSL map node via fn<sub_>
}

TEST_CASE("map_ over TSL: lifted standard kernels cover subtraction division and min")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<MapLiftedSubTslG>(
                     values<Value>(list_delta<TS<Int>>({10, 20, 30})),
                     values<Value>(list_delta<TS<Int>>({1, 2, 3})))),
                 values<Value>(list_delta<TS<Int>>({9, 18, 27})));

    CHECK_OUTPUT((eval_node<MapLiftedDivTslG>(
                     values<Value>(list_delta<TS<Int>>({10, 20, 30})),
                     values<Value>(list_delta<TS<Int>>({2, 5, 4})))),
                 values<Value>(list_delta<TS<Float>>({Float{5.0}, Float{4.0}, Float{7.5}})));

    CHECK_OUTPUT((eval_node<MapOperatorDivTslG>(
                     values<Value>(list_delta<TS<Int>>({10, 20, 30})),
                     values<Value>(list_delta<TS<Int>>({2, 5, 4})))),
                 values<Value>(list_delta<TS<Float>>({Float{5.0}, Float{4.0}, Float{7.5}})));

    CHECK_OUTPUT((eval_node<MapLiftedMinBroadcastG>(
                     values<Value>(list_delta<TS<Int>>({10, 2, 30})),
                     values<Int>(5))),
                 values<Value>(list_delta<TS<Int>>({5, 2, 5})));
}

TEST_CASE("map_ over TSL: no_key is rejected (TSD maps only)")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    REQUIRE_THROWS_AS((eval_node<MapTslNoKeyG>(values<Value>(list_delta<TS<Int>>({1, 2, 3})))),
                      std::invalid_argument);
}

TEST_CASE("map_ publishes keyed nested-graph debugger navigation")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphBuilder builder = build_graph<MapKwargsMixedCollectionGraph>();
    bool found = false;
    for (const NodeBuilder &node : builder.nodes())
    {
        const DebugDescriptor *debug = node.type().record()->debug;
        if (node.type().schema()->node_kind != NodeKind::Nested || debug == nullptr ||
            debug->dynamic_layout == nullptr)
            continue;
        REQUIRE(debug->layout == DebugLayoutKind::Node);
        REQUIRE(debug->key_type != nullptr);
        REQUIRE(debug->element_type != nullptr);
        REQUIRE(debug->dynamic_layout->kind == DebugDynamicKind::StableSlots);
        REQUIRE(has_flag(debug->dynamic_layout->flags, DebugDynamicFlags::HasSlotState));
        REQUIRE(has_flag(debug->dynamic_layout->flags, DebugDynamicFlags::ElementsArePointers));
        REQUIRE(has_flag(debug->dynamic_layout->flags, DebugDynamicFlags::KeysAreOwners));
        found = true;
    }
    REQUIRE(found);
}

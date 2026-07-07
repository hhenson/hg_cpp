#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/operators/impl/json_impl.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/json_codec.h>
#include <hgraph/types/value/value_builder.h>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <string>

// JSON serialization — step 1 of the record/replay/table design record: the
// interned per-schema JsonConverter (serializer-ops pattern) plus the
// to_json/from_json operators. The wire format mirrors the Python
// implementation (ext/main/hgraph/_impl/_operators/_to_json.py).

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    template <typename T>
    std::string round_trip(const T &input)
    {
        Value       value{input};
        std::string text = to_json_string(value.view());
        Value       back = from_json_string(value.view().schema(), text);
        CHECK(back.view().template checked_as<T>() == input);
        return text;
    }
}  // namespace

TEST_CASE("json: atomic values round-trip in the Python wire format")
{
    CHECK(round_trip(Int{42}) == "42");
    CHECK(round_trip(Int{-7}) == "-7");
    CHECK(round_trip(Bool{true}) == "true");
    CHECK(round_trip(Float{2.5}) == "2.5");
    CHECK(round_trip(Str{"plain"}) == "\"plain\"");
    CHECK(round_trip(Str{"quote \" slash \\ tab \t"}) == "\"quote \\\" slash \\\\ tab \\t\"");

    using namespace std::chrono;
    CHECK(round_trip(Date{year{2020}, month{1}, day{31}}) == "\"2020-01-31\"");
    CHECK(round_trip(DateTime{sys_days{Date{year{2021}, month{6}, day{2}}}.time_since_epoch() +
                              microseconds{3'723'000'014}}) ==
          "\"2021-06-02 01:02:03.000014\"");
    CHECK(round_trip(TimeDelta{microseconds{2 * 86'400'000'000 + 3'723'500'000}}) == "\"2:1:2:3.500000\"");
    CHECK(round_trip(time_of_day(9, 30, 5, 250)) == "\"09:30:05.000250\"");
}

TEST_CASE("json: containers round-trip (list, set, map with string and int keys)")
{
    const Value list = stdlib::make_list<Int>({1, 2, 3});
    CHECK(to_json_string(list.view()) == "[1, 2, 3]");
    const Value list_back = from_json_string(list.view().schema(), "[1, 2, 3]");
    CHECK(list_back.view().as_list().size() == 3);
    CHECK(list_back.view().as_list().at(1).checked_as<Int>() == Int{2});

    const Value set = stdlib::make_set<Int>({5});
    CHECK(to_json_string(set.view()) == "[5]");
    const Value set_back = from_json_string(set.view().schema(), "[5, 6]");
    CHECK(set_back.view().as_set().size() == 2);

    const Value map = stdlib::make_map<Str, Int>({{Str{"a"}, Int{1}}});
    CHECK(to_json_string(map.view()) == "{\"a\": 1}");
    const Value map_back = from_json_string(map.view().schema(), "{\"a\": 1, \"b\": 2}");
    CHECK(map_back.view().as_map().size() == 2);

    // Non-string keys render their token quoted (the Python rule).
    const Value int_map = stdlib::make_map<Int, Str>({{Int{42}, Str{"x"}}});
    CHECK(to_json_string(int_map.view()) == "{\"42\": \"x\"}");
    const Value int_map_back = from_json_string(int_map.view().schema(), "{\"42\": \"x\"}");
    CHECK(int_map_back.view().as_map().size() == 1);
}

TEST_CASE("json: bundles serialize as objects; unknown fields are skipped on read")
{
    auto &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *str_meta = registry.register_scalar<Str>("str");
    const auto *bundle_meta = registry.un_named_bundle({{"count", int_meta}, {"label", str_meta}});

    const auto *binding = ValuePlanFactory::instance().binding_for(bundle_meta);
    BundleBuilder builder{*binding};
    builder.set("count", Value{Int{3}});
    builder.set("label", Value{Str{"here"}});
    const Value bundle = builder.build();

    const std::string text = to_json_string(bundle.view());
    CHECK(text == "{\"count\": 3, \"label\": \"here\"}");

    // Field order and unknown fields are tolerated on read.
    const Value back = from_json_string(
        bundle_meta, "{\"label\": \"here\", \"ignored\": [1, {\"x\": 2}], \"count\": 3}");
    CHECK(back.view().as_bundle().at(0).checked_as<Int>() == Int{3});
    CHECK(back.view().as_bundle().at(1).checked_as<Str>() == Str{"here"});
}

TEST_CASE("json: unset bundle fields are omitted on write and null on read")
{
    auto &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *str_meta = registry.register_scalar<Str>("str");
    const auto *bundle_meta = registry.un_named_bundle({{"count", int_meta}, {"label", str_meta}});

    const auto *binding = ValuePlanFactory::instance().binding_for(bundle_meta);
    BundleBuilder builder{*binding};
    builder.set("count", Value{Int{4}});
    const Value partial = builder.build();

    CHECK(to_json_string(partial.view()) == "{\"count\": 4}");

    const Value back = from_json_string(bundle_meta, "{\"count\": 5, \"label\": null}");
    auto        bundle = back.view().as_bundle();
    CHECK(bundle.at("count").checked_as<Int>() == Int{5});
    CHECK_FALSE(bundle.at("label").has_value());
}

namespace
{
    struct FromJsonGraph
    {
        [[maybe_unused]] static constexpr auto name = "from_json_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Str>> ts)
        {
            return wire<stdlib::from_json, TS<Int>>(w, ts).as<TS<Int>>();
        }
    };

    struct JsonRoundTripGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_round_trip_graph";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> ts)
        {
            auto text = wire<stdlib::to_json>(w, ts);
            return wire<stdlib::from_json, TS<Float>>(w, text).as<TS<Float>>();
        }
    };

    struct JsonDynamicLeafGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_dynamic_leaf_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Str>> ts)
        {
            auto decoded = wire<stdlib::json_decode>(w, ts);
            auto nested  = wire<stdlib::getitem_>(w, decoded, Str{"nested"});
            auto values  = wire<stdlib::getitem_>(w, nested, Str{"values"});
            auto last    = wire<stdlib::getitem_>(w, values, Int{-1});
            return wire<stdlib::json_as_int>(w, last).as<TS<Int>>();
        }
    };

    struct JsonDynamicEncodeGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_dynamic_encode_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> ts)
        {
            auto decoded = wire<stdlib::json_decode>(w, ts);
            return wire<stdlib::json_encode, TS<Str>>(w, decoded).as<TS<Str>>();
        }
    };

    struct JsonDynamicEqualityGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_dynamic_equality_graph";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Str>> lhs, Port<TS<Str>> rhs)
        {
            auto decoded_lhs = wire<stdlib::json_decode>(w, lhs);
            auto decoded_rhs = wire<stdlib::json_decode>(w, rhs);
            return wire<stdlib::eq_>(w, decoded_lhs, decoded_rhs).as<TS<Bool>>();
        }
    };

    [[nodiscard]] Value eager_reference_json()
    {
        const auto &str_binding = *ValuePlanFactory::instance().binding_for(scalar_descriptor<Str>::value_meta());

        MapBuilder target_entries{str_binding, stdlib::json_tree::json_value_binding()};
        Value      answer_key{Str{"answer"}};
        Value      answer_node = stdlib::json_tree::box(Value{Int{41}});
        target_entries.set_item_copy(answer_key.view().data(), answer_node.view().data());

        MapBuilder root_entries{str_binding, stdlib::json_tree::json_value_binding()};
        Value      target_key{Str{"target"}};
        Value      target_node = stdlib::json_tree::box(target_entries.build());
        root_entries.set_item_copy(target_key.view().data(), target_node.view().data());
        return stdlib::json_tree::box(root_entries.build());
    }

    struct EagerJsonReferenceNode
    {
        static constexpr auto name = "eager_json_reference_node";

        static void resolve_default_types(ResolutionMap &resolution)
        {
            if (resolution.find_ts("O") != nullptr) { return; }
            resolution.bind_ts("O", TypeRegistry::instance().ts(stdlib::json_tree::json_meta()));
        }

        static void eval(In<"trigger", TS<Str>> trigger, Out<TsVar<"O">> out)
        {
            static_cast<void>(trigger);
            stdlib::json_tree::publish(static_cast<const TSOutputView &>(out), eager_reference_json());
        }
    };

    struct JsonDynamicLazyEagerEqualityGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_dynamic_lazy_eager_equality_graph";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Str>> raw)
        {
            auto decoded = wire<stdlib::json_decode>(w, raw);
            auto eager   = wire<EagerJsonReferenceNode>(w, raw);
            return wire<stdlib::eq_>(w, decoded, eager).as<TS<Bool>>();
        }
    };

    struct JsonDynamicLazyEagerInequalityGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_dynamic_lazy_eager_inequality_graph";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Str>> raw)
        {
            auto decoded = wire<stdlib::json_decode>(w, raw);
            auto eager   = wire<EagerJsonReferenceNode>(w, raw);
            return wire<stdlib::ne_>(w, decoded, eager).as<TS<Bool>>();
        }
    };

    struct JsonDynamicLazyEagerCompareGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_dynamic_lazy_eager_compare_graph";

        static Port<TS<stdlib::CmpResult>> compose(Wiring &w, Port<TS<Str>> raw)
        {
            auto decoded = wire<stdlib::json_decode>(w, raw);
            auto eager   = wire<EagerJsonReferenceNode>(w, raw);
            return wire<stdlib::cmp_>(w, decoded, eager).as<TS<stdlib::CmpResult>>();
        }
    };
}  // namespace

TEST_CASE("json operators: to_json serializes per tick")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::to_json>(values<Int>(42, none, -1)),
                 values<Str>(Str{"42"}, none, Str{"-1"}));
    CHECK_OUTPUT(eval_node<stdlib::to_json>(values<Str>(Str{"a\"b"})), values<Str>(Str{"\"a\\\"b\""}));
    // The delta of a TS<scalar> IS its value: delta mode matches value mode.
    CHECK_OUTPUT(eval_node<stdlib::to_json>(values<Int>(7), true), values<Str>(Str{"7"}));
}

TEST_CASE("json operators: from_json parses into the resolved output type")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<FromJsonGraph>(values<Str>(Str{"3"}, none, Str{"-9"})),
                 values<Int>(3, none, -9));
}

TEST_CASE("json operators: to_json -> from_json round-trips through a graph")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<JsonRoundTripGraph>(values<Float>(1.5, none, -0.25)),
                 values<Float>(1.5, none, -0.25));
}

TEST_CASE("dynamic json operators: decoded values support lazy path extraction")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<JsonDynamicLeafGraph>(
                     values<Str>(Str{"{\"nested\":{\"values\":[1,2,3,4]}}"},
                                 Str{"{\"nested\":{\"values\":[5,6]}}"})),
                 values<Int>(4, 6));
}

TEST_CASE("dynamic json operators: decoded values encode canonically")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<JsonDynamicEncodeGraph>(
                     values<Str>(Str{"{\"a\":1,\"b\":[true,null,\"x\"]}"})),
                 values<Str>(Str{"{\"a\": 1, \"b\": [true, null, \"x\"]}"}));
}

TEST_CASE("dynamic json operators: equality is semantic not raw string equality")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<JsonDynamicEqualityGraph>(
                     values<Str>(Str{"{\"a\":1,\"b\":[2,3]}"}),
                     values<Str>(Str{"{ \"b\" : [2, 3], \"a\" : 1 }"})),
                 values<Bool>(true));
}

TEST_CASE("dynamic json operators: equality is independent of lazy or eager storage")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<JsonDynamicLazyEagerEqualityGraph>(
                     values<Str>(Str{"{\"target\":{\"answer\":41}}"},
                                 Str{"{\"target\":{\"answer\":42}}"})),
                 values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<JsonDynamicLazyEagerInequalityGraph>(
                     values<Str>(Str{"{\"target\":{\"answer\":41}}"},
                                 Str{"{\"target\":{\"answer\":42}}"})),
                 values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<JsonDynamicLazyEagerCompareGraph>(
                     values<Str>(Str{"{\"target\":{\"answer\":41}}"})),
                 values<stdlib::CmpResult>(stdlib::CmpResult::EQ));
}

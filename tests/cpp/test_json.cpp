#include <hgraph/lib/std/std_operators.h>
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

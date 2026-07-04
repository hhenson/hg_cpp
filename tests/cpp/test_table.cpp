#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/value/table_codec.h>
#include <hgraph/types/value/value_builder.h>

#include <arrow/api.h>

#include <catch2/catch_test_macros.hpp>

// Step 3 of the record/replay/table design record: the Arrow-backed Frame
// value kind + the interned per-schema TableConverter (bitemporal
// [date, as_of, *columns] rows written directly into Arrow builders) and the
// to_table / from_table operators.

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    [[nodiscard]] std::int64_t timestamp_at(const Frame &frame, const std::string &column, std::int64_t row)
    {
        const auto chunked = frame.table->GetColumnByName(column);
        REQUIRE(chunked != nullptr);
        return static_cast<const arrow::TimestampArray &>(*chunked->chunk(0)).Value(row);
    }
}  // namespace

TEST_CASE("table codec: an atomic value round-trips through a bitemporal single-row frame")
{
    const auto &registry = TypeRegistry::instance();
    static_cast<void>(registry);
    const auto &converter = table_converter(scalar_descriptor<Int>::value_meta());

    CHECK(converter.date_key == "__date_time__");
    CHECK(converter.as_of_key == "__as_of__");
    REQUIRE(converter.columns.size() == 1);
    CHECK(converter.columns[0].name == "value");
    CHECK(converter.arrow_schema->num_fields() == 3);

    const Value value{Int{42}};
    const Frame frame = single_row_frame(converter, MIN_ST, MIN_ST + TimeDelta{5}, value.view());
    REQUIRE(frame_rows(frame) == 1);
    CHECK(timestamp_at(frame, "__date_time__", 0) == MIN_ST.time_since_epoch().count());
    CHECK(timestamp_at(frame, "__as_of__", 0) == (MIN_ST + TimeDelta{5}).time_since_epoch().count());

    const Value back = read_row(converter, frame, 0);
    CHECK(back.view().checked_as<Int>() == Int{42});
}

TEST_CASE("table codec: a depth-1 bundle flattens to named columns and round-trips")
{
    auto &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *str_meta = registry.register_scalar<Str>("str");
    const auto *bundle_meta = registry.un_named_bundle({{"qty", int_meta}, {"symbol", str_meta}});

    const auto &converter = table_converter(bundle_meta);
    REQUIRE(converter.columns.size() == 2);
    CHECK(converter.columns[0].name == "qty");
    CHECK(converter.columns[1].name == "symbol");

    const auto *binding = ValuePlanFactory::instance().binding_for(bundle_meta);
    BundleBuilder builder{*binding};
    builder.set("qty", Value{Int{7}});
    builder.set("symbol", Value{Str{"VOD"}});
    const Value value = builder.build();

    const Frame frame = single_row_frame(converter, MIN_ST, MIN_ST, value.view());
    const Value back  = read_row(converter, frame, 0);
    CHECK(back.view().as_bundle().at(0).checked_as<Int>() == Int{7});
    CHECK(back.view().as_bundle().at(1).checked_as<Str>() == Str{"VOD"});
}

TEST_CASE("table codec: input schemas are a minimum - extra columns pass, missing columns throw")
{
    auto &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *str_meta = registry.register_scalar<Str>("str");
    const auto *wide_meta   = registry.un_named_bundle({{"qty", int_meta}, {"symbol", str_meta}});
    const auto *narrow_meta = registry.un_named_bundle({{"qty", int_meta}});
    const auto *other_meta  = registry.un_named_bundle({{"price", int_meta}});

    const auto *binding = ValuePlanFactory::instance().binding_for(wide_meta);
    BundleBuilder builder{*binding};
    builder.set("qty", Value{Int{9}});
    builder.set("symbol", Value{Str{"BP"}});
    const Frame wide = single_row_frame(table_converter(wide_meta), MIN_ST, MIN_ST, builder.build().view());

    // Narrow reader over a wide frame: the extra column is ignored.
    const Value narrow = read_row(table_converter(narrow_meta), wide, 0);
    CHECK(narrow.view().as_bundle().at(0).checked_as<Int>() == Int{9});

    // A required column that is absent throws.
    CHECK_THROWS_AS((void)read_row(table_converter(other_meta), wide, 0), std::invalid_argument);
}

namespace
{
    struct TableRoundTripGraph
    {
        [[maybe_unused]] static constexpr auto name = "table_round_trip_graph";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> ts)
        {
            auto frame = wire<stdlib::to_table>(w, ts);
            return wire<stdlib::from_table, TS<Float>>(w, frame).as<TS<Float>>();
        }
    };
}  // namespace

TEST_CASE("table operators: to_table emits one bitemporal row per tick")
{
    stdlib::register_standard_operators();

    // eval_node<Operator> returns type-erased Values; unwrap the Frame scalar.
    auto frames = eval_node<stdlib::to_table>(values<Int>(42, none, -1));
    REQUIRE(frames.size() == 3);
    REQUIRE(frames[0].has_value());
    CHECK_FALSE(frames[1].has_value());
    REQUIRE(frames[2].has_value());

    const Frame &first = frames[0]->view().checked_as<Frame>();
    const Frame &last  = frames[2]->view().checked_as<Frame>();
    CHECK(frame_rows(first) == 1);
    const auto &converter = table_converter(scalar_descriptor<Int>::value_meta());
    CHECK(read_row(converter, first, 0).view().checked_as<Int>() == Int{42});
    CHECK(read_row(converter, last, 0).view().checked_as<Int>() == Int{-1});
    // The date column carries the tick's evaluation time (cycle 2 = start + 2 ticks).
    CHECK(timestamp_at(last, "__date_time__", 0) == (MIN_ST + TimeDelta{2}).time_since_epoch().count());
}

TEST_CASE("table operators: to_table honours the configured as_of override")
{
    stdlib::register_standard_operators();
    const DateTime fixed = MIN_ST + TimeDelta{123456};
    record_replay::set_config(record_replay::Config{.as_of = fixed});

    auto frames = eval_node<stdlib::to_table>(values<Int>(1));
    REQUIRE(frames[0].has_value());
    CHECK(timestamp_at(frames[0]->view().checked_as<Frame>(), "__as_of__", 0) ==
          fixed.time_since_epoch().count());
}

TEST_CASE("table operators: to_table -> from_table round-trips through a graph")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<TableRoundTripGraph>(values<Float>(1.5, none, -0.25)),
                 values<Float>(1.5, none, -0.25));
}

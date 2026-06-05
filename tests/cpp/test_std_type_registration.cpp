#include <catch2/catch_test_macros.hpp>

#include <hgraph/lib/std/standard_types.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/util/date_time.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <type_traits>

TEST_CASE("stdlib::register_standard_types registers scalar aliases")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto  types    = stdlib::register_standard_types(registry);

    CHECK(registry.value_type("bool") == types.bool_type);
    CHECK(registry.value_type("int") == types.int_type);
    CHECK(registry.value_type("int64") == types.int_type);
    CHECK(registry.value_type("float") == types.float_type);
    CHECK(registry.value_type("float64") == types.float_type);
    CHECK(registry.value_type("double") == types.float_type);
    CHECK(registry.value_type("date") == types.date_type);
    CHECK(registry.value_type("datetime") == types.datetime_type);
    CHECK(registry.value_type("timedelta") == types.timedelta_type);
    CHECK(registry.value_type("str") == types.str_type);
    CHECK(registry.value_type("string") == types.str_type);

    CHECK(registry.scalar_binding<std::int64_t>()->type_meta == types.int_type);
    CHECK(registry.scalar_binding<double>()->type_meta == types.float_type);
    CHECK(registry.scalar_binding<engine_date_t>()->type_meta == types.date_type);
    CHECK(registry.scalar_binding<engine_time_t>()->type_meta == types.datetime_type);
    CHECK(registry.scalar_binding<engine_time_delta_t>()->type_meta == types.timedelta_type);
    CHECK(registry.scalar_binding<std::string>()->type_meta == types.str_type);

    CHECK(registry.value_type("int8") == types.int8_type);
    CHECK(registry.value_type("int16") == types.int16_type);
    CHECK(registry.value_type("int32") == types.int32_type);
    CHECK(registry.value_type("uint8") == types.uint8_type);
    CHECK(registry.value_type("uint16") == types.uint16_type);
    CHECK(registry.value_type("uint32") == types.uint32_type);
    CHECK(registry.value_type("uint64") == types.uint64_type);
    CHECK(registry.value_type("float32") == types.float32_type);
}

TEST_CASE("stdlib::register_standard_types registers common TS and TSS aliases")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto  types    = stdlib::register_standard_types(registry);

    CHECK(registry.time_series_type("TS[bool]") == types.ts_bool);
    CHECK(registry.time_series_type("TS[int]") == types.ts_int);
    CHECK(registry.time_series_type("TS[int64]") == types.ts_int);
    CHECK(registry.time_series_type("TS[float]") == types.ts_float);
    CHECK(registry.time_series_type("TS[double]") == types.ts_float);
    CHECK(registry.time_series_type("TS[date]") == types.ts_date);
    CHECK(registry.time_series_type("TS[datetime]") == types.ts_datetime);
    CHECK(registry.time_series_type("TS[timedelta]") == types.ts_timedelta);
    CHECK(registry.time_series_type("TS[str]") == types.ts_str);
    CHECK(registry.time_series_type("TS[string]") == types.ts_str);

    CHECK(registry.time_series_type("TSS[bool]") == types.tss_bool);
    CHECK(registry.time_series_type("TSS[int]") == types.tss_int);
    CHECK(registry.time_series_type("TSS[int64]") == types.tss_int);
    CHECK(registry.time_series_type("TSS[float]") == types.tss_float);
    CHECK(registry.time_series_type("TSS[double]") == types.tss_float);
    CHECK(registry.time_series_type("TSS[date]") == types.tss_date);
    CHECK(registry.time_series_type("TSS[datetime]") == types.tss_datetime);
    CHECK(registry.time_series_type("TSS[timedelta]") == types.tss_timedelta);
    CHECK(registry.time_series_type("TSS[str]") == types.tss_str);
    CHECK(registry.time_series_type("TSS[string]") == types.tss_str);

    CHECK(types.ts_int == registry.ts(types.int_type));
    CHECK(types.tss_int == registry.tss(types.int_type));
    CHECK(types.ts_datetime->value_schema == types.datetime_type);
    CHECK(types.tss_datetime->value_schema == registry.set(types.datetime_type));

    CHECK(registry.time_series_type("TS[int8]") == registry.ts(types.int8_type));
    CHECK(registry.time_series_type("TSS[uint32]") == registry.tss(types.uint32_type));
    CHECK(registry.time_series_type("TS[float32]") == registry.ts(types.float32_type));
}

TEST_CASE("stdlib::register_standard_types is idempotent")
{
    using namespace hgraph;

    const auto first  = stdlib::register_standard_types();
    const auto second = stdlib::register_standard_types();

    CHECK(first.int_type == second.int_type);
    CHECK(first.ts_int == second.ts_int);
    CHECK(first.tss_str == second.tss_str);
    CHECK(TypeRegistry::instance().value_type("int") == first.int_type);
    CHECK(TypeRegistry::instance().time_series_type("TS[str]") == first.ts_str);
}

TEST_CASE("date-time aliases and constants match the 2603 runtime definitions")
{
    using namespace hgraph;

    STATIC_REQUIRE(std::is_same_v<engine_clock, std::chrono::system_clock>);
    STATIC_REQUIRE(std::is_same_v<engine_time_t,
                                  std::chrono::time_point<engine_clock, std::chrono::microseconds>>);
    STATIC_REQUIRE(std::is_same_v<engine_time_delta_t, std::chrono::microseconds>);
    STATIC_REQUIRE(std::is_same_v<engine_date_t, std::chrono::year_month_day>);

    CHECK(MIN_DT == min_time());
    CHECK(MAX_DT == max_time());
    CHECK(MIN_ST == min_start_time());
    CHECK(MAX_ET == max_end_time());
    CHECK(MIN_TD == smallest_time_increment());

    CHECK(std::string{scalar_descriptor<engine_date_t>::value_meta()->name()} == "date");
    CHECK(std::string{scalar_descriptor<engine_time_t>::value_meta()->name()} == "datetime");
    CHECK(std::string{scalar_descriptor<engine_time_delta_t>::value_meta()->name()} == "timedelta");
}

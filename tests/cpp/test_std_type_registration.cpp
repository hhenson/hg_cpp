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

    STATIC_REQUIRE(std::is_same_v<Bool, bool>);
    STATIC_REQUIRE(std::is_same_v<Int, std::int64_t>);
    STATIC_REQUIRE(std::is_same_v<Float, double>);
    STATIC_REQUIRE(std::is_same_v<Str, std::string>);

    const auto *bool_type      = registry.scalar_binding<Bool>()->type_meta;
    const auto *int_type       = registry.scalar_binding<Int>()->type_meta;
    const auto *float_type     = registry.scalar_binding<Float>()->type_meta;
    const auto *date_type      = registry.scalar_binding<engine_date_t>()->type_meta;
    const auto *datetime_type  = registry.scalar_binding<engine_time_t>()->type_meta;
    const auto *timedelta_type = registry.scalar_binding<engine_time_delta_t>()->type_meta;
    const auto *str_type       = registry.scalar_binding<Str>()->type_meta;

    CHECK(registry.value_type("bool") == bool_type);
    CHECK(registry.value_type("int") == int_type);
    CHECK(registry.value_type("int64") == int_type);
    CHECK(registry.value_type("float") == float_type);
    CHECK(registry.value_type("float64") == float_type);
    CHECK(registry.value_type("double") == float_type);
    CHECK(registry.value_type("date") == date_type);
    CHECK(registry.value_type("datetime") == datetime_type);
    CHECK(registry.value_type("timedelta") == timedelta_type);
    CHECK(registry.value_type("str") == str_type);
    CHECK(registry.value_type("string") == str_type);

    CHECK(std::string{bool_type->name()} == "bool");
    CHECK(std::string{int_type->name()} == "int");
    CHECK(std::string{float_type->name()} == "float");
    CHECK(std::string{str_type->name()} == "str");

    CHECK(scalar_descriptor<Bool>::value_meta() == bool_type);
    CHECK(scalar_descriptor<Int>::value_meta() == int_type);
    CHECK(scalar_descriptor<Float>::value_meta() == float_type);
    CHECK(scalar_descriptor<Str>::value_meta() == str_type);

    CHECK(registry.value_type("int8") == registry.scalar_binding<std::int8_t>()->type_meta);
    CHECK(registry.value_type("int16") == registry.scalar_binding<std::int16_t>()->type_meta);
    CHECK(registry.value_type("int32") == registry.scalar_binding<std::int32_t>()->type_meta);
    CHECK(registry.value_type("uint8") == registry.scalar_binding<std::uint8_t>()->type_meta);
    CHECK(registry.value_type("uint16") == registry.scalar_binding<std::uint16_t>()->type_meta);
    CHECK(registry.value_type("uint32") == registry.scalar_binding<std::uint32_t>()->type_meta);
    CHECK(registry.value_type("uint64") == registry.scalar_binding<std::uint64_t>()->type_meta);
    CHECK(registry.value_type("float32") == registry.scalar_binding<float>()->type_meta);

    const auto types = stdlib::register_standard_types();
    CHECK(types.bool_type == bool_type);
    CHECK(types.int_type == int_type);
    CHECK(types.int64_type == int_type);
    CHECK(types.float_type == float_type);
    CHECK(types.float64_type == float_type);
    CHECK(types.str_type == str_type);
    CHECK(types.int32_type != int_type);
    CHECK(types.float32_type != float_type);
}

TEST_CASE("stdlib::register_standard_types registers common TS and TSS aliases")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *bool_type      = registry.value_type("bool");
    const auto *int_type       = registry.value_type("int");
    const auto *float_type     = registry.value_type("float");
    const auto *date_type      = registry.value_type("date");
    const auto *datetime_type  = registry.value_type("datetime");
    const auto *timedelta_type = registry.value_type("timedelta");
    const auto *str_type       = registry.value_type("str");

    CHECK(registry.time_series_type("TS[bool]") == registry.ts(bool_type));
    CHECK(registry.time_series_type("TS[int]") == registry.ts(int_type));
    CHECK(registry.time_series_type("TS[int64]") == registry.ts(int_type));
    CHECK(registry.time_series_type("TS[float]") == registry.ts(float_type));
    CHECK(registry.time_series_type("TS[float64]") == registry.ts(float_type));
    CHECK(registry.time_series_type("TS[double]") == registry.ts(float_type));
    CHECK(registry.time_series_type("TS[date]") == registry.ts(date_type));
    CHECK(registry.time_series_type("TS[datetime]") == registry.ts(datetime_type));
    CHECK(registry.time_series_type("TS[timedelta]") == registry.ts(timedelta_type));
    CHECK(registry.time_series_type("TS[str]") == registry.ts(str_type));
    CHECK(registry.time_series_type("TS[string]") == registry.ts(str_type));

    CHECK(registry.time_series_type("TSS[bool]") == registry.tss(bool_type));
    CHECK(registry.time_series_type("TSS[int]") == registry.tss(int_type));
    CHECK(registry.time_series_type("TSS[int64]") == registry.tss(int_type));
    CHECK(registry.time_series_type("TSS[float]") == registry.tss(float_type));
    CHECK(registry.time_series_type("TSS[float64]") == registry.tss(float_type));
    CHECK(registry.time_series_type("TSS[double]") == registry.tss(float_type));
    CHECK(registry.time_series_type("TSS[date]") == registry.tss(date_type));
    CHECK(registry.time_series_type("TSS[datetime]") == registry.tss(datetime_type));
    CHECK(registry.time_series_type("TSS[timedelta]") == registry.tss(timedelta_type));
    CHECK(registry.time_series_type("TSS[str]") == registry.tss(str_type));
    CHECK(registry.time_series_type("TSS[string]") == registry.tss(str_type));

    CHECK(registry.time_series_type("TS[datetime]")->value_schema == datetime_type);
    CHECK(registry.time_series_type("TSS[datetime]")->value_schema == registry.set(datetime_type));

    CHECK(registry.time_series_type("TS[int8]") == registry.ts(registry.value_type("int8")));
    CHECK(registry.time_series_type("TS[int32]") == registry.ts(registry.value_type("int32")));
    CHECK(registry.time_series_type("TSS[uint32]") == registry.tss(registry.value_type("uint32")));
    CHECK(registry.time_series_type("TS[float32]") == registry.ts(registry.value_type("float32")));

    const auto types = stdlib::register_standard_types();
    CHECK(types.ts_bool == registry.time_series_type("TS[bool]"));
    CHECK(types.ts_int == registry.time_series_type("TS[int]"));
    CHECK(types.ts_float == registry.time_series_type("TS[float]"));
    CHECK(types.ts_str == registry.time_series_type("TS[str]"));
    CHECK(types.tss_bool == registry.time_series_type("TSS[bool]"));
    CHECK(types.tss_int == registry.time_series_type("TSS[int]"));
    CHECK(types.tss_float == registry.time_series_type("TSS[float]"));
    CHECK(types.tss_str == registry.time_series_type("TSS[str]"));
    CHECK(types.ts_int == registry.time_series_type("TS[int64]"));
    CHECK(types.ts_float == registry.time_series_type("TS[float64]"));
    CHECK(types.ts_str == registry.time_series_type("TS[string]"));
    CHECK(registry.time_series_type("TS[int32]")->value_schema == types.int32_type);
    CHECK(registry.time_series_type("TS[int32]") != types.ts_int);
}

TEST_CASE("stdlib::register_standard_types is idempotent")
{
    using namespace hgraph;

    const auto *existing_int = TypeRegistry::instance().value_type("int");
    const auto *existing_str = TypeRegistry::instance().value_type("str");

    const auto first  = stdlib::register_standard_types();
    const auto second = stdlib::register_standard_types();

    CHECK(first.int_type == existing_int);
    CHECK(first.str_type == existing_str);
    CHECK(first.int_type == second.int_type);
    CHECK(first.float_type == second.float_type);
    CHECK(first.bool_type == second.bool_type);
    CHECK(first.ts_int == second.ts_int);
    CHECK(first.ts_float == second.ts_float);
    CHECK(first.tss_str == second.tss_str);
    CHECK(TypeRegistry::instance().value_type("int") == first.int_type);
    CHECK(TypeRegistry::instance().value_type("int64") == first.int_type);
    CHECK(TypeRegistry::instance().value_type("float64") == first.float_type);
    CHECK(TypeRegistry::instance().value_type("string") == first.str_type);
    CHECK(TypeRegistry::instance().time_series_type("TS[str]") == first.ts_str);
    CHECK(TypeRegistry::instance().time_series_type("TS[string]") == first.ts_str);
    CHECK(TypeRegistry::instance().time_series_type("TSS[int64]") == first.tss_int);
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

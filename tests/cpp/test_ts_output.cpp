#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/date_time.h>

#include <catch2/catch_test_macros.hpp>

#include <stdexcept>
#include <string>

namespace
{
    template <typename Range>
    std::size_t range_count(const Range &range)
    {
        std::size_t count = 0;
        for (const auto _ : range)
        {
            (void)_;
            ++count;
        }
        return count;
    }
}

TEST_CASE("TSOutput owns root TSData and exposes TS validity")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    TSOutput output{*ts_int};
    REQUIRE(output.has_value());
    REQUIRE(output.schema() == ts_int);
    REQUIRE_FALSE(output.dirty());

    const auto t1 = MIN_ST;
    const auto t2 = t1 + engine_time_delta_t{1};

    auto initial = output.view(t1);
    REQUIRE(initial.bound());
    REQUIRE_FALSE(initial.valid());
    REQUIRE_FALSE(initial.all_valid());
    REQUIRE_FALSE(initial.modified());
    REQUIRE(initial.last_modified_time() == MIN_DT);
    REQUIRE(initial.value().checked_as<int>() == 0);
    REQUIRE_FALSE(initial.delta_value().has_value());

    Value forty_two{42};
    {
        auto mutation = output.begin_mutation(t1);
        REQUIRE(mutation.current_mutation_time() == t1);
        REQUIRE(mutation.copy_value_from(forty_two.view()));
        REQUIRE(mutation.modified());
        REQUIRE(output.dirty());
    }

    auto modified = output.view(t1);
    REQUIRE(modified.valid());
    REQUIRE(modified.all_valid());
    REQUIRE(modified.modified());
    REQUIRE_FALSE(modified.modified(t2));
    REQUIRE(modified.last_modified_time() == t1);
    REQUIRE(modified.value().checked_as<int>() == 42);
    REQUIRE(modified.delta_value().checked_as<int>() == 42);
    REQUIRE_FALSE(output.view(t2).delta_value().has_value());

    output.cleanup_delta();
    REQUIRE_FALSE(output.dirty());
}

TEST_CASE("TSOutput dirty cleanup finalizes slot deltas")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *tss      = registry.tss(int_meta);

    TSOutput output{*tss};
    auto     root = output.data_view();
    auto     set = root.as_set();

    const auto t1 = MIN_ST;
    const auto t2 = t1 + engine_time_delta_t{1};
    Value      one{1};

    {
        auto mutation = set.begin_mutation(t1);
        REQUIRE(mutation.add(one.view()));
    }

    REQUIRE(output.dirty());
    REQUIRE(range_count(set.added()) == 1);
    output.cleanup_delta();
    REQUIRE_FALSE(output.dirty());
    REQUIRE(range_count(set.added()) == 0);
    REQUIRE(set.contains(one.view()));

    {
        auto mutation = set.begin_mutation(t2);
        REQUIRE(mutation.remove(one.view()));
    }

    REQUIRE(output.dirty());
    REQUIRE(range_count(set.removed()) == 1);
    output.cleanup_delta();
    REQUIRE_FALSE(output.dirty());
    REQUIRE(range_count(set.removed()) == 0);
    REQUIRE_FALSE(set.contains(one.view()));
    REQUIRE_THROWS_AS(output.begin_mutation(MIN_DT), std::invalid_argument);
}

TEST_CASE("TSOutput root parent is reattached after copy and move")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    const auto t1 = MIN_ST;
    const auto t2 = t1 + engine_time_delta_t{1};
    const auto t3 = t2 + engine_time_delta_t{1};

    Value one{1};
    Value two{2};
    Value three{3};

    TSOutput source{*ts_int};
    {
        auto mutation = source.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(one.view()));
    }
    REQUIRE(source.dirty());
    source.cleanup_delta();

    TSOutput copied{source};
    REQUIRE(copied.data_view().has_parent());
    {
        auto mutation = copied.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(two.view()));
    }
    REQUIRE(copied.dirty());
    REQUIRE_FALSE(source.dirty());
    copied.cleanup_delta();

    TSOutput moved{std::move(copied)};
    REQUIRE(moved.data_view().has_parent());
    {
        auto mutation = moved.begin_mutation(t3);
        REQUIRE(mutation.copy_value_from(three.view()));
    }
    REQUIRE(moved.dirty());
}

TEST_CASE("TSOutputView all_valid recurses through fixed bundle children")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsb      = registry.tsb("TSOutputAllValidBundle", {{"a", ts_int}, {"b", ts_int}});

    TSOutput output{*tsb};

    const auto t1 = MIN_ST;
    Value      one{1};
    Value      two{2};

    auto root = output.data_view();
    auto bundle = root.as_bundle();
    auto a = bundle.field("a");
    auto b = bundle.field("b");

    {
        auto mutation = a.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(one.view()));
    }

    REQUIRE(output.dirty());

    auto partially_valid = output.view(t1);
    REQUIRE(partially_valid.valid());
    REQUIRE_FALSE(partially_valid.all_valid());

    {
        auto mutation = b.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(two.view()));
    }

    auto fully_valid = output.view(t1);
    REQUIRE(fully_valid.valid());
    REQUIRE(fully_valid.all_valid());

    auto fully_valid_bundle = fully_valid.as_bundle();
    REQUIRE(fully_valid_bundle.field("a").value().checked_as<int>() == 1);
    REQUIRE(fully_valid_bundle.field("b").value().checked_as<int>() == 2);
}

TEST_CASE("TSOutputView delegates validity through slot TSData ops")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *tss      = registry.tss(int_meta);
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsd      = registry.tsd(int_meta, ts_int);

    const auto t1 = MIN_ST;
    Value      one{1};

    TSOutput set_output{*tss};
    REQUIRE(set_output.view(t1).binding() == set_output.binding());
    REQUIRE_FALSE(set_output.view(t1).valid());
    REQUIRE_FALSE(set_output.view(t1).all_valid());

    auto set_root = set_output.data_view();
    auto set      = set_root.as_set();
    {
        auto mutation = set.begin_mutation(t1);
        REQUIRE(mutation.add(one.view()));
    }

    REQUIRE(set_output.view(t1).valid());
    REQUIRE(set_output.view(t1).all_valid());

    TSOutput dict_output{*tsd};
    auto     dict_root = dict_output.data_view();
    auto     dict      = dict_root.as_dict();
    Value    key{7};
    Value    value{42};

    {
        auto mutation = dict.begin_mutation(t1);
        static_cast<void>(mutation.at(key.view()));
    }

    REQUIRE(dict_output.view(t1).valid());
    REQUIRE_FALSE(dict_output.view(t1).all_valid());

    {
        auto child = dict.at(key.view());
        auto mutation = child.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(value.view()));
    }

    REQUIRE(dict_output.view(t1).all_valid());
}

TEST_CASE("TSOutputView delegates window all_valid through TSData ops")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *tsw      = registry.tsw(int_meta, 2, 2);

    const auto t1 = MIN_ST;
    const auto t2 = t1 + engine_time_delta_t{1};
    Value      one{1};
    Value      two{2};

    TSOutput output{*tsw};
    auto     root = output.data_view();
    auto     window = root.as_window();

    {
        auto mutation = window.begin_mutation(t1);
        mutation.push(one.view());
    }

    REQUIRE(output.view(t1).valid());
    REQUIRE_FALSE(output.view(t1).all_valid());

    {
        auto mutation = window.begin_mutation(t2);
        mutation.push(two.view());
    }

    REQUIRE(output.view(t2).valid());
    REQUIRE(output.view(t2).all_valid());
}

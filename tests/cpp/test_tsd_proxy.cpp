#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/value/value.h>

#include <cstddef>
#include <stdexcept>

namespace
{
    template <typename Range>
    std::size_t range_count(const Range &range)
    {
        std::size_t count = 0;
        for (auto it = range.begin(); it != range.end(); ++it) { ++count; }
        return count;
    }

    void key_value_builder(hgraph::TSDProxy      &proxy,
                           std::size_t           slot,
                           const hgraph::TSDataView &target,
                           const hgraph::TSDataView &source,
                           hgraph::DateTime modified_time,
                           const void           *)
    {
        (void)source;
        const auto key = proxy.source_dict().key_at_slot(slot);
        auto       mutation = target.begin_mutation(modified_time);
        if (!mutation.copy_value_from(key))
        {
            throw std::logic_error("TSDProxy test value builder did not materialise the source key");
        }
    }
}

TEST_CASE("TSDProxy constructs builder-created values from source key slots")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsd_int  = registry.tsd(int_meta, ts_int);

    const auto *source_binding = TSDataPlanFactory::instance().binding_for(tsd_int);
    const auto *element_binding = TSDataPlanFactory::instance().binding_for(ts_int);
    REQUIRE(source_binding != nullptr);
    REQUIRE(element_binding != nullptr);

    TSData source{*source_binding};
    TSData proxy{tsd_proxy_binding_for(*tsd_int, *element_binding)};

    Value key_one{1};
    Value key_two{2};
    {
        auto source_view = source.view();
        auto source_dict = source_view.as_dict();
        auto mutation    = source_dict.begin_mutation(MIN_ST);
        auto child       = mutation.at(key_one.view());
        Value value{11};
        auto  child_mutation = child.begin_mutation(MIN_ST);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    auto proxy_view = proxy.view();
    auto bind_source_view = source.view();
    bind_tsd_proxy(proxy_view, bind_source_view.as_dict(), &key_value_builder, nullptr, MIN_ST);

    auto initial_view = proxy.view();
    auto initial = initial_view.as_dict();
    REQUIRE(initial.value().binding()->ops_ref().kind == ValueOpsKind::Map);
    REQUIRE(initial.delta_value(MIN_ST).binding()->ops_ref().kind == ValueOpsKind::Indexed);
    REQUIRE(initial.value().as_map().key_set().binding()->ops_ref().kind == ValueOpsKind::Set);
    REQUIRE(initial.modified(MIN_ST));
    REQUIRE(initial.contains(key_one.view()));
    REQUIRE(range_count(initial.items()) == 1);
    REQUIRE(initial.at(key_one.view()).value().checked_as<std::int32_t>() == 1);
    REQUIRE_THROWS_AS(proxy.view().begin_mutation(MIN_ST), std::logic_error);

    const auto t2 = MIN_ST + TimeDelta{1};
    {
        auto source_view = source.view();
        auto source_dict = source_view.as_dict();
        auto mutation    = source_dict.begin_mutation(t2);
        auto child       = mutation.at(key_two.view());
        Value value{22};
        auto  child_mutation = child.begin_mutation(t2);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    auto after_add_view = proxy.view();
    auto after_add = after_add_view.as_dict();
    REQUIRE(after_add.modified(t2));
    REQUIRE(after_add.contains(key_one.view()));
    REQUIRE(after_add.contains(key_two.view()));
    REQUIRE(range_count(after_add.items()) == 2);
    REQUIRE(after_add.at(key_two.view()).value().checked_as<std::int32_t>() == 2);

    const auto t3 = t2 + TimeDelta{1};
    {
        auto source_view = source.view();
        auto source_dict = source_view.as_dict();
        auto mutation    = source_dict.begin_mutation(t3);
        REQUIRE(mutation.erase(key_one.view()));
    }

    auto after_remove_view = proxy.view();
    auto after_remove = after_remove_view.as_dict();
    REQUIRE(after_remove.modified(t3));
    REQUIRE_FALSE(after_remove.contains(key_one.view()));
    REQUIRE(after_remove.contains(key_two.view()));
    REQUIRE(range_count(after_remove.items()) == 1);

    auto removed = after_remove.key_set().removed();
    REQUIRE(range_count(removed) == 1);
    REQUIRE((*removed.begin()).checked_as<std::int32_t>() == 1);

    const auto t4 = t3 + TimeDelta{1};
    {
        auto source_view = source.view();
        auto source_dict = source_view.as_dict();
        auto child       = source_dict.at(key_two.view());
        Value value{222};
        auto  child_mutation = child.begin_mutation(t4);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    auto after_value_tick_view = proxy.view();
    auto after_value_tick = after_value_tick_view.as_dict();
    REQUIRE_FALSE(after_value_tick.modified(t4));
    REQUIRE(after_value_tick.at(key_two.view()).value().checked_as<std::int32_t>() == 2);
}

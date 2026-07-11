#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
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

    void window_key_builder(hgraph::TSDProxy         &proxy,
                            std::size_t                slot,
                            const hgraph::TSDataView  &target,
                            const hgraph::TSDataView  &source,
                            hgraph::DateTime           modified_time,
                            const void                *)
    {
        (void)source;
        auto window = target.as_window();
        auto mutation = window.begin_mutation(modified_time);
        mutation.push(proxy.source_dict().key_at_slot(slot));
    }

    void sparse_bundle_key_builder(hgraph::TSDProxy         &proxy,
                                   std::size_t                slot,
                                   const hgraph::TSDataView  &target,
                                   const hgraph::TSDataView  &source,
                                   hgraph::DateTime           modified_time,
                                   const void                *)
    {
        (void)source;
        auto bundle = target.as_bundle();
        auto child = bundle.at(0);
        auto mutation = child.begin_mutation(modified_time);
        if (!mutation.copy_value_from(proxy.source_dict().key_at_slot(slot)))
        {
            throw std::logic_error("TSDProxy sparse-list builder did not materialise the source key");
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
    REQUIRE(initial.value().binding().ops_ref().kind == ValueOpsKind::Map);
    REQUIRE(initial.delta_value(MIN_ST).binding().ops_ref().kind == ValueOpsKind::Indexed);
    REQUIRE(initial.value().as_map().key_set().binding().ops_ref().kind == ValueOpsKind::Set);
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

TEST_CASE("TSDProxy projected values preserve canonical hash equality comparison and capabilities")
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

    TSData source_a{*source_binding};
    TSData source_b{*source_binding};
    TSData source_different{*source_binding};
    TSData proxy_a{tsd_proxy_binding_for(*tsd_int, *element_binding)};
    TSData proxy_b{tsd_proxy_binding_for(*tsd_int, *element_binding)};
    TSData proxy_different{tsd_proxy_binding_for(*tsd_int, *element_binding)};

    const auto add_key = [](TSData &source, std::int32_t key, std::int32_t value) {
        Value key_value{key};
        Value child_value{value};
        auto source_view = source.view();
        auto mutation = source_view.as_dict().begin_mutation(MIN_ST);
        auto child = mutation.at(key_value.view());
        auto child_mutation = child.begin_mutation(MIN_ST);
        REQUIRE(child_mutation.copy_value_from(child_value.view()));
    };
    add_key(source_a, 1, 11);
    add_key(source_a, 2, 22);
    add_key(source_b, 2, 222);
    add_key(source_b, 1, 111);
    add_key(source_different, 1, 11);
    add_key(source_different, 3, 33);

    const auto bind_proxy = [](TSData &proxy, TSData &source) {
        auto proxy_view = proxy.view();
        auto source_view = source.view();
        bind_tsd_proxy(proxy_view, source_view.as_dict(), &key_value_builder, nullptr, MIN_ST);
    };
    bind_proxy(proxy_a, source_a);
    bind_proxy(proxy_b, source_b);
    bind_proxy(proxy_different, source_different);

    auto proxy_a_view = proxy_a.view();
    auto proxy_b_view = proxy_b.view();
    auto proxy_different_view = proxy_different.view();
    auto dict_a = proxy_a_view.as_dict();
    auto dict_b = proxy_b_view.as_dict();
    auto dict_different = proxy_different_view.as_dict();
    auto map_a = dict_a.value();
    auto map_b = dict_b.value();
    auto map_different = dict_different.value();
    auto keys_a = map_a.as_map().key_set();
    auto keys_b = map_b.as_map().key_set();
    auto keys_different = map_different.as_map().key_set();
    auto delta_a = dict_a.delta_value(MIN_ST);
    auto delta_b = dict_b.delta_value(MIN_ST);
    auto delta_different = dict_different.delta_value(MIN_ST);

    REQUIRE(map_a.hash() == map_b.hash());
    REQUIRE(map_a.equals(map_b));
    REQUIRE(std::is_eq(map_a.compare(map_b)));
    REQUIRE_FALSE(map_a.equals(map_different));
    REQUIRE(map_a.compare(map_different) == std::partial_ordering::unordered);
    REQUIRE_FALSE(map_a.to_string().empty());

    REQUIRE(keys_a.hash() == keys_b.hash());
    REQUIRE(keys_a.equals(keys_b));
    REQUIRE(std::is_eq(keys_a.compare(keys_b)));
    REQUIRE_FALSE(keys_a.equals(keys_different));
    REQUIRE(keys_a.compare(keys_different) == std::partial_ordering::unordered);
    REQUIRE_FALSE(keys_a.to_string().empty());

    REQUIRE(delta_a.hash() == delta_b.hash());
    REQUIRE(delta_a.equals(delta_b));
    REQUIRE(std::is_eq(delta_a.compare(delta_b)));
    REQUIRE_FALSE(delta_a.equals(delta_different));
    REQUIRE(delta_a.compare(delta_different) == std::partial_ordering::unordered);
    REQUIRE_FALSE(delta_a.to_string().empty());

    constexpr auto semantic_capabilities = TypeCapabilities::Hashable |
                                           TypeCapabilities::Equatable |
                                           TypeCapabilities::Comparable;
    const auto require_canonical_capabilities = [&](const ValueView &projected) {
        const auto canonical = ValuePlanFactory::instance().type_for(projected.schema());
        REQUIRE(canonical);
        REQUIRE((projected.binding().capabilities() & semantic_capabilities) ==
                (canonical.capabilities() & semantic_capabilities));
    };
    require_canonical_capabilities(map_a);
    require_canonical_capabilities(keys_a);
    require_canonical_capabilities(delta_a);
}

TEST_CASE("TSDProxy non-atomic maps project child value and delta storage")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);
    const auto *source_schema = registry.tsd(int_meta, ts_int);
    const auto *window_schema = registry.tsw(int_meta, 1, 1);
    const auto *proxy_schema = registry.tsd(int_meta, window_schema);
    const auto *source_binding = TSDataPlanFactory::instance().binding_for(source_schema);
    const auto *window_binding = TSDataPlanFactory::instance().binding_for(window_schema);
    REQUIRE(source_binding != nullptr);
    REQUIRE(window_binding != nullptr);

    TSData source{*source_binding};
    TSData proxy{tsd_proxy_binding_for(*proxy_schema, *window_binding)};
    Value key{7};
    const auto t1 = MIN_ST + TimeDelta{1};
    {
        auto source_view = source.view();
        auto mutation = source_view.as_dict().begin_mutation(t1);
        auto child = mutation.at(key.view());
        Value source_value{70};
        auto child_mutation = child.begin_mutation(t1);
        REQUIRE(child_mutation.copy_value_from(source_value.view()));
    }
    {
        auto proxy_view = proxy.view();
        auto source_view = source.view();
        bind_tsd_proxy(proxy_view, source_view.as_dict(), &window_key_builder, nullptr, t1);
    }

    auto proxy_view = proxy.view();
    auto dict = proxy_view.as_dict();
    auto live = dict.value();
    auto delta = dict.delta_value(t1).as_bundle();
    auto modified = delta.at(1);
    const auto *live_ops = checked_value_ops<MapValueOps>(live.binding(), "TSDProxy live map test");
    const auto *modified_ops = checked_value_ops<MapValueOps>(modified.binding(), "TSDProxy delta map test");
    const auto live_type = live_ops->value_binding(live_ops->context, live.data());
    const auto modified_type = modified_ops->value_binding(modified_ops->context, modified.data());
    REQUIRE(live_type.schema() == window_schema->value_schema);
    REQUIRE(modified_type.schema() == window_schema->delta_value_schema);
    REQUIRE(live_type != modified_type);

    const auto *live_keyed = live_ops->value_at(live_ops->context, live.data(), key.view().data());
    const auto *live_indexed = live_ops->value_at_index(live_ops->context, live.data(), 0);
    REQUIRE(live_keyed != nullptr);
    REQUIRE(live_indexed == live_keyed);
    auto live_direct = ValueView{live_type, live_keyed}.as_list();
    REQUIRE(live_direct.size() == 1);
    REQUIRE(live_direct.at(0).checked_as<std::int32_t>() == 7);
    auto live_values = live_ops->make_values_range(live_ops->context, live.data());
    auto live_projected = *live_values.begin();
    REQUIRE(live_projected.binding() == live_type);
    REQUIRE(live_projected.data() == live_keyed);

    const auto *modified_keyed = modified_ops->value_at(modified_ops->context, modified.data(), key.view().data());
    const auto *modified_indexed = modified_ops->value_at_index(modified_ops->context, modified.data(), 0);
    REQUIRE(modified_keyed != nullptr);
    REQUIRE(modified_indexed == modified_keyed);
    REQUIRE(ValueView{modified_type, modified_keyed}.checked_as<std::int32_t>() == 7);
    auto modified_values = modified_ops->make_values_range(modified_ops->context, modified.data());
    auto modified_projected = *modified_values.begin();
    REQUIRE(modified_projected.binding() == modified_type);
    REQUIRE(modified_projected.data() == modified_keyed);

    Value live_owned{live};
    Value modified_owned{modified};
    CAPTURE(live.to_string(), live_owned.to_string(), live.hash(), live_owned.view().hash());
    REQUIRE(live.hash() == live_owned.view().hash());
    REQUIRE(live.equals(live_owned.view()));
    REQUIRE(std::is_eq(live.compare(live_owned.view())));
    REQUIRE(live.to_string() == live_owned.to_string());
    REQUIRE(modified.hash() == modified_owned.view().hash());
    REQUIRE(modified.equals(modified_owned.view()));
    REQUIRE(std::is_eq(modified.compare(modified_owned.view())));
    REQUIRE(modified.to_string() == modified_owned.to_string());
}

TEST_CASE("TSDProxy non-atomic owning copy preserves nested typed holes")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);
    const auto *source_schema = registry.tsd(int_meta, ts_int);
    const auto *bundle_schema = registry.tsb("TSDProxySparseBundle", {{"present", ts_int}, {"hole", ts_int}});
    const auto *proxy_schema = registry.tsd(int_meta, bundle_schema);
    const auto *source_binding = TSDataPlanFactory::instance().binding_for(source_schema);
    const auto *bundle_binding = TSDataPlanFactory::instance().binding_for(bundle_schema);
    REQUIRE(source_binding != nullptr);
    REQUIRE(bundle_binding != nullptr);

    TSData source{*source_binding};
    TSData proxy{tsd_proxy_binding_for(*proxy_schema, *bundle_binding)};
    Value key{9};
    {
        auto source_view = source.view();
        auto mutation = source_view.as_dict().begin_mutation(MIN_ST);
        auto child = mutation.at(key.view());
        Value source_value{90};
        auto child_mutation = child.begin_mutation(MIN_ST);
        REQUIRE(child_mutation.copy_value_from(source_value.view()));
    }
    {
        auto proxy_view = proxy.view();
        auto source_view = source.view();
        bind_tsd_proxy(proxy_view, source_view.as_dict(), &sparse_bundle_key_builder, nullptr, MIN_ST);
    }

    auto proxy_view = proxy.view();
    auto live = proxy_view.as_dict().value();
    auto sparse = live.as_map().at(key.view()).as_bundle();
    REQUIRE(sparse.size() == 2);
    REQUIRE(sparse.at(0).checked_as<std::int32_t>() == 9);
    REQUIRE(sparse.at(1).bound());
    REQUIRE_FALSE(sparse.at(1).has_value());

    Value owned{live};
    auto owned_sparse = owned.view().as_map().at(key.view()).as_bundle();
    REQUIRE(owned_sparse.size() == 2);
    REQUIRE(owned_sparse.at(0).checked_as<std::int32_t>() == 9);
    REQUIRE(owned_sparse.at(1).bound());
    REQUIRE_FALSE(owned_sparse.at(1).has_value());
    REQUIRE(live.equals(owned.view()));
    REQUIRE(live.hash() == owned.view().hash());
}

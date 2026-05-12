#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/compact_container_ops.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>

#include <compare>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace
{
    struct MoveAssignableOnlyScalar
    {
        int value{0};

        MoveAssignableOnlyScalar() = default;
        explicit MoveAssignableOnlyScalar(int v) : value(v) {}
        MoveAssignableOnlyScalar(const MoveAssignableOnlyScalar &) = default;
        MoveAssignableOnlyScalar(MoveAssignableOnlyScalar &&) noexcept = default;
        MoveAssignableOnlyScalar &operator=(const MoveAssignableOnlyScalar &) = delete;
        MoveAssignableOnlyScalar &operator=(MoveAssignableOnlyScalar &&other) noexcept
        {
            value = other.value;
            return *this;
        }

        [[nodiscard]] bool operator==(const MoveAssignableOnlyScalar &) const = default;
        [[nodiscard]] auto operator<=>(const MoveAssignableOnlyScalar &) const = default;
    };
}  // namespace

TEST_CASE("ValuePlanFactory: atomic round-trip via TypeRegistry")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *plan     = factory.plan_for(int_meta);

    REQUIRE(plan != nullptr);
    REQUIRE(plan == &MemoryUtils::plan_for<int>());
    REQUIRE(plan->layout.size == sizeof(int));
    REQUIRE(plan->layout.alignment == alignof(int));
}

TEST_CASE("ValuePlanFactory::find returns null for unregistered atomic schemas")
{
    using namespace hgraph;
    auto                 &factory = ValuePlanFactory::instance();
    ValueTypeMetaData     orphan(ValueTypeKind::Atomic, ValueTypeFlags::None);
    REQUIRE(factory.find(&orphan) == nullptr);
}

TEST_CASE("ValuePlanFactory::plan_for throws for unregistered atomic schemas")
{
    using namespace hgraph;
    auto             &factory = ValuePlanFactory::instance();
    ValueTypeMetaData orphan(ValueTypeKind::Atomic, ValueTypeFlags::None);
    REQUIRE_THROWS_AS(factory.plan_for(&orphan), std::logic_error);
}

TEST_CASE("ValuePlanFactory::plan_for handles null schemas")
{
    using namespace hgraph;
    auto &factory = ValuePlanFactory::instance();
    REQUIRE(factory.plan_for(nullptr) == nullptr);
    REQUIRE(factory.find(nullptr) == nullptr);
}

TEST_CASE("ValuePlanFactory: tuple synthesis matches MemoryUtils::tuple_plan")
{
    using namespace hgraph;
    auto       &registry   = TypeRegistry::instance();
    auto       &factory    = ValuePlanFactory::instance();
    const auto *int_meta   = registry.register_scalar<int>("int");
    const auto *float_meta = registry.register_scalar<float>("float");
    const auto *tuple_meta = registry.tuple({int_meta, float_meta});
    const auto *plan       = factory.plan_for(tuple_meta);

    REQUIRE(plan != nullptr);
    REQUIRE(plan->is_tuple());
    REQUIRE(plan->component_count() == 2);
    REQUIRE(plan == &MemoryUtils::tuple_plan(
                        {&MemoryUtils::plan_for<int>(), &MemoryUtils::plan_for<float>()}));
}

TEST_CASE("ValuePlanFactory: bundle synthesis preserves field names and shape")
{
    using namespace hgraph;
    auto       &registry    = TypeRegistry::instance();
    auto       &factory     = ValuePlanFactory::instance();
    const auto *int_meta    = registry.register_scalar<int>("int");
    const auto *float_meta  = registry.register_scalar<float>("float");
    const auto *bundle_meta = registry.bundle("PlanFactoryBundleA", {{"x", int_meta}, {"y", float_meta}});
    const auto *plan        = factory.plan_for(bundle_meta);

    REQUIRE(plan != nullptr);
    REQUIRE(plan->is_named_tuple());
    REQUIRE(plan->component_count() == 2);

    const auto *x_comp = plan->find_component("x");
    REQUIRE(x_comp != nullptr);
    REQUIRE(x_comp->plan == &MemoryUtils::plan_for<int>());

    const auto *y_comp = plan->find_component("y");
    REQUIRE(y_comp != nullptr);
    REQUIRE(y_comp->plan == &MemoryUtils::plan_for<float>());
}

TEST_CASE("ValuePlanFactory: fixed list synthesis matches MemoryUtils::array_plan")
{
    using namespace hgraph;
    auto       &registry  = TypeRegistry::instance();
    auto       &factory   = ValuePlanFactory::instance();
    const auto *int_meta  = registry.register_scalar<int>("int");
    const auto *list_meta = registry.list(int_meta, 4);
    const auto *plan      = factory.plan_for(list_meta);

    REQUIRE(plan != nullptr);
    REQUIRE(plan->is_array());
    REQUIRE(plan->array_count() == 4);
    REQUIRE(plan == &MemoryUtils::array_plan(MemoryUtils::plan_for<int>(), 4));
}

TEST_CASE("ValuePlanFactory: nested composites synthesise correctly")
{
    using namespace hgraph;
    auto       &registry   = TypeRegistry::instance();
    auto       &factory    = ValuePlanFactory::instance();
    const auto *int_meta   = registry.register_scalar<int>("int");
    const auto *float_meta = registry.register_scalar<float>("float");
    const auto *inner      = registry.tuple({int_meta, float_meta});
    const auto *outer      = registry.tuple({inner, int_meta});
    const auto *plan       = factory.plan_for(outer);

    REQUIRE(plan != nullptr);
    REQUIRE(plan->is_tuple());
    REQUIRE(plan->component_count() == 2);

    const auto *inner_plan = factory.plan_for(inner);
    REQUIRE(plan->component(0).plan == inner_plan);
    REQUIRE(plan->component(1).plan == &MemoryUtils::plan_for<int>());
}

TEST_CASE("ValuePlanFactory: caching returns the same pointer on repeat lookups")
{
    using namespace hgraph;
    auto       &registry   = TypeRegistry::instance();
    auto       &factory    = ValuePlanFactory::instance();
    const auto *int_meta   = registry.register_scalar<int>("int");
    const auto *float_meta = registry.register_scalar<float>("float");
    const auto *tuple_meta = registry.tuple({int_meta, float_meta});

    const auto *first    = factory.plan_for(tuple_meta);
    const auto *second   = factory.plan_for(tuple_meta);
    const auto *via_find = factory.find(tuple_meta);

    REQUIRE(first != nullptr);
    REQUIRE(first == second);
    REQUIRE(first == via_find);
}

TEST_CASE("ValuePlanFactory: dynamic list uses compact value-layer storage")
{
    using namespace hgraph;
    auto       &registry  = TypeRegistry::instance();
    auto       &factory   = ValuePlanFactory::instance();
    const auto *int_meta  = registry.register_scalar<int>("int");
    const auto *list_meta = registry.list(int_meta, 0);
    const auto *int_binding = registry.scalar_binding<int>();

    const auto *plan = factory.plan_for(list_meta);
    const auto *binding = factory.binding_for(list_meta);

    REQUIRE(int_binding != nullptr);
    REQUIRE(plan == &compact_list_plan(*int_binding));
    REQUIRE(binding != nullptr);
    REQUIRE(binding->type_meta == list_meta);
    REQUIRE(binding->plan() == plan);
    REQUIRE(binding->ops == &compact_list_ops());
}

TEST_CASE("ValuePlanFactory: container kinds use compact value-layer storage")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *int_binding = registry.scalar_binding<int>();
    REQUIRE(int_binding != nullptr);

    const auto *set_meta = registry.set(int_meta);
    const auto *set_plan = factory.plan_for(set_meta);
    const auto *set_binding = factory.binding_for(set_meta);
    REQUIRE(set_plan == &compact_set_plan(*int_binding));
    REQUIRE(set_binding != nullptr);
    REQUIRE(set_binding->type_meta == set_meta);
    REQUIRE(set_binding->ops == &compact_set_ops());

    const auto *map_meta = registry.map(int_meta, int_meta);
    const auto *map_plan = factory.plan_for(map_meta);
    const auto *map_binding = factory.binding_for(map_meta);
    REQUIRE(map_plan == &compact_map_plan(*int_binding, *int_binding));
    REQUIRE(map_binding != nullptr);
    REQUIRE(map_binding->type_meta == map_meta);
    REQUIRE(map_binding->ops == &compact_map_ops());

    const auto *cyclic_meta = registry.cyclic_buffer(int_meta, 4);
    const auto *cyclic_plan = factory.plan_for(cyclic_meta);
    const auto *cyclic_binding = factory.binding_for(cyclic_meta);
    REQUIRE(cyclic_plan == &compact_cyclic_buffer_plan(*int_binding, 4));
    REQUIRE(cyclic_binding != nullptr);
    REQUIRE(cyclic_binding->type_meta == cyclic_meta);
    REQUIRE(cyclic_binding->ops == &compact_cyclic_buffer_ops());

    const auto *queue_meta = registry.queue(int_meta, 4);
    const auto *queue_plan = factory.plan_for(queue_meta);
    const auto *queue_binding = factory.binding_for(queue_meta);
    REQUIRE(queue_plan == &compact_queue_plan(*int_binding, 4));
    REQUIRE(queue_binding != nullptr);
    REQUIRE(queue_binding->type_meta == queue_meta);
    REQUIRE(queue_binding->ops == &compact_queue_ops());
}

TEST_CASE("ValuePlanFactory: binding_for synthesises structured composite bindings")
{
    using namespace hgraph;
    auto       &registry   = TypeRegistry::instance();
    auto       &factory    = ValuePlanFactory::instance();
    const auto *int_meta   = registry.register_scalar<int>("int");
    const auto *float_meta = registry.register_scalar<float>("float");

    const auto *tuple_meta = registry.tuple({int_meta, float_meta});
    const auto *tuple_binding = factory.binding_for(tuple_meta);
    REQUIRE(tuple_binding != nullptr);
    REQUIRE(tuple_binding->type_meta == tuple_meta);
    REQUIRE(tuple_binding->plan() == factory.plan_for(tuple_meta));

    const auto *bundle_meta = registry.bundle("PlanFactoryBindingBundle", {{"x", int_meta}, {"y", float_meta}});
    const auto *bundle_binding = factory.binding_for(bundle_meta);
    REQUIRE(bundle_binding != nullptr);
    REQUIRE(bundle_binding->type_meta == bundle_meta);
    REQUIRE(bundle_binding->plan() == factory.plan_for(bundle_meta));

    const auto *fixed_list_meta = registry.list(int_meta, 3);
    const auto *fixed_list_binding = factory.binding_for(fixed_list_meta);
    REQUIRE(fixed_list_binding != nullptr);
    REQUIRE(fixed_list_binding->type_meta == fixed_list_meta);
    REQUIRE(fixed_list_binding->plan() == factory.plan_for(fixed_list_meta));
    REQUIRE(fixed_list_binding->plan()->is_array());
}

TEST_CASE("ValuePlanFactory::register_atomic is idempotent for the same plan")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");

    REQUIRE_NOTHROW(factory.register_atomic(int_meta, &MemoryUtils::plan_for<int>()));
    REQUIRE(factory.find(int_meta) == &MemoryUtils::plan_for<int>());
}

TEST_CASE("ValuePlanFactory::register_atomic rejects conflicting plans")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");

    REQUIRE_THROWS_AS(
        factory.register_atomic(int_meta, &MemoryUtils::plan_for<float>()),
        std::logic_error);
}

TEST_CASE("ValuePlanFactory::register_atomic ignores null inputs")
{
    using namespace hgraph;
    auto &factory = ValuePlanFactory::instance();
    REQUIRE_NOTHROW(factory.register_atomic(nullptr, &MemoryUtils::plan_for<int>()));
    ValueTypeMetaData orphan(ValueTypeKind::Atomic, ValueTypeFlags::None);
    REQUIRE_NOTHROW(factory.register_atomic(&orphan, nullptr));
}

TEST_CASE("TSDataPlanFactory: atomic TSData uses value storage and last-modified tracking")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    const auto *plan    = factory.plan_for(ts_int);
    const auto *binding = factory.binding_for(ts_int);

    REQUIRE(plan != nullptr);
    REQUIRE(plan->is_named_tuple());
    REQUIRE(plan->component_count() == 2);
    REQUIRE(plan->find_component("value") != nullptr);
    REQUIRE(plan->find_component("delta") == nullptr);
    REQUIRE(plan->find_component("tracking") != nullptr);
    REQUIRE(plan->component("value").plan == &MemoryUtils::plan_for<int>());
    REQUIRE(plan->component("tracking").plan == &MemoryUtils::plan_for<TSDataTracking>());
    REQUIRE(plan->component("tracking").offset != plan->component("value").offset);

    REQUIRE(binding != nullptr);
    REQUIRE(binding->type_meta == ts_int);
    REQUIRE(binding->plan() == plan);
    REQUIRE(binding->checked_ops().allows_mutation);
    TSData data{*binding};
    REQUIRE(data.view().layout().value_binding == registry.scalar_binding<int>());
    REQUIRE(data.view().layout().delta_binding == registry.scalar_binding<int>());
    REQUIRE_FALSE(data.view().parent_link().has_parent());
}

TEST_CASE("TSDataView: mutation capability is supplied by TSDataOps")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *binding  = factory.binding_for(ts_int);
    REQUIRE(binding != nullptr);

    TSDataOps immutable_ops = binding->checked_ops();
    immutable_ops.allows_mutation = false;
    TSDataBinding immutable_binding{binding->type_meta, binding->plan(), &immutable_ops};

    TSData immutable_data{immutable_binding};
    auto   immutable_view = immutable_data.view();
    REQUIRE_THROWS_AS(immutable_view.mutable_data(), std::logic_error);
    REQUIRE_THROWS_AS(immutable_view.begin_mutation(MIN_ST), std::logic_error);
}

TEST_CASE("TSDataPlanFactory: compact atomic TSData tracks deltas by modified time")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *binding  = factory.binding_for(ts_int);
    REQUIRE(binding != nullptr);

    TSData data{*binding};
    auto   view = data.view();
    REQUIRE(view.value().checked_as<int>() == 0);
    REQUIRE(view.last_modified_time() == MIN_DT);

    const auto t1 = MIN_ST;
    const auto t2 = t1 + engine_time_delta_t{1};
    REQUIRE_FALSE(view.modified(t1));
    REQUIRE_FALSE(view.delta_value(t1).has_value());

    Value source{42};
    {
        auto mutation = view.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(source.view()));
    }
    REQUIRE(view.value().checked_as<int>() == 42);
    REQUIRE(view.delta_value(t1).checked_as<int>() == 42);
    REQUIRE(view.delta_value(t1).data() == view.value().data());
    REQUIRE(view.last_modified_time() == t1);
    REQUIRE(view.modified(t1));
    REQUIRE_FALSE(view.modified(t2));
    REQUIRE_FALSE(view.delta_value(t2).has_value());

    Value same_tick_overwrite{99};
    {
        auto mutation = view.begin_mutation(t1);
        REQUIRE_FALSE(mutation.copy_value_from(same_tick_overwrite.view()));
    }
    REQUIRE(view.value().checked_as<int>() == 99);
    REQUIRE(view.delta_value(t1).checked_as<int>() == 99);
    REQUIRE(view.last_modified_time() == t1);

    {
        auto mutation = view.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(source.view()));
        REQUIRE(view.value().checked_as<int>() == 42);
        REQUIRE(view.delta_value(t2).checked_as<int>() == 42);
        REQUIRE(view.last_modified_time() == t2);
    }
}

TEST_CASE("TSDataView: child modifications propagate through parent view")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsl      = registry.tsl(ts_int, 2);
    const auto *binding  = factory.binding_for(tsl);
    REQUIRE(binding != nullptr);

    TSData data{*binding};
    auto   parent = data.view();
    auto   list   = parent.as_list();
    auto   child  = list.at(0);
    auto   sibling = list.at(1);
    REQUIRE_FALSE(parent.has_parent());
    REQUIRE(child.has_parent());
    REQUIRE(child.child_id() == 0);
    REQUIRE(child.parent_link().parent_binding() == parent.binding());
    REQUIRE(child.parent_link().parent_data() == parent.data());
    REQUIRE(child.parent_link().child_id == 0);
    REQUIRE(sibling.has_parent());
    REQUIRE(sibling.child_id() == 1);

    const auto t1 = MIN_ST;
    const auto t2 = t1 + engine_time_delta_t{1};
    const auto t3 = t2 + engine_time_delta_t{1};
    Value      first{1};
    Value      second{2};
    Value      third{3};

    {
        auto outer = child.begin_mutation(t1);
        REQUIRE(outer.copy_value_from(first.view()));
        REQUIRE(parent.last_modified_time() == t1);
        REQUIRE(parent.modified(t1));

        {
            auto nested = child.begin_mutation(t1);
            REQUIRE_FALSE(nested.copy_value_from(second.view()));
        }

        REQUIRE(parent.last_modified_time() == t1);
        REQUIRE(child.value().checked_as<int>() == 2);
        REQUIRE(child.delta_value(t1).checked_as<int>() == 2);
    }

    REQUIRE(parent.last_modified_time() == t1);

    {
        auto mutation = sibling.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(first.view()));
    }
    REQUIRE(parent.last_modified_time() == t1);
    auto        t1_delta = parent.delta_value(t1).as_map();
    Value       key_zero{std::int64_t{0}};
    Value       key_one{std::int64_t{1}};
    const auto  key_zero_view = key_zero.view();
    const auto  key_one_view  = key_one.view();
    REQUIRE(t1_delta.contains(key_zero_view));
    REQUIRE(t1_delta.contains(key_one_view));

    {
        auto same_tick = child.begin_mutation(t1);
        REQUIRE_FALSE(same_tick.copy_value_from(third.view()));
    }

    REQUIRE(parent.last_modified_time() == t1);
    REQUIRE(child.value().checked_as<int>() == 3);
    REQUIRE(child.delta_value(t1).checked_as<int>() == 3);

    {
        auto next_tick = child.begin_mutation(t2);
        REQUIRE(next_tick.copy_value_from(first.view()));
        REQUIRE(parent.last_modified_time() == t2);
    }

    REQUIRE(parent.last_modified_time() == t2);
    REQUIRE(parent.modified(t2));
    auto t2_delta = parent.delta_value(t2).as_map();
    REQUIRE(t2_delta.contains(key_zero_view));
    REQUIRE_FALSE(t2_delta.contains(key_one_view));

    {
        auto mutation = child.begin_mutation(t3);
        mutation.mark_modified();
    }
    REQUIRE(child.last_modified_time() == t3);
    REQUIRE(parent.last_modified_time() == t3);
    REQUIRE(parent.delta_value(t3).as_map().contains(key_zero_view));
}

TEST_CASE("TSDataView: child parent link is stored in TSData tracking")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsd      = registry.tsd(int_meta, ts_int);
    const auto *binding  = factory.binding_for(tsd);
    REQUIRE(binding != nullptr);

    TSData data{*binding};
    Value  key{7};
    Value  initial{1};
    Value  updated{12};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + engine_time_delta_t{1};

    {
        auto view = data.view();
        auto dict = view.as_dict();
        auto mutation = dict.begin_mutation(t1);
        mutation.set(key.view(), initial.view());
    }

    TSDataView child;
    {
        auto view = data.view();
        auto dict = view.as_dict();
        child = dict.at(key.view());
        REQUIRE(child.has_parent());
        REQUIRE(child.parent_link().parent_binding() == dict.binding());
        REQUIRE(child.parent_link().parent_data() == dict.base().data());
    }

    REQUIRE(child.has_parent());
    REQUIRE(child.path_from_root() == std::vector<std::size_t>{child.child_id()});
    auto root = child.root_view();
    REQUIRE(root.binding() == data.binding());
    REQUIRE(root.data() == data.view().data());
    {
        auto mutation = child.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(updated.view()));
    }

    auto view = data.view();
    auto dict = view.as_dict();
    REQUIRE(dict.modified(t2));
    REQUIRE(dict.modified_keys(t2).begin() != dict.modified_keys(t2).end());
    REQUIRE(dict.at(key.view()).value().checked_as<int>() == 12);
}

TEST_CASE("TSDataPlanFactory: REF and SIGNAL use compact atomic TSData")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    REQUIRE(factory.binding_for(registry.signal()) != nullptr);
    REQUIRE(factory.binding_for(registry.ref(ts_int)) != nullptr);
}

TEST_CASE("TSDataPlanFactory: fixed TSB groups current values before child tracking")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsb      = registry.tsb("PlanFactoryTSB", {{"a", ts_int}, {"b", ts_int}});

    const auto *binding = factory.binding_for(tsb);
    REQUIRE(binding != nullptr);
    REQUIRE(binding->plan()->is_named_tuple());
    const auto *value_component = binding->plan()->find_component("value");
    const auto *aux_component   = binding->plan()->find_component("aux");
    REQUIRE(value_component != nullptr);
    REQUIRE(aux_component != nullptr);
    REQUIRE(value_component->offset == 0);
    REQUIRE(value_component->plan == ValuePlanFactory::instance().plan_for(tsb->value_schema));
    REQUIRE(aux_component->plan->find_component("field_0") != nullptr);
    REQUIRE(aux_component->plan->find_component("field_1") != nullptr);
    REQUIRE(aux_component->plan->find_component("tracking") != nullptr);

    TSData data{*binding};
    auto   view = data.view();
    auto   tsb_view = view.as_bundle();
    REQUIRE(tsb_view.size() == 2);
    REQUIRE_FALSE(tsb_view.empty());
    REQUIRE(tsb_view.has_field("a"));
    REQUIRE_FALSE(tsb_view.has_field("missing"));
    auto first_child = tsb_view.at(0);
    REQUIRE(tsb_view.field("a").binding() == first_child.binding());
    REQUIRE(tsb_view["a"].binding() == first_child.binding());
    REQUIRE(first_child.binding()->type_meta == ts_int);
    REQUIRE(first_child.binding()->plan() == binding->plan());
    const auto &child_ops = first_child.binding()->checked_ops();
    REQUIRE(&first_child.layout() == child_ops.layout_impl(child_ops.context));
    REQUIRE(view.value().is_bundle());
    REQUIRE(view.value().binding() == ValuePlanFactory::instance().binding_for(tsb->value_schema));
    REQUIRE(tsb_view.valid_items().begin() == tsb_view.valid_items().end());

    auto current = view.value().as_bundle();
    REQUIRE(current.at("a").checked_as<int>() == 0);
    REQUIRE(current.at("b").checked_as<int>() == 0);

    auto immutable_ops = static_cast<const IndexedTSDataOps &>(binding->checked_ops());
    immutable_ops.allows_mutation = false;
    TSDataBinding immutable_binding{binding->type_meta, binding->plan(), &immutable_ops};
    TSData        immutable_data{immutable_binding};
    auto          immutable_view   = immutable_data.view();
    auto          immutable_bundle = immutable_view.as_bundle();
    REQUIRE(immutable_bundle.at(0).value().checked_as<int>() == 0);

    std::size_t keyed_items = 0;
    for (const auto [name, element] : tsb_view.items())
    {
        REQUIRE(element.binding() != nullptr);
        if (keyed_items == 0) { REQUIRE(std::string{name} == "a"); }
        if (keyed_items == 1) { REQUIRE(std::string{name} == "b"); }
        ++keyed_items;
    }
    REQUIRE(keyed_items == 2);

    const auto t1 = MIN_ST;
    Value      seven{7};
    {
        auto mutation = first_child.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(seven.view()));
    }

    REQUIRE(view.modified(t1));
    REQUIRE(view.last_modified_time() == t1);
    REQUIRE(view.value().as_bundle().at("a").checked_as<int>() == 7);
    {
        auto valid_items = tsb_view.valid_items();
        auto it = valid_items.begin();
        REQUIRE(it != valid_items.end());
        const auto [name, element] = *it;
        REQUIRE(std::string{name} == "a");
        REQUIRE(element.value().checked_as<int>() == 7);
        ++it;
        REQUIRE(it == valid_items.end());
    }
    {
        auto modified_items = tsb_view.modified_items(t1);
        auto it = modified_items.begin();
        REQUIRE(it != modified_items.end());
        const auto [name, element] = *it;
        REQUIRE(std::string{name} == "a");
        REQUIRE(element.delta_value(t1).checked_as<int>() == 7);
        ++it;
        REQUIRE(it == modified_items.end());
    }

    auto delta = view.delta_value(t1).as_bundle();
    REQUIRE(delta.at("a").checked_as<int>() == 7);
    REQUIRE_FALSE(delta.at("b").has_value());
}

TEST_CASE("TSDataPlanFactory: fixed TSL stores current values as a fixed value-layer list")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsl      = registry.tsl(ts_int, 3);

    const auto *binding = factory.binding_for(tsl);
    REQUIRE(binding != nullptr);
    REQUIRE(binding->plan()->is_named_tuple());
    const auto *value_component = binding->plan()->find_component("value");
    const auto *aux_component   = binding->plan()->find_component("aux");
    REQUIRE(value_component != nullptr);
    REQUIRE(aux_component != nullptr);
    REQUIRE(value_component->offset == 0);
    REQUIRE(value_component->plan == ValuePlanFactory::instance().plan_for(tsl->value_schema));
    REQUIRE(value_component->plan->is_array());
    REQUIRE(value_component->plan->array_count() == 3);
    REQUIRE(value_component->plan->array_stride() == sizeof(int));
    const auto *elements_component = aux_component->plan->find_component("elements");
    REQUIRE(elements_component != nullptr);
    REQUIRE(elements_component->plan != nullptr);
    REQUIRE(elements_component->plan->is_array());
    REQUIRE(elements_component->plan->array_count() == 3);
    REQUIRE(elements_component->plan->array_element_plan().find_component("tracking") != nullptr);
    REQUIRE(aux_component->plan->find_component("tracking") != nullptr);

    TSData data{*binding};
    auto   view = data.view();
    auto   tsl_view = view.as_list();
    REQUIRE(tsl_view.size() == 3);
    REQUIRE_FALSE(tsl_view.empty());
    const auto &tsl_layout = static_cast<const FixedTSLDataLayout &>(view.layout());
    REQUIRE(tsl_layout.size() == 3);
    REQUIRE(tsl_layout.element_value_stride == value_component->plan->array_stride());
    REQUIRE(tsl_layout.element_value_offset(2) ==
            value_component->offset + 2 * value_component->plan->array_stride());
    REQUIRE(tsl_layout.element_auxiliary_offset == aux_component->offset + elements_component->offset);
    REQUIRE(tsl_layout.element_auxiliary_stride == elements_component->plan->array_stride());
    REQUIRE(tsl_layout.element_auxiliary_offset_at(2) ==
            aux_component->offset + elements_component->offset + 2 * elements_component->plan->array_stride());
    REQUIRE(view.value().is_list());
    REQUIRE(view.value().as_list().size() == 3);
    REQUIRE(view.value().as_list().at(2).checked_as<int>() == 0);
    REQUIRE(tsl_view.valid_values().begin() == tsl_view.valid_values().end());
    REQUIRE(tsl_view.valid_items().begin() == tsl_view.valid_items().end());

    const auto list = view.value().as_list();
    const auto *first = static_cast<const std::byte *>(list.at(0).data());
    const auto *second = static_cast<const std::byte *>(list.at(1).data());
    REQUIRE(static_cast<std::size_t>(second - first) == value_component->plan->array_stride());

    const auto t1 = MIN_ST;
    Value      eleven{11};
    {
        auto child = tsl_view.at(2);
        REQUIRE(child.value().data() == list.at(2).data());
        auto mutation = child.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(eleven.view()));
    }

    REQUIRE(view.modified(t1));
    REQUIRE(view.value().as_list().at(2).checked_as<int>() == 11);
    {
        auto valid_values = tsl_view.valid_values();
        auto it = valid_values.begin();
        REQUIRE(it != valid_values.end());
        REQUIRE((*it).value().checked_as<int>() == 11);
        ++it;
        REQUIRE(it == valid_values.end());
    }
    {
        auto modified_items = tsl_view.modified_items(t1);
        auto it = modified_items.begin();
        REQUIRE(it != modified_items.end());
        const auto [index, element] = *it;
        REQUIRE(index == 2);
        REQUIRE(element.delta_value(t1).checked_as<int>() == 11);
        ++it;
        REQUIRE(it == modified_items.end());
    }

    auto        delta = view.delta_value(t1).as_map();
    Value       key{std::int64_t{2}};
    Value       miss{std::int64_t{1}};
    const auto  key_view  = key.view();
    const auto  miss_view = miss.view();
    REQUIRE(delta.size() == 1);
    REQUIRE(delta.contains(key_view));
    REQUIRE_FALSE(delta.contains(miss_view));
    REQUIRE(delta.at(key_view).checked_as<int>() == 11);
    REQUIRE(delta.key_set().contains(key_view));
}

TEST_CASE("TSDataPlanFactory: tick TSW stores a fixed cyclic current window")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *tsw      = registry.tsw(int_meta, 3, 2);

    const auto *binding = factory.binding_for(tsw);
    REQUIRE(binding != nullptr);
    REQUIRE(binding->plan()->is_named_tuple());
    const auto *window_component   = binding->plan()->find_component("window");
    const auto *tracking_component = binding->plan()->find_component("tracking");
    REQUIRE(window_component != nullptr);
    REQUIRE(tracking_component != nullptr);
    REQUIRE(window_component->offset == 0);
    REQUIRE(tracking_component->plan == &MemoryUtils::plan_for<TSDataTracking>());

    TSData data{*binding};
    auto   view = data.view();
    auto   window = view.as_window();
    REQUIRE(window.size_based());
    const auto &layout = window.size_layout();
    REQUIRE(layout.period == 3);
    REQUIRE(layout.min_period == 2);
    REQUIRE(layout.element_binding == registry.scalar_binding<int>());
    REQUIRE(layout.time_binding == registry.scalar_binding<engine_time_t>());
    REQUIRE_THROWS_AS(window.time_layout(), std::logic_error);
    REQUIRE(window.value().binding()->plan() == window_component->plan);
    REQUIRE(window.value().is_list());
    REQUIRE(window.value().as_list().size() == 0);
    REQUIRE(window.size() == 0);
    REQUIRE(window.empty());
    REQUIRE_FALSE(window.full());
    REQUIRE_FALSE(window.all_valid());
    REQUIRE_FALSE(window.delta_value(MIN_ST).has_value());

    const auto t1 = MIN_ST;
    const auto t2 = t1 + engine_time_delta_t{1};
    const auto t3 = t2 + engine_time_delta_t{1};
    const auto t4 = t3 + engine_time_delta_t{1};

    Value one{1};
    {
        auto mutation = window.begin_mutation(t1);
        mutation.push(one.view());
        REQUIRE(mutation.size() == 1);
        REQUIRE(mutation.back().checked_as<int>() == 1);
    }
    REQUIRE(window.size() == 1);
    REQUIRE_FALSE(window.all_valid());
    REQUIRE(window.front().checked_as<int>() == 1);
    REQUIRE(window.back().checked_as<int>() == 1);
    REQUIRE(window.time_at(0) == t1);
    REQUIRE(window.time_value_at(0).checked_as<engine_time_t>() == t1);
    REQUIRE(window.delta_value(t1).checked_as<int>() == 1);

    Value two{2};
    {
        auto mutation = window.begin_mutation(t2);
        mutation.push(two.view());
    }
    REQUIRE(window.size() == 2);
    REQUIRE(window.all_valid());
    REQUIRE(window.value().as_list().at(0).checked_as<int>() == 1);
    REQUIRE(window.value().as_list().at(1).checked_as<int>() == 2);

    Value three{3};
    {
        auto mutation = window.begin_mutation(t3);
        mutation.push(three.view());
    }
    REQUIRE(window.size() == 3);
    REQUIRE(window.full());
    REQUIRE(window.back().checked_as<int>() == 3);

    Value four{4};
    {
        auto mutation = window.begin_mutation(t4);
        mutation.push(four.view());
    }
    REQUIRE(window.size() == 3);
    REQUIRE(window.full());
    REQUIRE(window.first_modified_time() == t2);
    REQUIRE(window.time_at(0) == t2);
    REQUIRE(window.time_at(1) == t3);
    REQUIRE(window.time_at(2) == t4);
    REQUIRE(window.time_value_at(0).binding() == layout.time_binding);
    REQUIRE(window.time_value_at(0).checked_as<engine_time_t>() == t2);
    REQUIRE(window.time_value_at(2).checked_as<engine_time_t>() == t4);
    auto times = window.value_times();
    auto time_it = times.begin();
    REQUIRE(*time_it == t2);
    ++time_it;
    REQUIRE(*time_it == t3);
    ++time_it;
    REQUIRE(*time_it == t4);
    ++time_it;
    REQUIRE(time_it == times.end());
    REQUIRE(window.at(0).checked_as<int>() == 2);
    REQUIRE(window.at(1).checked_as<int>() == 3);
    REQUIRE(window.at(2).checked_as<int>() == 4);
    REQUIRE(window.value().as_list().at(0).checked_as<int>() == 2);
    REQUIRE(window.value().as_list().at(2).checked_as<int>() == 4);
    REQUIRE(window.delta_value(t4).checked_as<int>() == 4);
    REQUIRE_FALSE(window.delta_value(t3).has_value());

    Value duplicate{5};
    auto  duplicate_mutation = window.begin_mutation(t4);
    REQUIRE_THROWS_AS(duplicate_mutation.push(duplicate.view()), std::logic_error);

    const auto *move_assign_meta = registry.register_scalar<MoveAssignableOnlyScalar>("move_assign_only");
    const auto *move_assign_tsw  = registry.tsw(move_assign_meta, 1, 1);
    const auto *move_binding     = factory.binding_for(move_assign_tsw);
    REQUIRE(move_binding != nullptr);

    TSData move_data{*move_binding};
    auto   move_view   = move_data.view();
    auto   move_window = move_view.as_window();
    Value  first_custom{MoveAssignableOnlyScalar{1}};
    Value  second_custom{MoveAssignableOnlyScalar{2}};
    {
        auto mutation = move_window.begin_mutation(t1);
        mutation.push(first_custom.view());
    }
    {
        auto mutation = move_window.begin_mutation(t2);
        REQUIRE_THROWS_AS(mutation.push(second_custom.view()), std::logic_error);
    }
    REQUIRE(move_window.size() == 1);
    REQUIRE(move_window.time_at(0) == t1);
    REQUIRE(move_window.back().checked_as<MoveAssignableOnlyScalar>().value == 1);
}

TEST_CASE("TSDataPlanFactory: duration TSW stores a timestamped queue current window")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *tsw      = registry.tsw_duration(int_meta, engine_time_delta_t{10}, engine_time_delta_t{5});

    const auto *binding = factory.binding_for(tsw);
    REQUIRE(binding != nullptr);
    const auto *window_component = binding->plan()->find_component("window");
    REQUIRE(window_component != nullptr);

    TSData data{*binding};
    auto   view = data.view();
    auto   window = view.as_window();
    REQUIRE(window.time_based());
    const auto &layout = window.time_layout();
    REQUIRE(layout.time_range == engine_time_delta_t{10});
    REQUIRE(layout.min_time_range == engine_time_delta_t{5});
    REQUIRE(layout.element_binding == registry.scalar_binding<int>());
    REQUIRE(layout.time_binding == registry.scalar_binding<engine_time_t>());
    REQUIRE_THROWS_AS(window.size_layout(), std::logic_error);
    REQUIRE(window.value().binding()->plan() == window_component->plan);
    REQUIRE(window.value().is_list());
    REQUIRE(window.size() == 0);
    REQUIRE_FALSE(window.all_valid());
    REQUIRE(window.capacity() == 0);

    const auto t1 = MIN_ST;
    const auto t2 = t1 + engine_time_delta_t{5};
    const auto t3 = t1 + engine_time_delta_t{15};

    Value one{1};
    {
        auto mutation = window.begin_mutation(t1);
        mutation.push(one.view());
        REQUIRE(mutation.size() == 1);
        REQUIRE(mutation.back().checked_as<int>() == 1);
    }
    REQUIRE(window.size() == 1);
    REQUIRE_FALSE(window.all_valid());
    REQUIRE(window.time_at(0) == t1);
    REQUIRE(window.time_value_at(0).checked_as<engine_time_t>() == t1);
    REQUIRE(window.delta_value(t1).checked_as<int>() == 1);

    Value two{2};
    {
        auto mutation = window.begin_mutation(t2);
        mutation.push(two.view());
    }
    REQUIRE(window.size() == 2);
    REQUIRE(window.all_valid());
    REQUIRE(window.at(0).checked_as<int>() == 1);
    REQUIRE(window.at(1).checked_as<int>() == 2);

    Value three{3};
    {
        auto mutation = window.begin_mutation(t3);
        mutation.push(three.view());
    }
    REQUIRE(window.size() == 2);
    REQUIRE(window.first_modified_time() == t2);
    REQUIRE(window.time_at(0) == t2);
    REQUIRE(window.time_at(1) == t3);
    REQUIRE(window.time_value_at(0).binding() == layout.time_binding);
    REQUIRE(window.time_value_at(0).checked_as<engine_time_t>() == t2);
    REQUIRE(window.time_value_at(1).checked_as<engine_time_t>() == t3);
    REQUIRE(window.at(0).checked_as<int>() == 2);
    REQUIRE(window.at(1).checked_as<int>() == 3);
    REQUIRE(window.value().as_list().size() == 2);
    REQUIRE(window.value().as_list().at(0).checked_as<int>() == 2);
    REQUIRE(window.value().as_list().at(1).checked_as<int>() == 3);
    REQUIRE(window.delta_value(t3).checked_as<int>() == 3);
    REQUIRE_FALSE(window.delta_value(t2).has_value());
}

TEST_CASE("TSDataPlanFactory: fixed structured TSData recursively embeds child layouts")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsl      = registry.tsl(ts_int, 2);
    const auto *tsb      = registry.tsb("NestedPlanFactoryTSB", {{"xs", tsl}});

    const auto *binding = factory.binding_for(tsb);
    REQUIRE(binding != nullptr);
    const auto *root_value_component = binding->plan()->find_component("value");
    REQUIRE(root_value_component != nullptr);
    REQUIRE(root_value_component->offset == 0);
    REQUIRE(root_value_component->plan->is_named_tuple());
    const auto &xs_value_component = root_value_component->plan->component(0);
    REQUIRE(xs_value_component.plan->is_array());
    REQUIRE(xs_value_component.plan->array_count() == 2);
    REQUIRE(xs_value_component.plan->array_stride() == sizeof(int));

    TSData data{*binding};
    auto   root = data.view();
    auto   root_tsb = root.as_bundle();
    REQUIRE(root_tsb.size() == 1);
    auto list_child = root_tsb.field("xs");
    auto list_view = list_child.as_list();
    REQUIRE(list_view.size() == 2);
    auto item_path_probe = list_view.at(1);
    REQUIRE(item_path_probe.path_from_root() == std::vector<std::size_t>{0, 1});
    auto resolved_root = item_path_probe.root_view();
    REQUIRE(resolved_root.binding() == root.binding());
    REQUIRE(resolved_root.data() == root.data());

    const auto t1 = MIN_ST;
    Value      value{23};
    {
        auto item = list_view.at(1);
        auto mutation = item.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(value.view()));
    }

    REQUIRE(root.modified(t1));
    REQUIRE(list_child.modified(t1));
    REQUIRE(root.value().as_bundle().at("xs").as_list().at(1).checked_as<int>() == 23);

    auto  nested_delta = root.delta_value(t1).as_bundle().at("xs").as_map();
    Value key{std::int64_t{1}};
    REQUIRE(nested_delta.size() == 1);
    REQUIRE(nested_delta.at(key.view()).checked_as<int>() == 23);
}

TEST_CASE("TSDataPlanFactory: TSS uses slot storage with added and removed deltas")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *tss      = registry.tss(int_meta);

    const auto *binding = factory.binding_for(tss);
    REQUIRE(binding != nullptr);

    TSData data{*binding};
    auto   view = data.view();
    auto   set  = view.as_set();
    REQUIRE(set.empty());
    REQUIRE(view.value().is_set());

    const auto t1 = MIN_ST;
    Value      one{1};
    Value      two{2};
    {
        auto mutation = set.begin_mutation(t1);
        REQUIRE(mutation.add(one.view()));
        REQUIRE(mutation.add(two.view()));
        REQUIRE_FALSE(mutation.add(one.view()));
    }

    REQUIRE(view.modified(t1));
    REQUIRE(set.size() == 2);
    REQUIRE(set.contains(one.view()));
    REQUIRE(view.value().as_set().contains(two.view()));

    auto delta = view.delta_value(t1).as_bundle();
    REQUIRE(delta.at("added").as_set().contains(one.view()));
    REQUIRE(delta.at("added").as_set().contains(two.view()));
    REQUIRE(delta.at("removed").as_set().empty());
    REQUIRE(set.added_values().begin() != set.added_values().end());
    REQUIRE(set.removed_values().begin() == set.removed_values().end());

    const auto t2 = t1 + engine_time_delta_t{1};
    {
        auto mutation = set.begin_mutation(t2);
        REQUIRE(mutation.remove(one.view()));
    }

    REQUIRE_FALSE(set.contains(one.view()));
    REQUIRE(set.contains(two.view()));
    auto next_delta = view.delta_value(t2).as_bundle();
    REQUIRE(next_delta.at("added").as_set().empty());
    REQUIRE(next_delta.at("removed").as_set().contains(one.view()));
    REQUIRE(set.added_values().begin() == set.added_values().end());
    REQUIRE(set.removed_values().begin() != set.removed_values().end());
    REQUIRE(view.last_modified_time() == t2);

    const auto t3 = t2 + engine_time_delta_t{1};
    Value      three{3};
    {
        auto mutation = set.begin_mutation(t3);
        REQUIRE(mutation.add(three.view()));
        REQUIRE(mutation.remove(three.view()));
    }

    REQUIRE_FALSE(set.contains(three.view()));
    REQUIRE_FALSE(view.modified(t3));
    REQUIRE_FALSE(view.delta_value(t3).has_value());
    REQUIRE(view.last_modified_time() == t2);
}

TEST_CASE("TSDataPlanFactory: TSD uses slot storage with key-set and modified deltas")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsd      = registry.tsd(int_meta, ts_int);

    const auto *binding = factory.binding_for(tsd);
    REQUIRE(binding != nullptr);

    TSData data{*binding};
    auto   view = data.view();
    auto   dict = view.as_dict();
    REQUIRE(dict.empty());
    REQUIRE(view.value().is_map());

    const auto t1 = MIN_ST;
    Value      key{7};
    Value      value{42};
    MapBuilder source_builder{*registry.scalar_binding<int>(), *registry.scalar_binding<int>()};
    source_builder.set_item<int, int>(7, 42);
    auto source_map = source_builder.build();
    {
        auto generic_mutation = view.begin_mutation(t1);
        REQUIRE_THROWS_AS(generic_mutation.copy_value_from(source_map.view()), std::logic_error);
    }
    {
        auto mutation = dict.begin_mutation(t1);
        mutation.set(key.view(), value.view());
    }

    REQUIRE(view.modified(t1));
    REQUIRE(dict.size() == 1);
    REQUIRE(dict.contains(key.view()));
    REQUIRE(dict.at(key.view()).value().checked_as<int>() == 42);
    REQUIRE(view.value().as_map().at(key.view()).checked_as<int>() == 42);

    auto immutable_ops = static_cast<const TSDDataOps &>(binding->checked_ops());
    immutable_ops.allows_mutation = false;
    TSDataBinding immutable_binding{binding->type_meta, binding->plan(), &immutable_ops};
    auto          immutable_view = TSDataView{&immutable_binding, view.data()};
    auto          immutable_child = immutable_view.as_dict().at(key.view());
    REQUIRE(immutable_child.value().checked_as<int>() == 42);
    REQUIRE(immutable_child.has_parent());
    REQUIRE(immutable_child.parent_link().parent_binding() == binding);
    REQUIRE(immutable_child.root_view().data() == view.data());

    REQUIRE(dict.key_set().contains(key.view()));
    REQUIRE(dict.key_set().added().begin() != dict.key_set().added().end());
    REQUIRE(dict.added_keys().begin() != dict.added_keys().end());
    REQUIRE(dict.added_values().begin() != dict.added_values().end());
    REQUIRE(dict.added_items().begin() != dict.added_items().end());
    REQUIRE(dict.valid_keys().begin() != dict.valid_keys().end());
    REQUIRE(dict.valid_values().begin() != dict.valid_values().end());
    REQUIRE(dict.valid_items().begin() != dict.valid_items().end());
    REQUIRE(dict.modified_keys(t1).begin() != dict.modified_keys(t1).end());
    REQUIRE(dict.modified_values(t1).begin() != dict.modified_values(t1).end());
    REQUIRE(dict.modified_items(t1).begin() != dict.modified_items(t1).end());

    auto delta = view.delta_value(t1).as_bundle();
    REQUIRE(delta.at("removed").as_set().empty());
    REQUIRE(delta.at("modified").as_map().contains(key.view()));
    REQUIRE(delta.at("modified").as_map().at(key.view()).checked_as<int>() == 42);

    const auto t2 = t1 + engine_time_delta_t{1};
    Value      updated{84};
    {
        auto values = dict.values();
        auto it     = values.begin();
        REQUIRE(it != values.end());
        auto child = *it;
        REQUIRE(child.has_parent());
        REQUIRE(child.parent_link().parent_binding() == dict.binding());
        REQUIRE(child.parent_link().parent_data() == dict.base().data());
        REQUIRE(child.child_id() == dict.find_slot(key.view()));
        auto mutation = child.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(updated.view()));
    }

    REQUIRE(view.modified(t2));
    REQUIRE(dict.at(key.view()).value().checked_as<int>() == 84);
    REQUIRE(dict.modified_keys(t2).begin() != dict.modified_keys(t2).end());
    auto modified_delta = view.delta_value(t2).as_bundle();
    REQUIRE(modified_delta.at("removed").as_set().empty());
    REQUIRE(modified_delta.at("modified").as_map().contains(key.view()));
    REQUIRE(modified_delta.at("modified").as_map().at(key.view()).checked_as<int>() == 84);

    const auto t3 = t2 + engine_time_delta_t{1};
    {
        auto mutation = dict.begin_mutation(t3);
        REQUIRE(mutation.erase(key.view()));
    }

    REQUIRE_FALSE(dict.contains(key.view()));
    REQUIRE(dict.removed_keys().begin() != dict.removed_keys().end());
    REQUIRE(dict.removed_values().begin() != dict.removed_values().end());
    REQUIRE(dict.removed_items().begin() != dict.removed_items().end());
    auto next_delta = view.delta_value(t3).as_bundle();
    REQUIRE(next_delta.at("removed").as_set().contains(key.view()));
    REQUIRE_FALSE(next_delta.at("modified").as_map().contains(key.view()));
    REQUIRE(view.last_modified_time() == t3);

    const auto t4 = t3 + engine_time_delta_t{1};
    Value      other_key{8};
    Value      other_value{11};
    {
        auto mutation = dict.begin_mutation(t4);
        mutation.set(other_key.view(), other_value.view());
        REQUIRE(mutation.erase(other_key.view()));
    }

    REQUIRE_FALSE(dict.contains(other_key.view()));
    REQUIRE_FALSE(view.modified(t4));
    REQUIRE_FALSE(view.delta_value(t4).has_value());
    REQUIRE(view.last_modified_time() == t3);
}

TEST_CASE("TSDataPlanFactory::plan_for throws for dynamic TSL until slot list storage is implemented")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    REQUIRE_THROWS_AS(factory.plan_for(registry.tsl(ts_int, 0)), std::logic_error);
}

TEST_CASE("TSDataPlanFactory::find returns null and null schemas return null")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    REQUIRE(factory.find(ts_int) == nullptr);
    REQUIRE(factory.find(nullptr) == nullptr);
    REQUIRE(factory.plan_for(nullptr) == nullptr);
}

TEST_CASE("TSDataPlanFactory::instance is a stable singleton")
{
    REQUIRE(&hgraph::TSDataPlanFactory::instance() == &hgraph::TSDataPlanFactory::instance());
}

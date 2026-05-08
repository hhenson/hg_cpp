#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/compact_container_ops.h>

#include <stdexcept>

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

TEST_CASE("TSDataPlanFactory::plan_for throws (TS data layer not yet ported)")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    REQUIRE_THROWS_AS(factory.plan_for(ts_int), std::logic_error);
    REQUIRE_THROWS_AS(factory.plan_for(registry.tss(int_meta)), std::logic_error);
    REQUIRE_THROWS_AS(factory.plan_for(registry.tsd(int_meta, ts_int)), std::logic_error);
    REQUIRE_THROWS_AS(factory.plan_for(registry.tsl(ts_int, 4)), std::logic_error);
    REQUIRE_THROWS_AS(factory.plan_for(registry.signal()), std::logic_error);
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

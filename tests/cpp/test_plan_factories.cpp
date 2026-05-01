#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/metadata/ts_value_plan_factory.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/utils/memory_utils.h>

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
    const auto *bundle_meta = registry.bundle({{"x", int_meta}, {"y", float_meta}}, "PlanFactoryBundleA");
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

TEST_CASE("ValuePlanFactory: dynamic list throws (value-layer not yet ported)")
{
    using namespace hgraph;
    auto       &registry  = TypeRegistry::instance();
    auto       &factory   = ValuePlanFactory::instance();
    const auto *int_meta  = registry.register_scalar<int>("int");
    const auto *list_meta = registry.list(int_meta, 0);

    REQUIRE_THROWS_AS(factory.plan_for(list_meta), std::logic_error);
}

TEST_CASE("ValuePlanFactory: container kinds throw (value-layer not yet ported)")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");

    REQUIRE_THROWS_AS(factory.plan_for(registry.set(int_meta)), std::logic_error);
    REQUIRE_THROWS_AS(factory.plan_for(registry.map(int_meta, int_meta)), std::logic_error);
    REQUIRE_THROWS_AS(factory.plan_for(registry.cyclic_buffer(int_meta, 4)), std::logic_error);
    REQUIRE_THROWS_AS(factory.plan_for(registry.queue(int_meta, 4)), std::logic_error);
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

TEST_CASE("TSValuePlanFactory::plan_for throws (TS layer not yet ported)")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    REQUIRE_THROWS_AS(factory.plan_for(ts_int), std::logic_error);
    REQUIRE_THROWS_AS(factory.plan_for(registry.tss(int_meta)), std::logic_error);
    REQUIRE_THROWS_AS(factory.plan_for(registry.tsd(int_meta, ts_int)), std::logic_error);
    REQUIRE_THROWS_AS(factory.plan_for(registry.tsl(ts_int, 4)), std::logic_error);
    REQUIRE_THROWS_AS(factory.plan_for(registry.signal()), std::logic_error);
}

TEST_CASE("TSValuePlanFactory::find returns null and null schemas return null")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    REQUIRE(factory.find(ts_int) == nullptr);
    REQUIRE(factory.find(nullptr) == nullptr);
    REQUIRE(factory.plan_for(nullptr) == nullptr);
}

TEST_CASE("TSValuePlanFactory::instance is a stable singleton")
{
    REQUIRE(&hgraph::TSValuePlanFactory::instance() == &hgraph::TSValuePlanFactory::instance());
}

// Ensures the ported foundation headers compile cleanly together.
// This test instantiates a few of the templated entry points so the
// compiler chases through the heavier portions of memory_utils, the
// slot stores, and intern_table.

#include <hgraph/types/metadata/ts_value_plan_factory.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/type_binding.h>
#include <hgraph/types/metadata/type_meta_data.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/utils/intern_table.h>
#include <hgraph/types/utils/key_slot_store.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/utils/slot_observer.h>
#include <hgraph/types/utils/stable_slot_storage.h>
#include <hgraph/types/utils/value_slot_store.h>
#include <hgraph/util/date_time.h>
#include <hgraph/util/scope.h>
#include <hgraph/util/tagged_ptr.h>

#include <cassert>
#include <cstddef>

namespace {

void instantiate_memory_utils() {
    using hgraph::MemoryUtils;
    const auto &plan = MemoryUtils::plan_for<int>();
    assert(plan.valid());
    assert(plan.layout.size == sizeof(int));

    const auto &tuple_plan = MemoryUtils::tuple_plan({&plan, &plan});
    assert(tuple_plan.is_tuple());
    assert(tuple_plan.component_count() == 2);

    const auto &array_plan = MemoryUtils::array_plan(plan, 4);
    assert(array_plan.is_array());
    assert(array_plan.array_count() == 4);

    MemoryUtils::StorageHandle<MemoryUtils::InlineStoragePolicy<>, void> handle(plan);
    assert(handle.has_value());
}

void instantiate_intern_table() {
    hgraph::InternTable<int, int> table;
    const int &v = table.emplace(7, 7);
    assert(v == 7);
}

void instantiate_slot_stores() {
    using hgraph::MemoryUtils;
    using hgraph::KeySlotStore;
    using hgraph::ValueSlotStore;

    KeySlotStore keys(MemoryUtils::plan_for<int>(), hgraph::key_slot_store_ops_for<int>());
    int          k = 42;
    auto         result = keys.insert(k);
    assert(result.inserted);
    assert(keys.contains(k));

    ValueSlotStore values(MemoryUtils::plan_for<int>());
    values.reserve_to(8);
    values.construct_at<int>(0, 13);
    assert(values.has_slot(0));
}

}  // namespace

void instantiate_schema() {
    using hgraph::TypeRegistry;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<int>("int");
    assert(int_meta != nullptr);
    assert(int_meta->kind == hgraph::ValueTypeKind::Atomic);
    assert(int_meta == registry.value_type("int"));

    const auto *ts_int = registry.ts(int_meta);
    assert(ts_int != nullptr);
    assert(ts_int->kind == hgraph::TSValueTypeKind::Value);
}

void instantiate_plan_factory() {
    using hgraph::MemoryUtils;
    using hgraph::TypeRegistry;
    using hgraph::ValuePlanFactory;

    auto &registry = TypeRegistry::instance();
    auto &factory  = ValuePlanFactory::instance();

    const auto *int_meta   = registry.register_scalar<int>("int");
    const auto *float_meta = registry.register_scalar<float>("float");

    const auto *int_plan   = factory.plan_for(int_meta);
    const auto *float_plan = factory.plan_for(float_meta);
    assert(int_plan == &MemoryUtils::plan_for<int>());
    assert(float_plan == &MemoryUtils::plan_for<float>());

    const auto *tuple_meta = registry.tuple({int_meta, float_meta});
    const auto *tuple_plan = factory.plan_for(tuple_meta);
    assert(tuple_plan != nullptr);
    assert(tuple_plan->is_tuple());
    assert(tuple_plan->component_count() == 2);
    // Composite plans are interned by MemoryUtils — same triple yields same pointer.
    assert(tuple_plan == &MemoryUtils::tuple_plan({int_plan, float_plan}));

    // Caching: a second lookup returns the same pointer without re-synthesising.
    assert(factory.plan_for(tuple_meta) == tuple_plan);
    assert(factory.find(tuple_meta) == tuple_plan);
}

int main() {
    instantiate_memory_utils();
    instantiate_intern_table();
    instantiate_slot_stores();
    instantiate_schema();
    instantiate_plan_factory();
    return 0;
}

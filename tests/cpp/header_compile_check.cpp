// Ensures the ported foundation headers compile cleanly together.
// This test instantiates a few of the templated entry points so the
// compiler chases through the heavier portions of memory_utils, the
// slot stores, and intern_table.

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

int main() {
    instantiate_memory_utils();
    instantiate_intern_table();
    instantiate_slot_stores();
    return 0;
}

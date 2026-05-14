#include <hgraph/types/time_series/ts_data/ops.h>

#include <stdexcept>
#include <string>

namespace hgraph::ts_data_detail
{
    [[noreturn]] void missing_ts_data_op(const char *name)
    {
        throw std::logic_error(std::string{"TSDataOps is missing "} + name + " implementation");
    }

    const TSDataLayout *missing_layout(const void *)
    {
        missing_ts_data_op("layout");
    }

    const TSDataTracking *missing_tracking(const void *, const void *)
    {
        missing_ts_data_op("tracking");
    }

    TSDataTracking *missing_mutable_tracking(const void *, void *)
    {
        missing_ts_data_op("mutable tracking");
    }

    bool missing_has_current_value(const void *, const void *)
    {
        missing_ts_data_op("validity");
    }

    bool missing_all_valid(const void *, const void *)
    {
        missing_ts_data_op("recursive validity");
    }

    const void *missing_value_memory(const void *, const void *)
    {
        missing_ts_data_op("value memory");
    }

    void *missing_mutable_value_memory(const void *, void *)
    {
        missing_ts_data_op("mutable value memory");
    }

    const void *missing_delta_memory(const void *, const void *)
    {
        missing_ts_data_op("delta memory");
    }

    void *missing_mutable_delta_memory(const void *, void *)
    {
        missing_ts_data_op("mutable delta memory");
    }

    void noop_reset_delta(const void *, void *) {}

    void noop_cleanup_delta(const void *, void *, engine_time_t) {}

    void noop_record_child_modified(const void *, void *, std::size_t, engine_time_t) {}

    bool missing_copy_value_from(const void *, void *, const ValueView &, engine_time_t)
    {
        missing_ts_data_op("copy value");
    }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
    bool missing_from_python(const void *, void *, nb::handle, engine_time_t)
    {
        missing_ts_data_op("from Python");
    }

    nb::object missing_to_python(const void *, const void *)
    {
        missing_ts_data_op("to Python");
    }

    nb::object missing_delta_to_python(const void *, const void *, engine_time_t)
    {
        missing_ts_data_op("delta to Python");
    }
#endif

    std::size_t missing_indexed_size(const void *, const void *)
    {
        missing_ts_data_op("indexed size");
    }

    const TSDataBinding *missing_indexed_element_binding(const void *, const void *, std::size_t)
    {
        missing_ts_data_op("indexed element binding");
    }

    const void *missing_indexed_element_memory(const void *, const void *, std::size_t)
    {
        missing_ts_data_op("indexed element memory");
    }

    void *missing_mutable_indexed_element_memory(const void *, void *, std::size_t)
    {
        missing_ts_data_op("mutable indexed element memory");
    }

    std::size_t missing_slot_size(const void *, const void *)
    {
        missing_ts_data_op("slot collection size");
    }

    std::size_t missing_slot_capacity(const void *, const void *)
    {
        missing_ts_data_op("slot collection capacity");
    }

    bool missing_slot_predicate(const void *, const void *, std::size_t)
    {
        missing_ts_data_op("slot predicate");
    }

    const void *missing_key_at_slot(const void *, const void *, std::size_t)
    {
        missing_ts_data_op("key at slot");
    }

    bool missing_contains_key(const void *, const void *, const ValueView &)
    {
        missing_ts_data_op("key containment");
    }

    std::size_t missing_find_key_slot(const void *, const void *, const ValueView &)
    {
        missing_ts_data_op("key slot lookup");
    }

    Range<ValueView> missing_value_range(const void *, const void *)
    {
        missing_ts_data_op("value range");
    }

    KeyValueRange<ValueView, TSDataView> missing_ts_data_kv_range(const void *, const void *)
    {
        missing_ts_data_op("TSData key/value range");
    }

    Range<TSDataView> missing_ts_data_range(const void *, const void *)
    {
        missing_ts_data_op("TSData value range");
    }

    SlotTSDataMutationResult missing_insert_key(const void *, void *, const ValueView &, engine_time_t)
    {
        missing_ts_data_op("key insertion");
    }

    SlotTSDataMutationResult missing_remove_key(const void *, void *, const ValueView &, engine_time_t)
    {
        missing_ts_data_op("key removal");
    }

    bool missing_touch_slots(const void *, void *, engine_time_t)
    {
        missing_ts_data_op("slot collection touch");
    }

    void missing_reserve_slots(const void *, void *, std::size_t)
    {
        missing_ts_data_op("slot reservation");
    }

    const void *missing_child_at_slot(const void *, const void *, std::size_t)
    {
        missing_ts_data_op("child at slot");
    }
}  // namespace hgraph::ts_data_detail

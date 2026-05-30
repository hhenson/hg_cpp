#ifndef HGRAPH_CPP_TS_DATA_OPS_H
#define HGRAPH_CPP_TS_DATA_OPS_H

#include <hgraph/types/time_series/ts_data/types.h>
#include <hgraph/types/utils/slot_observer.h>
#include <hgraph/types/value/value_range.h>
#include <hgraph/types/value/value_view.h>
#include <cstddef>
#include <stdexcept>
#include <string>

namespace hgraph
{
    namespace ts_data_detail
    {
        /** Throw the standard missing-operation exception used by default ops thunks. */
        [[noreturn]] void missing_ts_data_op(const char *name);

        [[nodiscard]] const TSDataLayout *missing_layout(const void *);
        [[nodiscard]] const TSDataTracking *missing_tracking(const void *, const void *);
        [[nodiscard]] TSDataTracking *missing_mutable_tracking(const void *, void *);
        [[nodiscard]] bool missing_has_current_value(const void *, const void *);
        [[nodiscard]] bool missing_all_valid(const void *, const void *);
        [[nodiscard]] const void *missing_value_memory(const void *, const void *);
        [[nodiscard]] void *missing_mutable_value_memory(const void *, void *);
        [[nodiscard]] const void *missing_delta_memory(const void *, const void *);
        [[nodiscard]] void *missing_mutable_delta_memory(const void *, void *);
        void noop_reset_delta(const void *, void *);
        void noop_cleanup_delta(const void *, void *, engine_time_t);
        void noop_record_child_modified(const void *, void *, std::size_t, engine_time_t);
        [[nodiscard]] bool missing_copy_value_from(const void *, void *, const ValueView &, engine_time_t);
#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] bool missing_from_python(const void *, void *, nb::handle, engine_time_t);
        [[nodiscard]] nb::object missing_to_python(const void *, const void *);
        [[nodiscard]] nb::object missing_delta_to_python(const void *, const void *, engine_time_t);
#endif
        [[nodiscard]] std::size_t missing_indexed_size(const void *, const void *);
        [[nodiscard]] const TSDataBinding *missing_indexed_element_binding(const void *, const void *, std::size_t);
        [[nodiscard]] const void *missing_indexed_element_memory(const void *, const void *, std::size_t);
        [[nodiscard]] void *missing_mutable_indexed_element_memory(const void *, void *, std::size_t);
        [[nodiscard]] std::size_t missing_slot_size(const void *, const void *);
        [[nodiscard]] std::size_t missing_slot_capacity(const void *, const void *);
        [[nodiscard]] bool missing_slot_predicate(const void *, const void *, std::size_t);
        [[nodiscard]] const void *missing_key_at_slot(const void *, const void *, std::size_t);
        [[nodiscard]] bool missing_contains_key(const void *, const void *, const ValueView &);
        [[nodiscard]] std::size_t missing_find_key_slot(const void *, const void *, const ValueView &);
        [[nodiscard]] Range<ValueView> missing_value_range(const void *, const void *);
        [[nodiscard]] KeyValueRange<ValueView, TSDataView> missing_ts_data_kv_range(const void *, const void *);
        [[nodiscard]] Range<TSDataView> missing_ts_data_range(const void *, const void *);
        [[nodiscard]] SlotTSDataMutationResult missing_insert_key(const void *, void *, const ValueView &,
                                                                  engine_time_t);
        [[nodiscard]] SlotTSDataMutationResult missing_remove_key(const void *, void *, const ValueView &,
                                                                  engine_time_t);
        [[nodiscard]] bool missing_touch_slots(const void *, void *, engine_time_t);
        void missing_reserve_slots(const void *, void *, std::size_t);
        void missing_subscribe_slot_observer(const void *, void *, SlotObserver *);
        void missing_unsubscribe_slot_observer(const void *, void *, SlotObserver *);
        [[nodiscard]] const void *missing_child_at_slot(const void *, const void *, std::size_t);

    }  // namespace ts_data_detail

    /**
     * Type-erased operation table over a TSData memory region.
     *
     * This is intentionally a passive table of function pointers plus context.
     * Generic read, mutation, delta, and parent-propagation policy lives on
     * TSDataView / TSDataMutationView.
     */
    struct TSDataOps
    {
        const void *context{nullptr};
        TSTypeKind  kind{TSTypeKind::TS};
        bool        allows_mutation{false};

        const TSDataLayout *(*layout_impl)(const void *context) = &ts_data_detail::missing_layout;
        const TSDataTracking *(*tracking_impl)(const void *context,
                                               const void *memory) = &ts_data_detail::missing_tracking;
        TSDataTracking *(*mutable_tracking_impl)(const void *context,
                                                 void *memory) = &ts_data_detail::missing_mutable_tracking;
        bool (*has_current_value_impl)(const void *context,
                                       const void *memory) = &ts_data_detail::missing_has_current_value;
        bool (*all_valid_impl)(const void *context, const void *memory) = &ts_data_detail::missing_all_valid;
        const void *(*value_memory_impl)(const void *context,
                                         const void *memory) = &ts_data_detail::missing_value_memory;
        void *(*mutable_value_memory_impl)(const void *context,
                                           void *memory) = &ts_data_detail::missing_mutable_value_memory;
        const void *(*delta_memory_impl)(const void *context,
                                         const void *memory) = &ts_data_detail::missing_delta_memory;
        void *(*mutable_delta_memory_impl)(const void *context,
                                           void *memory) = &ts_data_detail::missing_mutable_delta_memory;
        void (*reset_delta_impl)(const void *context, void *memory) = &ts_data_detail::noop_reset_delta;
        void (*cleanup_delta_impl)(const void *context, void *memory,
                                   engine_time_t modified_time) = &ts_data_detail::noop_cleanup_delta;
        void (*record_child_modified_impl)(const void *context,
                                           void *memory,
                                           std::size_t child_id,
                                           engine_time_t modified_time) = &ts_data_detail::noop_record_child_modified;
        bool (*copy_value_from_impl)(const void *context, void *memory, const ValueView &source,
                                     engine_time_t modified_time) = &ts_data_detail::missing_copy_value_from;
#if HGRAPH_ENABLE_PYTHON_USER_NODES
        bool (*from_python_impl)(const void *context, void *memory, nb::handle source,
                                 engine_time_t modified_time) = &ts_data_detail::missing_from_python;
        nb::object (*to_python_impl)(const void *context,
                                     const void *memory) = &ts_data_detail::missing_to_python;
        nb::object (*delta_to_python_impl)(const void *context,
                                           const void *memory,
                                           engine_time_t evaluation_time) = &ts_data_detail::missing_delta_to_python;
#endif
    };

    struct TSSDataOps : TSDataOps
    {
        std::size_t (*size_impl)(const void *context, const void *memory) = &ts_data_detail::missing_slot_size;
        std::size_t (*slot_capacity_impl)(const void *context,
                                          const void *memory) = &ts_data_detail::missing_slot_capacity;
        bool (*slot_occupied_impl)(const void *context, const void *memory,
                                   std::size_t slot) = &ts_data_detail::missing_slot_predicate;
        bool (*slot_live_impl)(const void *context, const void *memory,
                               std::size_t slot) = &ts_data_detail::missing_slot_predicate;
        bool (*slot_added_impl)(const void *context, const void *memory,
                                std::size_t slot) = &ts_data_detail::missing_slot_predicate;
        bool (*slot_removed_impl)(const void *context, const void *memory,
                                  std::size_t slot) = &ts_data_detail::missing_slot_predicate;
        const void *(*key_at_slot_impl)(const void *context, const void *memory,
                                        std::size_t slot) = &ts_data_detail::missing_key_at_slot;
        bool (*contains_impl)(const void *context, const void *memory,
                              const ValueView &key) = &ts_data_detail::missing_contains_key;
        std::size_t (*find_slot_impl)(const void *context, const void *memory,
                                      const ValueView &key) = &ts_data_detail::missing_find_key_slot;
        Range<ValueView> (*make_values_range_impl)(const void *context,
                                                   const void *memory) = &ts_data_detail::missing_value_range;
        Range<ValueView> (*make_added_values_range_impl)(const void *context,
                                                         const void *memory) = &ts_data_detail::missing_value_range;
        Range<ValueView> (*make_removed_values_range_impl)(const void *context,
                                                           const void *memory) = &ts_data_detail::missing_value_range;
        SlotTSDataMutationResult (*insert_key_impl)(const void *context, void *memory, const ValueView &key,
                                                    engine_time_t modified_time) = &ts_data_detail::missing_insert_key;
        SlotTSDataMutationResult (*remove_key_impl)(const void *context, void *memory, const ValueView &key,
                                                    engine_time_t modified_time) = &ts_data_detail::missing_remove_key;
        bool (*touch_impl)(const void *context, void *memory,
                           engine_time_t modified_time) = &ts_data_detail::missing_touch_slots;
        void (*reserve_impl)(const void *context, void *memory,
                             std::size_t capacity) = &ts_data_detail::missing_reserve_slots;
        void (*subscribe_slot_observer_impl)(const void *context,
                                             void *memory,
                                             SlotObserver *observer) =
            &ts_data_detail::missing_subscribe_slot_observer;
        void (*unsubscribe_slot_observer_impl)(const void *context,
                                               void *memory,
                                               SlotObserver *observer) =
            &ts_data_detail::missing_unsubscribe_slot_observer;
    };

    struct TSDDataOps : TSSDataOps
    {
        const void *(*child_at_slot_impl)(const void *context, const void *memory,
                                          std::size_t slot) = &ts_data_detail::missing_child_at_slot;
        bool (*slot_modified_impl)(const void *context, const void *memory,
                                   std::size_t slot) = &ts_data_detail::missing_slot_predicate;
        Range<TSDataView> (*make_ts_values_range_impl)(const void *context,
                                                       const void *memory) = &ts_data_detail::missing_ts_data_range;
        Range<ValueView> (*make_valid_keys_range_impl)(const void *context,
                                                       const void *memory) = &ts_data_detail::missing_value_range;
        Range<TSDataView> (*make_valid_ts_values_range_impl)(
            const void *context,
            const void *memory) = &ts_data_detail::missing_ts_data_range;
        Range<ValueView> (*make_modified_keys_range_impl)(const void *context,
                                                          const void *memory) = &ts_data_detail::missing_value_range;
        Range<TSDataView> (*make_modified_ts_values_range_impl)(
            const void *context,
            const void *memory) = &ts_data_detail::missing_ts_data_range;
        Range<TSDataView> (*make_added_ts_values_range_impl)(
            const void *context,
            const void *memory) = &ts_data_detail::missing_ts_data_range;
        Range<TSDataView> (*make_removed_ts_values_range_impl)(
            const void *context,
            const void *memory) = &ts_data_detail::missing_ts_data_range;
        KeyValueRange<ValueView, TSDataView> (*make_ts_kv_range_impl)(const void *context,
                                                                      const void *memory) =
            &ts_data_detail::missing_ts_data_kv_range;
        KeyValueRange<ValueView, TSDataView> (*make_valid_ts_kv_range_impl)(const void *context,
                                                                            const void *memory) =
            &ts_data_detail::missing_ts_data_kv_range;
        KeyValueRange<ValueView, TSDataView> (*make_modified_ts_kv_range_impl)(const void *context,
                                                                               const void *memory) =
            &ts_data_detail::missing_ts_data_kv_range;
        KeyValueRange<ValueView, TSDataView> (*make_added_ts_kv_range_impl)(const void *context,
                                                                            const void *memory) =
            &ts_data_detail::missing_ts_data_kv_range;
        KeyValueRange<ValueView, TSDataView> (*make_removed_ts_kv_range_impl)(const void *context,
                                                                              const void *memory) =
            &ts_data_detail::missing_ts_data_kv_range;
    };

    /**
     * Extension ops for TSData shapes with indexed element access.
     *
     * Fixed and dynamic TSL can share this view-facing surface while keeping
     * separate storage-specific implementations underneath.
     */
    struct IndexedTSDataOps : TSDataOps
    {
        std::size_t (*size_impl)(const void *context, const void *memory) = &ts_data_detail::missing_indexed_size;
        const TSDataBinding *(*element_binding_impl)(
            const void *context,
            const void *memory,
            std::size_t index) = &ts_data_detail::missing_indexed_element_binding;
        const void *(*element_memory_impl)(
            const void *context,
            const void *memory,
            std::size_t index) = &ts_data_detail::missing_indexed_element_memory;
        void *(*mutable_element_memory_impl)(
            const void *context,
            void *memory,
            std::size_t index) = &ts_data_detail::missing_mutable_indexed_element_memory;
    };

    struct TSWDataOps : TSDataOps
    {
        std::size_t (*size_impl)(const void *context, const void *memory) = nullptr;
        const void *(*element_at_impl)(const void *context, const void *memory, std::size_t index) = nullptr;
        engine_time_t (*time_at_impl)(const void *context, const void *memory, std::size_t index) = nullptr;
        const void *(*time_element_at_impl)(const void *context, const void *memory, std::size_t index) = nullptr;
        std::size_t (*capacity_impl)(const void *context, const void *memory) = nullptr;
        bool (*full_impl)(const void *context, const void *memory) = nullptr;
        void (*push_impl)(const void *context, void *memory, const ValueView &source,
                          engine_time_t modified_time) = nullptr;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_TS_DATA_OPS_H

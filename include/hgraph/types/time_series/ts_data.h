#ifndef HGRAPH_CPP_ROOT_TS_DATA_H
#define HGRAPH_CPP_ROOT_TS_DATA_H

#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/type_binding.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value_range.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>

#include <algorithm>
#include <cstddef>
#include <exception>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    struct TSDataOps;
    using TSDataBinding = TypeBinding<TSValueTypeMetaData, TSDataOps>;

    struct TSDataTracking;
    class TSDataView;
    class TSDataMutationView;
    class IndexedTSDataView;
    class TSSDataView;
    class TSSDataMutationView;
    class TSDDataView;
    class TSDDataMutationView;
    class TSBDataView;
    class TSLDataView;
    class TSWDataView;
    class TSWDataMutationView;

    inline constexpr std::size_t TS_DATA_NO_CHILD_ID = static_cast<std::size_t>(-1);

    /**
     * Stable parent identity for one TSData node.
     *
     * This is stored in the child node's value-owned tracking region. It does
     * not point at transient view objects, so a child view may outlive the
     * parent view that projected it.
     */
    struct TSDataParentLink
    {
        const TSDataBinding *parent_binding{nullptr};
        const void          *parent_data{nullptr};
        std::size_t          child_id{TS_DATA_NO_CHILD_ID};

        constexpr TSDataParentLink() noexcept = default;
        constexpr TSDataParentLink(const TSDataBinding *binding,
                                   const void          *data,
                                   std::size_t          parent_child_id) noexcept
            : parent_binding(binding),
              parent_data(data),
              child_id(parent_child_id)
        {
        }

        [[nodiscard]] bool has_parent() const noexcept
        {
            return parent_binding != nullptr && parent_data != nullptr;
        }
        void notify_child_modified(engine_time_t mutation_time) const;
        [[nodiscard]] std::vector<std::size_t> path_from_root() const;
        [[nodiscard]] TSDataView root_view() const;

      private:
        [[nodiscard]] const TSDataTracking &parent_tracking() const;
        [[nodiscard]] TSDataTracking &mutable_parent_tracking() const;
    };

    /**
     * Common per-TSData tracking state.
     *
     * Every TSData shape, including compact atomic TSData, stores this in its
     * value-owned memory. Root values keep ``parent`` empty; projected child
     * values use it for local bubble-up. TSState owns graph-level notification,
     * and TSData owns the timestamp needed to decide whether its local delta
     * view belongs to a given evaluation time.
     */
    struct TSDataTracking
    {
        engine_time_t last_modified_time{MIN_DT};
        TSDataParentLink parent{};
    };

    /**
     * Memory offsets for one TSData implementation.
     *
     * Current value bytes and tracking bytes are separate plan regions. Some
     * implementations also expose a separate delta memory region; compact
     * atomic TSData aliases ``delta_value(t)`` to the current value region
     * when ``last_modified_time == t``.
     */
    struct TSDataLayout
    {
        const ValueTypeBinding *value_binding{nullptr};
        const ValueTypeBinding *delta_binding{nullptr};
        std::size_t             value_offset{0};
        std::size_t             tracking_offset{0};
    };

    /**
     * Field layout entry for fixed-size bundle TSData.
     *
     * The child binding owns its own TSData ops/layout. The parent layout only
     * records where that field's data pointer starts inside the parent
     * allocation.
     */
    struct FixedTSDataFieldLayout
    {
        const TSDataBinding *binding{nullptr};
        const TSDataLayout  *layout{nullptr};
        std::size_t          data_offset{0};

        [[nodiscard]] const ValueTypeBinding *value_binding() const noexcept
        {
            return layout != nullptr ? layout->value_binding : nullptr;
        }

        [[nodiscard]] const ValueTypeBinding *delta_binding() const noexcept
        {
            return layout != nullptr ? layout->delta_binding : nullptr;
        }
    };

    /**
     * Layout for fixed-size bundle TSData.
     *
     * TSB fields may have different TSData schemas and therefore different
     * embedded offsets/layouts.
     */
    struct FixedTSBDataLayout : TSDataLayout
    {
        std::vector<FixedTSDataFieldLayout> fields{};

        [[nodiscard]] std::size_t size() const noexcept { return fields.size(); }
        [[nodiscard]] const FixedTSDataFieldLayout &field(std::size_t index) const { return fields.at(index); }
    };

    /**
     * Layout for fixed-size list TSData.
     *
     * All elements have the same TSData schema. The current value bytes live in
     * a fixed value-layer array and the auxiliary element state lives in a
     * matching fixed array, so element offsets are computed from strides rather
     * than stored as one layout entry per element.
     */
    struct FixedTSLDataLayout : TSDataLayout
    {
        const TSDataBinding *element_binding{nullptr};
        const TSDataLayout  *element_layout{nullptr};
        std::size_t          element_count{0};
        std::size_t          element_value_stride{0};
        std::size_t          element_auxiliary_offset{0};
        std::size_t          element_auxiliary_stride{0};

        [[nodiscard]] std::size_t size() const noexcept { return element_count; }
        [[nodiscard]] std::size_t element_value_offset(std::size_t index) const
        {
            if (index >= element_count) { throw std::out_of_range("FixedTSLDataLayout element index out of range"); }
            return value_offset + index * element_value_stride;
        }
        [[nodiscard]] std::size_t element_auxiliary_offset_at(std::size_t index) const
        {
            if (index >= element_count) { throw std::out_of_range("FixedTSLDataLayout element index out of range"); }
            return element_auxiliary_offset + index * element_auxiliary_stride;
        }
    };

    struct TSSDataLayout : TSDataLayout
    {
        const ValueTypeBinding *key_binding{nullptr};
    };

    struct TSDDataLayout : TSSDataLayout
    {
        const TSDataBinding    *element_binding{nullptr};
        const TSDataLayout     *element_layout{nullptr};
        const TSDataBinding    *key_set_binding{nullptr};
        const ValueTypeBinding *element_value_binding{nullptr};
        const ValueTypeBinding *element_delta_binding{nullptr};
    };

    struct SlotTSDataMutationResult
    {
        std::size_t slot{TS_DATA_NO_CHILD_ID};
        bool        changed{false};
        bool        has_delta{false};
        engine_time_t previous_modified_time{MIN_DT};
    };

    namespace ts_data_detail
    {
        [[noreturn]] inline void missing_ts_data_op(const char *name)
        {
            throw std::logic_error(std::string{"TSDataOps is missing "} + name + " implementation");
        }

        [[nodiscard]] inline const TSDataLayout *missing_layout(const void *)
        {
            missing_ts_data_op("layout");
        }

        [[nodiscard]] inline const TSDataTracking *missing_tracking(const void *, const void *)
        {
            missing_ts_data_op("tracking");
        }

        [[nodiscard]] inline TSDataTracking *missing_mutable_tracking(const void *, void *)
        {
            missing_ts_data_op("mutable tracking");
        }

        [[nodiscard]] inline bool missing_has_current_value(const void *, const void *)
        {
            missing_ts_data_op("validity");
        }

        [[nodiscard]] inline bool missing_all_valid(const void *, const void *)
        {
            missing_ts_data_op("recursive validity");
        }

        [[nodiscard]] inline const void *missing_value_memory(const void *, const void *)
        {
            missing_ts_data_op("value memory");
        }

        [[nodiscard]] inline void *missing_mutable_value_memory(const void *, void *)
        {
            missing_ts_data_op("mutable value memory");
        }

        [[nodiscard]] inline const void *missing_delta_memory(const void *, const void *)
        {
            missing_ts_data_op("delta memory");
        }

        [[nodiscard]] inline void *missing_mutable_delta_memory(const void *, void *)
        {
            missing_ts_data_op("mutable delta memory");
        }

        inline void noop_reset_delta(const void *, void *) {}

        inline void noop_record_child_modified(const void *, void *, std::size_t, engine_time_t) {}

        [[nodiscard]] inline bool missing_copy_value_from(const void *,
                                                          void *,
                                                          const ValueView &,
                                                          engine_time_t)
        {
            missing_ts_data_op("copy value");
        }

        [[nodiscard]] inline std::size_t missing_indexed_size(const void *, const void *)
        {
            missing_ts_data_op("indexed size");
        }

        [[nodiscard]] inline const TSDataBinding *missing_indexed_element_binding(const void *, const void *,
                                                                                  std::size_t)
        {
            missing_ts_data_op("indexed element binding");
        }

        [[nodiscard]] inline const void *missing_indexed_element_memory(const void *, const void *, std::size_t)
        {
            missing_ts_data_op("indexed element memory");
        }

        [[nodiscard]] inline void *missing_mutable_indexed_element_memory(const void *, void *, std::size_t)
        {
            missing_ts_data_op("mutable indexed element memory");
        }

        [[nodiscard]] inline std::size_t missing_slot_size(const void *, const void *)
        {
            missing_ts_data_op("slot collection size");
        }

        [[nodiscard]] inline std::size_t missing_slot_capacity(const void *, const void *)
        {
            missing_ts_data_op("slot collection capacity");
        }

        [[nodiscard]] inline bool missing_slot_predicate(const void *, const void *, std::size_t)
        {
            missing_ts_data_op("slot predicate");
        }

        [[nodiscard]] inline const void *missing_key_at_slot(const void *, const void *, std::size_t)
        {
            missing_ts_data_op("key at slot");
        }

        [[nodiscard]] inline bool missing_contains_key(const void *, const void *, const ValueView &)
        {
            missing_ts_data_op("key containment");
        }

        [[nodiscard]] inline std::size_t missing_find_key_slot(const void *, const void *, const ValueView &)
        {
            missing_ts_data_op("key slot lookup");
        }

        [[nodiscard]] inline Range<ValueView> missing_value_range(const void *, const void *)
        {
            missing_ts_data_op("value range");
        }

        [[nodiscard]] inline KeyValueRange<ValueView, TSDataView> missing_ts_data_kv_range(const void *, const void *)
        {
            missing_ts_data_op("TSData key/value range");
        }

        [[nodiscard]] inline Range<TSDataView> missing_ts_data_range(const void *, const void *)
        {
            missing_ts_data_op("TSData value range");
        }

        [[nodiscard]] inline SlotTSDataMutationResult missing_insert_key(const void *, void *, const ValueView &,
                                                                         engine_time_t)
        {
            missing_ts_data_op("key insertion");
        }

        [[nodiscard]] inline SlotTSDataMutationResult missing_remove_key(const void *, void *, const ValueView &,
                                                                         engine_time_t)
        {
            missing_ts_data_op("key removal");
        }

        inline void missing_reserve_slots(const void *, void *, std::size_t)
        {
            missing_ts_data_op("slot reservation");
        }

        [[nodiscard]] inline const void *missing_child_at_slot(const void *, const void *, std::size_t)
        {
            missing_ts_data_op("child at slot");
        }

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
        void (*record_child_modified_impl)(const void *context,
                                           void *memory,
                                           std::size_t child_id,
                                           engine_time_t modified_time) = &ts_data_detail::noop_record_child_modified;
        bool (*copy_value_from_impl)(const void *context, void *memory, const ValueView &source,
                                     engine_time_t modified_time) = &ts_data_detail::missing_copy_value_from;
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
        void (*reserve_impl)(const void *context, void *memory,
                             std::size_t capacity) = &ts_data_detail::missing_reserve_slots;
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

    struct TSWDataLayout : TSDataLayout
    {
        const ValueTypeBinding *element_binding{nullptr};
        const ValueTypeBinding *time_binding{nullptr};
    };

    struct SizeTSWDataLayout : TSWDataLayout
    {
        std::size_t period{0};
        std::size_t min_period{0};
    };

    struct TimeTSWDataLayout : TSWDataLayout
    {
        engine_time_delta_t     time_range{};
        engine_time_delta_t     min_time_range{};
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

    /**
     * Non-owning view over TSData.
     */
    class TSDataView
    {
      public:
        constexpr TSDataView() noexcept = default;

        TSDataView(const TSDataBinding *binding, void *data) noexcept
            : binding_(binding), data_(data)
        {
        }

        TSDataView(const TSDataBinding *binding, const void *data) noexcept
            : binding_(binding), data_(data)
        {
        }

        [[nodiscard]] bool valid() const noexcept { return binding_ != nullptr && data_ != nullptr; }
        explicit operator bool() const noexcept { return valid(); }
        [[nodiscard]] const TSDataBinding *binding() const noexcept { return binding_; }
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept
        {
            return binding_ != nullptr ? binding_->type_meta : nullptr;
        }
        [[nodiscard]] const void *data() const noexcept { return data_; }
        [[nodiscard]] const TSDataParentLink &parent_link() const { return tracking().parent; }
        [[nodiscard]] std::size_t child_id() const
        {
            if (!valid()) { return TS_DATA_NO_CHILD_ID; }
            const auto &link = tracking().parent;
            return link.has_parent() ? link.child_id : TS_DATA_NO_CHILD_ID;
        }
        [[nodiscard]] bool has_parent() const { return valid() && tracking().parent.has_parent(); }
        [[nodiscard]] std::vector<std::size_t> path_from_root() const
        {
            return valid() ? tracking().parent.path_from_root() : std::vector<std::size_t>{};
        }
        [[nodiscard]] TSDataView root_view() const
        {
            if (!valid() || !has_parent()) { return *this; }
            return tracking().parent.root_view();
        }
        [[nodiscard]] void *mutable_data() const
        {
            if (!valid()) { throw std::logic_error("TSDataView::mutable_data requires a live view"); }
            if (!ops().allows_mutation) { throw std::logic_error("TSDataView::mutable_data requires mutable TSData ops"); }
            return const_cast<void *>(data_);
        }

        [[nodiscard]] const TSDataOps &ops() const
        {
            if (binding_ == nullptr) { throw std::logic_error("TSDataView is not bound"); }
            return binding_->checked_ops();
        }

        [[nodiscard]] const TSDataLayout &layout() const
        {
            const auto &table = ops();
            return *table.layout_impl(table.context);
        }

        [[nodiscard]] const TSDataTracking &tracking() const
        {
            require_live("TSDataView::tracking");
            const auto &table = ops();
            return *table.tracking_impl(table.context, data_);
        }

        [[nodiscard]] ValueView value() const
        {
            require_live("TSDataView::value");
            const auto &table = ops();
            return ValueView{layout().value_binding, table.value_memory_impl(table.context, data_)};
        }

        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const
        {
            require_live("TSDataView::delta_value");
            const auto &data_layout = layout();
            if (!modified(evaluation_time)) { return ValueView{data_layout.delta_binding, nullptr}; }

            const auto &table = ops();
            return ValueView{data_layout.delta_binding, table.delta_memory_impl(table.context, data_)};
        }
        [[nodiscard]] engine_time_t last_modified_time() const { return tracking().last_modified_time; }
        [[nodiscard]] bool modified(engine_time_t evaluation_time) const
        {
            return tracking().last_modified_time == evaluation_time;
        }
        [[nodiscard]] bool has_current_value() const
        {
            require_live("TSDataView::has_current_value");
            const auto &table = ops();
            return table.has_current_value_impl(table.context, data_);
        }
        [[nodiscard]] bool all_valid() const
        {
            require_live("TSDataView::all_valid");
            if (!has_current_value()) { return false; }
            const auto &table = ops();
            return table.all_valid_impl(table.context, data_);
        }
        [[nodiscard]] TSSDataView as_set() &;
        [[nodiscard]] TSSDataView as_set() const &;
        void as_set() && = delete;
        void as_set() const && = delete;
        [[nodiscard]] TSDDataView as_dict() &;
        [[nodiscard]] TSDDataView as_dict() const &;
        void as_dict() && = delete;
        void as_dict() const && = delete;
        [[nodiscard]] TSBDataView as_bundle() &;
        [[nodiscard]] TSBDataView as_bundle() const &;
        void as_bundle() && = delete;
        void as_bundle() const && = delete;
        [[nodiscard]] TSLDataView as_list() &;
        [[nodiscard]] TSLDataView as_list() const &;
        void as_list() && = delete;
        void as_list() const && = delete;
        [[nodiscard]] TSWDataView as_window() &;
        [[nodiscard]] TSWDataView as_window() const &;
        void as_window() && = delete;
        void as_window() const && = delete;

        [[nodiscard]] TSDataMutationView begin_mutation(engine_time_t evaluation_time) const;

      private:
        friend class TSDataMutationView;
        friend class IndexedTSDataView;
        friend class TSDDataView;
        friend class TSDDataMutationView;

        TSDataView(const TSDataBinding *binding, void *data, TSDataView &parent, std::size_t child_id)
            : binding_(binding),
              data_(data)
        {
            bind_parent(parent, child_id);
        }

        TSDataView(const TSDataBinding *binding, const void *data, TSDataView &parent, std::size_t child_id)
            : binding_(binding), data_(data)
        {
            bind_parent(parent, child_id);
        }

        void require_live(const char *what) const
        {
            if (!valid()) { throw std::logic_error(std::string{what} + " requires a live view"); }
        }

        [[nodiscard]] TSDataTracking &mutable_tracking() const
        {
            const auto &table = ops();
            return *table.mutable_tracking_impl(table.context, mutable_data());
        }

        void bind_parent(const TSDataView &parent, std::size_t child_id) const
        {
            if (!valid()) { throw std::logic_error("TSDataView child requires a live view"); }
            if (!parent.valid()) { throw std::logic_error("TSDataView child requires a live parent view"); }
            if (!parent.ops().allows_mutation)
            {
                throw std::logic_error("TSDataView child requires mutable parent TSData ops");
            }
            mutable_tracking().parent = TSDataParentLink{parent.binding(), parent.data(), child_id};
        }

        const TSDataBinding *binding_{nullptr};
        const void          *data_{nullptr};
    };

    class TSDataMutationView
    {
      public:
        TSDataMutationView(TSDataView view, engine_time_t evaluation_time)
            : view_(view), mutation_time_(evaluation_time)
        {
            validate_mutation_view();
        }

        TSDataMutationView(const TSDataMutationView &) = delete;
        TSDataMutationView &operator=(const TSDataMutationView &) = delete;

        TSDataMutationView(TSDataMutationView &&other) noexcept
            : view_(other.view_),
              mutation_time_(std::exchange(other.mutation_time_, MIN_DT))
        {
        }

        TSDataMutationView &operator=(TSDataMutationView &&) = delete;

        ~TSDataMutationView() noexcept = default;

        [[nodiscard]] const TSDataView &view() const noexcept { return view_; }
        [[nodiscard]] TSDataView &view() noexcept { return view_; }
        [[nodiscard]] const TSDataOps &ops() const { return view_.ops(); }
        [[nodiscard]] void *mutable_data() const
        {
            require_active_mutation();
            return view_.mutable_data();
        }
        [[nodiscard]] ValueView value() const { return view_.value(); }
        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const
        {
            return view_.delta_value(evaluation_time);
        }
        [[nodiscard]] engine_time_t current_mutation_time() const { return mutation_time_; }
        [[nodiscard]] bool modified(engine_time_t evaluation_time) const { return view_.modified(evaluation_time); }

        void mark_modified()
        {
            if (record_modified_local()) { notify_parent_modified(); }
        }

        void restore_last_modified_time(engine_time_t previous_modified_time)
        {
            require_active_mutation();

            auto &state = view_.mutable_tracking();
            if (state.last_modified_time == mutation_time_) { state.last_modified_time = previous_modified_time; }
        }

        [[nodiscard]] bool copy_value_from(const ValueView &source)
        {
            require_active_mutation();

            const auto &table = view_.ops();
            const bool newly_modified =
                table.copy_value_from_impl(table.context, view_.mutable_data(), source, mutation_time_);
            if (newly_modified && !record_modified_local())
            {
                throw std::logic_error(
                    "TSDataMutationView::copy_value_from reported a new modification that was already recorded");
            }
            if (newly_modified) { notify_parent_modified(); }
            return newly_modified;
        }

      private:
        void require_active_mutation() const
        {
            if (mutation_time_ == MIN_DT)
            {
                throw std::logic_error("TSData mutation requires an active mutation scope");
            }
            (void)view_.mutable_data();
        }

        void validate_mutation_view() const
        {
            if (mutation_time_ == MIN_DT)
            {
                throw std::invalid_argument("TSDataMutationView requires a concrete engine time");
            }
            (void)view_.mutable_data();
        }

        [[nodiscard]] bool record_modified_local() const
        {
            require_active_mutation();

            auto &state = view_.mutable_tracking();
            if (state.last_modified_time == mutation_time_) { return false; }

            state.last_modified_time = mutation_time_;
            return true;
        }

        void notify_parent_modified() const
        {
            view_.parent_link().notify_child_modified(mutation_time_);
        }

        TSDataView    view_{};
        engine_time_t mutation_time_{MIN_DT};
    };

    inline void apply_slot_mutation_result(TSDataMutationView &mutation, const SlotTSDataMutationResult &result)
    {
        if (!result.changed) { return; }
        if (result.has_delta)
        {
            mutation.mark_modified();
            return;
        }
        mutation.restore_last_modified_time(result.previous_modified_time);
    }

    class IndexedTSDataView
    {
      public:
        [[nodiscard]] const TSDataView &base() const noexcept { return *view_; }
        [[nodiscard]] TSDataView &base() noexcept { return *view_; }
        [[nodiscard]] const TSDataBinding *binding() const noexcept { return view_->binding(); }
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept { return view_->schema(); }
        [[nodiscard]] const TSDataLayout &layout() const { return view_->layout(); }
        [[nodiscard]] ValueView value() const { return view_->value(); }
        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const
        {
            return view_->delta_value(evaluation_time);
        }
        [[nodiscard]] engine_time_t last_modified_time() const { return view_->last_modified_time(); }
        [[nodiscard]] bool modified(engine_time_t evaluation_time) const { return view_->modified(evaluation_time); }
        [[nodiscard]] std::size_t size() const
        {
            const auto &ops = indexed_ops();
            return ops.size_impl(ops.context, view_->data());
        }
        [[nodiscard]] bool empty() const { return size() == 0; }

        [[nodiscard]] TSDataView at(std::size_t index) & { return at_impl(index); }
        [[nodiscard]] TSDataView at(std::size_t index) const &
        {
            return const_cast<IndexedTSDataView *>(this)->at_impl(index);
        }
        TSDataView at(std::size_t) && = delete;
        [[nodiscard]] TSDataView operator[](std::size_t index) & { return at(index); }
        [[nodiscard]] TSDataView operator[](std::size_t index) const & { return at(index); }
        TSDataView operator[](std::size_t) && = delete;

        [[nodiscard]] Range<TSDataView> values() const
        {
            return values_range(nullptr);
        }

        [[nodiscard]] Range<TSDataView> valid_values() const
        {
            return values_range(&child_valid_predicate);
        }

        [[nodiscard]] Range<TSDataView> modified_values(engine_time_t evaluation_time) const
        {
            if (!modified(evaluation_time)) { return empty_values_range(); }
            return values_range(&child_modified_predicate);
        }

        [[nodiscard]] KeyValueRange<std::size_t, TSDataView> items() const
        {
            return items_range(nullptr);
        }

        [[nodiscard]] KeyValueRange<std::size_t, TSDataView> valid_items() const
        {
            return items_range(&child_valid_predicate);
        }

        [[nodiscard]] KeyValueRange<std::size_t, TSDataView> modified_items(engine_time_t evaluation_time) const
        {
            if (!modified(evaluation_time)) { return empty_items_range(); }
            return items_range(&child_modified_predicate);
        }

      protected:
        IndexedTSDataView(TSDataView &view, TSTypeKind expected_kind, const char *what)
            : view_(&view)
        {
            validate_kind(view, expected_kind, what);
        }

        [[nodiscard]] bool child_valid(std::size_t index) const
        {
            return child_last_modified_time(index) != MIN_DT;
        }

        [[nodiscard]] bool child_modified_at_parent_time(std::size_t index) const
        {
            return child_last_modified_time(index) == last_modified_time();
        }

        [[nodiscard]] const IndexedTSDataOps &indexed_ops() const
        {
            return static_cast<const IndexedTSDataOps &>(view_->ops());
        }

        [[nodiscard]] Range<TSDataView> values_range(Range<TSDataView>::predicate_fn predicate) const
        {
            return Range<TSDataView>{
                .context   = this,
                .memory    = nullptr,
                .limit     = size(),
                .predicate = predicate,
                .projector = &project_value,
            };
        }

        [[nodiscard]] KeyValueRange<std::size_t, TSDataView> items_range(
            KeyValueRange<std::size_t, TSDataView>::predicate_fn predicate) const
        {
            return KeyValueRange<std::size_t, TSDataView>{
                .context   = this,
                .memory    = nullptr,
                .limit     = size(),
                .predicate = predicate,
                .projector = &project_item,
            };
        }

      private:
        static void validate_kind(const TSDataView &view, TSTypeKind expected_kind, const char *what)
        {
            if (!view.valid()) { throw std::logic_error(std::string{what} + " requires a live view"); }
            const auto *schema = view.schema();
            if (schema == nullptr || schema->kind != expected_kind)
            {
                throw std::invalid_argument(std::string{what} + " requires the matching TSData kind");
            }
            (void)static_cast<const IndexedTSDataOps &>(view.ops());
        }

        [[nodiscard]] engine_time_t child_last_modified_time(std::size_t index) const
        {
            const auto &ops = indexed_ops();
            if (index >= ops.size_impl(ops.context, view_->data())) { return MIN_DT; }
            const auto *element_binding = ops.element_binding_impl(ops.context, view_->data(), index);
            const auto *element_memory  = ops.element_memory_impl(ops.context, view_->data(), index);
            if (element_binding == nullptr || element_memory == nullptr) { return MIN_DT; }
            const auto &element_ops = element_binding->checked_ops();
            return element_ops.tracking_impl(element_ops.context, element_memory)->last_modified_time;
        }

        [[nodiscard]] TSDataView at_impl(std::size_t index)
        {
            const auto &ops = indexed_ops();
            if (index >= ops.size_impl(ops.context, view_->data()))
            {
                throw std::out_of_range("IndexedTSDataView::at: index out of range");
            }
            const auto *element_binding = ops.element_binding_impl(ops.context, view_->data(), index);
            if (element_binding == nullptr)
            {
                throw std::logic_error("IndexedTSDataView::at: element binding is not resolved");
            }
            const auto *element_memory = ops.element_memory_impl(ops.context, view_->data(), index);
            if (element_memory == nullptr)
            {
                throw std::logic_error("IndexedTSDataView::at: element memory is not resolved");
            }
            if (!view_->ops().allows_mutation)
            {
                return TSDataView{element_binding, element_memory};
            }
            return TSDataView{element_binding, element_memory, *view_, index};
        }

        [[nodiscard]] static Range<TSDataView> empty_values_range() noexcept
        {
            return Range<TSDataView>{.context = nullptr, .memory = nullptr, .limit = 0, .predicate = nullptr,
                                     .projector = nullptr};
        }

        [[nodiscard]] static KeyValueRange<std::size_t, TSDataView> empty_items_range() noexcept
        {
            return KeyValueRange<std::size_t, TSDataView>{.context = nullptr, .memory = nullptr, .limit = 0,
                                                          .predicate = nullptr, .projector = nullptr};
        }

        [[nodiscard]] static bool child_valid_predicate(const void *context, const void *, std::size_t index)
        {
            return static_cast<const IndexedTSDataView *>(context)->child_valid(index);
        }

        [[nodiscard]] static bool child_modified_predicate(const void *context, const void *, std::size_t index)
        {
            return static_cast<const IndexedTSDataView *>(context)->child_modified_at_parent_time(index);
        }

        [[nodiscard]] static TSDataView project_value(const void *context, const void *, std::size_t index)
        {
            return const_cast<IndexedTSDataView *>(static_cast<const IndexedTSDataView *>(context))->at(index);
        }

        [[nodiscard]] static std::pair<std::size_t, TSDataView> project_item(const void *context, const void *,
                                                                             std::size_t index)
        {
            return {index, project_value(context, nullptr, index)};
        }

        TSDataView *view_{nullptr};
    };

    class TSSDataView
    {
      public:
        explicit TSSDataView(TSDataView view)
            : view_(view)
        {
            validate_kind(view_);
        }

        [[nodiscard]] const TSDataView &base() const noexcept { return view_; }
        [[nodiscard]] TSDataView &base() noexcept { return view_; }
        [[nodiscard]] const TSDataBinding *binding() const noexcept { return view_.binding(); }
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept { return view_.schema(); }
        [[nodiscard]] const TSSDataLayout &layout() const
        {
            return static_cast<const TSSDataLayout &>(view_.layout());
        }
        [[nodiscard]] ValueView value() const { return view_.value(); }
        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const
        {
            return view_.delta_value(evaluation_time);
        }
        [[nodiscard]] engine_time_t last_modified_time() const { return view_.last_modified_time(); }
        [[nodiscard]] bool modified(engine_time_t evaluation_time) const { return view_.modified(evaluation_time); }

        [[nodiscard]] std::size_t size() const
        {
            const auto &ops = set_ops();
            return ops.size_impl(ops.context, view_.data());
        }
        [[nodiscard]] bool empty() const { return size() == 0; }
        [[nodiscard]] std::size_t slot_capacity() const
        {
            const auto &ops = set_ops();
            return ops.slot_capacity_impl(ops.context, view_.data());
        }
        [[nodiscard]] bool slot_occupied(std::size_t slot) const
        {
            const auto &ops = set_ops();
            return ops.slot_occupied_impl(ops.context, view_.data(), slot);
        }
        [[nodiscard]] bool slot_live(std::size_t slot) const
        {
            const auto &ops = set_ops();
            return ops.slot_live_impl(ops.context, view_.data(), slot);
        }
        [[nodiscard]] bool slot_added(std::size_t slot) const
        {
            const auto &ops = set_ops();
            return ops.slot_added_impl(ops.context, view_.data(), slot);
        }
        [[nodiscard]] bool slot_removed(std::size_t slot) const
        {
            const auto &ops = set_ops();
            return ops.slot_removed_impl(ops.context, view_.data(), slot);
        }
        [[nodiscard]] ValueView at_slot(std::size_t slot) const
        {
            if (!slot_occupied(slot)) { throw std::out_of_range("TSSDataView::at_slot: slot is not occupied"); }
            const auto &ops = set_ops();
            return ValueView{layout().key_binding, ops.key_at_slot_impl(ops.context, view_.data(), slot)};
        }
        [[nodiscard]] bool contains(const ValueView &key) const
        {
            const auto &ops = set_ops();
            return ops.contains_impl(ops.context, view_.data(), key);
        }
        [[nodiscard]] std::size_t find_slot(const ValueView &key) const
        {
            const auto &ops = set_ops();
            return ops.find_slot_impl(ops.context, view_.data(), key);
        }
        [[nodiscard]] Range<ValueView> values() const
        {
            const auto &ops = set_ops();
            return ops.make_values_range_impl(ops.context, view_.data());
        }
        [[nodiscard]] Range<ValueView> added() const
        {
            const auto &ops = set_ops();
            return ops.make_added_values_range_impl(ops.context, view_.data());
        }
        [[nodiscard]] Range<ValueView> removed() const
        {
            const auto &ops = set_ops();
            return ops.make_removed_values_range_impl(ops.context, view_.data());
        }
        [[nodiscard]] Range<ValueView> added_values() const { return added(); }
        [[nodiscard]] Range<ValueView> removed_values() const { return removed(); }
        [[nodiscard]] auto begin() const { return values().begin(); }
        [[nodiscard]] auto end() const { return values().end(); }

        [[nodiscard]] TSSDataMutationView begin_mutation(engine_time_t evaluation_time) const;

      protected:
        [[nodiscard]] const TSSDataOps &set_ops() const
        {
            return static_cast<const TSSDataOps &>(view_.ops());
        }

        static void validate_kind(const TSDataView &view)
        {
            if (!view.valid()) { throw std::logic_error("TSSDataView requires a live view"); }
            const auto *schema = view.schema();
            if (schema == nullptr || schema->kind != TSTypeKind::TSS)
            {
                throw std::invalid_argument("TSSDataView requires a TSS TSData kind");
            }
            (void)static_cast<const TSSDataOps &>(view.ops());
        }

        TSDataView view_{};
    };

    class TSDDataView
    {
      public:
        explicit TSDDataView(TSDataView view)
            : view_(view)
        {
            validate_kind(view_);
        }

        [[nodiscard]] const TSDataView &base() const noexcept { return view_; }
        [[nodiscard]] TSDataView &base() noexcept { return view_; }
        [[nodiscard]] const TSDataBinding *binding() const noexcept { return view_.binding(); }
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept { return view_.schema(); }
        [[nodiscard]] const TSDDataLayout &layout() const
        {
            return static_cast<const TSDDataLayout &>(view_.layout());
        }
        [[nodiscard]] ValueView value() const { return view_.value(); }
        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const
        {
            return view_.delta_value(evaluation_time);
        }
        [[nodiscard]] engine_time_t last_modified_time() const { return view_.last_modified_time(); }
        [[nodiscard]] bool modified(engine_time_t evaluation_time) const { return view_.modified(evaluation_time); }

        [[nodiscard]] std::size_t size() const
        {
            const auto &ops = dict_ops();
            return ops.size_impl(ops.context, view_.data());
        }
        [[nodiscard]] bool empty() const { return size() == 0; }
        [[nodiscard]] std::size_t slot_capacity() const
        {
            const auto &ops = dict_ops();
            return ops.slot_capacity_impl(ops.context, view_.data());
        }
        [[nodiscard]] bool slot_occupied(std::size_t slot) const
        {
            const auto &ops = dict_ops();
            return ops.slot_occupied_impl(ops.context, view_.data(), slot);
        }
        [[nodiscard]] bool slot_live(std::size_t slot) const
        {
            const auto &ops = dict_ops();
            return ops.slot_live_impl(ops.context, view_.data(), slot);
        }
        [[nodiscard]] bool slot_added(std::size_t slot) const
        {
            const auto &ops = dict_ops();
            return ops.slot_added_impl(ops.context, view_.data(), slot);
        }
        [[nodiscard]] bool slot_removed(std::size_t slot) const
        {
            const auto &ops = dict_ops();
            return ops.slot_removed_impl(ops.context, view_.data(), slot);
        }
        [[nodiscard]] bool slot_modified(std::size_t slot) const
        {
            const auto &ops = dict_ops();
            return ops.slot_modified_impl(ops.context, view_.data(), slot);
        }
        [[nodiscard]] ValueView key_at_slot(std::size_t slot) const
        {
            if (!slot_occupied(slot)) { throw std::out_of_range("TSDDataView::key_at_slot: slot is not occupied"); }
            const auto &ops = dict_ops();
            return ValueView{layout().key_binding, ops.key_at_slot_impl(ops.context, view_.data(), slot)};
        }
        [[nodiscard]] bool contains(const ValueView &key) const
        {
            const auto &ops = dict_ops();
            return ops.contains_impl(ops.context, view_.data(), key);
        }
        [[nodiscard]] std::size_t find_slot(const ValueView &key) const
        {
            const auto &ops = dict_ops();
            return ops.find_slot_impl(ops.context, view_.data(), key);
        }
        [[nodiscard]] TSDataView at_slot(std::size_t slot) const
        {
            if (!slot_occupied(slot)) { throw std::out_of_range("TSDDataView::at_slot: slot is not occupied"); }
            const auto &ops = dict_ops();
            const auto *child_memory = ops.child_at_slot_impl(ops.context, view_.data(), slot);
            if (!view_.ops().allows_mutation) { return TSDataView{layout().element_binding, child_memory}; }
            return TSDataView{layout().element_binding, child_memory, const_cast<TSDataView &>(view_), slot};
        }
        [[nodiscard]] TSDataView at(const ValueView &key) const
        {
            const auto slot = find_slot(key);
            if (slot == TS_DATA_NO_CHILD_ID)
            {
                return TSDataView{layout().element_binding, static_cast<const void *>(nullptr)};
            }
            return at_slot(slot);
        }
        [[nodiscard]] TSDataView operator[](const ValueView &key) const { return at(key); }
        [[nodiscard]] Range<ValueView> keys() const
        {
            const auto &ops = dict_ops();
            return ops.make_values_range_impl(ops.context, view_.data());
        }
        [[nodiscard]] Range<TSDataView> values() const
        {
            return ts_data_values_range(&slot_live_predicate);
        }
        [[nodiscard]] KeyValueRange<ValueView, TSDataView> items() const
        {
            return ts_data_items_range(&slot_live_predicate);
        }
        [[nodiscard]] Range<ValueView> valid_keys() const
        {
            const auto &ops = dict_ops();
            return ops.make_valid_keys_range_impl(ops.context, view_.data());
        }
        [[nodiscard]] Range<TSDataView> valid_values() const
        {
            return ts_data_values_range(&slot_valid_predicate);
        }
        [[nodiscard]] KeyValueRange<ValueView, TSDataView> valid_items() const
        {
            return ts_data_items_range(&slot_valid_predicate);
        }
        [[nodiscard]] Range<ValueView> modified_keys(engine_time_t evaluation_time) const
        {
            if (!modified(evaluation_time)) { return empty_value_range(); }
            const auto &ops = dict_ops();
            return ops.make_modified_keys_range_impl(ops.context, view_.data());
        }
        [[nodiscard]] Range<TSDataView> modified_values(engine_time_t evaluation_time) const
        {
            if (!modified(evaluation_time)) { return empty_ts_data_range(); }
            return ts_data_values_range(&slot_modified_predicate);
        }
        [[nodiscard]] KeyValueRange<ValueView, TSDataView> modified_items(engine_time_t evaluation_time) const
        {
            if (!modified(evaluation_time)) { return empty_ts_data_kv_range(); }
            return ts_data_items_range(&slot_modified_predicate);
        }
        [[nodiscard]] Range<ValueView> added_keys() const { return key_set().added(); }
        [[nodiscard]] Range<TSDataView> added_values() const
        {
            return ts_data_values_range(&slot_added_predicate);
        }
        [[nodiscard]] KeyValueRange<ValueView, TSDataView> added_items() const
        {
            return ts_data_items_range(&slot_added_predicate);
        }
        [[nodiscard]] Range<ValueView> removed_keys() const { return key_set().removed(); }
        [[nodiscard]] Range<TSDataView> removed_values() const
        {
            return ts_data_values_range(&slot_removed_predicate);
        }
        [[nodiscard]] KeyValueRange<ValueView, TSDataView> removed_items() const
        {
            return ts_data_items_range(&slot_removed_predicate);
        }
        [[nodiscard]] TSSDataView key_set() const
        {
            const auto *key_set_binding = layout().key_set_binding;
            if (key_set_binding == nullptr)
            {
                throw std::logic_error("TSDDataView::key_set requires a key-set binding");
            }
            return TSSDataView{TSDataView{key_set_binding, view_.data()}};
        }

        [[nodiscard]] TSDDataMutationView begin_mutation(engine_time_t evaluation_time) const;

      private:
        [[nodiscard]] static Range<ValueView> empty_value_range() noexcept
        {
            return Range<ValueView>{.context = nullptr, .memory = nullptr, .limit = 0, .predicate = nullptr,
                                    .projector = nullptr};
        }

        [[nodiscard]] static Range<TSDataView> empty_ts_data_range() noexcept
        {
            return Range<TSDataView>{.context = nullptr, .memory = nullptr, .limit = 0, .predicate = nullptr,
                                     .projector = nullptr};
        }

        [[nodiscard]] static KeyValueRange<ValueView, TSDataView> empty_ts_data_kv_range() noexcept
        {
            return KeyValueRange<ValueView, TSDataView>{.context = nullptr, .memory = nullptr, .limit = 0,
                                                        .predicate = nullptr, .projector = nullptr};
        }

        [[nodiscard]] Range<TSDataView> ts_data_values_range(Range<TSDataView>::predicate_fn predicate) const
        {
            return Range<TSDataView>{
                .context   = this,
                .memory    = nullptr,
                .limit     = slot_capacity(),
                .predicate = predicate,
                .projector = &project_ts_value_at_slot,
            };
        }

        [[nodiscard]] KeyValueRange<ValueView, TSDataView> ts_data_items_range(
            KeyValueRange<ValueView, TSDataView>::predicate_fn predicate) const
        {
            return KeyValueRange<ValueView, TSDataView>{
                .context   = this,
                .memory    = nullptr,
                .limit     = slot_capacity(),
                .predicate = predicate,
                .projector = &project_ts_item_at_slot,
            };
        }

        [[nodiscard]] static bool slot_live_predicate(const void *context, const void *, std::size_t slot)
        {
            return static_cast<const TSDDataView *>(context)->slot_live(slot);
        }

        [[nodiscard]] static bool slot_valid_predicate(const void *context, const void *, std::size_t slot)
        {
            const auto *self = static_cast<const TSDDataView *>(context);
            return self->slot_live(slot) && self->at_slot(slot).last_modified_time() != MIN_DT;
        }

        [[nodiscard]] static bool slot_modified_predicate(const void *context, const void *, std::size_t slot)
        {
            const auto *self = static_cast<const TSDDataView *>(context);
            return self->slot_live(slot) && self->slot_modified(slot);
        }

        [[nodiscard]] static bool slot_added_predicate(const void *context, const void *, std::size_t slot)
        {
            const auto *self = static_cast<const TSDDataView *>(context);
            return self->slot_occupied(slot) && self->slot_added(slot);
        }

        [[nodiscard]] static bool slot_removed_predicate(const void *context, const void *, std::size_t slot)
        {
            const auto *self = static_cast<const TSDDataView *>(context);
            return self->slot_occupied(slot) && self->slot_removed(slot);
        }

        [[nodiscard]] static TSDataView project_ts_value_at_slot(const void *context, const void *, std::size_t slot)
        {
            return static_cast<const TSDDataView *>(context)->at_slot(slot);
        }

        [[nodiscard]] static std::pair<ValueView, TSDataView> project_ts_item_at_slot(const void *context,
                                                                                      const void *,
                                                                                      std::size_t slot)
        {
            const auto *self = static_cast<const TSDDataView *>(context);
            return {self->key_at_slot(slot), self->at_slot(slot)};
        }

        [[nodiscard]] const TSDDataOps &dict_ops() const
        {
            return static_cast<const TSDDataOps &>(view_.ops());
        }

        static void validate_kind(const TSDataView &view)
        {
            if (!view.valid()) { throw std::logic_error("TSDDataView requires a live view"); }
            const auto *schema = view.schema();
            if (schema == nullptr || schema->kind != TSTypeKind::TSD)
            {
                throw std::invalid_argument("TSDDataView requires a TSD TSData kind");
            }
            (void)static_cast<const TSDDataOps &>(view.ops());
        }

        TSDataView view_{};
    };

    class TSBDataView : public IndexedTSDataView
    {
      public:
        using IndexedTSDataView::at;
        using IndexedTSDataView::operator[];

        explicit TSBDataView(TSDataView &view) : IndexedTSDataView(view, TSTypeKind::TSB, "TSBDataView") {}

        [[nodiscard]] TSDataView at(std::string_view name) &
        {
            return IndexedTSDataView::at(field_index(name));
        }
        [[nodiscard]] TSDataView at(std::string_view name) const &
        {
            return const_cast<TSBDataView *>(this)->at(name);
        }
        TSDataView at(std::string_view) && = delete;
        [[nodiscard]] TSDataView field(std::string_view name) & { return at(name); }
        [[nodiscard]] TSDataView field(std::string_view name) const & { return at(name); }
        TSDataView field(std::string_view) && = delete;
        [[nodiscard]] TSDataView operator[](std::string_view name) & { return at(name); }
        [[nodiscard]] TSDataView operator[](std::string_view name) const & { return at(name); }
        TSDataView operator[](std::string_view) && = delete;

        [[nodiscard]] bool has_field(std::string_view name) const noexcept
        {
            return find_field_index(name) != npos;
        }

        [[nodiscard]] Range<std::string_view> keys() const
        {
            return Range<std::string_view>{
                .context   = this,
                .memory    = nullptr,
                .limit     = size(),
                .predicate = nullptr,
                .projector = &project_key,
            };
        }

        [[nodiscard]] KeyValueRange<std::string_view, TSDataView> items() const
        {
            return named_items_range(nullptr);
        }

        [[nodiscard]] KeyValueRange<std::string_view, TSDataView> valid_items() const
        {
            return named_items_range(&named_child_valid_predicate);
        }

        [[nodiscard]] KeyValueRange<std::string_view, TSDataView> modified_items(engine_time_t evaluation_time) const
        {
            if (!modified(evaluation_time)) { return empty_named_items_range(); }
            return named_items_range(&named_child_modified_predicate);
        }

      private:
        static constexpr std::size_t npos = static_cast<std::size_t>(-1);

        [[nodiscard]] std::size_t field_index(std::string_view name) const
        {
            const auto index = find_field_index(name);
            if (index == npos) { throw std::out_of_range("TSBDataView::at: field not found"); }
            return index;
        }

        [[nodiscard]] std::size_t find_field_index(std::string_view name) const noexcept
        {
            const auto *meta = schema();
            if (meta == nullptr || meta->kind != TSTypeKind::TSB) { return npos; }
            for (std::size_t index = 0; index < meta->field_count(); ++index)
            {
                const char *field_name = meta->fields()[index].name;
                if (field_name != nullptr && name == field_name) { return index; }
            }
            return npos;
        }

        [[nodiscard]] std::string_view key_at(std::size_t index) const noexcept
        {
            const auto *meta = schema();
            if (meta == nullptr || meta->kind != TSTypeKind::TSB || index >= meta->field_count()) { return {}; }
            const char *field_name = meta->fields()[index].name;
            return field_name != nullptr ? std::string_view{field_name} : std::string_view{};
        }

        [[nodiscard]] KeyValueRange<std::string_view, TSDataView> named_items_range(
            KeyValueRange<std::string_view, TSDataView>::predicate_fn predicate) const
        {
            return KeyValueRange<std::string_view, TSDataView>{
                .context   = this,
                .memory    = nullptr,
                .limit     = size(),
                .predicate = predicate,
                .projector = &project_named_item,
            };
        }

        [[nodiscard]] static KeyValueRange<std::string_view, TSDataView> empty_named_items_range() noexcept
        {
            return KeyValueRange<std::string_view, TSDataView>{.context = nullptr, .memory = nullptr, .limit = 0,
                                                               .predicate = nullptr, .projector = nullptr};
        }

        [[nodiscard]] static bool named_child_valid_predicate(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSBDataView *>(context)->child_valid(index);
        }

        [[nodiscard]] static bool named_child_modified_predicate(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSBDataView *>(context)->child_modified_at_parent_time(index);
        }

        [[nodiscard]] static std::string_view project_key(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSBDataView *>(context)->key_at(index);
        }

        [[nodiscard]] static std::pair<std::string_view, TSDataView> project_named_item(const void *context,
                                                                                        const void *,
                                                                                        std::size_t index)
        {
            auto *self = const_cast<TSBDataView *>(static_cast<const TSBDataView *>(context));
            return {self->key_at(index), self->IndexedTSDataView::at(index)};
        }
    };

    class TSLDataView : public IndexedTSDataView
    {
      public:
        explicit TSLDataView(TSDataView &view) : IndexedTSDataView(view, TSTypeKind::TSL, "TSLDataView") {}
    };

    class TSWDataView
    {
      public:
        explicit TSWDataView(TSDataView view)
            : view_(view)
        {
            validate_kind(view_);
        }

        [[nodiscard]] const TSDataView &base() const noexcept { return view_; }
        [[nodiscard]] TSDataView &base() noexcept { return view_; }
        [[nodiscard]] const TSDataBinding *binding() const noexcept { return view_.binding(); }
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept { return view_.schema(); }
        [[nodiscard]] const TSWDataLayout &layout() const
        {
            return static_cast<const TSWDataLayout &>(view_.layout());
        }
        [[nodiscard]] const SizeTSWDataLayout &size_layout() const
        {
            if (duration_based()) { throw std::logic_error("TSWDataView::size_layout requires a size-based TSW"); }
            return static_cast<const SizeTSWDataLayout &>(view_.layout());
        }
        [[nodiscard]] const TimeTSWDataLayout &time_layout() const
        {
            if (!duration_based()) { throw std::logic_error("TSWDataView::time_layout requires a time-based TSW"); }
            return static_cast<const TimeTSWDataLayout &>(view_.layout());
        }
        [[nodiscard]] ValueView value() const { return view_.value(); }
        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const
        {
            return view_.delta_value(evaluation_time);
        }
        [[nodiscard]] engine_time_t last_modified_time() const { return view_.last_modified_time(); }
        [[nodiscard]] bool modified(engine_time_t evaluation_time) const { return view_.modified(evaluation_time); }
        [[nodiscard]] bool duration_based() const noexcept { return schema()->is_duration_based(); }
        [[nodiscard]] bool size_based() const noexcept { return !duration_based(); }
        [[nodiscard]] bool time_based() const noexcept { return duration_based(); }
        [[nodiscard]] std::size_t period() const { return size_layout().period; }
        [[nodiscard]] std::size_t min_period() const { return size_layout().min_period; }
        [[nodiscard]] engine_time_delta_t time_range() const { return time_layout().time_range; }
        [[nodiscard]] engine_time_delta_t min_time_range() const { return time_layout().min_time_range; }
        [[nodiscard]] std::size_t capacity() const
        {
            const auto &ops = window_ops();
            return ops.capacity_impl(ops.context, view_.data());
        }

        [[nodiscard]] std::size_t size() const
        {
            const auto &ops = window_ops();
            return ops.size_impl(ops.context, view_.data());
        }
        [[nodiscard]] bool empty() const { return size() == 0; }
        [[nodiscard]] bool full() const
        {
            const auto &ops = window_ops();
            return ops.full_impl(ops.context, view_.data());
        }
        [[nodiscard]] bool all_valid() const
        {
            return view_.all_valid();
        }
        [[nodiscard]] engine_time_t first_modified_time() const
        {
            return empty() ? MIN_DT : time_at(0);
        }
        [[nodiscard]] engine_time_t time_at(std::size_t index) const
        {
            const auto &ops = window_ops();
            if (index >= size()) { throw std::out_of_range("TSWDataView::time_at: index out of range"); }
            return ops.time_at_impl(ops.context, view_.data(), index);
        }
        [[nodiscard]] ValueView time_value_at(std::size_t index) const
        {
            const auto &ops = window_ops();
            if (index >= size()) { throw std::out_of_range("TSWDataView::time_value_at: index out of range"); }
            return ValueView{layout().time_binding, ops.time_element_at_impl(ops.context, view_.data(), index)};
        }
        [[nodiscard]] ValueView at(std::size_t index) const
        {
            const auto &ops = window_ops();
            if (index >= size()) { throw std::out_of_range("TSWDataView::at: index out of range"); }
            return ValueView{layout().element_binding, ops.element_at_impl(ops.context, view_.data(), index)};
        }
        [[nodiscard]] ValueView operator[](std::size_t index) const { return at(index); }
        [[nodiscard]] ValueView front() const
        {
            if (empty()) { throw std::out_of_range("TSWDataView::front on empty window"); }
            return at(0);
        }
        [[nodiscard]] ValueView back() const
        {
            if (empty()) { throw std::out_of_range("TSWDataView::back on empty window"); }
            return at(size() - 1);
        }

        [[nodiscard]] Range<ValueView> values() const
        {
            return Range<ValueView>{
                .context   = this,
                .memory    = nullptr,
                .limit     = size(),
                .predicate = nullptr,
                .projector = &project_value,
            };
        }

        [[nodiscard]] Range<ValueView> time_values() const
        {
            return Range<ValueView>{
                .context   = this,
                .memory    = nullptr,
                .limit     = size(),
                .predicate = nullptr,
                .projector = &project_time_value,
            };
        }

        [[nodiscard]] Range<engine_time_t> value_times() const
        {
            return Range<engine_time_t>{
                .context   = this,
                .memory    = nullptr,
                .limit     = size(),
                .predicate = nullptr,
                .projector = &project_time,
            };
        }

        [[nodiscard]] auto begin() const { return values().begin(); }
        [[nodiscard]] auto end() const { return values().end(); }

        [[nodiscard]] TSWDataMutationView begin_mutation(engine_time_t evaluation_time) const;

      private:
        static void validate_kind(const TSDataView &view)
        {
            if (!view.valid()) { throw std::logic_error("TSWDataView requires a live view"); }
            const auto *schema = view.schema();
            if (schema == nullptr || schema->kind != TSTypeKind::TSW)
            {
                throw std::invalid_argument("TSWDataView requires a TSW TSData kind");
            }
            (void)static_cast<const TSWDataOps &>(view.ops());
        }

        [[nodiscard]] const TSWDataOps &window_ops() const
        {
            return static_cast<const TSWDataOps &>(view_.ops());
        }

        [[nodiscard]] static ValueView project_value(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSWDataView *>(context)->at(index);
        }

        [[nodiscard]] static ValueView project_time_value(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSWDataView *>(context)->time_value_at(index);
        }

        [[nodiscard]] static engine_time_t project_time(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSWDataView *>(context)->time_at(index);
        }

        TSDataView view_{};
    };

    class TSWDataMutationView : public TSWDataView
    {
      public:
        TSWDataMutationView(TSDataView view, engine_time_t evaluation_time)
            : TSWDataView(view),
              mutation_(view.begin_mutation(evaluation_time))
        {
            if (view.schema() == nullptr || view.schema()->kind != TSTypeKind::TSW)
            {
                throw std::invalid_argument("TSWDataMutationView requires a TSW TSData kind");
            }
        }

        TSWDataMutationView(const TSWDataMutationView &) = delete;
        TSWDataMutationView &operator=(const TSWDataMutationView &) = delete;
        TSWDataMutationView(TSWDataMutationView &&) noexcept = default;
        TSWDataMutationView &operator=(TSWDataMutationView &&) = delete;

        [[nodiscard]] TSWDataView view() { return TSWDataView{base()}; }
        [[nodiscard]] engine_time_t current_mutation_time() const { return mutation_.current_mutation_time(); }

        void push(const ValueView &source)
        {
            if (mutation_.modified(current_mutation_time()))
            {
                throw std::logic_error("TSWDataMutationView::push allows only one window tick per engine time");
            }
            const auto &ops = static_cast<const TSWDataOps &>(mutation_.ops());
            if (ops.push_impl == nullptr)
            {
                throw std::logic_error("TSWDataMutationView::push is not supported by this TSW ops");
            }
            ops.push_impl(ops.context, mutation_.mutable_data(), source, current_mutation_time());
            mutation_.mark_modified();
        }

        [[nodiscard]] bool copy_value_from(const ValueView &source)
        {
            return mutation_.copy_value_from(source);
        }

      private:
        TSDataMutationView mutation_;
    };

    class TSSDataMutationView : public TSSDataView
    {
      public:
        TSSDataMutationView(TSDataView view, engine_time_t evaluation_time)
            : TSSDataView(view),
              mutation_(view.begin_mutation(evaluation_time))
        {
            if (view.schema() == nullptr || view.schema()->kind != TSTypeKind::TSS)
            {
                throw std::invalid_argument("TSSDataMutationView requires a TSS TSData kind");
            }
        }

        TSSDataMutationView(const TSSDataMutationView &) = delete;
        TSSDataMutationView &operator=(const TSSDataMutationView &) = delete;
        TSSDataMutationView(TSSDataMutationView &&) noexcept = default;
        TSSDataMutationView &operator=(TSSDataMutationView &&) = delete;

        [[nodiscard]] TSSDataView view() { return TSSDataView{base()}; }
        [[nodiscard]] engine_time_t current_mutation_time() const { return mutation_.current_mutation_time(); }

        void reserve(std::size_t capacity)
        {
            const auto &ops = static_cast<const TSSDataOps &>(mutation_.ops());
            ops.reserve_impl(ops.context, mutation_.mutable_data(), capacity);
        }

        [[nodiscard]] bool add(const ValueView &key)
        {
            const auto &ops    = static_cast<const TSSDataOps &>(mutation_.ops());
            const auto  result = ops.insert_key_impl(ops.context, mutation_.mutable_data(), key,
                                                     current_mutation_time());
            apply_slot_mutation_result(mutation_, result);
            return result.changed;
        }

        [[nodiscard]] bool remove(const ValueView &key)
        {
            const auto &ops    = static_cast<const TSSDataOps &>(mutation_.ops());
            const auto  result = ops.remove_key_impl(ops.context, mutation_.mutable_data(), key,
                                                     current_mutation_time());
            apply_slot_mutation_result(mutation_, result);
            return result.changed;
        }

        void clear()
        {
            std::vector<ValueView> keys;
            for (const auto key : values()) { keys.push_back(key); }
            for (const auto key : keys) { static_cast<void>(remove(key)); }
        }

        [[nodiscard]] bool copy_value_from(const ValueView &source)
        {
            return mutation_.copy_value_from(source);
        }

      private:
        TSDataMutationView mutation_;
    };

    class TSDDataMutationView : public TSDDataView
    {
      public:
        TSDDataMutationView(TSDataView view, engine_time_t evaluation_time)
            : TSDDataView(view),
              mutation_(view.begin_mutation(evaluation_time))
        {
            if (view.schema() == nullptr || view.schema()->kind != TSTypeKind::TSD)
            {
                throw std::invalid_argument("TSDDataMutationView requires a TSD TSData kind");
            }
        }

        TSDDataMutationView(const TSDDataMutationView &) = delete;
        TSDDataMutationView &operator=(const TSDDataMutationView &) = delete;
        TSDDataMutationView(TSDDataMutationView &&) noexcept = default;
        TSDDataMutationView &operator=(TSDDataMutationView &&) = delete;

        [[nodiscard]] TSDDataView view() { return TSDDataView{base()}; }
        [[nodiscard]] engine_time_t current_mutation_time() const { return mutation_.current_mutation_time(); }

        void reserve(std::size_t capacity)
        {
            const auto &ops = static_cast<const TSDDataOps &>(mutation_.ops());
            ops.reserve_impl(ops.context, mutation_.mutable_data(), capacity);
        }

        [[nodiscard]] TSDataView at(const ValueView &key)
        {
            const auto &ops    = static_cast<const TSDDataOps &>(mutation_.ops());
            const auto  result = ops.insert_key_impl(ops.context, mutation_.mutable_data(), key,
                                                     current_mutation_time());
            apply_slot_mutation_result(mutation_, result);
            return at_slot(result.slot);
        }

        [[nodiscard]] TSDataView operator[](const ValueView &key) { return at(key); }

        void set(const ValueView &key, const ValueView &value)
        {
            auto child = at(key);
            auto child_mutation = child.begin_mutation(current_mutation_time());
            static_cast<void>(child_mutation.copy_value_from(value));
        }

        [[nodiscard]] bool erase(const ValueView &key)
        {
            const auto &ops    = static_cast<const TSDDataOps &>(mutation_.ops());
            const auto  result = ops.remove_key_impl(ops.context, mutation_.mutable_data(), key,
                                                     current_mutation_time());
            apply_slot_mutation_result(mutation_, result);
            return result.changed;
        }

        void clear()
        {
            std::vector<ValueView> current_keys;
            for (const auto key : keys()) { current_keys.push_back(key); }
            for (const auto key : current_keys) { static_cast<void>(erase(key)); }
        }

        [[nodiscard]] bool copy_value_from(const ValueView &source)
        {
            if (!source.has_value())
            {
                throw std::invalid_argument("TSDDataMutationView::copy_value_from requires a live source");
            }
            if (source.schema() != layout().value_binding->type_meta)
            {
                throw std::invalid_argument("TSDDataMutationView::copy_value_from requires the map value schema");
            }

            const bool was_modified = mutation_.modified(current_mutation_time());
            auto       source_map   = source.as_map();
            for (const auto [key, value] : source_map.items()) { set(key, value); }

            std::vector<ValueView> removals;
            for (const auto key : keys())
            {
                if (!source_map.contains(key)) { removals.push_back(key); }
            }
            for (const auto key : removals) { static_cast<void>(erase(key)); }

            return !was_modified && mutation_.modified(current_mutation_time());
        }

      private:
        TSDataView at_slot(std::size_t slot)
        {
            const auto &ops = static_cast<const TSDDataOps &>(mutation_.ops());
            if (!ops.slot_occupied_impl(ops.context, mutation_.mutable_data(), slot))
            {
                throw std::out_of_range("TSDDataMutationView::at_slot: slot is not occupied");
            }
            const auto *child_memory = ops.child_at_slot_impl(ops.context, mutation_.mutable_data(), slot);
            return TSDataView{layout().element_binding, child_memory, mutation_.view(), slot};
        }

        TSDataMutationView mutation_;
    };

    inline const TSDataTracking &TSDataParentLink::parent_tracking() const
    {
        if (!has_parent()) { throw std::logic_error("TSDataParentLink requires a parent"); }
        const auto &table = parent_binding->checked_ops();
        return *table.tracking_impl(table.context, parent_data);
    }

    inline TSDataTracking &TSDataParentLink::mutable_parent_tracking() const
    {
        if (!has_parent()) { throw std::logic_error("TSDataParentLink requires a parent"); }
        const auto &table = parent_binding->checked_ops();
        auto       *memory = const_cast<void *>(parent_data);
        return *table.mutable_tracking_impl(table.context, memory);
    }

    inline std::vector<std::size_t> TSDataParentLink::path_from_root() const
    {
        std::vector<std::size_t> reversed_path;
        auto                     current = *this;
        while (current.has_parent())
        {
            reversed_path.push_back(current.child_id);
            const auto &next = current.parent_tracking().parent;
            if (!next.has_parent()) { break; }
            current = next;
        }

        std::reverse(reversed_path.begin(), reversed_path.end());
        return reversed_path;
    }

    inline TSDataView TSDataParentLink::root_view() const
    {
        if (!has_parent()) { return TSDataView{}; }

        const TSDataBinding *root_binding = parent_binding;
        const void          *root_data    = parent_data;
        auto                 current      = *this;
        while (current.has_parent())
        {
            root_binding = current.parent_binding;
            root_data    = current.parent_data;
            const auto &next = current.parent_tracking().parent;
            if (!next.has_parent()) { break; }
            current = next;
        }
        return TSDataView{root_binding, root_data};
    }

    inline void TSDataParentLink::notify_child_modified(engine_time_t mutation_time) const
    {
        if (!has_parent()) { return; }

        const auto &table = parent_binding->checked_ops();
        auto       *memory = const_cast<void *>(parent_data);
        table.record_child_modified_impl(table.context, memory, child_id, mutation_time);

        auto &state = mutable_parent_tracking();
        if (state.last_modified_time == mutation_time) { return; }

        state.last_modified_time = mutation_time;
        state.parent.notify_child_modified(mutation_time);
    }

    inline TSDataMutationView TSDataView::begin_mutation(engine_time_t evaluation_time) const
    {
        return TSDataMutationView{*this, evaluation_time};
    }

    inline TSSDataView TSDataView::as_set() &
    {
        return TSSDataView{*this};
    }

    inline TSSDataView TSDataView::as_set() const &
    {
        return TSSDataView{const_cast<TSDataView &>(*this)};
    }

    inline TSDDataView TSDataView::as_dict() &
    {
        return TSDDataView{*this};
    }

    inline TSDDataView TSDataView::as_dict() const &
    {
        return TSDDataView{const_cast<TSDataView &>(*this)};
    }

    inline TSBDataView TSDataView::as_bundle() &
    {
        return TSBDataView{*this};
    }

    inline TSBDataView TSDataView::as_bundle() const &
    {
        return TSBDataView{const_cast<TSDataView &>(*this)};
    }

    inline TSLDataView TSDataView::as_list() &
    {
        return TSLDataView{*this};
    }

    inline TSLDataView TSDataView::as_list() const &
    {
        return TSLDataView{const_cast<TSDataView &>(*this)};
    }

    inline TSWDataView TSDataView::as_window() &
    {
        return TSWDataView{*this};
    }

    inline TSWDataView TSDataView::as_window() const &
    {
        return TSWDataView{const_cast<TSDataView &>(*this)};
    }

    inline TSWDataMutationView TSWDataView::begin_mutation(engine_time_t evaluation_time) const
    {
        return TSWDataMutationView{view_, evaluation_time};
    }

    inline TSSDataMutationView TSSDataView::begin_mutation(engine_time_t evaluation_time) const
    {
        return TSSDataMutationView{view_, evaluation_time};
    }

    inline TSDDataMutationView TSDDataView::begin_mutation(engine_time_t evaluation_time) const
    {
        return TSDDataMutationView{view_, evaluation_time};
    }

    /**
     * Owning TSData storage handle.
     */
    class TSData
    {
      public:
        using storage_type = MemoryUtils::StorageHandle<MemoryUtils::InlineStoragePolicy<>, TSDataBinding>;

        TSData() noexcept = default;

        explicit TSData(const TSDataBinding &binding)
            : storage_(binding)
        {
        }

        [[nodiscard]] bool has_value() const noexcept { return storage_.has_value(); }
        [[nodiscard]] const TSDataBinding *binding() const noexcept { return storage_.binding(); }
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept
        {
            const auto *bound = binding();
            return bound != nullptr ? bound->type_meta : nullptr;
        }

        [[nodiscard]] TSDataView view() { return TSDataView{binding(), storage_.data()}; }
        [[nodiscard]] TSDataView view() const { return TSDataView{binding(), storage_.data()}; }

      private:
        storage_type storage_{};
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_DATA_H

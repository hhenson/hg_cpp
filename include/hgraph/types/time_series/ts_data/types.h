#ifndef HGRAPH_CPP_TS_DATA_TYPES_H
#define HGRAPH_CPP_TS_DATA_TYPES_H

#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/type_binding.h>
#include <hgraph/types/value/value_ops.h>
#include <hgraph/util/date_time.h>
#include <cstddef>
#include <stdexcept>
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

        /** True when this child has a recorded parent TSData allocation. */
        [[nodiscard]] bool has_parent() const noexcept;

        /**
         * Record a child modification against the parent and bubble that
         * modification towards the root. No-op for a root link.
         */
        void notify_child_modified(engine_time_t mutation_time) const;

        /**
         * Return parent-relative navigation ids from the root to this child.
         * Field, index, and slot ids are all represented as integer ids.
         */
        [[nodiscard]] std::vector<std::size_t> path_from_root() const;

        /** Resolve the root TSData view reached by repeatedly following links. */
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

        /** Current-value binding for this field, or null when the child layout is absent. */
        [[nodiscard]] const ValueTypeBinding *value_binding() const noexcept;

        /** Delta-value binding for this field, or null when the child layout is absent. */
        [[nodiscard]] const ValueTypeBinding *delta_binding() const noexcept;
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

        /** Number of statically planned fields. */
        [[nodiscard]] std::size_t size() const noexcept;

        /** Layout entry for one field; throws when index is out of range. */
        [[nodiscard]] const FixedTSDataFieldLayout &field(std::size_t index) const;
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

        /** Number of statically planned list elements. */
        [[nodiscard]] std::size_t size() const noexcept;

        /** Byte offset for one element's current value storage. */
        [[nodiscard]] std::size_t element_value_offset(std::size_t index) const;

        /** Byte offset for one element's auxiliary TSData state. */
        [[nodiscard]] std::size_t element_auxiliary_offset_at(std::size_t index) const;
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
}  // namespace hgraph

#endif  // HGRAPH_CPP_TS_DATA_TYPES_H

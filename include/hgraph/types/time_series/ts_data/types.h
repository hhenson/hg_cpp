#ifndef HGRAPH_CPP_TS_DATA_TYPES_H
#define HGRAPH_CPP_TS_DATA_TYPES_H

#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/type_binding.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/value/value_ops.h>
#include <hgraph/util/date_time.h>
#include <hgraph/util/tagged_ptr.h>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace hgraph
{
    struct TSDataOps;
    using TSDataBinding = TypeBinding<TSValueTypeMetaData, TSDataOps>;

    struct TSDataTracking;
    struct TSDataParent;
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
     * Terminal parent for a TSData bubble-up chain.
     *
     * Non-TSData endpoint owners, such as ``TSOutput``, implement this small
     * interface so the root TSData node can use the same parent-notification
     * path as nested TSData children.
     */
    struct TSDataParent
    {
        virtual ~TSDataParent() = default;

        /** Record that ``child_id`` modified at ``mutation_time``. */
        virtual void record_child_modified(std::size_t child_id, engine_time_t mutation_time) = 0;
    };

    enum class TSDataParentLinkKind : std::uintptr_t
    {
        None     = 0,
        TSData   = 1,
        Endpoint = 2,
    };

    /**
     * Stable parent identity for one TSData node.
     *
     * This is stored in the child node's value-owned tracking region. It does
     * not point at transient view objects, so a child view may outlive the
     * parent view that projected it. The representation is a compact tagged
     * union: TSData parents use ``binding + data`` and endpoint parents use
     * the same payload word for the endpoint pointer.
     */
    struct TSDataParentLink
    {
        using ParentIdentity = tagged_ptr<const TSDataBinding, 2, TSDataParentLinkKind>;

        union ParentPayload
        {
            const void   *ts_data;
            TSDataParent *endpoint;

            constexpr ParentPayload() noexcept : ts_data(nullptr) {}
            constexpr explicit ParentPayload(const void *data) noexcept : ts_data(data) {}
            constexpr explicit ParentPayload(TSDataParent *parent) noexcept : endpoint(parent) {}
        };

      private:
        ParentIdentity parent_{};
        ParentPayload  payload_{};

      public:
        std::size_t child_id{TS_DATA_NO_CHILD_ID};

        constexpr TSDataParentLink() noexcept = default;
        constexpr TSDataParentLink(const TSDataBinding *binding,
                                   const void          *data,
                                   std::size_t          parent_child_id) noexcept
            : parent_(binding, TSDataParentLinkKind::TSData),
              payload_(data),
              child_id(parent_child_id)
        {
        }
        constexpr TSDataParentLink(TSDataParent &endpoint,
                                   std::size_t   parent_child_id = TS_DATA_NO_CHILD_ID) noexcept
            : parent_(static_cast<const TSDataBinding *>(nullptr), TSDataParentLinkKind::Endpoint),
              payload_(&endpoint),
              child_id(parent_child_id)
        {
        }

        /** Parent kind encoded into the parent identity pointer. */
        [[nodiscard]] TSDataParentLinkKind kind() const noexcept;

        /** True when this child has either a TSData parent or an endpoint parent. */
        [[nodiscard]] bool has_parent() const noexcept;

        /** True when this child has a recorded parent TSData allocation. */
        [[nodiscard]] bool has_ts_data_parent() const noexcept;

        /** True when this child bubbles directly to an endpoint parent. */
        [[nodiscard]] bool has_endpoint_parent() const noexcept;

        /** Parent TSData binding, or null when this link does not target TSData. */
        [[nodiscard]] const TSDataBinding *parent_binding() const noexcept;

        /** Parent TSData memory, or null when this link does not target TSData. */
        [[nodiscard]] const void *parent_data() const noexcept;

        /** Endpoint parent, or null when this link targets TSData or is empty. */
        [[nodiscard]] TSDataParent *parent_endpoint() const noexcept;

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

    static_assert(sizeof(TSDataParentLink) <= sizeof(void *) * 3,
                  "TSDataParentLink should remain a compact three-word navigation handle");

    /**
     * Compact per-level observer set for TSData modification notifications.
     *
     * The common case stores no payload at all: an empty set is a null tagged
     * pointer. A single observer is stored directly in the tagged pointer. The
     * allocation for a vector is only introduced once a second observer is
     * registered. Observers are not copied with TSData payload copies.
     */
    class TSDataObserverSet
    {
      public:
        TSDataObserverSet() noexcept = default;
        TSDataObserverSet(const TSDataObserverSet &) noexcept;
        TSDataObserverSet &operator=(const TSDataObserverSet &) noexcept;
        TSDataObserverSet(TSDataObserverSet &&other) noexcept;
        TSDataObserverSet &operator=(TSDataObserverSet &&other) noexcept;
        ~TSDataObserverSet() noexcept;

        /** True when this level has no registered observers. */
        [[nodiscard]] bool empty() const noexcept;

        /** True when ``observer`` is currently registered. */
        [[nodiscard]] bool contains(const Notifiable *observer) const noexcept;

        /** Number of observers registered at this level. */
        [[nodiscard]] std::size_t size() const noexcept;

        /** Register an observer pointer; null is ignored and duplicates assert. */
        void subscribe(Notifiable *observer);

        /** Remove an observer pointer; null is ignored and missing observers assert. */
        void unsubscribe(Notifiable *observer);

        /** Replace a registered observer pointer without changing list shape. */
        void replace(Notifiable *observer, Notifiable *replacement) noexcept;

        /** Notify all registered observers for ``modified_time``. */
        void notify(engine_time_t modified_time) const;

        /** Clear all observer registrations without notifying them. */
        void clear() noexcept;

      private:
        struct ObserverList
        {
            std::vector<Notifiable *> entries{};
            std::size_t               notify_depth{0};
            bool                      compact_pending{false};
        };
        using ObserverStorage = discriminated_ptr<Notifiable, ObserverList>;

        [[nodiscard]] Notifiable *single() const noexcept;
        [[nodiscard]] ObserverList *many() const noexcept;
        void set_single(Notifiable *observer) noexcept;
        void set_many(ObserverList *observers) noexcept;
        void compact_many(ObserverList &observers) noexcept;

        ObserverStorage observers_{};
    };

    static_assert(sizeof(TSDataObserverSet) <= sizeof(void *),
                  "TSDataObserverSet should remain a one-word tagged observer handle");

    /**
     * Common per-TSData tracking state.
     *
     * Every TSData shape, including compact atomic TSData, stores this in its
     * value-owned memory. Projected child values use ``parent`` for local
     * bubble-up; root values may carry a terminal endpoint parent such as the
     * owning ``TSOutput``. ``observers`` owns the compact per-level
     * notification set, while ``last_modified_time`` decides whether this
     * node's local delta view belongs to a given evaluation time.
     */
    struct TSDataTracking
    {
        TSDataTracking() noexcept = default;
        TSDataTracking(const TSDataTracking &) = default;
        TSDataTracking &operator=(const TSDataTracking &) = default;
        TSDataTracking(TSDataTracking &&) noexcept = default;
        TSDataTracking &operator=(TSDataTracking &&) noexcept = default;
        ~TSDataTracking() noexcept = default;

        /**
         * Record the first modification for ``modified_time`` and notify local
         * observers once. Returns false when this level was already marked for
         * the same engine time.
         */
        [[nodiscard]] bool record_modified(engine_time_t modified_time);

        engine_time_t last_modified_time{MIN_DT};
        TSDataParentLink parent{};
        TSDataObserverSet observers{};
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

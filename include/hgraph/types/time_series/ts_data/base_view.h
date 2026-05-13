#ifndef HGRAPH_CPP_TS_DATA_BASE_VIEW_H
#define HGRAPH_CPP_TS_DATA_BASE_VIEW_H

#include <hgraph/types/time_series/ts_data/ops.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace hgraph
{
    class TSDataView
    {
      public:
        constexpr TSDataView() noexcept = default;

        TSDataView(const TSDataBinding *binding, void *data) noexcept;
        TSDataView(const TSDataBinding *binding, const void *data) noexcept;

        /** True when the view has both a binding and a live TSData memory pointer. */
        [[nodiscard]] bool valid() const noexcept;
        explicit operator bool() const noexcept;

        /** Type-erased binding that describes this TSData memory region. */
        [[nodiscard]] const TSDataBinding *binding() const noexcept;

        /** Time-series schema associated with the binding, or null when unbound. */
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;

        /** Borrowed pointer to the underlying TSData memory. */
        [[nodiscard]] const void *data() const noexcept;

        /** Parent link stored in this TSData node's tracking region. */
        [[nodiscard]] const TSDataParentLink &parent_link() const;

        /** Parent-relative id for this node, or ``TS_DATA_NO_CHILD_ID`` for roots. */
        [[nodiscard]] std::size_t child_id() const;

        /** True when this view's tracking record carries a parent link. */
        [[nodiscard]] bool has_parent() const;

        /** Integer path from the root TSData node to this node. */
        [[nodiscard]] std::vector<std::size_t> path_from_root() const;

        /** Root TSData view reached by following parent links. */
        [[nodiscard]] TSDataView root_view() const;

        /** Writable memory pointer; throws unless the ops table allows mutation. */
        [[nodiscard]] void *mutable_data() const;

        /** Checked access to the type-erased operation table. */
        [[nodiscard]] const TSDataOps &ops() const;

        /** Common TSData layout prefix for this node. */
        [[nodiscard]] const TSDataLayout &layout() const;

        /** Local tracking record for modification time and parent link. */
        [[nodiscard]] const TSDataTracking &tracking() const;

        /** Current value view for this TSData node. */
        [[nodiscard]] ValueView value() const;

        /** Delta value for ``evaluation_time``, or a typed null view when unchanged. */
        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const;

        /** Last engine time that modified this TSData node, or ``MIN_DT`` if never valid. */
        [[nodiscard]] engine_time_t last_modified_time() const;

        /** True when this node was modified at ``evaluation_time``. */
        [[nodiscard]] bool modified(engine_time_t evaluation_time) const;

        /** Register / remove a per-level modification observer. */
        void subscribe(Notifiable *observer) const;
        void unsubscribe(Notifiable *observer) const;
        void replace_observer(Notifiable *observer, Notifiable *replacement) const;

        /** True when this level currently has one or more observers. */
        [[nodiscard]] bool has_observers() const;

        /** Number of observers currently registered at this level. */
        [[nodiscard]] std::size_t observer_count() const;

        /** True when the node currently holds a valid time-series value. */
        [[nodiscard]] bool has_current_value() const;

        /** True when this node and all required descendants are valid. */
        [[nodiscard]] bool all_valid() const;

        /**
         * Clear transient delta state for this node and modified descendants.
         *
         * ``modified_time`` is normally the root node's last modified time.
         * Branches whose tracking timestamp differs are skipped.
         */
        void cleanup_delta(engine_time_t modified_time) const;
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
        friend class TSOutput;
        friend class TSInputView;
        friend class TSDDataView;
        friend class TSDDataMutationView;

        TSDataView(const TSDataBinding *binding, void *data, TSDataView &parent, std::size_t child_id);
        TSDataView(const TSDataBinding *binding, const void *data, TSDataView &parent, std::size_t child_id);

        void require_live(const char *what) const;
        [[nodiscard]] TSDataTracking &mutable_tracking() const;
        void bind_parent(const TSDataView &parent, std::size_t child_id) const;
        void bind_parent(TSDataParent &parent, std::size_t child_id) const;

        const TSDataBinding *binding_{nullptr};
        const void          *data_{nullptr};
    };

    class TSDataMutationView
    {
      public:
        /** Begin a mutation-capable projection of a live TSData view. */
        TSDataMutationView(TSDataView view, engine_time_t evaluation_time);

        TSDataMutationView(const TSDataMutationView &) = delete;
        TSDataMutationView &operator=(const TSDataMutationView &) = delete;

        TSDataMutationView(TSDataMutationView &&other) noexcept;

        TSDataMutationView &operator=(TSDataMutationView &&) = delete;

        ~TSDataMutationView() noexcept;

        /** Read-only projection of the view being mutated. */
        [[nodiscard]] const TSDataView &view() const noexcept;

        /** Mutable projection of the view being mutated. */
        [[nodiscard]] TSDataView &view() noexcept;

        /** Operation table used by the underlying TSData binding. */
        [[nodiscard]] const TSDataOps &ops() const;

        /** Writable TSData memory; throws if this mutation view has been moved from. */
        [[nodiscard]] void *mutable_data() const;

        /** Current value view after any mutations applied through this scope. */
        [[nodiscard]] ValueView value() const;

        /** Delta value for ``evaluation_time`` using the underlying TSData semantics. */
        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const;

        /** Engine time associated with this mutation scope. */
        [[nodiscard]] engine_time_t current_mutation_time() const;

        /** True when the underlying TSData was modified at ``evaluation_time``. */
        [[nodiscard]] bool modified(engine_time_t evaluation_time) const;

        /**
         * Mark this TSData node as modified and notify its parent once.
         *
         * Repeated calls in the same engine time are coalesced by
         * ``last_modified_time`` and therefore do not re-notify ancestors.
         */
        void mark_modified();

        /** Copy a value-layer view into this TSData node using the binding's type-erased copy op. */
        [[nodiscard]] bool copy_value_from(const ValueView &source);

      private:
        void require_active_mutation() const;
        void validate_mutation_view() const;
        [[nodiscard]] bool record_modified_local() const;
        void notify_parent_modified() const;

        TSDataView    view_{};
        engine_time_t mutation_time_{MIN_DT};
    };

    /** Apply a slot mutation result to the owning mutation view's timestamp state. */
    void apply_slot_mutation_result(TSDataMutationView &mutation, const SlotTSDataMutationResult &result);
}  // namespace hgraph

#endif  // HGRAPH_CPP_TS_DATA_BASE_VIEW_H

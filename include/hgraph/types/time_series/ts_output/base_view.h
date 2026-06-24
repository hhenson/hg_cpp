#ifndef HGRAPH_CPP_ROOT_TS_OUTPUT_BASE_VIEW_H
#define HGRAPH_CPP_ROOT_TS_OUTPUT_BASE_VIEW_H

#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/time_series/endpoint_owner.h>
#include <hgraph/util/date_time.h>
#include <cstddef>

namespace hgraph
{
    class GraphView;
    class NodeView;
    class TSOutput;
    class TSOutputHandle;
    class TSOutputView;
    class TSOutputMutationView;
    class TSBOutputView;
    class TSLOutputView;
    class TSSOutputView;
    class TSDOutputView;
    class TSWOutputView;

    /**
     * Stable output endpoint handle without an evaluation time.
     *
     * This is the storage form of ``TSOutputView`` used by link state: it
     * carries the owning output identity and the borrowed TSData cursor, but
     * leaves time-sensitive ``modified`` / ``delta`` interpretation to views.
     */
    class TSOutputHandle
    {
      public:
        TSOutputHandle() noexcept;
        TSOutputHandle(const TSOutput *output, const TSDataView &data) noexcept;
        explicit TSOutputHandle(const TSOutputView &view) noexcept;

        /** Owning output and underlying TSData view. */
        [[nodiscard]] const TSOutput *output() const noexcept;
        [[nodiscard]] TSDataView data_view() const noexcept;

        /** Binding and schema for the borrowed TSData. */
        [[nodiscard]] const TSDataBinding *binding() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;

        /** True when both output identity and TSData cursor are present. */
        [[nodiscard]] bool bound() const noexcept;
        [[nodiscard]] bool same_as(const TSOutputHandle &other) const noexcept;

        /** Recreate a read-only output view for ``evaluation_time``. */
        [[nodiscard]] TSOutputView view(DateTime evaluation_time) const noexcept;
        void reset() noexcept;

      private:
        const TSOutput *output_{nullptr};
        TSDataStorageRef<> data_{};
    };

    /**
     * Read-only endpoint view carrying an evaluation time.
     *
     * ``TSDataView::valid()`` means "live handle"; this view exposes the
     * time-series validity rule where ``MIN_DT`` means no current value.
     */
    class TSOutputView
    {
      public:
        TSOutputView() noexcept;
        TSOutputView(const TSOutput *output, const TSDataView &data, DateTime evaluation_time) noexcept;
        TSOutputView(TSOutputHandle handle, DateTime evaluation_time) noexcept;

        TSOutputView(const TSOutputView &) = delete;
        TSOutputView &operator=(const TSOutputView &) = delete;
        TSOutputView(TSOutputView &&) noexcept = default;
        TSOutputView &operator=(TSOutputView &&) noexcept = default;

        /** Explicitly recreate a transient cursor over the same output position. */
        [[nodiscard]] TSOutputView borrowed_ref() const noexcept;

        /** Owning output and underlying TSData view. */
        [[nodiscard]] const TSOutput *output() const noexcept;
        [[nodiscard]] TSOutputHandle handle() const noexcept;
        [[nodiscard]] const TSDataView &data_view() const noexcept;
        [[nodiscard]] TSDataView &data_view() noexcept;

        /** Evaluation time associated with delta/modified checks. */
        [[nodiscard]] DateTime evaluation_time() const noexcept;

        /** Node owner for this output endpoint, if it is graph-attached. */
        [[nodiscard]] NodeView owner_node() const;
        [[nodiscard]] GraphView owner_graph() const;
        [[nodiscard]] TSEndpointOwnerPort owner_port() const noexcept;

        /** Binding and schema for the borrowed TSData. */
        [[nodiscard]] const TSDataBinding *binding() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;

        /** Current and delta value projections. */
        [[nodiscard]] ValueView value() const;
        [[nodiscard]] ValueView delta_value() const;

        /** Modification and validity status. */
        [[nodiscard]] bool bound() const noexcept;
        [[nodiscard]] DateTime last_modified_time() const;
        [[nodiscard]] bool modified() const;
        [[nodiscard]] bool valid() const;
        [[nodiscard]] bool all_valid() const;

        /** True when this output endpoint forwards reads to another output. */
        [[nodiscard]] bool forwarding() const noexcept;
        [[nodiscard]] bool forwarding_bound() const noexcept;
        [[nodiscard]] TSOutputHandle forwarding_target() const noexcept;
        void bind_forwarding_target(const TSOutputView &source) const;
        void clear_forwarding_target() const;

        /** Register / remove an observer at this view's TSData level. */
        void subscribe(Notifiable *observer) const;
        void unsubscribe(Notifiable *observer) const;

        /**
         * Return output binding data that exposes this view as ``requested_schema``.
         *
         * When the requested schema is canonical this is this view's own
         * handle. When the requested schema differs only by REF markers, the
         * owning output may return a handle into its alternative store.
         */
        [[nodiscard]] TSOutputHandle binding_for(const TSValueTypeMetaData &requested_schema) const;

        /** Begin a mutation through this output endpoint view. */
        [[nodiscard]] TSDataMutationView begin_mutation(DateTime evaluation_time) const;

        /** Shape-erased indexed child projection for TSB/TSL-like outputs. */
        [[nodiscard]] TSOutputView indexed_child_at(std::size_t index) const;

        /** Shape-specific projections for root TSData. */
        [[nodiscard]] TSSOutputView as_set() &;
        [[nodiscard]] TSSOutputView as_set() const &;
        void as_set() && = delete;
        void as_set() const && = delete;
        [[nodiscard]] TSDOutputView as_dict() &;
        [[nodiscard]] TSDOutputView as_dict() const &;
        void as_dict() && = delete;
        void as_dict() const && = delete;
        [[nodiscard]] TSBOutputView as_bundle() &;
        [[nodiscard]] TSBOutputView as_bundle() const &;
        void as_bundle() && = delete;
        void as_bundle() const && = delete;
        [[nodiscard]] TSLOutputView as_list() &;
        [[nodiscard]] TSLOutputView as_list() const &;
        void as_list() && = delete;
        void as_list() const && = delete;
        [[nodiscard]] TSWOutputView as_window() &;
        [[nodiscard]] TSWOutputView as_window() const &;
        void as_window() && = delete;
        void as_window() const && = delete;

      private:
        friend class TSInputView;

        const TSOutput *output_{nullptr};
        TSDataView      data_{};
        DateTime   evaluation_time_{MIN_DT};
    };

}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_OUTPUT_BASE_VIEW_H

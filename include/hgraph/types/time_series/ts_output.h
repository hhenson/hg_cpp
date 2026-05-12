#ifndef HGRAPH_CPP_ROOT_TS_OUTPUT_H
#define HGRAPH_CPP_ROOT_TS_OUTPUT_H

#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/util/date_time.h>
#include <cstddef>

namespace hgraph
{
    class TSOutputView;
    class TSOutputMutationView;

    /**
     * Owning output-side time-series endpoint.
     *
     * ``TSOutput`` owns the root TSData allocation and exposes lifecycle hooks
     * used by nodes to clean up transient delta state after evaluation. Root
     * subscriptions are delegated to the root TSData observer set; child-level
     * subscriptions are registered on the projected child TSData views.
     */
    class TSOutput : private TSDataParent
    {
      public:
        TSOutput() noexcept;
        explicit TSOutput(const TSDataBinding &binding);
        explicit TSOutput(const TSValueTypeMetaData &schema);
        explicit TSOutput(const TSValueTypeMetaData *schema);

        TSOutput(const TSOutput &other);
        TSOutput &operator=(const TSOutput &other);
        TSOutput(TSOutput &&other) noexcept;
        TSOutput &operator=(TSOutput &&other) noexcept;

        /** True when this output owns a bound TSData root. */
        [[nodiscard]] bool has_value() const noexcept;

        /** Root TSData binding and schema, or null when unbound. */
        [[nodiscard]] const TSDataBinding *binding() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;

        /** Borrowed root TSData view. */
        [[nodiscard]] TSDataView data_view();
        [[nodiscard]] TSDataView data_view() const;

        /** True after this output has been modified and before cleanup runs. */
        [[nodiscard]] bool dirty() const noexcept;

        /**
         * Clear transient delta state for the current dirty root.
         *
         * Nodes call this from their post-evaluation cleanup hook. The cleanup
         * walks only branches whose modification time matches the root's last
         * modified time.
         */
        void cleanup_delta();

        /** Clear the dirty flag without touching TSData delta state. */
        void clear_dirty() noexcept;

        /** Register / remove an observer at the root TSData level. */
        void subscribe(Notifiable *observer);
        void unsubscribe(Notifiable *observer);

        /** Read view at ``evaluation_time``. */
        [[nodiscard]] TSOutputView view(engine_time_t evaluation_time = MIN_DT);
        [[nodiscard]] TSOutputView view(engine_time_t evaluation_time = MIN_DT) const;

        /** Begin a root mutation scope. */
        [[nodiscard]] TSOutputMutationView begin_mutation(engine_time_t evaluation_time);

      private:
        friend class TSOutputMutationView;

        static const TSDataBinding &checked_binding_for(const TSValueTypeMetaData *schema);
        static const TSData &copyable_data(const TSOutput &other);

        void attach_root_parent();
        void record_child_modified(std::size_t child_id, engine_time_t mutation_time) override;

        TSData        data_{};
        bool          dirty_{false};
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
        TSOutputView(const TSOutput *output, TSDataView data, engine_time_t evaluation_time) noexcept;

        /** Owning output and underlying TSData view. */
        [[nodiscard]] const TSOutput *output() const noexcept;
        [[nodiscard]] const TSDataView &data_view() const noexcept;
        [[nodiscard]] TSDataView &data_view() noexcept;

        /** Evaluation time associated with delta/modified checks. */
        [[nodiscard]] engine_time_t evaluation_time() const noexcept;

        /** Binding, schema, and bound-state helpers. */
        [[nodiscard]] const TSDataBinding *binding() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;
        [[nodiscard]] bool bound() const noexcept;

        /** Current and delta value projections. */
        [[nodiscard]] ValueView value() const;
        [[nodiscard]] ValueView delta_value() const;
        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const;

        /** Modification and validity status. */
        [[nodiscard]] engine_time_t last_modified_time() const;
        [[nodiscard]] bool modified() const;
        [[nodiscard]] bool modified(engine_time_t evaluation_time) const;
        [[nodiscard]] bool valid() const;
        [[nodiscard]] bool all_valid() const;

        /** Register / remove an observer at this view's TSData level. */
        void subscribe(Notifiable *observer) const;
        void unsubscribe(Notifiable *observer) const;

        /** Shape-specific projections for root TSData. */
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

      private:
        const TSOutput *output_{nullptr};
        TSDataView      data_{};
        engine_time_t   evaluation_time_{MIN_DT};
    };

    /** Root mutation view for a TSOutput. */
    class TSOutputMutationView
    {
      public:
        TSOutputMutationView(TSOutput &output, engine_time_t evaluation_time);

        TSOutputMutationView(const TSOutputMutationView &) = delete;
        TSOutputMutationView &operator=(const TSOutputMutationView &) = delete;
        TSOutputMutationView(TSOutputMutationView &&other) noexcept;
        TSOutputMutationView &operator=(TSOutputMutationView &&) = delete;
        ~TSOutputMutationView() noexcept;

        /** Root TSData mutation view. */
        [[nodiscard]] TSDataMutationView &data_mutation() noexcept;
        [[nodiscard]] const TSDataMutationView &data_mutation() const noexcept;

        /** Current and delta value projections. */
        [[nodiscard]] ValueView value() const;
        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const;

        /** Mutation time and modification status. */
        [[nodiscard]] engine_time_t current_mutation_time() const;
        [[nodiscard]] bool modified() const;

        /** Mark the root output as modified. */
        void mark_modified();

        /** Copy a value-layer view into the root TSData. */
        [[nodiscard]] bool copy_value_from(const ValueView &source);

      private:
        static TSDataMutationView begin_root_mutation(TSOutput &output, engine_time_t evaluation_time);

        TSDataMutationView mutation_;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_OUTPUT_H

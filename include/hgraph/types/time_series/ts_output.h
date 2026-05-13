#ifndef HGRAPH_CPP_ROOT_TS_OUTPUT_H
#define HGRAPH_CPP_ROOT_TS_OUTPUT_H

#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/util/date_time.h>
#include <cstddef>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    class TSOutputView;
    class TSOutputMutationView;
    class TSBOutputView;
    class TSLOutputView;
    class TSSOutputView;
    class TSDOutputView;
    class TSWOutputView;

    template <typename Derived, typename EndpointView>
    class TSTypedTimeSeriesView
    {
      public:
        [[nodiscard]] const EndpointView &base() const noexcept { return view_; }
        [[nodiscard]] EndpointView &base() noexcept { return view_; }

        [[nodiscard]] engine_time_t evaluation_time() const noexcept { return view_.evaluation_time(); }
        [[nodiscard]] const TSDataBinding *binding() const noexcept { return view_.binding(); }
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept { return view_.schema(); }
        [[nodiscard]] bool bound() const noexcept { return view_.bound(); }
        [[nodiscard]] bool valid() const { return view_.valid(); }
        [[nodiscard]] bool all_valid() const { return view_.all_valid(); }
        [[nodiscard]] engine_time_t last_modified_time() const { return view_.last_modified_time(); }
        [[nodiscard]] bool modified() const { return view_.modified(); }
        [[nodiscard]] ValueView value() const { return view_.value(); }
        [[nodiscard]] ValueView delta_value() const { return view_.delta_value(); }

      protected:
        explicit TSTypedTimeSeriesView(EndpointView view)
            : view_(std::move(view))
        {
        }

        EndpointView view_{};
    };

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

        /** Binding and schema for the borrowed TSData. */
        [[nodiscard]] const TSDataBinding *binding() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;

        /** Current and delta value projections. */
        [[nodiscard]] ValueView value() const;
        [[nodiscard]] ValueView delta_value() const;

        /** Modification and validity status. */
        [[nodiscard]] bool bound() const noexcept;
        [[nodiscard]] engine_time_t last_modified_time() const;
        [[nodiscard]] bool modified() const;
        [[nodiscard]] bool valid() const;
        [[nodiscard]] bool all_valid() const;

        /** Register / remove an observer at this view's TSData level. */
        void subscribe(Notifiable *observer) const;
        void unsubscribe(Notifiable *observer) const;

        /** Begin a mutation through this output endpoint view. */
        [[nodiscard]] TSDataMutationView begin_mutation(engine_time_t evaluation_time) const;

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
        engine_time_t   evaluation_time_{MIN_DT};
    };

    template <typename Derived>
    class TSOutputTypedView : public TSTypedTimeSeriesView<Derived, TSOutputView>
    {
      public:
        using base_type = TSTypedTimeSeriesView<Derived, TSOutputView>;

        void subscribe(Notifiable *observer) const { this->view_.subscribe(observer); }
        void unsubscribe(Notifiable *observer) const { this->view_.unsubscribe(observer); }
        [[nodiscard]] TSDataMutationView begin_mutation(engine_time_t evaluation_time) const
        {
            return this->view_.begin_mutation(evaluation_time);
        }

      protected:
        using base_type::base_type;
    };

    class TSBOutputView : public TSOutputTypedView<TSBOutputView>
    {
      public:
        explicit TSBOutputView(TSOutputView view);

        [[nodiscard]] TSBDataView data_view() const;
        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] bool has_field(std::string_view name) const noexcept;
        [[nodiscard]] Range<std::string_view> keys() const;
        [[nodiscard]] Range<TSOutputView> values() const;
        [[nodiscard]] Range<TSOutputView> valid_values() const;
        [[nodiscard]] Range<TSOutputView> modified_values() const;
        [[nodiscard]] KeyValueRange<std::string_view, TSOutputView> items() const;
        [[nodiscard]] KeyValueRange<std::string_view, TSOutputView> valid_items() const;
        [[nodiscard]] KeyValueRange<std::string_view, TSOutputView> modified_items() const;

        [[nodiscard]] TSOutputView at(std::size_t index) &;
        [[nodiscard]] TSOutputView at(std::size_t index) const &;
        TSOutputView at(std::size_t) && = delete;
        [[nodiscard]] TSOutputView operator[](std::size_t index) &;
        [[nodiscard]] TSOutputView operator[](std::size_t index) const &;
        TSOutputView operator[](std::size_t) && = delete;

        [[nodiscard]] TSOutputView at(std::string_view name) &;
        [[nodiscard]] TSOutputView at(std::string_view name) const &;
        TSOutputView at(std::string_view) && = delete;
        [[nodiscard]] TSOutputView field(std::string_view name) &;
        [[nodiscard]] TSOutputView field(std::string_view name) const &;
        TSOutputView field(std::string_view) && = delete;
        [[nodiscard]] TSOutputView operator[](std::string_view name) &;
        [[nodiscard]] TSOutputView operator[](std::string_view name) const &;
        TSOutputView operator[](std::string_view) && = delete;
    };

    class TSLOutputView : public TSOutputTypedView<TSLOutputView>
    {
      public:
        explicit TSLOutputView(TSOutputView view);

        [[nodiscard]] TSLDataView data_view() const;
        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] Range<TSOutputView> values() const;
        [[nodiscard]] Range<TSOutputView> valid_values() const;
        [[nodiscard]] Range<TSOutputView> modified_values() const;
        [[nodiscard]] KeyValueRange<std::size_t, TSOutputView> items() const;
        [[nodiscard]] KeyValueRange<std::size_t, TSOutputView> valid_items() const;
        [[nodiscard]] KeyValueRange<std::size_t, TSOutputView> modified_items() const;

        [[nodiscard]] TSOutputView at(std::size_t index) &;
        [[nodiscard]] TSOutputView at(std::size_t index) const &;
        TSOutputView at(std::size_t) && = delete;
        [[nodiscard]] TSOutputView operator[](std::size_t index) &;
        [[nodiscard]] TSOutputView operator[](std::size_t index) const &;
        TSOutputView operator[](std::size_t) && = delete;
    };

    class TSSOutputView : public TSOutputTypedView<TSSOutputView>
    {
      public:
        explicit TSSOutputView(TSOutputView view);

        [[nodiscard]] TSSDataView data_view() const;
        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] std::size_t slot_capacity() const;
        [[nodiscard]] bool slot_occupied(std::size_t slot) const;
        [[nodiscard]] bool slot_live(std::size_t slot) const;
        [[nodiscard]] bool slot_added(std::size_t slot) const;
        [[nodiscard]] bool slot_removed(std::size_t slot) const;
        [[nodiscard]] ValueView at_slot(std::size_t slot) const;
        [[nodiscard]] bool contains(const ValueView &key) const;
        [[nodiscard]] std::size_t find_slot(const ValueView &key) const;
        [[nodiscard]] Range<ValueView> values() const;
        [[nodiscard]] Range<ValueView> added() const;
        [[nodiscard]] Range<ValueView> removed() const;
        [[nodiscard]] Range<ValueView> added_values() const;
        [[nodiscard]] Range<ValueView> removed_values() const;
        [[nodiscard]] Range<ValueView>::iterator begin() const;
        [[nodiscard]] Range<ValueView>::iterator end() const;
        [[nodiscard]] TSSDataMutationView begin_mutation(engine_time_t evaluation_time) const;
    };

    class TSDOutputView : public TSOutputTypedView<TSDOutputView>
    {
      public:
        explicit TSDOutputView(TSOutputView view);

        [[nodiscard]] TSDDataView data_view() const;
        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] std::size_t slot_capacity() const;
        [[nodiscard]] bool slot_occupied(std::size_t slot) const;
        [[nodiscard]] bool slot_live(std::size_t slot) const;
        [[nodiscard]] bool slot_added(std::size_t slot) const;
        [[nodiscard]] bool slot_removed(std::size_t slot) const;
        [[nodiscard]] bool slot_modified(std::size_t slot) const;
        [[nodiscard]] ValueView key_at_slot(std::size_t slot) const;
        [[nodiscard]] TSOutputView at_slot(std::size_t slot) const;
        [[nodiscard]] bool contains(const ValueView &key) const;
        [[nodiscard]] std::size_t find_slot(const ValueView &key) const;
        [[nodiscard]] TSOutputView at(const ValueView &key) const;
        [[nodiscard]] TSOutputView operator[](const ValueView &key) const;
        [[nodiscard]] Range<ValueView> keys() const;
        [[nodiscard]] Range<TSOutputView> values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSOutputView> items() const;
        [[nodiscard]] Range<ValueView> valid_keys() const;
        [[nodiscard]] Range<TSOutputView> valid_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSOutputView> valid_items() const;
        [[nodiscard]] Range<ValueView> modified_keys() const;
        [[nodiscard]] Range<TSOutputView> modified_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSOutputView> modified_items() const;
        [[nodiscard]] Range<ValueView> added_keys() const;
        [[nodiscard]] Range<TSOutputView> added_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSOutputView> added_items() const;
        [[nodiscard]] Range<ValueView> removed_keys() const;
        [[nodiscard]] Range<TSOutputView> removed_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSOutputView> removed_items() const;
        [[nodiscard]] TSDDataMutationView begin_mutation(engine_time_t evaluation_time) const;
    };

    class TSWOutputView : public TSOutputTypedView<TSWOutputView>
    {
      public:
        explicit TSWOutputView(TSOutputView view);

        [[nodiscard]] TSWDataView data_view() const;
        [[nodiscard]] bool duration_based() const noexcept;
        [[nodiscard]] bool size_based() const noexcept;
        [[nodiscard]] bool time_based() const noexcept;
        [[nodiscard]] std::size_t period() const;
        [[nodiscard]] std::size_t min_period() const;
        [[nodiscard]] engine_time_delta_t time_range() const;
        [[nodiscard]] engine_time_delta_t min_time_range() const;
        [[nodiscard]] std::size_t capacity() const;
        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] bool full() const;
        [[nodiscard]] engine_time_t first_modified_time() const;
        [[nodiscard]] engine_time_t time_at(std::size_t index) const;
        [[nodiscard]] ValueView time_value_at(std::size_t index) const;
        [[nodiscard]] ValueView at(std::size_t index) const;
        [[nodiscard]] ValueView operator[](std::size_t index) const;
        [[nodiscard]] ValueView front() const;
        [[nodiscard]] ValueView back() const;
        [[nodiscard]] Range<ValueView> values() const;
        [[nodiscard]] Range<ValueView> time_values() const;
        [[nodiscard]] Range<engine_time_t> value_times() const;
        [[nodiscard]] Range<ValueView>::iterator begin() const;
        [[nodiscard]] Range<ValueView>::iterator end() const;
        [[nodiscard]] TSWDataMutationView begin_mutation(engine_time_t evaluation_time) const;
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
        [[nodiscard]] ValueView delta_value() const;

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

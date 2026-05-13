#ifndef HGRAPH_CPP_ROOT_TS_INPUT_H
#define HGRAPH_CPP_ROOT_TS_INPUT_H

#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_output.h>

#include <cstddef>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace detail
    {
        struct TSInputNode;
    }

    class TSInput;
    class TSInputView;
    class TSBInputView;
    class TSLInputView;
    class TSSInputView;
    class TSDInputView;
    class TSWInputView;

    /**
     * Construction plan for one node input bundle.
     *
     * The plan is compiled from a canonical TS schema plus a generic endpoint
     * annotation tree. TSInput requires a non-peered TSB root, with nested
     * non-peered TSB/fixed-TSL prefixes and peered terminals beneath it. A
     * peered terminal compiles to input-side TargetLink state.
     */
    class TSInputConstructionPlan
    {
      public:
        TSInputConstructionPlan(const TSValueTypeMetaData &root_schema,
                                TSEndpointSchema           endpoint_schema);

        [[nodiscard]] const TSValueTypeMetaData &schema() const noexcept;
        [[nodiscard]] const TSEndpointSchema &endpoint_schema() const noexcept;

      private:
        const TSValueTypeMetaData *schema_{nullptr};
        TSEndpointSchema          endpoint_schema_{};
    };

    class TSInputPlanFactory
    {
      public:
        [[nodiscard]] static TSInputConstructionPlan compile(
            const TSValueTypeMetaData                  &root_schema,
            const TSEndpointSchema                     &endpoint_schema);
    };

    /**
     * Cached builder for TSInput endpoint state.
     *
     * Unlike TSOutput construction, an input builder does not allocate payload
     * storage for the visible time-series value. It builds the non-peered input
     * tree and peered terminal state used to borrow output TSData at runtime.
     */
    class TSInputBuilder
    {
      public:
        [[nodiscard]] const TSValueTypeMetaData &schema() const noexcept;
        [[nodiscard]] TSInput make_input() const;

      private:
        friend class TSInputBuilderFactory;
        friend class TSInput;

        explicit TSInputBuilder(TSInputConstructionPlan plan);

        TSInputConstructionPlan plan_;
    };

    class TSInputBuilderFactory
    {
      public:
        [[nodiscard]] static const TSInputBuilder *builder_for(const TSValueTypeMetaData &root_schema,
                                                               const TSEndpointSchema    &endpoint_schema);
        [[nodiscard]] static const TSInputBuilder &checked_builder_for(const TSValueTypeMetaData &root_schema,
                                                                       const TSEndpointSchema    &endpoint_schema);
        [[nodiscard]] static const TSInputBuilder *builder_for(const TSInputConstructionPlan &plan);
        [[nodiscard]] static const TSInputBuilder &checked_builder_for(const TSInputConstructionPlan &plan);
    };

    /**
     * Owning input-side time-series endpoint.
     *
     * TSInput owns binding and activation state. The root is always a
     * non-peered TSB; peered terminals inside that tree borrow TSOutput TSData
     * through input-side TargetLink state.
     */
    class TSInput
    {
      public:
        TSInput() noexcept;
        explicit TSInput(const TSInputBuilder &builder);
        TSInput(const TSInput &other);
        TSInput &operator=(const TSInput &other);
        TSInput(TSInput &&other) noexcept;
        TSInput &operator=(TSInput &&other) noexcept;
        ~TSInput();

        [[nodiscard]] bool has_value() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;

        /**
         * Root input view.
         *
         * ``scheduling_notifier`` is normally the owning node. Active views use
         * it as the final notification target.
         */
        [[nodiscard]] TSInputView view(Notifiable *scheduling_notifier = nullptr,
                                       engine_time_t evaluation_time = MIN_DT);
        [[nodiscard]] TSInputView view(Notifiable *scheduling_notifier = nullptr,
                                       engine_time_t evaluation_time = MIN_DT) const;

      private:
        friend class TSInputView;
        friend class TSBInputView;
        friend class TSLInputView;
        friend class TSSInputView;
        friend class TSDInputView;
        friend class TSWInputView;

        explicit TSInput(const TSInputConstructionPlan &plan);

        void rebuild_from_plan(const TSInputConstructionPlan &plan);
        void relink_nodes() noexcept;

        const TSInputBuilder             *builder_{nullptr};
        const TSValueTypeMetaData        *schema_{nullptr};
        std::unique_ptr<detail::TSInputNode> root_{};
    };

    /**
     * Input-side read/binding/activation view.
     *
     * A view can represent a non-peered input node or a position inside a
     * bound target output reached through a peered terminal.
     */
    class TSInputView
    {
      public:
        TSInputView() noexcept;

        /** Evaluation time associated with delta/modified checks. */
        [[nodiscard]] engine_time_t evaluation_time() const noexcept;

        /** Binding and schema for the input-side TSData projection. */
        [[nodiscard]] const TSDataBinding *binding() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;
        /** Underlying TSData projection; empty for unbound peered terminals. */
        [[nodiscard]] const TSDataView &data_view() const noexcept;

        /** True when this view or at least one structural child has a current value. */
        [[nodiscard]] bool valid() const;

        /** True when this view and all required structural descendants have current values. */
        [[nodiscard]] bool all_valid() const;

        /** Latest modification time observed at this view, including structural children. */
        [[nodiscard]] engine_time_t last_modified_time() const;

        /** True when this view or a structural child was modified at the view's evaluation time. */
        [[nodiscard]] bool modified() const;

        [[nodiscard]] ValueView value() const;
        [[nodiscard]] ValueView delta_value() const;

        /** For bindable target-link views, true when an output target is bound. */
        [[nodiscard]] bool bound() const noexcept;

        /** True when this view is backed by a peered input node whose target can be rebound. */
        [[nodiscard]] bool is_bindable() const noexcept;

        void bind_output(const TSOutputView &output);
        void unbind_output();

        void make_active();
        void make_passive();
        [[nodiscard]] bool active() const;

        [[nodiscard]] TSSInputView as_set() &;
        [[nodiscard]] TSSInputView as_set() const &;
        void as_set() && = delete;
        void as_set() const && = delete;
        [[nodiscard]] TSDInputView as_dict() &;
        [[nodiscard]] TSDInputView as_dict() const &;
        void as_dict() && = delete;
        void as_dict() const && = delete;
        [[nodiscard]] TSBInputView as_bundle() &;
        [[nodiscard]] TSBInputView as_bundle() const &;
        void as_bundle() && = delete;
        void as_bundle() const && = delete;
        [[nodiscard]] TSLInputView as_list() &;
        [[nodiscard]] TSLInputView as_list() const &;
        void as_list() && = delete;
        void as_list() const && = delete;
        [[nodiscard]] TSWInputView as_window() &;
        [[nodiscard]] TSWInputView as_window() const &;
        void as_window() && = delete;
        void as_window() const && = delete;

      private:
        friend class TSInput;
        friend class TSBInputView;
        friend class TSLInputView;
        friend class TSSInputView;
        friend class TSDInputView;
        friend class TSWInputView;

        TSInputView(TSInput                         *input,
                    detail::TSInputNode            *node,
                    TSDataView                      target_view,
                    std::vector<std::size_t>        target_path,
                    Notifiable                     *scheduling_notifier,
                    engine_time_t                   evaluation_time) noexcept;

        [[nodiscard]] bool is_target_position() const noexcept;
        [[nodiscard]] bool target_view_live() const noexcept;
        [[nodiscard]] TSDataView &checked_target_data_view(const char *what) const;
        [[nodiscard]] TSInputView child_from_target(TSDataView child, std::size_t index) const;
        [[nodiscard]] TSInputView child_from_node(detail::TSInputNode *child) const noexcept;

        TSInput                  *input_{nullptr};
        detail::TSInputNode      *node_{nullptr};
        TSDataView                data_view_{};
        std::vector<std::size_t>  target_path_{};
        Notifiable               *scheduling_notifier_{nullptr};
        engine_time_t             evaluation_time_{MIN_DT};
    };

    template <typename Derived>
    class TSInputTypedView : public TSTypedTimeSeriesView<Derived, TSInputView>
    {
      public:
        using base_type = TSTypedTimeSeriesView<Derived, TSInputView>;

        void bind_output(const TSOutputView &output) { this->view_.bind_output(output); }
        void unbind_output() { this->view_.unbind_output(); }
        [[nodiscard]] bool is_bindable() const noexcept { return this->view_.is_bindable(); }
        void make_active() { this->view_.make_active(); }
        void make_passive() { this->view_.make_passive(); }
        [[nodiscard]] bool active() const { return this->view_.active(); }

      protected:
        using base_type::base_type;
    };

    class TSBInputView : public TSInputTypedView<TSBInputView>
    {
      public:
        explicit TSBInputView(TSInputView view);

        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] bool has_field(std::string_view name) const noexcept;
        [[nodiscard]] TSBDataView data_view() const;

        /** Field names and child views in schema order. */
        [[nodiscard]] Range<std::string_view> keys() const;
        [[nodiscard]] Range<TSInputView> values() const;

        /** Child views filtered by current validity or modification time. */
        [[nodiscard]] Range<TSInputView> valid_values() const;
        [[nodiscard]] Range<TSInputView> modified_values() const;

        /** ``field name -> child`` pairs in schema order, optionally filtered. */
        [[nodiscard]] KeyValueRange<std::string_view, TSInputView> items() const;
        [[nodiscard]] KeyValueRange<std::string_view, TSInputView> valid_items() const;
        [[nodiscard]] KeyValueRange<std::string_view, TSInputView> modified_items() const;

        [[nodiscard]] TSInputView at(std::size_t index) &;
        [[nodiscard]] TSInputView at(std::size_t index) const &;
        TSInputView at(std::size_t) && = delete;
        [[nodiscard]] TSInputView operator[](std::size_t index) &;
        [[nodiscard]] TSInputView operator[](std::size_t index) const &;
        TSInputView operator[](std::size_t) && = delete;

        [[nodiscard]] TSInputView at(std::string_view name) &;
        [[nodiscard]] TSInputView at(std::string_view name) const &;
        TSInputView at(std::string_view) && = delete;
        [[nodiscard]] TSInputView field(std::string_view name) &;
        [[nodiscard]] TSInputView field(std::string_view name) const &;
        TSInputView field(std::string_view) && = delete;
        [[nodiscard]] TSInputView operator[](std::string_view name) &;
        [[nodiscard]] TSInputView operator[](std::string_view name) const &;
        TSInputView operator[](std::string_view) && = delete;

      private:
        static constexpr std::size_t npos = static_cast<std::size_t>(-1);

        [[nodiscard]] std::size_t field_index(std::string_view name) const;
        [[nodiscard]] std::size_t find_field_index(std::string_view name) const noexcept;
        [[nodiscard]] std::string_view key_at(std::size_t index) const noexcept;

    };

    class TSLInputView : public TSInputTypedView<TSLInputView>
    {
      public:
        explicit TSLInputView(TSInputView view);

        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] TSLDataView data_view() const;

        /** Child views in index order. */
        [[nodiscard]] Range<TSInputView> values() const;

        /** Child views filtered by current validity or modification time. */
        [[nodiscard]] Range<TSInputView> valid_values() const;
        [[nodiscard]] Range<TSInputView> modified_values() const;

        /** ``index -> child`` pairs in index order, optionally filtered. */
        [[nodiscard]] KeyValueRange<std::size_t, TSInputView> items() const;
        [[nodiscard]] KeyValueRange<std::size_t, TSInputView> valid_items() const;
        [[nodiscard]] KeyValueRange<std::size_t, TSInputView> modified_items() const;

        [[nodiscard]] TSInputView at(std::size_t index) &;
        [[nodiscard]] TSInputView at(std::size_t index) const &;
        TSInputView at(std::size_t) && = delete;
        [[nodiscard]] TSInputView operator[](std::size_t index) &;
        [[nodiscard]] TSInputView operator[](std::size_t index) const &;
        TSInputView operator[](std::size_t) && = delete;
    };

    class TSSInputView : public TSInputTypedView<TSSInputView>
    {
      public:
        explicit TSSInputView(TSInputView view);

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
    };

    class TSDInputView : public TSInputTypedView<TSDInputView>
    {
      public:
        explicit TSDInputView(TSInputView view);

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
        [[nodiscard]] TSInputView at_slot(std::size_t slot) const;
        [[nodiscard]] bool contains(const ValueView &key) const;
        [[nodiscard]] std::size_t find_slot(const ValueView &key) const;
        [[nodiscard]] TSInputView at(const ValueView &key) const;
        [[nodiscard]] TSInputView operator[](const ValueView &key) const;
        [[nodiscard]] Range<ValueView> keys() const;
        [[nodiscard]] Range<TSInputView> values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSInputView> items() const;
        [[nodiscard]] Range<ValueView> valid_keys() const;
        [[nodiscard]] Range<TSInputView> valid_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSInputView> valid_items() const;
        [[nodiscard]] Range<ValueView> modified_keys() const;
        [[nodiscard]] Range<TSInputView> modified_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSInputView> modified_items() const;
        [[nodiscard]] Range<ValueView> added_keys() const;
        [[nodiscard]] Range<TSInputView> added_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSInputView> added_items() const;
        [[nodiscard]] Range<ValueView> removed_keys() const;
        [[nodiscard]] Range<TSInputView> removed_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSInputView> removed_items() const;
    };

    class TSWInputView : public TSInputTypedView<TSWInputView>
    {
      public:
        explicit TSWInputView(TSInputView view);

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
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_H

#ifndef HGRAPH_CPP_TS_DATA_WINDOW_VIEW_H
#define HGRAPH_CPP_TS_DATA_WINDOW_VIEW_H

#include <hgraph/types/time_series/ts_data/base_view.h>
#include <hgraph/types/value/value_range.h>
#include <hgraph/util/date_time.h>
#include <cstddef>

namespace hgraph
{
    /** Read view over window-shaped TSData. */
    class TSWDataView
    {
      public:
        explicit TSWDataView(TSDataView view);

        /** Underlying generic TSData view. */
        [[nodiscard]] const TSDataView &base() const noexcept;
        [[nodiscard]] TSDataView &base() noexcept;

        /** Binding, schema, layout, and value projections for the window node. */
        [[nodiscard]] const TSDataBinding *binding() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;
        [[nodiscard]] const TSWDataLayout &layout() const;
        [[nodiscard]] const SizeTSWDataLayout &size_layout() const;
        [[nodiscard]] const TimeTSWDataLayout &time_layout() const;
        [[nodiscard]] ValueView value() const;
        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const;
        [[nodiscard]] engine_time_t last_modified_time() const;
        [[nodiscard]] bool modified(engine_time_t evaluation_time) const;

        /** True for time-duration windows; false for fixed-size windows. */
        [[nodiscard]] bool duration_based() const noexcept;
        [[nodiscard]] bool size_based() const noexcept;
        [[nodiscard]] bool time_based() const noexcept;

        /** Fixed-size or duration configuration for the window. */
        [[nodiscard]] std::size_t period() const;
        [[nodiscard]] std::size_t min_period() const;
        [[nodiscard]] engine_time_delta_t time_range() const;
        [[nodiscard]] engine_time_delta_t min_time_range() const;

        /** Allocated and occupied window sizes. */
        [[nodiscard]] std::size_t capacity() const;
        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] bool full() const;
        [[nodiscard]] bool all_valid() const;

        /** Time associated with the oldest element, or ``MIN_DT`` when empty. */
        [[nodiscard]] engine_time_t first_modified_time() const;

        /** Time metadata and value access by oldest-to-newest index. */
        [[nodiscard]] engine_time_t time_at(std::size_t index) const;
        [[nodiscard]] ValueView time_value_at(std::size_t index) const;
        [[nodiscard]] ValueView at(std::size_t index) const;
        [[nodiscard]] ValueView operator[](std::size_t index) const;
        [[nodiscard]] ValueView front() const;
        [[nodiscard]] ValueView back() const;

        /** Element, time-value, and raw engine-time ranges. */
        [[nodiscard]] Range<ValueView> values() const;
        [[nodiscard]] Range<ValueView> time_values() const;
        [[nodiscard]] Range<engine_time_t> value_times() const;
        [[nodiscard]] Range<ValueView>::iterator begin() const;
        [[nodiscard]] Range<ValueView>::iterator end() const;

        /** Begin a mutation view over this window. */
        [[nodiscard]] TSWDataMutationView begin_mutation(engine_time_t evaluation_time) const;

      private:
        static void validate_kind(const TSDataView &view);
        [[nodiscard]] const TSWDataOps &window_ops() const;
        [[nodiscard]] static ValueView project_value(const void *context, const void *, std::size_t index);
        [[nodiscard]] static ValueView project_time_value(const void *context, const void *, std::size_t index);
        [[nodiscard]] static engine_time_t project_time(const void *context, const void *, std::size_t index);

        TSDataView view_{};
    };

    /** Mutation view over window-shaped TSData. */
    class TSWDataMutationView : public TSWDataView
    {
      public:
        TSWDataMutationView(TSDataView view, engine_time_t evaluation_time);

        TSWDataMutationView(const TSWDataMutationView &) = delete;
        TSWDataMutationView &operator=(const TSWDataMutationView &) = delete;
        TSWDataMutationView(TSWDataMutationView &&) noexcept;
        TSWDataMutationView &operator=(TSWDataMutationView &&) = delete;
        ~TSWDataMutationView() noexcept;

        /** Read view for the same underlying window. */
        [[nodiscard]] TSWDataView view();

        /** Engine time associated with this mutation scope. */
        [[nodiscard]] engine_time_t current_mutation_time() const;

        /** Append one window tick. Only one tick is accepted per engine time. */
        void push(const ValueView &source);

        /** Replace the window from a value-layer window/list representation. */
        [[nodiscard]] bool copy_value_from(const ValueView &source);

      private:
        TSDataMutationView mutation_;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_TS_DATA_WINDOW_VIEW_H

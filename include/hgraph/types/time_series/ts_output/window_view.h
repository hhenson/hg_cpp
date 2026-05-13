#ifndef HGRAPH_CPP_ROOT_TS_OUTPUT_WINDOW_VIEW_H
#define HGRAPH_CPP_ROOT_TS_OUTPUT_WINDOW_VIEW_H

#include <hgraph/types/time_series/ts_output/typed_view.h>

namespace hgraph
{
    class TSOutput;
    class TSOutputView;
    class TSOutputMutationView;
    class TSBOutputView;
    class TSLOutputView;
    class TSSOutputView;
    class TSDOutputView;
    class TSWOutputView;

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
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_OUTPUT_WINDOW_VIEW_H

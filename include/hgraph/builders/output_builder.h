
#ifndef OUTPUT_BUILDER_H
#define OUTPUT_BUILDER_H

#include <hgraph/hgraph_forward_declarations.h>

#include <hgraph/types/tsb.h>

#include <hgraph/builders/builder.h>

#include <hgraph/types/ref.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/tsb.h>

namespace hgraph
{

    struct HGRAPH_EXPORT OutputBuilder : Builder
    {
        using ptr = nb::ref<OutputBuilder>;
        using Builder::Builder;

        virtual time_series_output_ptr make_instance(node_ptr owning_node) = 0;

        virtual time_series_output_ptr make_instance(time_series_output_ptr owning_output) = 0;

        virtual void release_instance(time_series_output_ptr item) {};

        static void register_with_nanobind(nb::module_ &m);
    };

    template <typename T> struct HGRAPH_EXPORT TimeSeriesValueOutputBuilder : OutputBuilder
    {
        using OutputBuilder::OutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) override {
            auto v{new TimeSeriesValueOutput<T>(owning_node)};
            return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
        }

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) override {
            auto v{new TimeSeriesValueOutput<T>(dynamic_cast_ref<TimeSeriesType>(owning_output))};
            return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
        }
    };

    struct HGRAPH_EXPORT TimeSeriesRefOutputBuilder : OutputBuilder
    {
        using OutputBuilder::OutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) override;

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) override;
    };

    struct HGRAPH_EXPORT TimeSeriesBundleOutputBuilder : OutputBuilder
    {
        TimeSeriesBundleOutputBuilder(TimeSeriesSchema::ptr schema, std::vector<OutputBuilder::ptr> output_builders);

        time_series_output_ptr make_instance(node_ptr owning_node) override;

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) override;

      private:
        time_series_output_ptr make_and_set_outputs(TimeSeriesBundleOutput *output);
        TimeSeriesSchema::ptr           schema;
        std::vector<OutputBuilder::ptr> output_builders;
    };
}  // namespace hgraph

#endif  // OUTPUT_BUILDER_H

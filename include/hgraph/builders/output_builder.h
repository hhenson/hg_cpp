
#ifndef OUTPUT_BUILDER_H
#define OUTPUT_BUILDER_H

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/builders/builder.h>

#include <hgraph/types/ref.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsl.h>

namespace hgraph
{

    struct HGRAPH_EXPORT OutputBuilder : Builder
    {
        using ptr = nb::ref<OutputBuilder>;
        using Builder::Builder;

        virtual time_series_output_ptr make_instance(node_ptr owning_node) const = 0;

        virtual time_series_output_ptr make_instance(time_series_output_ptr owning_output) const = 0;

        virtual void release_instance(time_series_output_ptr item) const {};

        static void register_with_nanobind(nb::module_ &m);
    };

    template <typename T> struct HGRAPH_EXPORT TimeSeriesValueOutputBuilder : OutputBuilder
    {
        using OutputBuilder::OutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) const override {
            auto v{new TimeSeriesValueOutput<T>(owning_node)};
            return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
        }

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override {
            auto v{new TimeSeriesValueOutput<T>(dynamic_cast_ref<TimeSeriesType>(owning_output))};
            return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
        }
    };

    struct HGRAPH_EXPORT TimeSeriesRefOutputBuilder : OutputBuilder
    {
        using OutputBuilder::OutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) const override;

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;
    };

    struct HGRAPH_EXPORT TimeSeriesListOutputBuilder : OutputBuilder
    {
        using ptr = nb::ref<TimeSeriesListOutputBuilder>;
        TimeSeriesListOutputBuilder(OutputBuilder::ptr output_builder, size_t size);

        time_series_output_ptr make_instance(node_ptr owning_node) const override;

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;

      private:
        time_series_output_ptr make_and_set_outputs(TimeSeriesListOutput *output) const;
        OutputBuilder::ptr     output_builder;
        size_t                 size;
    };

    struct HGRAPH_EXPORT TimeSeriesBundleOutputBuilder : OutputBuilder
    {
        TimeSeriesBundleOutputBuilder(TimeSeriesSchema::ptr schema, std::vector<OutputBuilder::ptr> output_builders);

        time_series_output_ptr make_instance(node_ptr owning_node) const override;

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;

      private:
        time_series_output_ptr          make_and_set_outputs(TimeSeriesBundleOutput *output) const;
        TimeSeriesSchema::ptr           schema;
        std::vector<OutputBuilder::ptr> output_builders;
    };
}  // namespace hgraph

#endif  // OUTPUT_BUILDER_H

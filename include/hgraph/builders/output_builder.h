
#ifndef OUTPUT_BUILDER_H
#define OUTPUT_BUILDER_H

#include <hgraph/builders/builder.h>

#include <hgraph/types/ref.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsd.h>
#include <hgraph/types/tsl.h>
#include <hgraph/types/tss.h>

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

    struct HGRAPH_EXPORT TimeSeriesSetOutputBuilder : OutputBuilder
    {
        using OutputBuilder::OutputBuilder;
    };

    template <typename T> struct HGRAPH_EXPORT TimeSeriesSetOutputBuilder_T : TimeSeriesSetOutputBuilder
    {
        using TimeSeriesSetOutputBuilder::TimeSeriesSetOutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) const override {
            auto v{new TimeSeriesSetOutput_T<T>(owning_node)};
            return v;
        }

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override {
            auto v{new TimeSeriesSetOutput_T<T>{dynamic_cast_ref<TimeSeriesType>(owning_output)}};
            return v;
        }
    };

    struct HGRAPH_EXPORT TimeSeriesDictOutputBuilder : OutputBuilder
    {
        output_builder_ptr ts_builder;
        output_builder_ptr ts_ref_builder;

        TimeSeriesDictOutputBuilder(output_builder_ptr ts_builder, output_builder_ptr ts_ref_builder);
    };

    template <typename T> struct HGRAPH_EXPORT TimeSeriesDictOutputBuilder_T : TimeSeriesDictOutputBuilder
    {
        using TimeSeriesDictOutputBuilder::TimeSeriesDictOutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) const override {
            auto v{new TimeSeriesDictOutput_T<T>(owning_node, ts_builder, ts_ref_builder)};
            return v;
        }

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override {
            auto v{new TimeSeriesDictOutput_T<T>{dynamic_cast_ref<TimeSeriesType>(owning_output), ts_builder, ts_ref_builder}};
            return v;
        }
    };
}  // namespace hgraph

#endif  // OUTPUT_BUILDER_H

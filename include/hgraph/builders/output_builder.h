
#ifndef OUTPUT_BUILDER_H
#define OUTPUT_BUILDER_H

#include <hgraph/builders/builder.h>
#include <ranges>

namespace hgraph
{

    struct HGRAPH_EXPORT OutputBuilder : Builder
    {
        using ptr = nb::ref<OutputBuilder>;
        using Builder::Builder;

        virtual time_series_output_ptr make_instance(node_ptr owning_node) const = 0;

        virtual time_series_output_ptr make_instance(time_series_output_ptr owning_output) const = 0;

        virtual void release_instance(time_series_output_ptr item) const {};

        virtual bool has_reference() const {return false;}

        static void register_with_nanobind(nb::module_ &m);
    };

    template <typename T> struct HGRAPH_EXPORT TimeSeriesValueOutputBuilder : OutputBuilder
    {
        using OutputBuilder::OutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) const override;

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;
    };

    struct HGRAPH_EXPORT TimeSeriesRefOutputBuilder : OutputBuilder
    {
        using OutputBuilder::OutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) const override;

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;

        bool has_reference() const override {return true;}
    };

    struct HGRAPH_EXPORT TimeSeriesListOutputBuilder : OutputBuilder
    {
        using ptr = nb::ref<TimeSeriesListOutputBuilder>;
        TimeSeriesListOutputBuilder(OutputBuilder::ptr output_builder, size_t size);

        time_series_output_ptr make_instance(node_ptr owning_node) const override;

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;

        bool has_reference() const override {return output_builder->has_reference();}

        [[nodiscard]] bool is_same_type(const Builder &other) const override {
            if (auto other_b = dynamic_cast<const TimeSeriesListOutputBuilder *>(&other)) {
                return output_builder->is_same_type(*other_b->output_builder);
            }
            return false;
        }

      private:
        time_series_output_ptr make_and_set_outputs(TimeSeriesListOutput *output) const;
        OutputBuilder::ptr     output_builder;
        size_t                 size;
    };

    struct HGRAPH_EXPORT TimeSeriesBundleOutputBuilder : OutputBuilder
    {
        TimeSeriesBundleOutputBuilder(time_series_schema_ptr schema, std::vector<OutputBuilder::ptr> output_builders);

        time_series_output_ptr make_instance(node_ptr owning_node) const override;

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;

        bool has_reference() const override {
            return std::ranges::any_of(output_builders, [](const auto &builder) { return builder->has_reference(); });
        }

        [[nodiscard]] bool is_same_type(const Builder &other) const override {
            if (auto other_b = dynamic_cast<const TimeSeriesBundleOutputBuilder *>(&other)) {
                if (output_builders.size() != other_b->output_builders.size()) { return false; }
                for (size_t i = 0; i < output_builders.size(); ++i) {
                    if (!output_builders[i]->is_same_type(*other_b->output_builders[i])) { return false; }
                }
                return true;
            }
            return false;
        }

      private:
        time_series_output_ptr          make_and_set_outputs(TimeSeriesBundleOutput *output) const;
        time_series_schema_ptr           schema;
        std::vector<OutputBuilder::ptr> output_builders;
    };

    struct HGRAPH_EXPORT TimeSeriesSetOutputBuilder : OutputBuilder
    {
        using OutputBuilder::OutputBuilder;
    };

    template <typename T> struct HGRAPH_EXPORT TimeSeriesSetOutputBuilder_T : TimeSeriesSetOutputBuilder
    {
        using TimeSeriesSetOutputBuilder::TimeSeriesSetOutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) const override;

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;
    };

    struct HGRAPH_EXPORT TimeSeriesDictOutputBuilder : OutputBuilder
    {
        output_builder_ptr ts_builder;
        output_builder_ptr ts_ref_builder;

        TimeSeriesDictOutputBuilder(output_builder_ptr ts_builder, output_builder_ptr ts_ref_builder);

        bool has_reference() const override {return ts_builder->has_reference();}
    };

    template <typename T> struct HGRAPH_EXPORT TimeSeriesDictOutputBuilder_T : TimeSeriesDictOutputBuilder
    {
        using TimeSeriesDictOutputBuilder::TimeSeriesDictOutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) const override;

        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;

        [[nodiscard]] bool is_same_type(const Builder &other) const override {
            if (auto other_b = dynamic_cast<const TimeSeriesDictOutputBuilder_T<T> *>(&other)) {
                return ts_builder->is_same_type(*other_b->ts_builder);
            }
            return false;
        }
    };
}  // namespace hgraph

#endif  // OUTPUT_BUILDER_H

//
// Created by Howard Henson on 27/12/2024.
//

#ifndef INPUT_BUILDER_H
#define INPUT_BUILDER_H

#include <hgraph/builders/builder.h>

#include <hgraph/types/ref.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsl.h>
#include <hgraph/types/tss.h>
#include <hgraph/types/tsd.h>

namespace hgraph
{

    // The InputBuilder class implementation

    struct InputBuilder : Builder
    {
        using ptr = nb::ref<InputBuilder>;

        /**
         * Create an instance of InputBuilder using an owning node
         */
        virtual time_series_input_ptr make_instance(node_ptr owning_node) const = 0;

        /**
         * Create an instance of InputBuilder using an parent input
         */
        virtual time_series_input_ptr make_instance(time_series_input_ptr owning_input) const = 0;

        /**
         * Release an instance of the input type.
         * By default, do nothing.
         */
        virtual void release_instance(time_series_input_ptr item) const {}

        static void register_with_nanobind(nb::module_ &m);
    };

    template <typename T> struct HGRAPH_EXPORT TimeSeriesValueInputBuilder : InputBuilder
    {
        using ptr = nb::ref<TimeSeriesValueInputBuilder<T>>;
        using InputBuilder::InputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) const override {
            auto v{new TimeSeriesValueInput<T>(owning_node)};
            return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
        }

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override {
            auto v{new TimeSeriesValueInput<T>(dynamic_cast_ref<TimeSeriesType>(owning_input))};
            return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
        }
    };

    struct HGRAPH_EXPORT TimeSeriesRefInputBuilder : InputBuilder
    {
        using InputBuilder::InputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) const override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;
    };

    struct HGRAPH_EXPORT TimeSeriesListInputBuilder : InputBuilder
    {
        using ptr = nb::ref<TimeSeriesListInputBuilder>;

        TimeSeriesListInputBuilder(InputBuilder::ptr input_builder, size_t size);

        time_series_input_ptr make_instance(node_ptr owning_node) const override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;

      private:
        time_series_input_ptr make_and_set_inputs(TimeSeriesListInput *input) const;

        InputBuilder::ptr input_builder;
        size_t            size;
    };

    struct HGRAPH_EXPORT TimeSeriesBundleInputBuilder : InputBuilder
    {
        using ptr = nb::ref<TimeSeriesBundleInputBuilder>;

        TimeSeriesBundleInputBuilder(TimeSeriesSchema::ptr schema, std::vector<InputBuilder::ptr> input_builders);

        time_series_input_ptr make_instance(node_ptr owning_node) const override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;

      private:
        time_series_input_ptr          make_and_set_inputs(TimeSeriesBundleInput *input) const;
        TimeSeriesSchema::ptr          schema;
        std::vector<InputBuilder::ptr> input_builders;
    };

    struct HGRAPH_EXPORT TimeSeriesSetInputBuilder : InputBuilder
    {
        using ptr = nb::ref<TimeSeriesSetInputBuilder>;
        using InputBuilder::InputBuilder;

    };

    template<typename T>
    struct HGRAPH_EXPORT TimeSeriesSetInputBuilder_T : TimeSeriesSetInputBuilder
    {
        using TimeSeriesSetInputBuilder::TimeSeriesSetInputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) const override {
            auto v{new TimeSeriesSetInput_T<T>{owning_node}};
            return v;
        }

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override {
            auto v{new TimeSeriesSetInput_T<T>{dynamic_cast_ref<TimeSeriesType>(owning_input)}};
            return v;
        }
    };

    struct HGRAPH_EXPORT TimeSeriesDictInputBuilder : InputBuilder
    {
        using ptr = nb::ref<TimeSeriesDictInputBuilder>;
        input_builder_ptr ts_builder;

        TimeSeriesDictInputBuilder(input_builder_ptr ts_builder);
    };

    template<typename T>
    struct HGRAPH_EXPORT TimeSeriesDictInputBuilder_T : TimeSeriesDictInputBuilder
    {
        using TimeSeriesDictInputBuilder::TimeSeriesDictInputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) const override {
            auto v{new TimeSeriesDictInput_T<T>(owning_node, ts_builder)};
            return v;
        }

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override {
            auto v{new TimeSeriesDictInput_T<T>{dynamic_cast_ref<TimeSeriesType>(owning_input), ts_builder}};
            return v;
        }
    };
}  // namespace hgraph

#endif  // INPUT_BUILDER_H

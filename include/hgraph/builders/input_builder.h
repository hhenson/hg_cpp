//
// Created by Howard Henson on 27/12/2024.
//

#ifndef INPUT_BUILDER_H
#define INPUT_BUILDER_H

#include <hgraph/builders/builder.h>
#include <hgraph/hgraph_forward_declarations.h>

#include <hgraph/types/ts.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/tsb.h>


namespace hgraph
{

    // The InputBuilder class implementation

    struct InputBuilder : Builder
    {
        using ptr = nb::ref<InputBuilder>;

        /**
         * Create an instance of InputBuilder using an owning node
         */
        virtual time_series_input_ptr make_instance(node_ptr owning_node) = 0;

        /**
         * Create an instance of InputBuilder using an parent input
         */
        virtual time_series_input_ptr make_instance(time_series_input_ptr owning_input) = 0;

        /**
         * Release an instance of the input type.
         * By default, do nothing.
         */
        virtual void release_instance(time_series_input_ptr item) {}

        static void register_with_nanobind(nb::module_ &m);
    };

    template <typename T> struct HGRAPH_EXPORT TimeSeriesValueInputBuilder : InputBuilder
    {
        using ptr = nb::ref<TimeSeriesValueInputBuilder<T>>;
        using InputBuilder::InputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) override {
            auto v{new TimeSeriesValueInput<T>(owning_node)};
            return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
        }

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) override {
            auto v{new TimeSeriesValueInput<T>(dynamic_cast_ref<TimeSeriesType>(owning_input))};
            return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
        }
    };

    struct HGRAPH_EXPORT TimeSeriesRefInputBuilder : InputBuilder
    {
        using InputBuilder::InputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) override;
    };

    struct HGRAPH_EXPORT TimeSeriesBundleInputBuilder : InputBuilder
    {
        using ptr = nb::ref<TimeSeriesBundleInputBuilder>;

        TimeSeriesBundleInputBuilder(TimeSeriesSchema::ptr schema, std::vector<InputBuilder::ptr> input_builders);

        time_series_input_ptr make_instance(node_ptr owning_node) override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) override;

      private:
        time_series_input_ptr          make_and_set_inputs(TimeSeriesBundleInput *input);
        TimeSeriesSchema::ptr          schema;
        std::vector<InputBuilder::ptr> input_builders;
    };
}  // namespace hgraph

#endif  // INPUT_BUILDER_H

//
// Created by Howard Henson on 27/12/2024.
//

#ifndef INPUT_BUILDER_H
#define INPUT_BUILDER_H

#include "hgraph/types/ref.h"

#include <hgraph/builders/builder.h>
#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/ts.h>

namespace hgraph
{

    // The InputBuilder class implementation

    struct InputBuilder : Builder
    {
        using ptr = nb::ref<InputBuilder>;

        /**
         * Create an instance of InputBuilder using an owning node
         */
        virtual time_series_input_ptr make_instance(node_ptr owning_node = nullptr) = 0;

        /**
         * Create an instance of InputBuilder using an parent input
         */
        virtual time_series_input_ptr make_instance(time_series_input_ptr owning_input = nullptr) = 0;

        /**
         * Release an instance of the input type.
         * By default, do nothing.
         */
        virtual void release_instance(time_series_input_ptr item) {}

        static void register_with_nanobind(nb::module_ &m);
    };

    template <typename T> struct HGRAPH_EXPORT TimeSeriesValueInputBuilder : InputBuilder
    {
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
        using InputBuilder::InputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) override;

    };
}  // namespace hgraph

#endif  // INPUT_BUILDER_H

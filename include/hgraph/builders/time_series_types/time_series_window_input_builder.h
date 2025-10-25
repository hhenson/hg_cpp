//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_WINDOW_INPUT_BUILDER_H
#define TIME_SERIES_WINDOW_INPUT_BUILDER_H

#include <hgraph/builders/input_builder.h>

namespace hgraph
{
    // TimeSeriesWindow (TSW) input builder for fixed-size windows
    template <typename T> struct HGRAPH_EXPORT TimeSeriesWindowInputBuilder_T : InputBuilder
    {
        using ptr = nb::ref<TimeSeriesWindowInputBuilder_T<T>>;
        size_t size;
        size_t min_size;

        TimeSeriesWindowInputBuilder_T(size_t size, size_t min_size) : size(size), min_size(min_size) {}

        time_series_input_ptr make_instance(node_ptr owning_node) const override;
        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;

        [[nodiscard]] bool is_same_type(const Builder &other) const override {
            if (auto other_b = dynamic_cast<const TimeSeriesWindowInputBuilder_T<T> *>(&other)) {
                return size == other_b->size && min_size == other_b->min_size;
            }
            return false;
        }
    };

    void time_series_window_input_builder_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph

#endif  // TIME_SERIES_WINDOW_INPUT_BUILDER_H

#include <hgraph/builders/time_series_types/time_series_window_output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsw.h>

namespace hgraph
{
    template <typename T>
    TimeSeriesWindowOutputBuilder_T<T>::TimeSeriesWindowOutputBuilder_T(size_t size, size_t min_size)
        : size(size), min_size(min_size) {}

    // TSW output builder implementations
    template <typename T> time_series_output_ptr TimeSeriesWindowOutputBuilder_T<T>::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesFixedWindowOutput<T>(owning_node, size, min_size)};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    template <typename T>
    time_series_output_ptr TimeSeriesWindowOutputBuilder_T<T>::make_instance(time_series_output_ptr owning_output) const {
        auto v{new TimeSeriesFixedWindowOutput<T>(dynamic_cast_ref<TimeSeriesType>(owning_output), size, min_size)};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    template <typename T> bool TimeSeriesWindowOutputBuilder_T<T>::is_same_type(const Builder &other) const {
        if (auto other_b = dynamic_cast<const TimeSeriesWindowOutputBuilder_T<T> *>(&other)) {
            return size == other_b->size && min_size == other_b->min_size;
        }
        return false;
    }

    template <typename T> void TimeSeriesWindowOutputBuilder_T<T>::release_instance(time_series_output_ptr item) const {
        OutputBuilder::release_instance(item);
        auto ts = dynamic_cast<TimeSeriesFixedWindowOutput<T> *>(item.get());
        if (ts) { ts->reset_value(); }
    }

    void time_series_window_output_builder_register_with_nanobind(nb::module_ &m) {
        using OutputBuilder_TSW_Bool      = TimeSeriesWindowOutputBuilder_T<bool>;
        using OutputBuilder_TSW_Int       = TimeSeriesWindowOutputBuilder_T<int64_t>;
        using OutputBuilder_TSW_Float     = TimeSeriesWindowOutputBuilder_T<double>;
        using OutputBuilder_TSW_Date      = TimeSeriesWindowOutputBuilder_T<engine_date_t>;
        using OutputBuilder_TSW_DateTime  = TimeSeriesWindowOutputBuilder_T<engine_time_t>;
        using OutputBuilder_TSW_TimeDelta = TimeSeriesWindowOutputBuilder_T<engine_time_delta_t>;
        using OutputBuilder_TSW_Object    = TimeSeriesWindowOutputBuilder_T<nb::object>;

        nb::class_<OutputBuilder_TSW_Bool, OutputBuilder>(m, "OutputBuilder_TSW_Bool")
            .def(nb::init<size_t, size_t>(), "size"_a, "min_size"_a);
        nb::class_<OutputBuilder_TSW_Int, OutputBuilder>(m, "OutputBuilder_TSW_Int")
            .def(nb::init<size_t, size_t>(), "size"_a, "min_size"_a);
        nb::class_<OutputBuilder_TSW_Float, OutputBuilder>(m, "OutputBuilder_TSW_Float")
            .def(nb::init<size_t, size_t>(), "size"_a, "min_size"_a);
        nb::class_<OutputBuilder_TSW_Date, OutputBuilder>(m, "OutputBuilder_TSW_Date")
            .def(nb::init<size_t, size_t>(), "size"_a, "min_size"_a);
        nb::class_<OutputBuilder_TSW_DateTime, OutputBuilder>(m, "OutputBuilder_TSW_DateTime")
            .def(nb::init<size_t, size_t>(), "size"_a, "min_size"_a);
        nb::class_<OutputBuilder_TSW_TimeDelta, OutputBuilder>(m, "OutputBuilder_TSW_TimeDelta")
            .def(nb::init<size_t, size_t>(), "size"_a, "min_size"_a);
        nb::class_<OutputBuilder_TSW_Object, OutputBuilder>(m, "OutputBuilder_TSW_Object")
            .def(nb::init<size_t, size_t>(), "size"_a, "min_size"_a);
    }

    // Template instantiations
    template struct TimeSeriesWindowOutputBuilder_T<bool>;
    template struct TimeSeriesWindowOutputBuilder_T<int64_t>;
    template struct TimeSeriesWindowOutputBuilder_T<double>;
    template struct TimeSeriesWindowOutputBuilder_T<engine_date_t>;
    template struct TimeSeriesWindowOutputBuilder_T<engine_time_t>;
    template struct TimeSeriesWindowOutputBuilder_T<engine_time_delta_t>;
    template struct TimeSeriesWindowOutputBuilder_T<nb::object>;

}  // namespace hgraph

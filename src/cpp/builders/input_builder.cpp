#include <hgraph/builders/input_builder.h>
#include <hgraph/python/pyb_wiring.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>

namespace hgraph
{

    void InputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<InputBuilder, Builder>(m, "InputBuilder")
            .def(
                "make_instance",
                [](InputBuilder::ptr self, nb::object owning_node, nb::object owning_output) -> time_series_input_ptr {
                    if (!owning_node.is_none()) { return self->make_instance(nb::cast<node_ptr>(owning_node)); }
                    if (!owning_output.is_none()) { return self->make_instance(nb::cast<time_series_input_ptr>(owning_output)); }
                    throw std::runtime_error("At least one of owning_node or owning_output must be provided");
                },
                "owning_node"_a = nb::none(), "owning_output"_a = nb::none())
            .def("release_instance", &InputBuilder::release_instance);

        using InputBuilder_TS_Bool      = TimeSeriesValueInputBuilder<bool>;
        using InputBuilder_TS_Int       = TimeSeriesValueInputBuilder<int64_t>;
        using InputBuilder_TS_Float     = TimeSeriesValueInputBuilder<double>;
        using InputBuilder_TS_Date      = TimeSeriesValueInputBuilder<engine_date_t>;
        using InputBuilder_TS_DateTime  = TimeSeriesValueInputBuilder<engine_time_t>;
        using InputBuilder_TS_TimeDelta = TimeSeriesValueInputBuilder<engine_time_delta_t>;
        using InputBuilder_TS_Object    = TimeSeriesValueInputBuilder<nb::object>;

        nb::class_<InputBuilder_TS_Bool, InputBuilder>(m, "InputBuilder_TS_Bool").def(nb::init<>());
        nb::class_<InputBuilder_TS_Int, InputBuilder>(m, "InputBuilder_TS_Int").def(nb::init<>());
        nb::class_<InputBuilder_TS_Float, InputBuilder>(m, "InputBuilder_TS_Float").def(nb::init<>());
        nb::class_<InputBuilder_TS_Date, InputBuilder>(m, "InputBuilder_TS_Date").def(nb::init<>());
        nb::class_<InputBuilder_TS_DateTime, InputBuilder>(m, "InputBuilder_TS_DateTime").def(nb::init<>());
        nb::class_<InputBuilder_TS_TimeDelta, InputBuilder>(m, "InputBuilder_TS_TimeDelta").def(nb::init<>());
        nb::class_<InputBuilder_TS_Object, InputBuilder>(m, "InputBuilder_TS_Object").def(nb::init<>());

        nb::class_<TimeSeriesRefInputBuilder, InputBuilder>(m, "InputBuilder_TS_Ref").def(nb::init<>());
        nb::class_<TimeSeriesBundleInputBuilder, InputBuilder>(m, "InputBuilder_TSB").def(nb::init<>());
    }

    time_series_input_ptr TimeSeriesRefInputBuilder::make_instance(node_ptr owning_node) {
        auto v{new TimeSeriesReferenceInput(owning_node)};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    time_series_input_ptr TimeSeriesRefInputBuilder::make_instance(time_series_input_ptr owning_input) {
        auto v{new TimeSeriesReferenceInput(dynamic_cast_ref<TimeSeriesType>(owning_input))};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    time_series_input_ptr TimeSeriesBundleInputBuilder::make_instance(node_ptr owning_node) { return nullptr; }

    time_series_input_ptr TimeSeriesBundleInputBuilder::make_instance(time_series_input_ptr owning_input) { return nullptr; }
}  // namespace hgraph
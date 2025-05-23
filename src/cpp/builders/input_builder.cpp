#include <hgraph/builders/input_builder.h>
#include <hgraph/types/node.h>

#include <ranges>
#include <utility>

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
        nb::class_<TimeSeriesListInputBuilder, InputBuilder>(m, "InputBuilder_TSL")
            .def(nb::init<ptr, size_t>(), "input_builder"_a, "size"_a);
        nb::class_<TimeSeriesBundleInputBuilder, InputBuilder>(m, "InputBuilder_TSB")
            .def(nb::init<TimeSeriesSchema::ptr, std::vector<InputBuilder::ptr>>(), "schema"_a, "input_builders"_a);

        nb::class_<TimeSeriesSetInputBuilder, InputBuilder>(m, "InputBuilder_TSS_Object").def(nb::init<>());
    }

    time_series_input_ptr TimeSeriesRefInputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesReferenceInput(owning_node)};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    time_series_input_ptr TimeSeriesRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesReferenceInput(dynamic_cast_ref<TimeSeriesType>(owning_input))};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    TimeSeriesListInputBuilder::TimeSeriesListInputBuilder(InputBuilder::ptr input_builder, size_t size)
        : input_builder{std::move(input_builder)}, size{size} {}

    time_series_input_ptr TimeSeriesListInputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesListInput{owning_node}};
        return make_and_set_inputs(v);
    }

    time_series_input_ptr TimeSeriesListInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesListInput{dynamic_cast_ref<TimeSeriesType>(owning_input)}};
        return make_and_set_inputs(v);
    }

    time_series_input_ptr TimeSeriesListInputBuilder::make_and_set_inputs(TimeSeriesListInput *input) const {
        std::vector<time_series_input_ptr> inputs;
        inputs.reserve(size);
        for (size_t i = 0; i < size; ++i) { inputs.push_back(input_builder->make_instance(input)); }
        input->set_ts_values(inputs);
        return input;
    }

    TimeSeriesBundleInputBuilder::TimeSeriesBundleInputBuilder(TimeSeriesSchema::ptr          schema,
                                                               std::vector<InputBuilder::ptr> input_builders)
        : InputBuilder(), schema{std::move(schema)}, input_builders{std::move(input_builders)} {}

    time_series_input_ptr TimeSeriesBundleInputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesBundleInput{owning_node, schema}};
        return make_and_set_inputs(v);
    }

    time_series_input_ptr TimeSeriesBundleInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesBundleInput{dynamic_cast_ref<TimeSeriesType>(owning_input), schema}};
        return make_and_set_inputs(v);
    }

    time_series_input_ptr TimeSeriesBundleInputBuilder::make_and_set_inputs(TimeSeriesBundleInput *input) const {
        std::vector<time_series_input_ptr> inputs;
        time_series_input_ptr              input_{input};
        inputs.reserve(input_builders.size());
        std::ranges::copy(input_builders | std::views::transform([&](auto &builder) { return builder->make_instance(input_); }),
                          std::back_inserter(inputs));
        input->set_ts_values(inputs);
        return input_;
    }

    time_series_input_ptr TimeSeriesSetInputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesSetInput{owning_node}};
        return v;
    }

    time_series_input_ptr TimeSeriesSetInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesSetInput{dynamic_cast_ref<TimeSeriesType>(owning_input)}};
        return v;
    }
}  // namespace hgraph
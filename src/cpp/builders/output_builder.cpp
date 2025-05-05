#include <hgraph/python/pyb_wiring.h>
#include <hgraph/builders/output_builder.h>
#include <ranges>
#include <utility>

namespace hgraph
{

    void OutputBuilder::register_with_nanobind(nb::module_ &m) {

        nb::class_<OutputBuilder, Builder>(m, "OutputBuilder")
            .def(
                "make_instance",
                [](OutputBuilder::ptr self, nb::object owning_node, nb::object owning_output) -> time_series_output_ptr {
                    if (!owning_node.is_none()) { return self->make_instance(nb::cast<node_ptr>(owning_node)); }
                    if (!owning_output.is_none()) { return self->make_instance(nb::cast<time_series_output_ptr>(owning_output)); }
                    throw std::runtime_error("At least one of owning_node or owning_output must be provided");
                },
                "owning_node"_a = nb::none(), "owning_output"_a = nb::none())
            .def("release_instance", &OutputBuilder::release_instance);

        using OutputBuilder_TS_Bool      = TimeSeriesValueOutputBuilder<bool>;
        using OutputBuilder_TS_Int       = TimeSeriesValueOutputBuilder<int64_t>;
        using OutputBuilder_TS_Float     = TimeSeriesValueOutputBuilder<double>;
        using OutputBuilder_TS_Date      = TimeSeriesValueOutputBuilder<engine_date_t>;
        using OutputBuilder_TS_DateTime  = TimeSeriesValueOutputBuilder<engine_time_t>;
        using OutputBuilder_TS_TimeDelta = TimeSeriesValueOutputBuilder<engine_time_delta_t>;
        using OutputBuilder_TS_Object    = TimeSeriesValueOutputBuilder<nb::object>;

        nb::class_<OutputBuilder_TS_Bool, OutputBuilder>(m, "OutputBuilder_TS_Bool").def(nb::init<>());
        nb::class_<OutputBuilder_TS_Int, OutputBuilder>(m, "OutputBuilder_TS_Int").def(nb::init<>());
        nb::class_<OutputBuilder_TS_Float, OutputBuilder>(m, "OutputBuilder_TS_Float").def(nb::init<>());
        nb::class_<OutputBuilder_TS_Date, OutputBuilder>(m, "OutputBuilder_TS_Date").def(nb::init<>());
        nb::class_<OutputBuilder_TS_DateTime, OutputBuilder>(m, "OutputBuilder_TS_DateTime").def(nb::init<>());
        nb::class_<OutputBuilder_TS_TimeDelta, OutputBuilder>(m, "OutputBuilder_TS_TimeDelta").def(nb::init<>());
        nb::class_<OutputBuilder_TS_Object, OutputBuilder>(m, "OutputBuilder_TS_Object").def(nb::init<>());

        nb::class_<TimeSeriesRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TS_Ref").def(nb::init<>());
        nb::class_<TimeSeriesListOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSL")
            .def(nb::init<OutputBuilder::ptr, size_t>(), "output_builder"_a, "size"_a);
        nb::class_<TimeSeriesBundleOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSB")
            .def(nb::init<TimeSeriesSchema::ptr, std::vector<OutputBuilder::ptr>>(), "schema"_a, "output_builders"_a);

        nb::class_<TimeSeriesSetOutputBuilder_object, OutputBuilder>(m, "OutputBuilder_TSS_Object").def(nb::init<>());
    }

    time_series_output_ptr TimeSeriesRefOutputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesReferenceOutput(owning_node)};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    time_series_output_ptr TimeSeriesRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        auto v{new TimeSeriesReferenceOutput(dynamic_cast_ref<TimeSeriesType>(owning_output))};
        return time_series_output_ptr{static_cast<TimeSeriesOutput *>(v)};
    }

    TimeSeriesListOutputBuilder::TimeSeriesListOutputBuilder(OutputBuilder::ptr output_builder, size_t size)
        : output_builder{std::move(output_builder)}, size{size} {}

    time_series_output_ptr TimeSeriesListOutputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesListOutput(owning_node)};
        return make_and_set_outputs(v);
    }

    time_series_output_ptr TimeSeriesListOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        auto v{new TimeSeriesListOutput(dynamic_cast_ref<TimeSeriesType>(owning_output))};
        return make_and_set_outputs(v);
    }

    time_series_output_ptr TimeSeriesListOutputBuilder::make_and_set_outputs(TimeSeriesListOutput *output) const {
        std::vector<time_series_output_ptr> outputs;
        outputs.reserve(size);
        for (size_t i = 0; i < size; ++i) { outputs.push_back(output_builder->make_instance(output)); }
        output->set_ts_values(outputs);
        return output;
    }

    TimeSeriesBundleOutputBuilder::TimeSeriesBundleOutputBuilder(TimeSeriesSchema::ptr           schema,
                                                                 std::vector<OutputBuilder::ptr> output_builders)
        : OutputBuilder(), schema{std::move(schema)}, output_builders{std::move(output_builders)} {}

    time_series_output_ptr TimeSeriesBundleOutputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesBundleOutput{owning_node, schema}};
        return make_and_set_outputs(v);
    }

    time_series_output_ptr TimeSeriesBundleOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        auto v{new TimeSeriesBundleOutput(dynamic_cast_ref<TimeSeriesType>(owning_output), schema)};
        return make_and_set_outputs(v);
    }

    time_series_output_ptr TimeSeriesBundleOutputBuilder::make_and_set_outputs(TimeSeriesBundleOutput *output) const {
        std::vector<time_series_output_ptr> outputs;
        time_series_output_ptr              output_{output};
        outputs.reserve(output_builders.size());
        std::ranges::copy(output_builders | std::views::transform([&](auto &builder) { return builder->make_instance(output_); }),
                          std::back_inserter(outputs));
        output->set_ts_values(outputs);
        return output_;
    }

    time_series_output_ptr TimeSeriesSetOutputBuilder_object::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesSetOutput_Object{owning_node}};
        return v;
    }

    time_series_output_ptr TimeSeriesSetOutputBuilder_object::make_instance(time_series_output_ptr owning_output) const {
        auto v{new TimeSeriesSetOutput_Object{dynamic_cast_ref<TimeSeriesType>(owning_output)}};
        return v;
    }

}  // namespace hgraph

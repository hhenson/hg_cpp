#include <fmt/format.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>

#include <hgraph/types/ref.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/ts_signal.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsd.h>
#include <hgraph/types/tsl.h>
#include <hgraph/types/tss.h>
#include <hgraph/types/tsw.h>

#include <ranges>
#include <utility>

namespace hgraph
{

    void InputBuilder::release_instance(time_series_input_ptr item) const {
        // We can't detect if we are escaping from an error condition or not, so change these to just log issues
        if(item->has_output()) {
            fmt::print("Input instance still has an output reference when released, this is a bug.");
        }
        item->reset_parent_or_node();
    }

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

        nb::class_<TimeSeriesSignalInputBuilder, InputBuilder>(m, "InputBuilder_TS_Signal").def(nb::init<>());

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

        nb::class_<TimeSeriesSetInputBuilder, InputBuilder>(m, "InputBuilder_TSS");

        nb::class_<TimeSeriesSetInputBuilder_T<bool>, TimeSeriesSetInputBuilder>(m, "InputBuilder_TSS_Bool").def(nb::init<>());
        nb::class_<TimeSeriesSetInputBuilder_T<int64_t>, TimeSeriesSetInputBuilder>(m, "InputBuilder_TSS_Int").def(nb::init<>());
        nb::class_<TimeSeriesSetInputBuilder_T<double>, TimeSeriesSetInputBuilder>(m, "InputBuilder_TSS_Float").def(nb::init<>());
        nb::class_<TimeSeriesSetInputBuilder_T<engine_date_t>, TimeSeriesSetInputBuilder>(m, "InputBuilder_TSS_Date")
            .def(nb::init<>());
        nb::class_<TimeSeriesSetInputBuilder_T<engine_time_t>, TimeSeriesSetInputBuilder>(m, "InputBuilder_TSS_DateTime")
            .def(nb::init<>());
        nb::class_<TimeSeriesSetInputBuilder_T<engine_time_delta_t>, TimeSeriesSetInputBuilder>(m, "InputBuilder_TSS_TimeDelta")
            .def(nb::init<>());
        nb::class_<TimeSeriesSetInputBuilder_T<nb::object>, TimeSeriesSetInputBuilder>(m, "InputBuilder_TSS_Object")
            .def(nb::init<>());

        nb::class_<TimeSeriesDictInputBuilder, InputBuilder>(m, "InputBuilder_TSD")
            .def_ro("ts_builder", &TimeSeriesDictInputBuilder::ts_builder);

        nb::class_<TimeSeriesDictInputBuilder_T<bool>, TimeSeriesDictInputBuilder>(m, "InputBuilder_TSD_Bool")
            .def(nb::init<input_builder_ptr>(), "ts_builder"_a);
        nb::class_<TimeSeriesDictInputBuilder_T<int64_t>, TimeSeriesDictInputBuilder>(m, "InputBuilder_TSD_Int")
            .def(nb::init<input_builder_ptr>(), "ts_builder"_a);
        nb::class_<TimeSeriesDictInputBuilder_T<double>, TimeSeriesDictInputBuilder>(m, "InputBuilder_TSD_Float")
            .def(nb::init<input_builder_ptr>(), "ts_builder"_a);
        nb::class_<TimeSeriesDictInputBuilder_T<engine_date_t>, TimeSeriesDictInputBuilder>(m, "InputBuilder_TSD_Date")
            .def(nb::init<input_builder_ptr>(), "ts_builder"_a);
        nb::class_<TimeSeriesDictInputBuilder_T<engine_time_t>, TimeSeriesDictInputBuilder>(m, "InputBuilder_TSD_DateTime")
            .def(nb::init<input_builder_ptr>(), "ts_builder"_a);
        nb::class_<TimeSeriesDictInputBuilder_T<engine_time_delta_t>, TimeSeriesDictInputBuilder>(m, "InputBuilder_TSD_TimeDelta")
            .def(nb::init<input_builder_ptr>(), "ts_builder"_a);
        nb::class_<TimeSeriesDictInputBuilder_T<nb::object>, TimeSeriesDictInputBuilder>(m, "InputBuilder_TSD_Object")
            .def(nb::init<input_builder_ptr>(), "ts_builder"_a);

        // TSW input builders (fixed-size windows)
        using InputBuilder_TSW_Bool      = TimeSeriesWindowInputBuilder_T<bool>;
        using InputBuilder_TSW_Int       = TimeSeriesWindowInputBuilder_T<int64_t>;
        using InputBuilder_TSW_Float     = TimeSeriesWindowInputBuilder_T<double>;
        using InputBuilder_TSW_Date      = TimeSeriesWindowInputBuilder_T<engine_date_t>;
        using InputBuilder_TSW_DateTime  = TimeSeriesWindowInputBuilder_T<engine_time_t>;
        using InputBuilder_TSW_TimeDelta = TimeSeriesWindowInputBuilder_T<engine_time_delta_t>;
        using InputBuilder_TSW_Object    = TimeSeriesWindowInputBuilder_T<nb::object>;

        nb::class_<InputBuilder_TSW_Bool, InputBuilder>(m, "InputBuilder_TSW_Bool")
            .def(nb::init<size_t, size_t>(), "size"_a, "min_size"_a);
        nb::class_<InputBuilder_TSW_Int, InputBuilder>(m, "InputBuilder_TSW_Int")
            .def(nb::init<size_t, size_t>(), "size"_a, "min_size"_a);
        nb::class_<InputBuilder_TSW_Float, InputBuilder>(m, "InputBuilder_TSW_Float")
            .def(nb::init<size_t, size_t>(), "size"_a, "min_size"_a);
        nb::class_<InputBuilder_TSW_Date, InputBuilder>(m, "InputBuilder_TSW_Date")
            .def(nb::init<size_t, size_t>(), "size"_a, "min_size"_a);
        nb::class_<InputBuilder_TSW_DateTime, InputBuilder>(m, "InputBuilder_TSW_DateTime")
            .def(nb::init<size_t, size_t>(), "size"_a, "min_size"_a);
        nb::class_<InputBuilder_TSW_TimeDelta, InputBuilder>(m, "InputBuilder_TSW_TimeDelta")
            .def(nb::init<size_t, size_t>(), "size"_a, "min_size"_a);
        nb::class_<InputBuilder_TSW_Object, InputBuilder>(m, "InputBuilder_TSW_Object")
            .def(nb::init<size_t, size_t>(), "size"_a, "min_size"_a);
    }

    time_series_input_ptr TimeSeriesSignalInputBuilder::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesSignalInput(owning_node)};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    time_series_input_ptr TimeSeriesSignalInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesSignalInput(dynamic_cast_ref<TimeSeriesType>(owning_input))};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    void TimeSeriesSignalInputBuilder::release_instance(time_series_input_ptr item) const {
        release_instance(dynamic_cast<TimeSeriesSignalInput *>(item.get()));
    }

    void TimeSeriesSignalInputBuilder::release_instance(TimeSeriesSignalInput *signal_input) const {
        if (signal_input == nullptr) { return; }
        InputBuilder::release_instance(signal_input);
        if (signal_input->_ts_values.empty()) { return; }
        for (auto &ts_value : signal_input->_ts_values) { release_instance(ts_value.get()); }
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
    bool TimeSeriesListInputBuilder::has_reference() const { return input_builder->has_reference(); }
    bool TimeSeriesListInputBuilder::is_same_type(const Builder &other) const {
        if (auto other_b = dynamic_cast<const TimeSeriesListInputBuilder *>(&other)) {
            if (size != other_b->size) { return false; }
            return input_builder->is_same_type(*other_b->input_builder);
        }
        return false;
    }

    void TimeSeriesListInputBuilder::release_instance(time_series_input_ptr item) const {
        InputBuilder::release_instance(item);
        auto list = dynamic_cast<TimeSeriesListInput *>(item.get());
        if (list == nullptr) { return; }
        for (auto &value : list->_ts_values) { input_builder->release_instance(value); }
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
    bool TimeSeriesBundleInputBuilder::has_reference() const {
        return std::ranges::any_of(input_builders, [](const auto &builder) { return builder->has_reference(); });
    }
    bool TimeSeriesBundleInputBuilder::is_same_type(const Builder &other) const {
        if (auto other_b = dynamic_cast<const TimeSeriesBundleInputBuilder *>(&other)) {
            if (input_builders.size() != other_b->input_builders.size()) { return false; }
            for (size_t i = 0; i < input_builders.size(); ++i) {
                if (!input_builders[i]->is_same_type(*other_b->input_builders[i])) { return false; }
            }
            return true;
        }
        return false;
    }
    
    void TimeSeriesBundleInputBuilder::release_instance(time_series_input_ptr item) const {
        InputBuilder::release_instance(item);
        auto bundle = dynamic_cast<TimeSeriesBundleInput *>(item.get());
        if (bundle == nullptr) { return; }
        for (size_t i = 0; i < input_builders.size(); ++i) { input_builders[i]->release_instance(bundle->_ts_values[i]); }
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

    TimeSeriesDictInputBuilder::TimeSeriesDictInputBuilder(input_builder_ptr ts_builder)
        : InputBuilder(), ts_builder{std::move(ts_builder)} {}


    template <typename T> time_series_input_ptr TimeSeriesValueInputBuilder<T>::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesValueInput<T>(owning_node)};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    template <typename T>
    time_series_input_ptr TimeSeriesValueInputBuilder<T>::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesValueInput<T>(dynamic_cast_ref<TimeSeriesType>(owning_input))};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    template <typename T> time_series_input_ptr TimeSeriesSetInputBuilder_T<T>::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesSetInput_T<T>{owning_node}};
        return v;
    }

    template <typename T>
    time_series_input_ptr TimeSeriesSetInputBuilder_T<T>::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesSetInput_T<T>{dynamic_cast_ref<TimeSeriesType>(owning_input)}};
        return v;
    }

    template <typename T> time_series_input_ptr TimeSeriesDictInputBuilder_T<T>::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesDictInput_T<T>(owning_node, ts_builder)};
        return v;
    }

    template <typename T>
    time_series_input_ptr TimeSeriesDictInputBuilder_T<T>::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesDictInput_T<T>{dynamic_cast_ref<TimeSeriesType>(owning_input), ts_builder}};
        return v;
    }
    template <typename T> bool TimeSeriesDictInputBuilder_T<T>::is_same_type(const Builder &other) const {
        if (auto other_b = dynamic_cast<const TimeSeriesDictInputBuilder_T<T> *>(&other)) {
            return ts_builder->is_same_type(*other_b->ts_builder);
        }
        return false;
    }

    template <typename T> void TimeSeriesDictInputBuilder_T<T>::release_instance(time_series_input_ptr item) const {
        InputBuilder::release_instance(item);
        auto dict = dynamic_cast<TimeSeriesDictInput_T<T> *>(item.get());
        if (dict == nullptr) { return; }
        for (auto &value : dict->_ts_values) { ts_builder->release_instance(value.second); }
    }

    // TSW input builder implementations
    template <typename T> time_series_input_ptr TimeSeriesWindowInputBuilder_T<T>::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesWindowInput<T>(owning_node)};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }

    template <typename T>
    time_series_input_ptr TimeSeriesWindowInputBuilder_T<T>::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesWindowInput<T>(dynamic_cast_ref<TimeSeriesType>(owning_input))};
        return time_series_input_ptr{static_cast<TimeSeriesInput *>(v)};
    }
}  // namespace hgraph
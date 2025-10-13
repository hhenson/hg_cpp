#include <hgraph/types/ts_indexed.h>
#include <hgraph/types/graph.h>

#include <algorithm>
#include <ranges>

namespace hgraph
{
    void IndexedTimeSeriesOutput::invalidate() {
        if (valid()) {
            for (auto &v : ts_values()) { v->invalidate(); }
        }
        mark_invalid();
    }

    void IndexedTimeSeriesOutput::copy_from_output(const TimeSeriesOutput &output) {
        if (auto *ndx_output = dynamic_cast<const IndexedTimeSeriesOutput *>(&output); ndx_output != nullptr) {
            if (ndx_output->size() == size()) {
                for (size_t i = 0; i < ts_values().size(); ++i) { ts_values()[i]->copy_from_output(*ndx_output->ts_values()[i]); }
            } else {
                // We could do a full check, but that would be too much to do each time, and in theory the wiring should ensure
                //  we don't do that, but there should be a quick sanity check.
                //  Simple validation at this level to ensure they are at least size compatible
                throw std::runtime_error(std::format("Incorrect shape provided to copy_from_output, expected {} items got {}",
                                                     size(), ndx_output->size()));
            }
        } else {
            throw std::invalid_argument(std::format("Expected IndexedTimeSeriesOutput, got {}", typeid(output).name()));
        }
    }

    void IndexedTimeSeriesOutput::copy_from_input(const TimeSeriesInput &input) {
        if (auto *ndx_inputs = dynamic_cast<const IndexedTimeSeriesInput *>(&input); ndx_inputs != nullptr) {
            if (ndx_inputs->size() == size()) {
                for (size_t i = 0; i < ts_values().size(); ++i) { ts_values()[i]->copy_from_input(ndx_inputs[i]); }
            } else {
                // Simple validation at this level to ensure they are at least size compatible
                throw std::runtime_error(std::format("Incorrect shape provided to copy_from_input, expected {} items got {}",
                                                     size(), ndx_inputs->size()));
            }
        } else {
            throw std::invalid_argument(std::format("Expected TimeSeriesBundleOutput, got {}", typeid(input).name()));
        }
    }

    void IndexedTimeSeriesOutput::clear() {
        for (auto &v : ts_values()) { v->clear(); }
    }

    void IndexedTimeSeriesOutput::register_with_nanobind(nb::module_ &m) {
        using IndexedTimeSeries_Output = IndexedTimeSeries<TimeSeriesOutput>;
        nb::class_<IndexedTimeSeries_Output, TimeSeriesOutput>(m, "IndexedTimeSeries_Output")
            .def(
                "__getitem__", [](const IndexedTimeSeries_Output &self, size_t idx) { return self[idx]; }, "index"_a)
            .def("values", static_cast<collection_type (IndexedTimeSeries_Output::*)() const>(&IndexedTimeSeries_Output::values))
            .def("valid_values",
                 static_cast<collection_type (IndexedTimeSeries_Output::*)() const>(&IndexedTimeSeries_Output::valid_values))
            .def("modified_values",
                 static_cast<collection_type (IndexedTimeSeries_Output::*)() const>(&IndexedTimeSeries_Output::modified_values))
            .def("__len__", &IndexedTimeSeries_Output::size)
            .def_prop_ro("empty", &IndexedTimeSeries_Output::empty);

        nb::class_<IndexedTimeSeriesOutput, IndexedTimeSeries_Output>(m, "IndexedTimeSeriesOutput")
            .def("copy_from_output", &IndexedTimeSeriesOutput::copy_from_output, "output"_a)
            .def("copy_from_input", &IndexedTimeSeriesOutput::copy_from_input, "input"_a);
    }

    bool IndexedTimeSeriesInput::modified() const {
        if (has_peer()) { return TimeSeriesInput::modified(); }
        return std::ranges::any_of(ts_values(), [](const time_series_input_ptr &ts) { return ts->modified(); });
    }

    bool IndexedTimeSeriesInput::valid() const {
        if (has_peer()) { return TimeSeriesInput::valid(); }
        return std::ranges::any_of(ts_values(), [](const time_series_input_ptr &ts) { return ts->valid(); });
    }

    engine_time_t IndexedTimeSeriesInput::last_modified_time() const {
        if (has_peer()) { return TimeSeriesInput::last_modified_time(); }
        if (ts_values().empty()) { return MIN_DT; }
        return std::ranges::max(ts_values() |
                                std::views::transform([](const time_series_input_ptr &ts) { return ts->last_modified_time(); }));
    }

    bool IndexedTimeSeriesInput::bound() const {
        return TimeSeriesInput::bound() ||
               std::ranges::any_of(ts_values(), [](const time_series_input_ptr &ts) { return ts->bound(); });
    }

    bool IndexedTimeSeriesInput::active() const {
        if (has_peer()) { return TimeSeriesInput::active(); }
        return std::ranges::any_of(ts_values(), [](const time_series_input_ptr &ts) { return ts->active(); });
    }

    void IndexedTimeSeriesInput::make_active() {
        if (has_peer()) {
            TimeSeriesInput::make_active();
        } else {
            for (auto &ts : ts_values()) { ts->make_active(); }
        }
    }

    void IndexedTimeSeriesInput::make_passive() {
        if (has_peer()) {
            TimeSeriesInput::make_passive();
        } else {
            for (auto &ts : ts_values()) { ts->make_passive(); }
        }
    }

    void IndexedTimeSeriesInput::set_subscribe_method(bool subscribe_input) {
        TimeSeriesInput::set_subscribe_method(subscribe_input);

        for (auto &ts : ts_values()) { ts->set_subscribe_method(subscribe_input); }
    }

    void IndexedTimeSeriesInput::register_with_nanobind(nb::module_ &m) {
        using IndexedTimeSeries_Input = IndexedTimeSeries<TimeSeriesInput>;

        nb::class_<IndexedTimeSeries_Input, TimeSeriesInput>(m, "IndexedTimeSeries_Input")
            .def(
                "__getitem__", [](const IndexedTimeSeries_Input &self, size_t index) { return self[index]; }, "index"_a)
            .def("values", static_cast<collection_type (IndexedTimeSeries_Input::*)() const>(&IndexedTimeSeries_Input::values))
            .def("valid_values",
                 static_cast<collection_type (IndexedTimeSeries_Input::*)() const>(&IndexedTimeSeries_Input::valid_values))
            .def("modified_values",
                 static_cast<collection_type (IndexedTimeSeries_Input::*)() const>(&IndexedTimeSeries_Input::modified_values))
            .def("__len__", &IndexedTimeSeries_Input::size)
            .def_prop_ro("empty", &IndexedTimeSeries_Input::empty);

        nb::class_<IndexedTimeSeriesInput, IndexedTimeSeries_Input>(m, "IndexedTimeSeriesInput");
    }

    bool IndexedTimeSeriesInput::do_bind_output(time_series_output_ptr value) {
        // Detect rebinding - if we already have an output, we're switching to a new one
        bool rebinding = has_output();

        auto output_bundle = dynamic_cast<IndexedTimeSeriesOutput *>(value.get());
        bool peer          = true;

        if (output_bundle) {
            for (size_t i = 0; i < ts_values().size(); ++i) { peer &= ts_values()[i]->bind_output((*output_bundle)[i]); }
        }

        TimeSeriesInput::do_bind_output(peer ? value : nullptr);

        // If rebinding occurred, notify parent so downstream nodes know to evaluate
        if (rebinding && peer) {
            notify(owning_graph().evaluation_clock().evaluation_time());
        }

        return peer;
    }

    void IndexedTimeSeriesInput::do_un_bind_output() {
        for (auto &ts : ts_values()) { ts->un_bind_output(); }
        if (has_peer()) { TimeSeriesInput::do_un_bind_output(); }
    }
}  // namespace hgraph

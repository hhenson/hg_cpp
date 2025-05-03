#include <hgraph/types/ts_indexed.h>
#include <hgraph/python/pyb_wiring.h>

namespace hgraph
{
    void IndexedTimeSeriesOutput::invalidate() {
        if (valid()) {
            for (auto &v : ts_values()) { v->invalidate(); }
        }
        mark_invalid();
    }

    void IndexedTimeSeriesOutput::copy_from_output(TimeSeriesOutput &output) {
        if (auto *ndx_output = dynamic_cast<IndexedTimeSeriesOutput *>(&output); ndx_output != nullptr) {
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

    void IndexedTimeSeriesOutput::copy_from_input(TimeSeriesInput &input) {
        if (auto *ndx_inputs = dynamic_cast<IndexedTimeSeriesInput *>(&input); ndx_inputs != nullptr) {
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
        nb::class_<IndexedTimeSeriesOutput, TimeSeriesOutput>(m, "IndexedTimeSeriesOutput")
            .def(
                "__getitem__", [](const IndexedTimeSeriesOutput &self, size_t idx) { return self[idx]; }, "index"_a)
            .def("values", static_cast<collection_type (IndexedTimeSeriesOutput::*)() const>(&IndexedTimeSeriesOutput::values))
            .def("valid_values",
                 static_cast<collection_type (IndexedTimeSeriesOutput::*)() const>(&IndexedTimeSeriesOutput::valid_values))
            .def("modified_values",
                 static_cast<collection_type (IndexedTimeSeriesOutput::*)() const>(&IndexedTimeSeriesOutput::modified_values))
            .def("__len__", &IndexedTimeSeriesOutput::size)
            .def("copy_from_output", &IndexedTimeSeriesOutput::copy_from_output, "output"_a)
            .def("copy_from_input", &IndexedTimeSeriesOutput::copy_from_input, "input"_a);
    }

    void IndexedTimeSeriesInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<IndexedTimeSeriesInput, TimeSeriesInput>(m, "IndexedTimeSeriesInput")
            .def(
                "__getitem__", [](const IndexedTimeSeriesInput &self, size_t index) { return self[index]; }, "index"_a)
            .def("values", static_cast<collection_type (IndexedTimeSeriesInput::*)() const>(&IndexedTimeSeriesInput::values))
            .def("valid_values",
                 static_cast<collection_type (IndexedTimeSeriesInput::*)() const>(&IndexedTimeSeriesInput::valid_values))
            .def("modified_values",
                 static_cast<collection_type (IndexedTimeSeriesInput::*)() const>(&IndexedTimeSeriesInput::modified_values))
            .def("__len__", &IndexedTimeSeriesInput::size);
    }
}  // namespace hgraph

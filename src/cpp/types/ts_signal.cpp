#include <hgraph/types/ts_signal.h>

namespace hgraph
{

    nb::object TimeSeriesSignalInput::py_value() const { return nb::cast(modified()); }

    nb::object TimeSeriesSignalInput::py_delta_value() const { return py_value(); }

    TimeSeriesInput::ptr TimeSeriesSignalInput::operator[](size_t index) {
        // This signal has been bound to a free bundle or a TSL so will be bound item-wise
        // Create child signals on demand, similar to Python implementation
        while (index >= _ts_values.size()) {
            auto new_item = ptr{new TimeSeriesSignalInput(TimeSeriesType::ptr{this})};
            _ts_values.push_back(new_item);
        }
        return TimeSeriesInput::ptr{_ts_values[index].get()};
    }

    void TimeSeriesSignalInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSignalInput, TimeSeriesInput>(m, "TS_Signal")
            .def("__getitem__", [](TimeSeriesSignalInput &self, size_t index) {
                return self[index];
            });
    }

}  // namespace hgraph
#include <hgraph/types/ts_signal.h>

namespace hgraph
{

    nb::object TimeSeriesSignalInput::py_value() const { return nb::cast(modified()); }

    nb::object TimeSeriesSignalInput::py_delta_value() const { return py_value(); }

    TimeSeriesInput::ptr TimeSeriesSignalInput::operator[](size_t index) {
        // This signal has been bound to a free bundle or a TSL so will be bound item-wise
        // Create child signals on demand, similar to Python implementation
        while (index >= _ts_values.size()) {
            // Create child with this as parent - child will notify parent, parent notifies node
            auto new_item = ptr{new TimeSeriesSignalInput(TimeSeriesType::ptr{this})};
            _ts_values.push_back(new_item);
        }
        return TimeSeriesInput::ptr{_ts_values[index].get()};
    }

    bool TimeSeriesSignalInput::valid() const {
        if (!_ts_values.empty()) {
            return std::ranges::any_of(_ts_values, [](const auto &item) { return item->valid(); });
        }
        return TimeSeriesInput::valid();
    }

    bool TimeSeriesSignalInput::modified() const {
        if (!_ts_values.empty()) {
            return std::ranges::any_of(_ts_values, [](const auto &item) { return item->modified(); });
        }
        return TimeSeriesInput::modified();
    }

    engine_time_t TimeSeriesSignalInput::last_modified_time() const {
        if (!_ts_values.empty()) {
            engine_time_t max_time = MIN_DT;
            for (const auto &item : _ts_values) {
                max_time = std::max(max_time, item->last_modified_time());
            }
            return max_time;
        }
        return TimeSeriesInput::last_modified_time();
    }

    void TimeSeriesSignalInput::make_active() {
        TimeSeriesInput::make_active();
        if (!_ts_values.empty()) {
            for (auto &item : _ts_values) {
                item->make_active();
            }
        }
    }

    void TimeSeriesSignalInput::make_passive() {
        TimeSeriesInput::make_passive();
        if (!_ts_values.empty()) {
            for (auto &item : _ts_values) {
                item->make_passive();
            }
        }
    }

    void TimeSeriesSignalInput::do_un_bind_output() {
        if (!_ts_values.empty()) {
            for (auto &item : _ts_values) {
                item->un_bind_output();
            }
        }
    }

    void TimeSeriesSignalInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSignalInput, TimeSeriesInput>(m, "TS_Signal")
            .def("__getitem__", [](TimeSeriesSignalInput &self, size_t index) {
                return self[index];
            });
    }

}  // namespace hgraph
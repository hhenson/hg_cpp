#include <hgraph/types/tsw.h>
#include <hgraph/types/graph.h>
#include <type_traits>

namespace hgraph
{

    // Template method definitions
    template <typename T> nb::object TimeSeriesFixedWindowOutput<T>::py_value() const {
        if (!valid() || _length < _min_size) return nb::none();

        // If the window is contiguous from the start (no rotation), we can expose a dynamic
        // ndarray view into the internal storage with proper ownership to avoid a copy.
        if (_start == 0) {
            size_t len = (_length < _size) ? _length : _size;
            if (len == 0) return nb::none();

            // Only expose zero-copy ndarray for POD-like types excluding nb::object and vector<bool>.
            if constexpr (std::is_arithmetic_v<T> && !std::is_same_v<T, bool>) {
                using ND = nb::ndarray<nb::numpy, T, nb::ndim<1>>;
                const T* data_ptr = _buffer.data();
                // Use the bound C++ instance as the owner to ensure lifetime safety.
                nb::object owner = nb::cast(const_cast<TimeSeriesFixedWindowOutput<T>*>(this));
                ND arr((void*) data_ptr, { len }, owner);
                return nb::cast(arr);
            }
            // For nb::object and bool, fall back to a copy-based Python sequence.
            std::vector<T> out(_buffer.begin(), _buffer.begin() + len);
            return nb::cast(out);
        }

        // General path: build ordered view (copy semantics for rotation)
        std::vector<T> out;
        if (_length < _size) {
            out.assign(_buffer.begin(), _buffer.begin() + _length);
        } else {
            out.resize(_size);
            for (size_t i = 0; i < _size; ++i) out[i] = _buffer[(i + _start) % _size];
        }
        return nb::cast(out);
    }

    template <typename T> nb::object TimeSeriesFixedWindowOutput<T>::py_delta_value() const {
        if (_length == 0) return nb::none();
        size_t pos = (_length < _size) ? (_length - 1) : ((_start + _length - 1) % _size);
        if (_times[pos] == owning_graph()->evaluation_clock()->evaluation_time()) {
            if constexpr (std::is_same_v<T, bool>) {
                bool v = static_cast<bool>(_buffer[pos]);
                return nb::cast(v);
            } else {
                return nb::cast(_buffer[pos]);
            }
        } else {
            return nb::none();
        }
    }

    template <typename T> void TimeSeriesFixedWindowOutput<T>::py_set_value(nb::object value) {
        if (value.is_none()) { invalidate(); return; }
        try {
            T v = nb::cast<T>(value);
            size_t capacity = _size;
            size_t start    = _start;
            size_t length   = _length + 1;
            if (length > capacity) {
                _removed_value.reset();
                _removed_value = _buffer[start];
                owning_graph()->evaluation_engine_api()->add_after_evaluation_notification([this]() { _removed_value.reset(); });
                start  = (start + 1) % capacity;
                _start = start;
                length = capacity;
            }
            _length       = length;
            size_t pos    = (start + length - 1) % capacity;
            _buffer[pos]  = v;
            _times[pos]   = owning_graph()->evaluation_clock()->evaluation_time();
            mark_modified();
        } catch (const std::exception &e) {
            throw std::runtime_error(std::string("Cannot apply node output: ") + e.what());
        }
    }

    template <typename T> void TimeSeriesFixedWindowOutput<T>::apply_result(nb::object value) {
        if (!value.is_valid() || value.is_none()) return;
        py_set_value(value);
    }

    template <typename T> void TimeSeriesFixedWindowOutput<T>::mark_invalid() {
        _start = 0;
        _length = 0;
        TimeSeriesOutput::mark_invalid();
    }

    template <typename T> nb::object TimeSeriesFixedWindowOutput<T>::py_value_times() const {
        if (!valid() || _length < _min_size) return nb::none();

        // Mirror value() semantics: if contiguous from start, return the active portion.
        if (_start == 0) {
            size_t len = (_length < _size) ? _length : _size;
            if (len == 0) return nb::none();
            std::vector<engine_time_t> out(_times.begin(), _times.begin() + len);
            return nb::cast(out);
        }

        // General path: build ordered times (copy semantics for rotation/partial)
        std::vector<engine_time_t> out;
        if (_length < _size) {
            out.assign(_times.begin(), _times.begin() + _length);
        } else {
            out.resize(_size);
            for (size_t i = 0; i < _size; ++i) out[i] = _times[(i + _start) % _size];
        }
        return nb::cast(out);
    }

    template <typename T> engine_time_t TimeSeriesFixedWindowOutput<T>::first_modified_time() const {
        return _times.empty() ? engine_time_t{} : _times[_start];
    }

    template <typename T> static void bind_tsw_for_type(nb::module_ &m, const char *suffix) {
        using Out = TimeSeriesFixedWindowOutput<T>;
        using In  = TimeSeriesWindowInput<T>;

        auto out_cls = nb::class_<Out, TimeSeriesOutput>(m, (std::string("TimeSeriesFixedWindowOutput_") + suffix).c_str())
                           .def_prop_ro("value_times", &Out::py_value_times)
                           .def_prop_ro("first_modified_time", &Out::first_modified_time)
                           .def_prop_ro("size", &Out::size)
                           .def_prop_ro("min_size", &Out::min_size)
                           .def_prop_ro("has_removed_value", &Out::has_removed_value)
                           .def_prop_ro("removed_value", [](const Out &o) { return o.has_removed_value() ? nb::cast(o.removed_value()) : nb::none(); })
                           .def("__len__", &Out::len);

        auto in_cls = nb::class_<In, TimeSeriesInput>(m, (std::string("TimeSeriesWindowInput_") + suffix).c_str())
                          .def_prop_ro("value_times", &In::py_value_times)
                          .def_prop_ro("first_modified_time", &In::first_modified_time)
                          .def_prop_ro("has_removed_value", &In::has_removed_value)
                          .def_prop_ro("removed_value", &In::removed_value)
                          .def("__len__", [](const In &self) { return self.output_t().len(); });

        (void)out_cls;
        (void)in_cls;
    }

    void tsw_register_with_nanobind(nb::module_ &m) {
        bind_tsw_for_type<bool>(m, "bool");
        bind_tsw_for_type<int64_t>(m, "int");
        bind_tsw_for_type<double>(m, "float");
        bind_tsw_for_type<engine_date_t>(m, "date");
        bind_tsw_for_type<engine_time_t>(m, "date_time");
        bind_tsw_for_type<engine_time_delta_t>(m, "time_delta");
        bind_tsw_for_type<nb::object>(m, "object");
    }

}  // namespace hgraph

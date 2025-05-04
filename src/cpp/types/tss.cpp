#include <hgraph/types/tss.h>

namespace hgraph
{

    void SetDelta::register_with_nanobind(nb::module_ &m) {
        nb::class_<SetDelta>(m, "SetDelta")
            .def_prop_ro("added_elements", &SetDelta::py_added_elements)
            .def_prop_ro("removed_elements", &SetDelta::py_removed_elements);

        using SetDelta_bool = SetDeltaImpl<bool>;
        nb::class_<SetDelta_bool, SetDelta>(m, "SetDelta_bool")
            .def(nb::init<const std::unordered_set<bool> &, const std::unordered_set<bool> &>(), "added_elements"_a,
                 "removed_elements"_a);
        using SetDelta_int = SetDeltaImpl<int64_t>;
        nb::class_<SetDelta_int, SetDelta>(m, "SetDelta_int")
            .def(nb::init<const std::unordered_set<int64_t> &, const std::unordered_set<int64_t> &>(), "added_elements"_a,
                 "removed_elements"_a);
        using SetDelta_float = SetDeltaImpl<float>;
        nb::class_<SetDelta_float, SetDelta>(m, "SetDelta_float")
            .def(nb::init<const std::unordered_set<float> &, const std::unordered_set<float> &>(), "added_elements"_a,
                 "removed_elements"_a);
        using SetDelta_date = SetDeltaImpl<engine_date_t>;
        nb::class_<SetDelta_date, SetDelta>(m, "SetDelta_date")
            .def(nb::init<const std::unordered_set<engine_date_t> &, const std::unordered_set<engine_date_t> &>(),
                 "added_elements"_a, "removed_elements"_a);
        using SetDelta_date_time = SetDeltaImpl<engine_time_t>;
        nb::class_<SetDelta_date_time, SetDelta>(m, "SetDelta_date_time")
            .def(nb::init<const std::unordered_set<engine_time_t> &, const std::unordered_set<engine_time_t> &>(),
                 "added_elements"_a, "removed_elements"_a);
        using SetDelta_time_delta = SetDeltaImpl<engine_time_delta_t>;
        nb::class_<SetDelta_time_delta, SetDelta>(m, "SetDelta_time_delta")
            .def(nb::init<const std::unordered_set<engine_time_delta_t> &, const std::unordered_set<engine_time_delta_t> &>(),
                 "added_elements"_a, "removed_elements"_a);

        nb::class_<SetDelta_Object, SetDelta>(m, "SetDelta_object")
            .def(nb::init<const nb::object &, const nb::object &>(), "added_elements"_a, "removed_elements"_a);
    }

    SetDelta_Object::SetDelta_Object(nb::object added_elements, nb::object removed_elements)
        : _added_elements(std::move(added_elements)), _removed_elements(std::move(removed_elements)) {}

    nb::object SetDelta_Object::py_removed_elements() const { return _removed_elements; }

    TimeSeriesSetOutput &TimeSeriesSetInput::set_output() const { return dynamic_cast<TimeSeriesSetOutput &>(*output()); }

    nb::object TimeSeriesSetOutput_Object::py_value() const { return _value; }

    nb::object TimeSeriesSetOutput_Object::py_delta_value() const {
        if (modified()) {
            return nb::cast(SetDelta_Object(_added, _removed));
        } else {
            return nb::cast(SetDelta_Object(nb::set(), nb::set()));
        }
    }

    void TimeSeriesSetOutput_Object::apply_result(nb::handle value) {
        try {
            if (nb::isinstance<SetDelta>(value)) {
                auto delta{nb::cast<SetDelta *>(value)};
                auto added_elements   = delta->py_added_elements();
                auto removed_elements = delta->py_removed_elements();

                if (added_elements.is_valid() && !added_elements.is_none()) {
                    for (const auto &e : nb::cast<nb::set>(added_elements)) {
                        if (!_value.contains(e)) {
                            _added.add(e);
                            _value.add(e);
                        }
                    }
                }

                for (const auto &e : removed_elements) {
                    if (_added.contains(e)) { throw std::runtime_error("Cannot remove and add the same element"); }
                    if (_value.contains(e)) {
                        _removed.add(e);
                        _value.discard(e);
                    }
                }
            } else {
                auto removed{nb::module_::import_("hgraph").attr("Removed")};
                _added   = nb::set();
                _removed = nb::set();

                auto v = nb::set(value);
                for (const auto &r : v) {
                    if (!nb::isinstance(r, removed)) {
                        if (_value.contains(r)) {
                            _added.add(r);
                            _value.add(r);
                        }
                    } else {
                        auto item{r.attr("item")};
                        if (_value.contains(item)) {
                            if (_added.contains(item)) { throw std::runtime_error("Cannot remove and add the same element"); }
                            _removed.add(item);
                            _value.discard(item);
                        }
                    }
                }
            }
        } catch (const std::exception &e) { throw std::runtime_error(std::string("Error in apply_result: ") + e.what()); }

        if (_added.size() > 0 || _removed.size() > 0 || !valid()) { mark_modified(); }
    }

    void TimeSeriesSetOutput_Object::invalidate() {
        clear();
        _reset_last_modified_time();
    }

    void TimeSeriesSetOutput_Object::copy_from_output(TimeSeriesOutput &output) {
        auto &output_obj = dynamic_cast<TimeSeriesSetOutput_Object &>(output);

        _added   = nb::set();
        _removed = nb::set();

        // Calculate added elements (elements in output but not in current value)
        for (const auto &item : output_obj._value) {
            if (!_value.contains(item)) { _added.add(item); }
        }

        // Calculate removed elements (elements in current value but not in output)
        for (const auto &item : _value) {
            if (!output_obj._value.contains(item)) { _removed.add(item); }
        }

        if (_added.size() > 0 || _removed.size() > 0) {
            _value = nb::set(output_obj._value);
            mark_modified();
        }
    }
    void TimeSeriesSetOutput_Object::copy_from_input(TimeSeriesInput &input) {
        auto &input_obj = dynamic_cast<TimeSeriesSetInput &>(input);

        _added   = nb::set();
        _removed = nb::set();

        // Calculate added elements (elements in input but not in current value)
        for (const auto &item : input_obj.py_values()) {
            if (!_value.contains(item)) { _added.add(item); }
        }

        // Calculate removed elements (elements in current value but not in input)
        auto _v{nb::cast<nb::set>(input_obj.py_values())};
        for (const auto &item : _value) {
            if (!_v.contains(item)) { _removed.add(item); }
        }

        if (_added.size() > 0 || _removed.size() > 0) {
            _value = nb::set(input_obj.py_values());
            mark_modified();
        }
    }

    bool TimeSeriesSetOutput_Object::py_contains(const nb::object &item) const { return _value.contains(item); }

    size_t TimeSeriesSetOutput_Object::size() const { return _value.size(); }

    const nb::object &TimeSeriesSetOutput_Object::py_values() const { return _value; }

    const nb::object &TimeSeriesSetOutput_Object::py_added() const { return _added; }

    bool TimeSeriesSetOutput_Object::py_was_added(const nb::object &item) const { return _added.contains(item); }

    const nb::object &TimeSeriesSetOutput_Object::py_removed() const { return _removed; }

    bool TimeSeriesSetOutput_Object::py_was_removed(const nb::object &item) const { return _removed.contains(item); }

    nb::object TimeSeriesSetInput::py_value() const { return output()->py_value(); }

    nb::object TimeSeriesSetInput::py_delta_value() const { return output()->py_delta_value(); }

    bool TimeSeriesSetInput::py_contains(const nb::object &item) const { return set_output().py_contains(item); }

    size_t TimeSeriesSetInput::size() const { return set_output().size(); }

    const nb::object &TimeSeriesSetInput::py_values() const { return set_output().py_values(); }

    const nb::object &TimeSeriesSetInput::py_added() const { return set_output().py_added(); }

    bool TimeSeriesSetInput::py_was_added(const nb::object &item) const { return set_output().py_was_added(item); }

    const nb::object &TimeSeriesSetInput::py_removed() const { return set_output().py_removed(); }

    bool TimeSeriesSetInput::py_was_removed(const nb::object &item) const { return set_output().py_was_removed(item); }

    nb::object SetDelta_Object::py_added_elements() const { return _added_elements; }

    void tss_register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSetInput, TimeSeriesInput>(m, "TimeSeriesSetInput")
            .def("__contains__", &TimeSeriesSetInput::py_contains)
            .def("__len__", &TimeSeriesSetInput::size)
            .def("values", &TimeSeriesSetInput::py_values)
            .def("added", &TimeSeriesSetInput::py_added)
            .def("removed", &TimeSeriesSetInput::py_removed)
            .def("was_added", &TimeSeriesSetInput::py_was_added)
            .def("was_removed", &TimeSeriesSetInput::py_was_removed);

        nb::class_<TimeSeriesSetOutput, TimeSeriesOutput>(m, "TimeSeriesSetOutput")
            .def("__contains__", &TimeSeriesSetOutput::py_contains)
            .def("__len__", &TimeSeriesSetOutput::size)
            .def("values", &TimeSeriesSetOutput::py_values)
            .def("added", &TimeSeriesSetOutput::py_added)
            .def("removed", &TimeSeriesSetOutput::py_removed)
            .def("was_added", &TimeSeriesSetOutput::py_was_added)
            .def("was_removed", &TimeSeriesSetOutput::py_was_removed);
    }
}  // namespace hgraph

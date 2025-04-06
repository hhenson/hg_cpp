#include <hgraph/python/pyb_wiring.h>
#include <hgraph/types/tsb.h>
#include <nanobind/make_iterator.h>
#include <numeric>

namespace hgraph
{
    TimeSeriesSchema::TimeSeriesSchema(std::vector<std::string> keys, const nb::type_object &type)
        : _keys{std::move(keys)}, _scalar_type{type} {}

    const std::vector<std::string> &TimeSeriesSchema::keys() const { return _keys; }

    const nb::type_object &TimeSeriesSchema::scalar_type() const { return _scalar_type; }

    void TimeSeriesSchema::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSchema, nb::intrusive_base>(m, "TimeSeriesSchema")
            .def(nb::init<std::vector<std::string>, const nb::type_object &>(), "keys"_a, "scalar_type"_a = nb::none())
            .def_prop_ro("keys", &TimeSeriesSchema::keys)
            .def_prop_ro("scalar_type", &TimeSeriesSchema::scalar_type);
    }

    TimeSeriesBundleOutput::TimeSeriesBundleOutput(const node_ptr &parent, const TimeSeriesSchema::ptr &schema)
        : TimeSeriesOutput(parent), _schema{schema} {}

    TimeSeriesBundleOutput::TimeSeriesBundleOutput(const TimeSeriesType::ptr &parent, const TimeSeriesSchema::ptr &schema)
        : TimeSeriesOutput(parent), _schema{schema} {}

    nb::object TimeSeriesBundleOutput::py_value() const {
        auto        out{nb::dict()};
        const auto &keys{_schema->keys()};
        for (size_t i = 0; i < keys.size(); ++i) {
            auto &ts{*_ts_values[i]};
            if (ts.valid()) { out[keys[i].c_str()] = ts.py_value(); }
        }
        if (_schema->scalar_type().is_none()) { return out; }
        return nb::cast<nb::object>(_schema->scalar_type()(**out));
    }

    nb::object TimeSeriesBundleOutput::py_delta_value() const {
        auto        out{nb::dict()};
        const auto &keys{_schema->keys()};
        for (size_t i = 0; i < keys.size(); ++i) {
            auto &ts{*_ts_values[i]};
            if (ts.modified() && ts.valid()) { out[keys[i].c_str()] = ts.py_value(); }
        }
        return out;
    }

    void TimeSeriesBundleOutput::apply_result(nb::handle value) {
        if (value.is_none()) { return; }
        if (value.is(_schema->scalar_type())) {
            for (const auto &key : _schema->keys()) {
                auto v = nb::getattr(value, key.c_str(), nb::none());
                if (!v.is_none()) { (*this)[key].apply_result(v); }
            }
        } else {
            for (auto [key, val] : nb::cast<nb::dict>(value)) {
                if (!val.is_none()) { (*this)[nb::cast<std::string>(key)].apply_result(val); }
            }
        }
    }

    TimeSeriesBundleOutput::iterator TimeSeriesBundleOutput::begin() { return _ts_values.begin(); }

    TimeSeriesBundleOutput::const_iterator TimeSeriesBundleOutput::begin() const { return _ts_values.begin(); }

    TimeSeriesBundleOutput::iterator TimeSeriesBundleOutput::end() { return _ts_values.end(); }

    TimeSeriesBundleOutput::const_iterator TimeSeriesBundleOutput::end() const { return _ts_values.end(); }

    TimeSeriesOutput &TimeSeriesBundleOutput::operator[](const std::string &key) {
        // Return the value of the ts_bundle for the schema key instance.
        auto it{std::ranges::find(_schema->keys(), key)};
        if (it != _schema->keys().end()) {
            size_t index = std::distance(_schema->keys().begin(), it);
            return *_ts_values[index];
        }
        throw std::out_of_range("Key not found in TimeSeriesSchema");
    }

    const TimeSeriesOutput &TimeSeriesBundleOutput::operator[](const std::string &key) const {
        return const_cast<TimeSeriesBundleOutput *>(this)->operator[](key);
    }

    TimeSeriesOutput &TimeSeriesBundleOutput::operator[](std::size_t ndx) { return *_ts_values.at(ndx); }

    const TimeSeriesOutput &TimeSeriesBundleOutput::operator[](std::size_t ndx) const {
        return const_cast<TimeSeriesBundleOutput *>(this)->operator[](ndx);
    }

    bool TimeSeriesBundleOutput::all_valid() const {
        return valid() && std::ranges::all_of(_ts_values, [](const auto &ts) { return ts->valid(); });
    }

    void TimeSeriesBundleOutput::invalidate() {
        if (valid()) {
            for (auto &v : _ts_values) { v->invalidate(); }
        }
        mark_invalid();
    }

    void TimeSeriesBundleOutput::copy_from_output(TimeSeriesOutput &output) {
        if (auto *bundle_output = dynamic_cast<TimeSeriesBundleOutput *>(&output); bundle_output == nullptr) {
            throw std::invalid_argument(std::format("Expected TimeSeriesBundleOutput, got {}", typeid(output).name()));
        } else {
            if (bundle_output->schema().keys().size() != schema().keys().size()) {
                // We could do a full check, but that should be over-kill each time and in theory the wiring should ensure
                //  we don't do that, but this should be a quick sanity check.
                //  Simple validation at this level to ensure they are at least size compatible
                throw std::runtime_error(std::format("Invalid number of inputs provided to TSD, expected {} got {}",
                                                     schema().keys().size(), bundle_output->schema().keys().size()));
            }
            for (size_t i = 0; i < _ts_values.size(); ++i) { _ts_values[i]->copy_from_output(*bundle_output->_ts_values[i]); }
        }
    }

    void TimeSeriesBundleOutput::copy_from_input(TimeSeriesInput &input) {
        if (auto *bundle_input = dynamic_cast<TimeSeriesBundleInput *>(&input); bundle_input == nullptr) {
            throw std::invalid_argument(std::format("Expected TimeSeriesBundleOutput, got {}", typeid(input).name()));
        } else {
            if (bundle_input->schema().keys().size() != schema().keys().size()) {
                // Simple validation at this level to ensure they are at least size compatible
                throw std::runtime_error(std::format("Invalid number of inputs provided to TSD, expected {} got {}",
                                                     _schema->keys().size(), bundle_input->schema().keys().size()));
            }
            for (size_t i = 0; i < _ts_values.size(); ++i) { _ts_values[i]->copy_from_input(bundle_input[i]); }
        }
    }

    const TimeSeriesSchema &TimeSeriesBundleOutput::schema() const { return *_schema; }

    bool TimeSeriesBundleOutput::contains(const std::string &key) const {
        return std::ranges::find(schema().keys(), key) != schema().keys().end();
    }

    void TimeSeriesBundleOutput::clear() {
        for (auto &v : _ts_values) { v->clear(); }
    }

    std::vector<c_string_ref> TimeSeriesBundleOutput::keys() const { return {schema().keys().begin(), schema().keys().end()}; }

    void TimeSeriesBundleOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesBundleOutput, TimeSeriesOutput>(m, "TimeSeriesBundleOutput")
            .def(nb::init<const node_ptr &, TimeSeriesSchema::ptr>(), "owning_node"_a, "schema"_a)
            .def(nb::init<const TimeSeriesType::ptr &, TimeSeriesSchema::ptr>(), "parent_input"_a, "schema"_a)
            .def("__getitem__",
                 [](TimeSeriesBundleOutput &self, const std::string &key) -> TimeSeriesOutput & {
                     return self[key];  // Use operator[] overload with string
                 })
            .def("__getitem__",
                 [](TimeSeriesBundleOutput &self, std::size_t ndx) -> TimeSeriesOutput & {
                     return self[ndx];  // Use operator[] overload with index
                 })
            .def("__contains__", &TimeSeriesBundleOutput::contains)
            .def(
                "__iter__",
                [](const TimeSeriesBundleOutput &self) {
                    return nb::make_iterator(nb::type<TimeSeriesBundleOutput>(), "iterator", self.begin(), self.end());
                },
                nb::keep_alive<0, 1>())
            .def("keys", &TimeSeriesBundleOutput::keys)
            .def("values", &TimeSeriesBundleOutput::values)
            .def("items", &TimeSeriesBundleOutput::items)
            .def("valid_keys", &TimeSeriesBundleOutput::valid_keys)
            .def("valid_values", &TimeSeriesBundleOutput::valid_values)
            .def("valid_items", &TimeSeriesBundleOutput::valid_items)
            .def("modified_keys", &TimeSeriesBundleOutput::modified_keys)
            .def("modified_values", &TimeSeriesBundleOutput::modified_values)
            .def("modified_items", &TimeSeriesBundleOutput::modified_items);
    }

    void TimeSeriesBundleOutput::set_outputs(std::vector<time_series_output_ptr> ts_values) {
        if (ts_values.size() != _schema->keys().size()) {
            throw std::runtime_error(std::format("Invalid number of inputs provided to TSD, expected {} got {}",
                                                 _schema->keys().size(), ts_values.size()));
        }
        _ts_values = std::move(ts_values);
    }

    std::vector<c_string_ref>
    TimeSeriesBundleOutput::keys_with_constraint(const std::function<bool(const TimeSeriesOutput &)> &constraint) const {
        std::vector<c_string_ref> result;
        result.reserve(_ts_values.size());
        for (size_t i = 0, l = _ts_values.size(); i < l; i++) {
            auto &ts{_ts_values[i]};
            if (constraint(*ts)) { result.emplace_back(schema().keys()[i]); }
        }
        return result;
    }

    std::vector<time_series_output_ptr>
    TimeSeriesBundleOutput::values_with_constraint(const std::function<bool(const TimeSeriesOutput &)> &constraint) const {
        std::vector<time_series_output_ptr> result;
        result.reserve(_ts_values.size());
        for (size_t i = 0, l = _ts_values.size(); i < l; i++) {
            auto &ts{_ts_values[i]};
            if (constraint(*ts)) { result.emplace_back(ts); }
        }
        return result;
    }

    std::vector<std::pair<c_string_ref, time_series_output_ptr>>
    TimeSeriesBundleOutput::items_with_constraint(const std::function<bool(const TimeSeriesOutput &)> &constraint) const {
        std::vector<std::pair<c_string_ref, time_series_output_ptr>> result;
        result.reserve(_ts_values.size());
        for (size_t i = 0, l = _ts_values.size(); i < l; i++) {
            auto &ts{_ts_values[i]};
            if (constraint(*ts)) { result.emplace_back(schema().keys()[i], ts); }
        }
        return result;
    }

    // Retrieves valid keys
    std::vector<c_string_ref> TimeSeriesBundleOutput::valid_keys() const {
        return keys_with_constraint([](const TimeSeriesOutput &ts) -> bool { return ts.valid(); });
    }

    std::vector<c_string_ref> TimeSeriesBundleOutput::modified_keys() const {
        return keys_with_constraint([](const TimeSeriesOutput &ts) -> bool { return ts.modified(); });
    }

    std::vector<time_series_output_ptr> TimeSeriesBundleOutput::values() const { return {begin(), end()}; }

    // Retrieves valid values
    std::vector<time_series_output_ptr> TimeSeriesBundleOutput::valid_values() const {
        return values_with_constraint([](const TimeSeriesOutput &ts) -> bool { return ts.valid(); });
    }

    std::vector<time_series_output_ptr> TimeSeriesBundleOutput::modified_values() const {
        return values_with_constraint([](const TimeSeriesOutput &ts) -> bool { return ts.modified(); });
    }

    std::vector<std::pair<c_string_ref, time_series_output_ptr>> TimeSeriesBundleOutput::items() const {
        return items_with_constraint([](const TimeSeriesOutput &ts) -> bool { return true; });
    }

    // Retrieves valid items
    std::vector<std::pair<c_string_ref, time_series_output_ptr>> TimeSeriesBundleOutput::valid_items() const {
        return items_with_constraint([](const TimeSeriesOutput &ts) -> bool { return ts.valid(); });
    }

    std::vector<std::pair<c_string_ref, time_series_output_ptr>> TimeSeriesBundleOutput::modified_items() const {
        return items_with_constraint([](const TimeSeriesOutput &ts) -> bool { return ts.modified(); });
    }

    TimeSeriesBundleInput::iterator TimeSeriesBundleInput::begin() { return _ts_values.begin(); }

    TimeSeriesBundleInput::const_iterator TimeSeriesBundleInput::begin() const { return _ts_values.begin(); }

    TimeSeriesBundleInput::iterator TimeSeriesBundleInput::end() { return _ts_values.end(); }

    TimeSeriesBundleInput::const_iterator TimeSeriesBundleInput::end() const { return _ts_values.end(); }

    TimeSeriesInput &TimeSeriesBundleInput::operator[](const std::string &key) {
        auto it{std::ranges::find(_schema->keys(), key)};
        if (it != _schema->keys().end()) {
            size_t index = std::distance(_schema->keys().begin(), it);
            return *_ts_values[index];
        }
        throw std::out_of_range("Key not found in TimeSeriesSchema");
    }

    const TimeSeriesInput &TimeSeriesBundleInput::operator[](const std::string &key) const {
        return const_cast<TimeSeriesBundleInput *>(this)->operator[](key);
    }

    TimeSeriesInput &TimeSeriesBundleInput::operator[](size_t ndx) { return *_ts_values.at(ndx); }

    const TimeSeriesInput &TimeSeriesBundleInput::operator[](size_t ndx) const { return *_ts_values.at(ndx); }

    bool TimeSeriesBundleInput::contains(const std::string &key) const {
        return std::ranges::find(schema().keys(), key) != schema().keys().end();
    }

    const TimeSeriesSchema &TimeSeriesBundleInput::schema() const { return *_schema; }

    void TimeSeriesBundleInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesBundleInput, TimeSeriesInput>(m, "TimeSeriesBundleInput")
            .def("__getitem__",
                 [](TimeSeriesBundleInput &self, const std::string &key) -> TimeSeriesInput & {
                     return self[key];  // Use operator[] overload with string
                 })
            .def("__getitem__",
                 [](TimeSeriesBundleInput &self, std::size_t ndx) -> TimeSeriesInput & {
                     return self[ndx];  // Use operator[] overload with index
                 })
            .def("__contains__", &TimeSeriesBundleInput::contains)
            .def(
                "__iter__",
                [](const TimeSeriesBundleInput &self) {
                    return nb::make_iterator(nb::type<TimeSeriesBundleInput>(), "iterator", self.begin(), self.end());
                },
                nb::keep_alive<0, 1>())
            // .def("__len__", &TimeSeriesBundleInput::size)
            // .def("keys", &TimeSeriesBundleInput::keys)
            // .def("items", &TimeSeriesBundleInput::items)
            // .def("values", &TimeSeriesBundleInput::values)
            // .def("modified_keys", &TimeSeriesBundleInput::modified_keys)
            // .def("valid_values", &TimeSeriesBundleInput::valid_values)
            // .def("valid_items", &TimeSeriesBundleInput::valid_items)
            ;
    }

}  // namespace hgraph

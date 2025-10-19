
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>

#include <algorithm>
#include <numeric>
#include <ranges>
#include <utility>

namespace hgraph
{
    TimeSeriesSchema::TimeSeriesSchema(std::vector<std::string> keys) : TimeSeriesSchema(std::move(keys), nb::none()) {}

    TimeSeriesSchema::TimeSeriesSchema(std::vector<std::string> keys, nb::object type)
        : _keys{std::move(keys)}, _scalar_type{std::move(type)} {}

    const std::vector<std::string> &TimeSeriesSchema::keys() const { return _keys; }

    const nb::object &TimeSeriesSchema::scalar_type() const { return _scalar_type; }

    void TimeSeriesSchema::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSchema, nb::intrusive_base>(m, "TimeSeriesSchema")
            .def(nb::init<std::vector<std::string>>(), "keys"_a)
            .def(nb::init<std::vector<std::string>, const nb::type_object &>(), "keys"_a, "scalar_type"_a)
            .def_prop_ro("keys", &TimeSeriesSchema::keys)
            .def_prop_ro("scalar_type", &TimeSeriesSchema::scalar_type)
            .def("__str__", [](const TimeSeriesSchema &self) {
                if (!self.scalar_type().is_valid() || self.scalar_type().is_none()) {
                    return nb::str("unnamed:{}").format(self.keys());
                }
                return nb::str("{}{}}").format(self.scalar_type(), self.keys());
            });
        ;
    }

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    TimeSeriesBundle<T_TS>::TimeSeriesBundle(const node_ptr &parent, TimeSeriesSchema::ptr schema)
        : T_TS(parent), _schema{std::move(schema)} {}
    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    TimeSeriesBundle<T_TS>::TimeSeriesBundle(const TimeSeriesType::ptr &parent, TimeSeriesSchema::ptr schema)
        : T_TS(parent), _schema{std::move(schema)} {}
    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    nb::object TimeSeriesBundle<T_TS>::py_value() const {
        return py_value_with_constraint<false>([](const ts_type &ts) { return ts.valid(); });
    }
    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    nb::object TimeSeriesBundle<T_TS>::py_delta_value() const {
        return py_value_with_constraint<true>([](const ts_type &ts) { return ts.modified(); });
    }
    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesBundle<T_TS>::raw_key_const_iterator TimeSeriesBundle<T_TS>::begin() const {
        return _schema->keys().begin();
    }
    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesBundle<T_TS>::raw_key_const_iterator TimeSeriesBundle<T_TS>::end() const {
        return _schema->keys().end();
    }

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesBundle<T_TS>::ts_type::ptr &TimeSeriesBundle<T_TS>::operator[](const std::string &key) {
        // Return the value of the ts_bundle for the schema key instance.
        auto it{std::ranges::find(_schema->keys(), key)};
        if (it != _schema->keys().end()) {
            size_t index{static_cast<size_t>(std::distance(_schema->keys().begin(), it))};
            return this->operator[](index);
        }
        throw std::out_of_range("Key not found in TimeSeriesSchema");
    }

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    const typename TimeSeriesBundle<T_TS>::ts_type::ptr &TimeSeriesBundle<T_TS>::operator[](const std::string &key) const {
        return const_cast<bundle_type *>(this)->operator[](key);
    }

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    bool TimeSeriesBundle<T_TS>::contains(const std::string &key) const {
        return std::ranges::find(_schema->keys(), key) != _schema->keys().end();
    }

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    const TimeSeriesSchema &TimeSeriesBundle<T_TS>::schema() const {
        return *_schema;
    }

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    TimeSeriesSchema &TimeSeriesBundle<T_TS>::schema() {
        return *_schema;
    }

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesBundle<T_TS>::key_collection_type TimeSeriesBundle<T_TS>::keys() const {
        return {_schema->keys().begin(), _schema->keys().end()};
    }

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesBundle<T_TS>::key_collection_type TimeSeriesBundle<T_TS>::valid_keys() const {
        return keys_with_constraint([](const ts_type &ts) -> bool { return ts.valid(); });
    }

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesBundle<T_TS>::key_collection_type TimeSeriesBundle<T_TS>::modified_keys() const {
        return keys_with_constraint([](const ts_type &ts) -> bool { return ts.modified(); });
    }

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesBundle<T_TS>::key_value_collection_type TimeSeriesBundle<T_TS>::items() {
        key_value_collection_type result;
        result.reserve(this->size());
        for (size_t i = 0; i < this->size(); ++i) { result.emplace_back(schema().keys()[i], operator[](i)); }
        return result;
    }

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesBundle<T_TS>::key_value_collection_type TimeSeriesBundle<T_TS>::items() const {
        return const_cast<bundle_type *>(this)->items();
    }

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesBundle<T_TS>::key_value_collection_type TimeSeriesBundle<T_TS>::valid_items() {
        auto                      index_result{this->items_with_constraint([](const ts_type &ts) -> bool { return ts.valid(); })};
        key_value_collection_type result;
        result.reserve(index_result.size());
        for (auto &[ndx, ts] : index_result) { result.emplace_back(schema().keys()[ndx], ts); }
        return result;
    }

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesBundle<T_TS>::key_value_collection_type TimeSeriesBundle<T_TS>::valid_items() const {
        return const_cast<bundle_type *>(this)->valid_items();
    }

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesBundle<T_TS>::key_value_collection_type TimeSeriesBundle<T_TS>::modified_items() {
        auto index_result{this->items_with_constraint([](const ts_type &ts) -> bool { return ts.modified(); })};
        key_value_collection_type result;
        result.reserve(index_result.size());
        for (auto &[ndx, ts] : index_result) { result.emplace_back(schema().keys()[ndx], ts); }
        return result;
    }

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesBundle<T_TS>::key_value_collection_type TimeSeriesBundle<T_TS>::modified_items() const {
        return const_cast<bundle_type *>(this)->modified_items();
    }

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    template <bool is_delta>
    nb::object TimeSeriesBundle<T_TS>::py_value_with_constraint(const std::function<bool(const ts_type &)> &constraint) const {
        nb::dict out;
        auto     include_nones{!_schema->scalar_type().is_none()};
        for (size_t i = 0, l = ts_values().size(); i < l; ++i) {
            if (auto ts{ts_values()[i]}; constraint(*ts)) {
                if constexpr (is_delta) {
                    out[_schema->keys()[i].c_str()] = ts->py_delta_value();
                } else {
                    out[_schema->keys()[i].c_str()] = ts->py_value();
                }
            } else if (include_nones) {
                out[_schema->keys()[i].c_str()] = nb::none();
            }
        }

        if (!include_nones) { return out; }
        return nb::cast<nb::object>(_schema->scalar_type()(**out));
    }

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesBundle<T_TS>::key_collection_type
    TimeSeriesBundle<T_TS>::keys_with_constraint(const std::function<bool(const ts_type &)> &constraint) const {
        auto                      index_results = index_with_constraint(constraint);
        std::vector<c_string_ref> result;
        result.reserve(index_results.size());
        for (auto i : index_results) { result.emplace_back(_schema->keys()[i]); }
        return result;
    }

    template <typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesBundle<T_TS>::key_value_collection_type
    TimeSeriesBundle<T_TS>::key_value_with_constraint(const std::function<bool(const ts_type &)> &constraint) const {
        auto                      index_results = items_with_constraint(constraint);
        key_value_collection_type result;
        result.reserve(index_results.size());
        for (auto &[ndx, ts] : index_results) { result.emplace_back(_schema->keys()[ndx], ts); }
        return result;
    }

    void TimeSeriesBundleOutput::py_set_value(nb::object v) {
        // Python implementation:
        // if v is None: self.invalidate()
        // else if isinstance(v, scalar_type): set each attribute
        // else: iterate dict and set values
        if (v.is_none()) {
            invalidate();
        } else {
            if (!schema().scalar_type().is_none() && nb::isinstance(v, schema().scalar_type())) {
                // Scalar type: iterate schema keys and get attributes
                for (const auto &key : schema().keys()) {
                    auto attr = nb::getattr(v, key.c_str(), nb::none());
                    if (!attr.is_none()) { (*this)[key]->py_set_value(attr); }
                }
            } else {
                // Dict-like: iterate items
                for (auto [key, val] : nb::cast<nb::dict>(v)) {
                    if (!val.is_none()) { (*this)[nb::cast<std::string>(key)]->py_set_value(nb::borrow(val)); }
                }
            }
        }
    }

    void TimeSeriesBundleOutput::mark_invalid() {
        // Python: super().mark_invalid() then children mark_invalid()
        if (valid()) {
            TimeSeriesOutput::mark_invalid();  // Call parent FIRST
            for (auto &v : ts_values()) { v->mark_invalid(); }
        }
    }

    bool TimeSeriesBundleOutput::can_apply_result(nb::object result) {
        // Python implementation:
        // if result is None: return True
        // if type(result) is scalar_type: return self.modified
        // else: check each child can_apply_result
        if (result.is_none()) { return true; }

        if (!schema().scalar_type().is_none() && nb::isinstance(result, schema().scalar_type())) {
            // If it's a scalar type, we can apply if this bundle is modified
            return modified();
        } else {
            // For dict-like results, check each child
            for (auto [key, val] : nb::cast<nb::dict>(result)) {
                if (!val.is_none()) {
                    if (!(*this)[nb::cast<std::string>(key)]->can_apply_result(nb::borrow(val))) { return false; }
                }
            }
        }
        return true;
    }
    bool TimeSeriesBundleOutput::is_same_type(const TimeSeriesType *other) const {
        auto other_b = dynamic_cast<const TimeSeriesBundleOutput *>(other);
        if (!other_b) { return false; }
        if (this->size() != other_b->size()) { return false; }
        for (size_t i = 0; i < this->size(); ++i) {
            if (!(*this)[i]->is_same_type((*other_b)[i])) { return false; }
        }
        return true;
    }

    void TimeSeriesBundleOutput::apply_result(nb::object value) {
        if (value.is_none()) { return; }
        // Check if value is an instance of the scalar type (not just identity check)
        py_set_value(value);
    }

    void TimeSeriesBundleOutput::register_with_nanobind(nb::module_ &m) {
        using TimeSeriesBundle_Output = TimeSeriesBundle<IndexedTimeSeriesOutput>;

        nb::class_<TimeSeriesBundle_Output, IndexedTimeSeriesOutput>(m, "TimeSeriesBundle_Output")
            .def("__getitem__",
                 [](TimeSeriesBundle_Output &self, const std::string &key) -> TimeSeriesOutput::ptr {
                     return self[key];  // Use operator[] overload with string
                 })
            .def("__getitem__",
                 [](TimeSeriesBundle_Output &self, size_t index) -> TimeSeriesOutput::ptr {
                     return self[index];  // Use operator[] overload with int
                 })
            .def("__iter__",
                 [](TimeSeriesBundle_Output &self) {
                     // Create a Python list of values to iterate over
                     nb::list values;
                     for (size_t i = 0; i < self.size(); ++i) { values.append(self[i]); }
                     return values.attr("__iter__")();
                 })
            .def("__contains__", &TimeSeriesBundle_Output::contains)
            .def("keys", &TimeSeriesBundle_Output::keys)
            .def("items",
                 static_cast<key_value_collection_type (TimeSeriesBundle_Output::*)() const>(&TimeSeriesBundle_Output::items))
            .def("valid_keys", &TimeSeriesBundle_Output::valid_keys)
            .def("valid_items",
                 static_cast<key_value_collection_type (TimeSeriesBundle_Output::*)() const>(&TimeSeriesBundle_Output::valid_items))
            .def("modified_keys", &TimeSeriesBundle_Output::modified_keys)
            .def("modified_items", static_cast<key_value_collection_type (TimeSeriesBundle_Output::*)() const>(
                                       &TimeSeriesBundle_Output::modified_items))
            .def_prop_ro("__schema__", static_cast<const TimeSeriesSchema &(TimeSeriesBundle_Output::*)() const>(
                                           &TimeSeriesBundle_Output::schema));

        nb::class_<TimeSeriesBundleOutput, IndexedTimeSeriesOutput>(m, "TimeSeriesBundleOutput")
            .def(nb::init<const node_ptr &, TimeSeriesSchema::ptr>(), "owning_node"_a, "schema"_a)
            .def(nb::init<const TimeSeriesType::ptr &, TimeSeriesSchema::ptr>(), "parent_input"_a, "schema"_a)
            .def_prop_rw(
                "value", [](const TimeSeriesBundleOutput &self) -> nb::object { return self.py_value(); },
                &TimeSeriesBundleOutput::py_set_value);
    }
    bool TimeSeriesBundleInput::is_same_type(const TimeSeriesType *other) const {
        auto other_b = dynamic_cast<const TimeSeriesBundleInput *>(other);
        if (!other_b) { return false; }
        if (this->size() != other_b->size()) { return false; }
        for (size_t i = 0; i < this->size(); ++i) {
            if (!(*this)[i]->is_same_type((*other_b)[i])) { return false; }
        }
        return true;
    }

    void TimeSeriesBundleInput::register_with_nanobind(nb::module_ &m) {
        using TimeSeriesBundle_Input = TimeSeriesBundle<IndexedTimeSeriesInput>;
        nb::class_<TimeSeriesBundle_Input, IndexedTimeSeriesInput>(m, "TimeSeriesBundle_Input")
            .def("__getitem__",
                 [](TimeSeriesBundle_Input &self, const std::string &key) {
                     return self[key];  // Use operator[] overload with string
                 })
            .def("__getitem__",
                 [](TimeSeriesBundle_Input &self, size_t index) {
                     return self[index];  // Use operator[] overload with int
                 })
            .def("__iter__",
                 [](TimeSeriesBundle_Input &self) {
                     // Create a Python list of values to iterate over
                     nb::list values;
                     for (size_t i = 0; i < self.size(); ++i) { values.append(self[i]); }
                     return values.attr("__iter__")();
                 })
            .def("__contains__", &TimeSeriesBundle_Input::contains)
            .def("keys", &TimeSeriesBundle_Input::keys)
            .def("items",
                 static_cast<key_value_collection_type (TimeSeriesBundle_Input::*)() const>(&TimeSeriesBundle_Input::items))
            .def("modified_keys", &TimeSeriesBundle_Input::modified_keys)
            .def("modified_items", static_cast<key_value_collection_type (TimeSeriesBundle_Input::*)() const>(
                                       &TimeSeriesBundle_Input::modified_items))
            .def("valid_keys", &TimeSeriesBundle_Input::valid_keys)
            .def("valid_items",
                 static_cast<key_value_collection_type (TimeSeriesBundle_Input::*)() const>(&TimeSeriesBundle_Input::valid_items))
            .def_prop_ro("__schema__",
                         static_cast<const TimeSeriesSchema &(TimeSeriesBundle_Input::*)() const>(&TimeSeriesBundle_Input::schema))
            .def("__getattr__", [](TimeSeriesBundle_Input &self, const std::string &key) -> TimeSeriesInput::ptr {
                if (self.contains(key)) { return self[key]; }
                throw nb::attribute_error(("Attribute '" + key + "' not found in TimeSeriesBundle").c_str());
            });

        nb::class_<TimeSeriesBundleInput, TimeSeriesBundle_Input>(m, "TimeSeriesBundleInput")
            .def(nb::init<const node_ptr &, TimeSeriesSchema::ptr>(), "owning_node"_a, "schema"_a)
            .def(nb::init<const TimeSeriesType::ptr &, TimeSeriesSchema::ptr>(), "parent_input"_a, "schema"_a);
    }

    TimeSeriesBundleInput::ptr TimeSeriesBundleInput::copy_with(const node_ptr &parent, collection_type ts_values) {
        auto v{new TimeSeriesBundleInput(parent, TimeSeriesSchema::ptr{&schema()})};
        v->set_ts_values(ts_values);
        return v;
    }

}  // namespace hgraph

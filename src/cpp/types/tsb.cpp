
#include <hgraph/types/tsb.h>
#include <hgraph/types/node.h>

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
                if (!self.scalar_type().is_valid() || self.scalar_type().is_none()) { return nb::str("unnamed:{}").format(self.keys()); }
                return nb::str("{}{}}").format(self.scalar_type(), self.keys());
            });
        ;
    }

    void TimeSeriesBundleOutput::apply_result(nb::object value) {
        if (value.is_none()) { return; }
        // Check if value is an instance of the scalar type (not just identity check)
        if (!schema().scalar_type().is_none() && nb::isinstance(value, schema().scalar_type())) {
            for (const auto &key : schema().keys()) {
                auto v = nb::getattr(value, key.c_str(), nb::none());
                if (!v.is_none()) { (*this)[key]->apply_result(v); }
            }
        } else {
            for (auto [key, val] : nb::cast<nb::dict>(value)) {
                if (!val.is_none()) { (*this)[nb::cast<std::string>(key)]->apply_result(nb::borrow(val)); }
            }
        }
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
            .def(
                "__iter__",
                [](TimeSeriesBundle_Output &self) {
                    // Create a Python list of values to iterate over
                    nb::list values;
                    for (size_t i = 0; i < self.size(); ++i) {
                        values.append(self[i]);
                    }
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
            .def_prop_ro("__schema__", static_cast<const TimeSeriesSchema& (TimeSeriesBundle_Output::*)() const>(&TimeSeriesBundle_Output::schema));

        nb::class_<TimeSeriesBundleOutput, IndexedTimeSeriesOutput>(m, "TimeSeriesBundleOutput")
            .def(nb::init<const node_ptr &, TimeSeriesSchema::ptr>(), "owning_node"_a, "schema"_a)
            .def(nb::init<const TimeSeriesType::ptr &, TimeSeriesSchema::ptr>(), "parent_input"_a, "schema"_a);
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
            .def(
                "__iter__",
                [](TimeSeriesBundle_Input &self) {
                    // Create a Python list of values to iterate over
                    nb::list values;
                    for (size_t i = 0; i < self.size(); ++i) {
                        values.append(self[i]);
                    }
                    return values.attr("__iter__")();
                })
            .def("__contains__", &TimeSeriesBundle_Input::contains)
            .def("keys", &TimeSeriesBundle_Input::keys)
            .def("items", static_cast<key_value_collection_type (TimeSeriesBundle_Input::*)() const>(&TimeSeriesBundle_Input::items))
            .def("modified_keys", &TimeSeriesBundle_Input::modified_keys)
            .def("modified_items",
                 static_cast<key_value_collection_type (TimeSeriesBundle_Input::*)() const>(&TimeSeriesBundle_Input::modified_items))
            .def("valid_keys", &TimeSeriesBundle_Input::valid_keys)
            .def("valid_items",
                 static_cast<key_value_collection_type (TimeSeriesBundle_Input::*)() const>(&TimeSeriesBundle_Input::valid_items))
            .def_prop_ro("__schema__", static_cast<const TimeSeriesSchema& (TimeSeriesBundle_Input::*)() const>(&TimeSeriesBundle_Input::schema))
            .def("__getattr__",
                 [](TimeSeriesBundle_Input &self, const std::string &key) -> TimeSeriesInput::ptr {
                     if (self.contains(key)) {
                         return self[key];
                     }
                     throw nb::attribute_error(("Attribute '" + key + "' not found in TimeSeriesBundle").c_str());
                 });

        nb::class_<TimeSeriesBundleInput, TimeSeriesBundle_Input>(m, "TimeSeriesBundleInput")
            .def(nb::init<const node_ptr &, TimeSeriesSchema::ptr>(), "owning_node"_a, "schema"_a)
            .def(nb::init<const TimeSeriesType::ptr &, TimeSeriesSchema::ptr>(), "parent_input"_a, "schema"_a);
    }

    TimeSeriesBundleInput::ptr TimeSeriesBundleInput::copy_with(const node_ptr &parent,
                                                           collection_type ts_values) {
        auto v {new TimeSeriesBundleInput(parent, TimeSeriesSchema::ptr{&schema()})};
        v->set_ts_values(ts_values);
        return v;
    }

}  // namespace hgraph

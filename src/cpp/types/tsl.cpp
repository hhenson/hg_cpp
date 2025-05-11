
#include <hgraph/types/tsl.h>
#include <hgraph/types/node.h>

namespace hgraph
{

    void TimeSeriesListOutput::apply_result(nb::object value) {
        if (value.is_none()) { return; }
        if (nb::isinstance<nb::tuple>(value) || nb::isinstance<nb::list>(value)) {
            for (size_t i = 0, l = nb::len(value); i < l; ++i) {
                const auto &v{value[i]};
                if (v.is_valid() && !v.is_none()) { (*this)[i]->apply_result(v); }
            }
        } else if (nb::isinstance<nb::dict>(value)) {
            for (auto [key, val] : nb::cast<nb::dict>(value)) {
                if (val.is_valid() && !val.is_none()) { (*this)[nb::cast<size_t>(key)]->apply_result(nb::borrow(val)); }
            }
        } else {
            throw std::runtime_error("Invalid value type for TimeSeriesListOutput");
        }
    }


    void TimeSeriesListOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesListOutput, IndexedTimeSeriesOutput>(m, "TimeSeriesListOutput")
            .def(nb::init<const node_ptr &>(), "owning_node"_a)
            .def(nb::init<const TimeSeriesType::ptr &>(), "parent_input"_a)
            .def(
                "__iter__",
                [](const TimeSeriesListOutput &self) {
                    nb::make_iterator(nb::type<collection_type>(), "iterator", self.begin(), self.end());
                },
                nb::keep_alive<0, 1>())
            .def("keys", &TimeSeriesListOutput::keys)
            .def("items", static_cast<enumerated_collection_type (TimeSeriesListOutput::*)() const>(&TimeSeriesListOutput::items))
            .def("valid_keys", &TimeSeriesListOutput::valid_keys)
            .def("valid_items",
                 static_cast<enumerated_collection_type (TimeSeriesListOutput::*)() const>(&TimeSeriesListOutput::valid_items))
            .def("modified_keys", &TimeSeriesListOutput::modified_keys)
            .def("modified_items",
                 static_cast<enumerated_collection_type (TimeSeriesListOutput::*)() const>(&TimeSeriesListOutput::modified_items));
    }

    void TimeSeriesListInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesListInput, IndexedTimeSeriesInput>(m, "TimeSeriesListInput")
            .def(nb::init<const node_ptr &>(), "owning_node"_a)
            .def(nb::init<const TimeSeriesType::ptr &>(), "parent_input"_a)
            .def(
                "__iter__",
                [](const TimeSeriesListInput &self) {
                    nb::make_iterator(nb::type<collection_type>(), "iterator", self.begin(), self.end());
                },
                nb::keep_alive<0, 1>())
            .def("keys", &TimeSeriesListInput::keys)
            .def("items", static_cast<enumerated_collection_type (TimeSeriesListInput::*)() const>(&TimeSeriesListInput::items))
            .def("valid_keys", &TimeSeriesListInput::valid_keys)
            .def("valid_items",
                 static_cast<enumerated_collection_type (TimeSeriesListInput::*)() const>(&TimeSeriesListInput::valid_items))
            .def("modified_keys", &TimeSeriesListInput::modified_keys)
            .def("modified_items",
                 static_cast<enumerated_collection_type (TimeSeriesListInput::*)() const>(&TimeSeriesListInput::modified_items))
        ;
    }
}  // namespace hgraph
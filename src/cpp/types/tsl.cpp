
#include <hgraph/python/pyb_wiring.h>
#include <hgraph/types/tsl.h>
#include <nanobind/make_iterator.h>

namespace hgraph
{
    nb::object TimeSeriesListOutput::py_value() const {
        nb::list result;
        for (const auto &ts : ts_values()) {
            if (ts->valid()) {
                result.append(ts->py_value());
            } else {
                result.append(nb::none());
            }
        }
        return nb::tuple(result);
    }

    nb::object TimeSeriesListOutput::py_delta_value() const {
        nb::dict result;
        for (auto &[ndx, ts] : modified_items()) { result[ndx] = ts->py_delta_value(); }
        return result;
    }

    void TimeSeriesListOutput::apply_result(nb::handle value) {
        if (value.is_none()) { return; }
        if (nb::isinstance<nb::tuple>(value) || nb::isinstance<nb::list>(value)) {
            for (size_t i = 0, l = nb::len(value); i < l; ++i) {
                const auto &v{value[i]};
                if (v.is_valid() && !v.is_none()) { (*this)[i]->apply_result(v); }
            }
        } else if (nb::isinstance<nb::dict>(value)) {
            for (auto [key, val] : nb::cast<nb::dict>(value)) {
                if (val.is_valid() && !val.is_none()) { (*this)[nb::cast<size_t>(key)]->apply_result(val); }
            }
        } else {
            throw std::runtime_error("Invalid value type for TimeSeriesListOutput");
        }
    }

    IndexedTimeSeriesOutput::value_iterator TimeSeriesListOutput::begin() { return ts_values().begin(); }

    IndexedTimeSeriesOutput::value_iterator TimeSeriesListOutput::end() { return ts_values().end(); }

    IndexedTimeSeriesOutput::value_const_iterator TimeSeriesListOutput::begin() const {
        return const_cast<TimeSeriesListOutput *>(this)->begin();
    }

    IndexedTimeSeriesOutput::value_const_iterator TimeSeriesListOutput::end() const {
        return const_cast<TimeSeriesListOutput *>(this)->end();
    }

    TimeSeriesListOutput::key_collection_type TimeSeriesListOutput::keys() const {
        key_collection_type result;
        result.reserve(size());
        for (size_t i = 0; i < size(); ++i) { result.push_back(i); }
        return result;
    }

    TimeSeriesListOutput::key_collection_type TimeSeriesListOutput::valid_keys() const {
        return index_with_constraint([](const TimeSeriesOutput &ts) { return ts.valid(); });
    }

    TimeSeriesListOutput::key_collection_type TimeSeriesListOutput::modified_keys() const {
        return index_with_constraint([](const TimeSeriesOutput &ts) { return ts.modified(); });
    }

    TimeSeriesListOutput::key_value_collection_type TimeSeriesListOutput::items() {
        key_value_collection_type result;
        result.reserve(size());
        for (size_t i = 0; i < size(); ++i) { result.push_back({i, ts_values()[i]}); }
        return result;
    }

    TimeSeriesListOutput::key_value_collection_type TimeSeriesListOutput::items() const {
        return const_cast<TimeSeriesListOutput *>(this)->items();
    }

    TimeSeriesListOutput::key_value_collection_type TimeSeriesListOutput::valid_items() {
        return items_with_constraint([](const TimeSeriesOutput &ts) { return ts.valid(); });
    }

    TimeSeriesListOutput::key_value_collection_type TimeSeriesListOutput::valid_items() const {
        return const_cast<TimeSeriesListOutput *>(this)->valid_items();
    }

    TimeSeriesListOutput::key_value_collection_type TimeSeriesListOutput::modified_items() const {
        return const_cast<TimeSeriesListOutput *>(this)->modified_items();
    }

    TimeSeriesListOutput::key_value_collection_type TimeSeriesListOutput::modified_items() {
        return items_with_constraint([](const TimeSeriesOutput &ts) { return ts.modified(); });
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
            .def("items", static_cast<key_value_collection_type (TimeSeriesListOutput::*)() const>(&TimeSeriesListOutput::items))
            .def("valid_keys", &TimeSeriesListOutput::valid_keys)
            .def("valid_items",
                 static_cast<key_value_collection_type (TimeSeriesListOutput::*)() const>(&TimeSeriesListOutput::valid_items))
            .def("modified_keys", &TimeSeriesListOutput::modified_keys)
            .def("modified_items",
                 static_cast<key_value_collection_type (TimeSeriesListOutput::*)() const>(&TimeSeriesListOutput::modified_items));
    }

    void TimeSeriesListInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesListInput, IndexedTimeSeriesInput>(m, "TimeSeriesListInput")
            .def(nb::init<const node_ptr &>(), "owning_node"_a)
            .def(nb::init<const TimeSeriesType::ptr &>(), "parent_input"_a)
            // .def(
            //     "__iter__",
            //     [](const TimeSeriesListInput &self) {
            //         nb::make_iterator(nb::type<collection_type>(), "iterator", self.begin(), self.end());
            //     },
            //     nb::keep_alive<0, 1>())
            // .def("keys", &TimeSeriesListInput::keys)
            // .def("items", static_cast<key_value_collection_type (TimeSeriesListInput::*)() const>(&TimeSeriesListInput::items))
            // .def("valid_keys", &TimeSeriesListInput::valid_keys)
            // .def("valid_items",
            //      static_cast<key_value_collection_type (TimeSeriesListInput::*)() const>(&TimeSeriesListInput::valid_items))
            // .def("modified_keys", &TimeSeriesListInput::modified_keys)
            // .def("modified_items",
            //      static_cast<key_value_collection_type (TimeSeriesListInput::*)() const>(&TimeSeriesListInput::modified_items))
        ;
    }
}  // namespace hgraph
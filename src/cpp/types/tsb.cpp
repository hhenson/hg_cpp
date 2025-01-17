#include <hgraph/types/tsb.h>
#include <nanobind/make_iterator.h>

namespace hgraph
{

    TimeSeriesBundleInput::iterator TimeSeriesBundleInput::begin() { return _ts_values.begin(); }

    TimeSeriesBundleInput::const_iterator TimeSeriesBundleInput::begin() const { return _ts_values.begin(); }

    TimeSeriesBundleInput::iterator TimeSeriesBundleInput::end() { return _ts_values.end(); }

    TimeSeriesBundleInput::const_iterator TimeSeriesBundleInput::end() const { return _ts_values.end(); }

    TimeSeriesInput &TimeSeriesBundleInput::operator[](const std::string &key) { return *_ts_values[key]; }

    const TimeSeriesInput &TimeSeriesBundleInput::operator[](const std::string &key) const { return *_ts_values.at(key); }

    bool TimeSeriesBundleInput::contains(const std::string &key) const { return _ts_values.find(key) != _ts_values.end(); }

    void TimeSeriesBundleInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesBundleInput, TimeSeriesInput>(m, "TimeSeriesBundleInput")
            .def("__getitem__", static_cast<const TimeSeriesInput &(TimeSeriesBundleInput::*)(const std::string &) const>(
                                    &TimeSeriesBundleInput::operator[]))
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

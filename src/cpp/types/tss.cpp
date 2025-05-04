#include <hgraph/types/tss.h>

namespace hgraph
{

    void SetDelta::register_with_nanobind(nb::module_ &m) {
        nb::class_<SetDelta, nanobind::intrusive_base>(m, "SetDelta")
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

    nb::object SetDelta_Object::py_added_elements() const { return _added_elements; }

    void tss_register_with_nanobind(nb::module_ &m) {
        using TSS_IN = TimeSeriesSet<TimeSeriesInput>;
        nb::class_<TSS_IN, TimeSeriesInput>(m, "TimeSeriesSetInput")
            .def("__contains__", &TSS_IN::py_contains)
            .def("__len__", &TSS_IN::size)
            .def("values", &TSS_IN::py_values)
            .def("added", &TSS_IN::py_added)
            .def("removed", &TSS_IN::py_removed)
            .def("was_added", &TSS_IN::py_was_added)
            .def("was_removed", &TSS_IN::py_was_removed);

        using TSS_OUT = TimeSeriesSet<TimeSeriesOutput>;
        nb::class_<TSS_OUT, TimeSeriesOutput>(m, "TimeSeriesSetOutput")
            .def("__contains__", &TSS_OUT::py_contains)
            .def("__len__", &TSS_OUT::size)
            .def("values", &TSS_OUT::py_values)
            .def("added", &TSS_OUT::py_added)
            .def("removed", &TSS_OUT::py_removed)
            .def("was_added", &TSS_OUT::py_was_added)
            .def("was_removed", &TSS_OUT::py_was_removed);
    }
}  // namespace hgraph

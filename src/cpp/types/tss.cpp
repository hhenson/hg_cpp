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

        using SetDelta_object = SetDeltaImpl<nb::object>;
        nb::class_<SetDelta_object, SetDelta>(m, "SetDelta_object")
            .def(nb::init<const std::unordered_set<nb::object> &, const std::unordered_set<nb::object> &>(), "added_elements"_a,
                 "removed_elements"_a);
    }

}  // namespace hgraph

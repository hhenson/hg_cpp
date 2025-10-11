#include <hgraph/builders/output_builder.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/tss.h>
#include <hgraph/types/constants.h>

namespace hgraph
{

    bool SetDelta::operator==(const nb::object &other) const {
        if (!nb::isinstance(other, get_frozenset()) && !nb::isinstance<nb::set>(other)) return false;
        nb::set added;
        nb::set removed;
        auto REMOVED = get_removed();
        for (auto i : nb::iter(other)) {
            if (nb::isinstance(i, REMOVED)) {
                removed.add(i.attr("item"));
            } else {
                added.add(i);
            }
        }
        return  removed.equal(py_removed()) && added.equal(py_added());
    }

    void SetDelta::register_with_nanobind(nb::module_ &m) {
        nb::class_<SetDelta, nb::intrusive_base>(m, "SetDelta")
            .def_prop_ro("added", &SetDelta::py_added)
            .def_prop_ro("removed", &SetDelta::py_removed)
            .def_prop_ro("tp", &SetDelta::py_type)
            .def(
                "__str__",
                [](SetDelta &self) { return nb::str("SetDelta(added={}, removed={})").format(self.py_added(), self.py_removed()); })
            .def(
                "__repr__",
                [](SetDelta &self) {
                    return nb::str("SetDelta[{}](added={}, removed={})").format(self.py_type(), self.py_added(), self.py_removed());
                })
            .def("__eq__", static_cast<bool (SetDelta::*)(const SetDelta&) const>(&SetDelta::operator==))
            .def("__eq__", static_cast<bool (SetDelta::*)(const nb::object&) const>(&SetDelta::operator==))
            .def("__hash__", &SetDelta::hash)
            .def("__add__", [](SetDelta &self, const SetDelta &other) -> SetDelta::ptr {
                // Try intrinsic specializations first
                if (auto *a = dynamic_cast<SetDeltaImpl<int64_t> *>(&self)) {
                    auto *b = dynamic_cast<const SetDeltaImpl<int64_t> *>(&other);
                    if (!b) throw nb::type_error("Cannot add SetDelta of different types");
                    using T = int64_t; using SetT = std::unordered_set<T>;
                    const SetT &A1 = a->added(); const SetT &R1 = a->removed();
                    SetT resA = A1; SetT resR = R1;
                    const SetT &A2 = const_cast<SetDeltaImpl<int64_t> *>(b)->added();
                    const SetT &R2 = const_cast<SetDeltaImpl<int64_t> *>(b)->removed();
                    for (const auto &x : R2) { resA.erase(x); }
                    for (const auto &x : A2) { resR.erase(x); resA.insert(x); }
                    for (const auto &x : R2) { if (A1.find(x) == A1.end()) resR.insert(x); }
                    return make_set_delta<int64_t>(std::move(resA), std::move(resR));
                }
                if (auto *a = dynamic_cast<SetDeltaImpl<bool> *>(&self)) {
                    auto *b = dynamic_cast<const SetDeltaImpl<bool> *>(&other);
                    if (!b) throw nb::type_error("Cannot add SetDelta of different types");
                    using T = bool; using SetT = std::unordered_set<T>;
                    const SetT &A1 = a->added(); const SetT &R1 = a->removed();
                    SetT resA = A1; SetT resR = R1;
                    const SetT &A2 = const_cast<SetDeltaImpl<bool> *>(b)->added();
                    const SetT &R2 = const_cast<SetDeltaImpl<bool> *>(b)->removed();
                    for (const auto &x : R2) { resA.erase(x); }
                    for (const auto &x : A2) { resR.erase(x); resA.insert(x); }
                    for (const auto &x : R2) { resR.insert(x); }
                    return make_set_delta<bool>(std::move(resA), std::move(resR));
                }
                if (auto *a = dynamic_cast<SetDeltaImpl<double> *>(&self)) {
                    auto *b = dynamic_cast<const SetDeltaImpl<double> *>(&other);
                    if (!b) throw nb::type_error("Cannot add SetDelta of different types");
                    using T = double; using SetT = std::unordered_set<T>;
                    const SetT &A1 = a->added(); const SetT &R1 = a->removed();
                    SetT resA = A1; SetT resR = R1;
                    const SetT &A2 = const_cast<SetDeltaImpl<double> *>(b)->added();
                    const SetT &R2 = const_cast<SetDeltaImpl<double> *>(b)->removed();
                    for (const auto &x : R2) { resA.erase(x); }
                    for (const auto &x : A2) { resR.erase(x); resA.insert(x); }
                    for (const auto &x : R2) { resR.insert(x); }
                    return make_set_delta<double>(std::move(resA), std::move(resR));
                }
                if (auto *a = dynamic_cast<SetDeltaImpl<engine_date_t> *>(&self)) {
                    auto *b = dynamic_cast<const SetDeltaImpl<engine_date_t> *>(&other);
                    if (!b) throw nb::type_error("Cannot add SetDelta of different types");
                    using T = engine_date_t; using SetT = std::unordered_set<T>;
                    const SetT &A1 = a->added(); const SetT &R1 = a->removed();
                    SetT resA = A1; SetT resR = R1;
                    const SetT &A2 = const_cast<SetDeltaImpl<engine_date_t> *>(b)->added();
                    const SetT &R2 = const_cast<SetDeltaImpl<engine_date_t> *>(b)->removed();
                    for (const auto &x : R2) { resA.erase(x); }
                    for (const auto &x : A2) { resR.erase(x); resA.insert(x); }
                    for (const auto &x : R2) { resR.insert(x); }
                    return make_set_delta<engine_date_t>(std::move(resA), std::move(resR));
                }
                if (auto *a = dynamic_cast<SetDeltaImpl<engine_time_t> *>(&self)) {
                    auto *b = dynamic_cast<const SetDeltaImpl<engine_time_t> *>(&other);
                    if (!b) throw nb::type_error("Cannot add SetDelta of different types");
                    using T = engine_time_t; using SetT = std::unordered_set<T>;
                    const SetT &A1 = a->added(); const SetT &R1 = a->removed();
                    SetT resA = A1; SetT resR = R1;
                    const SetT &A2 = const_cast<SetDeltaImpl<engine_time_t> *>(b)->added();
                    const SetT &R2 = const_cast<SetDeltaImpl<engine_time_t> *>(b)->removed();
                    for (const auto &x : R2) { resA.erase(x); }
                    for (const auto &x : A2) { resR.erase(x); resA.insert(x); }
                    for (const auto &x : R2) { resR.insert(x); }
                    return make_set_delta<engine_time_t>(std::move(resA), std::move(resR));
                }
                if (auto *a = dynamic_cast<SetDeltaImpl<engine_time_delta_t> *>(&self)) {
                    auto *b = dynamic_cast<const SetDeltaImpl<engine_time_delta_t> *>(&other);
                    if (!b) throw nb::type_error("Cannot add SetDelta of different types");
                    using T = engine_time_delta_t; using SetT = std::unordered_set<T>;
                    const SetT &A1 = a->added(); const SetT &R1 = a->removed();
                    SetT resA = A1; SetT resR = R1;
                    const SetT &A2 = const_cast<SetDeltaImpl<engine_time_delta_t> *>(b)->added();
                    const SetT &R2 = const_cast<SetDeltaImpl<engine_time_delta_t> *>(b)->removed();
                    for (const auto &x : R2) { resA.erase(x); }
                    for (const auto &x : A2) { resR.erase(x); resA.insert(x); }
                    for (const auto &x : R2) { resR.insert(x); }
                    return make_set_delta<engine_time_delta_t>(std::move(resA), std::move(resR));
                }
                // Fallback: object type composition using Python set operations
                if (auto *a_obj = dynamic_cast<SetDelta_Object *>(&self)) {
                    auto *b_obj = dynamic_cast<const SetDelta_Object *>(&other);
                    if (!b_obj) throw nb::type_error("Cannot add SetDelta of different types");
                    nb::object A1 = a_obj->py_added();
                    nb::object R1 = a_obj->py_removed();
                    nb::object A2 = b_obj->py_added();
                    nb::object R2 = b_obj->py_removed();
                    // resA = (set(A1) - set(R2)) | set(A2)
                    nb::object py_set = nb::module_::import_("builtins").attr("set");
                    nb::object setA1 = py_set(A1);
                    nb::object setR2 = py_set(R2);
                    nb::object setA2 = py_set(A2);
                    nb::object setR1 = py_set(R1);
                    nb::object resA = setA1.attr("difference")(setR2).attr("union")(setA2);
                    // resR = (set(R1) - set(A2)) | set(R2)
                    nb::object resR = setR1.attr("difference")(setA2).attr("union")(setR2);
                    nb::object frozenset_ = nb::module_::import_("builtins").attr("frozenset");
                    nb::object resA_f = frozenset_(resA);
                    nb::object resR_f = frozenset_(resR);
                    return SetDelta::ptr(new SetDelta_Object(resA_f, resR_f, self.py_type()));
                }
                throw nb::type_error("Unsupported SetDelta type for addition");
            })
            .def("__radd__", [](SetDelta &self, const SetDelta &other) -> SetDelta::ptr {
                // Compute other + self (reverse order)
                // Try intrinsic specializations first
                if (auto *a = dynamic_cast<const SetDeltaImpl<int64_t> *>(&other)) {
                    auto *b = dynamic_cast<SetDeltaImpl<int64_t> *>(&self);
                    if (!b) throw nb::type_error("Cannot add SetDelta of different types");
                    using T = int64_t; using SetT = std::unordered_set<T>;
                    const SetT &A1 = const_cast<SetDeltaImpl<int64_t> *>(a)->added();
                    const SetT &R1 = const_cast<SetDeltaImpl<int64_t> *>(a)->removed();
                    const SetT &A2 = b->added();
                    const SetT &R2 = b->removed();
                    SetT resA = A1; SetT resR = R1;
                    for (const auto &x : R2) { resA.erase(x); }
                    for (const auto &x : A2) { resR.erase(x); resA.insert(x); }
                    for (const auto &x : R2) { resR.insert(x); }
                    return make_set_delta<int64_t>(std::move(resA), std::move(resR));
                }
                if (auto *a = dynamic_cast<const SetDeltaImpl<bool> *>(&other)) {
                    auto *b = dynamic_cast<SetDeltaImpl<bool> *>(&self);
                    if (!b) throw nb::type_error("Cannot add SetDelta of different types");
                    using T = bool; using SetT = std::unordered_set<T>;
                    const SetT &A1 = const_cast<SetDeltaImpl<bool> *>(a)->added();
                    const SetT &R1 = const_cast<SetDeltaImpl<bool> *>(a)->removed();
                    const SetT &A2 = b->added();
                    const SetT &R2 = b->removed();
                    SetT resA = A1; SetT resR = R1;
                    for (const auto &x : R2) { resA.erase(x); }
                    for (const auto &x : A2) { resR.erase(x); resA.insert(x); }
                    for (const auto &x : R2) { resR.insert(x); }
                    return make_set_delta<bool>(std::move(resA), std::move(resR));
                }
                if (auto *a = dynamic_cast<const SetDeltaImpl<double> *>(&other)) {
                    auto *b = dynamic_cast<SetDeltaImpl<double> *>(&self);
                    if (!b) throw nb::type_error("Cannot add SetDelta of different types");
                    using T = double; using SetT = std::unordered_set<T>;
                    const SetT &A1 = const_cast<SetDeltaImpl<double> *>(a)->added();
                    const SetT &R1 = const_cast<SetDeltaImpl<double> *>(a)->removed();
                    const SetT &A2 = b->added();
                    const SetT &R2 = b->removed();
                    SetT resA = A1; SetT resR = R1;
                    for (const auto &x : R2) { resA.erase(x); }
                    for (const auto &x : A2) { resR.erase(x); resA.insert(x); }
                    for (const auto &x : R2) { resR.insert(x); }
                    return make_set_delta<double>(std::move(resA), std::move(resR));
                }
                if (auto *a = dynamic_cast<const SetDeltaImpl<engine_date_t> *>(&other)) {
                    auto *b = dynamic_cast<SetDeltaImpl<engine_date_t> *>(&self);
                    if (!b) throw nb::type_error("Cannot add SetDelta of different types");
                    using T = engine_date_t; using SetT = std::unordered_set<T>;
                    const SetT &A1 = const_cast<SetDeltaImpl<engine_date_t> *>(a)->added();
                    const SetT &R1 = const_cast<SetDeltaImpl<engine_date_t> *>(a)->removed();
                    const SetT &A2 = b->added();
                    const SetT &R2 = b->removed();
                    SetT resA = A1; SetT resR = R1;
                    for (const auto &x : R2) { resA.erase(x); }
                    for (const auto &x : A2) { resR.erase(x); resA.insert(x); }
                    for (const auto &x : R2) { resR.insert(x); }
                    return make_set_delta<engine_date_t>(std::move(resA), std::move(resR));
                }
                if (auto *a = dynamic_cast<const SetDeltaImpl<engine_time_t> *>(&other)) {
                    auto *b = dynamic_cast<SetDeltaImpl<engine_time_t> *>(&self);
                    if (!b) throw nb::type_error("Cannot add SetDelta of different types");
                    using T = engine_time_t; using SetT = std::unordered_set<T>;
                    const SetT &A1 = const_cast<SetDeltaImpl<engine_time_t> *>(a)->added();
                    const SetT &R1 = const_cast<SetDeltaImpl<engine_time_t> *>(a)->removed();
                    const SetT &A2 = b->added();
                    const SetT &R2 = b->removed();
                    SetT resA = A1; SetT resR = R1;
                    for (const auto &x : R2) { resA.erase(x); }
                    for (const auto &x : A2) { resR.erase(x); resA.insert(x); }
                    for (const auto &x : R2) { resR.insert(x); }
                    return make_set_delta<engine_time_t>(std::move(resA), std::move(resR));
                }
                if (auto *a = dynamic_cast<const SetDeltaImpl<engine_time_delta_t> *>(&other)) {
                    auto *b = dynamic_cast<SetDeltaImpl<engine_time_delta_t> *>(&self);
                    if (!b) throw nb::type_error("Cannot add SetDelta of different types");
                    using T = engine_time_delta_t; using SetT = std::unordered_set<T>;
                    const SetT &A1 = const_cast<SetDeltaImpl<engine_time_delta_t> *>(a)->added();
                    const SetT &R1 = const_cast<SetDeltaImpl<engine_time_delta_t> *>(a)->removed();
                    const SetT &A2 = b->added();
                    const SetT &R2 = b->removed();
                    SetT resA = A1; SetT resR = R1;
                    for (const auto &x : R2) { resA.erase(x); }
                    for (const auto &x : A2) { resR.erase(x); resA.insert(x); }
                    for (const auto &x : R2) { resR.insert(x); }
                    return make_set_delta<engine_time_delta_t>(std::move(resA), std::move(resR));
                }
                // Fallback: object type composition using Python set operations
                if (auto *a_obj = dynamic_cast<const SetDelta_Object *>(&other)) {
                    auto *b_obj = dynamic_cast<SetDelta_Object *>(&self);
                    if (!b_obj) throw nb::type_error("Cannot add SetDelta of different types");
                    nb::object A1 = a_obj->py_added();
                    nb::object R1 = a_obj->py_removed();
                    nb::object A2 = b_obj->py_added();
                    nb::object R2 = b_obj->py_removed();
                    nb::object py_set = nb::module_::import_("builtins").attr("set");
                    nb::object setA1 = py_set(A1);
                    nb::object setR2 = py_set(R2);
                    nb::object setA2 = py_set(A2);
                    nb::object setR1 = py_set(R1);
                    nb::object resA = setA1.attr("difference")(setR2).attr("union")(setA2);
                    nb::object resR = setR1.attr("difference")(setA2).attr("union")(setR2);
                    nb::object frozenset_ = nb::module_::import_("builtins").attr("frozenset");
                    nb::object resA_f = frozenset_(resA);
                    nb::object resR_f = frozenset_(resR);
                    return SetDelta::ptr(new SetDelta_Object(resA_f, resR_f, self.py_type()));
                }
                throw nb::type_error("Unsupported SetDelta type for addition");
            });

        using SetDelta_bool = SetDeltaImpl<bool>;
        nb::class_<SetDelta_bool, SetDelta>(m, "SetDelta_bool")
            .def(nb::init<const std::unordered_set<bool> &, const std::unordered_set<bool> &>(), "added"_a, "removed"_a);
        using SetDelta_int = SetDeltaImpl<int64_t>;
        nb::class_<SetDelta_int, SetDelta>(m, "SetDelta_int")
            .def(nb::init<const std::unordered_set<int64_t> &, const std::unordered_set<int64_t> &>(), "added"_a, "removed"_a);
        using SetDelta_float = SetDeltaImpl<double>;
        nb::class_<SetDelta_float, SetDelta>(m, "SetDelta_float")
            .def(nb::init<const std::unordered_set<double> &, const std::unordered_set<double> &>(), "added"_a, "removed"_a);
        using SetDelta_date = SetDeltaImpl<engine_date_t>;
        nb::class_<SetDelta_date, SetDelta>(m, "SetDelta_date")
            .def(nb::init<const std::unordered_set<engine_date_t> &, const std::unordered_set<engine_date_t> &>(), "added"_a,
                 "removed"_a);
        using SetDelta_date_time = SetDeltaImpl<engine_time_t>;
        nb::class_<SetDelta_date_time, SetDelta>(m, "SetDelta_date_time")
            .def(nb::init<const std::unordered_set<engine_time_t> &, const std::unordered_set<engine_time_t> &>(), "added"_a,
                 "removed"_a);
        using SetDelta_time_delta = SetDeltaImpl<engine_time_delta_t>;
        nb::class_<SetDelta_time_delta, SetDelta>(m, "SetDelta_time_delta")
            .def(nb::init<const std::unordered_set<engine_time_delta_t> &, const std::unordered_set<engine_time_delta_t> &>(),
                 "added"_a, "removed"_a);

        nb::class_<SetDelta_Object, SetDelta>(m, "SetDelta_object")
            .def(nb::init<nb::object, nb::object, nb::object>(), "added"_a, "removed"_a, "tp"_a);
    }

    SetDelta_Object::SetDelta_Object(nb::object added, nb::object removed, nb::object tp)
        : _tp{std::move(tp)}, _added(std::move(added)), _removed(std::move(removed)) {}

    nb::object SetDelta_Object::py_removed() const { return _removed; }
    nb::object SetDelta_Object::py_type() const { return _tp; }

    bool SetDelta_Object::operator==(const SetDelta &other) const {
        const auto *other_impl = dynamic_cast<const SetDelta_Object *>(&other);
        if (!other_impl) return false;
        return _added.equal(other_impl->_added) && _removed.equal(other_impl->_removed);
    }

    size_t SetDelta_Object::hash() const { return nb::hash(_added) ^ nb::hash(_removed); }

    void TimeSeriesSetOutput::invalidate() {
        clear();
        _reset_last_modified_time();
    }

    TimeSeriesSetOutput &TimeSeriesSetInput::set_output() const { return dynamic_cast<TimeSeriesSetOutput &>(*output()); }

    template <typename T_Key>
    TimeSeriesSetOutput_T<T_Key>::TimeSeriesSetOutput_T(const node_ptr &parent)
        : TimeSeriesSetOutput(parent),
          _contains_ref_outputs{this,
                                new TimeSeriesValueOutputBuilder<bool>(),
                                [](const TimeSeriesOutput &ts, TimeSeriesOutput &ref, const element_type &key) {
                                    reinterpret_cast<TimeSeriesValueOutput<bool> &>(ref).set_value(
                                        reinterpret_cast<const TimeSeriesSetOutput_T<element_type> &>(ts).contains(key));
                                },
                                {}} {
        _is_empty_ref_output =
            dynamic_cast_ref<TimeSeriesValueOutput<bool>>(TimeSeriesValueOutputBuilder<bool>().make_instance(this));
    }

    template <typename T_Key>
    TimeSeriesSetOutput_T<T_Key>::TimeSeriesSetOutput_T(const TimeSeriesType::ptr &parent)
        : TimeSeriesSetOutput(parent),
          _contains_ref_outputs{this,
                                new TimeSeriesValueOutputBuilder<bool>(),
                                [](const TimeSeriesOutput &ts, TimeSeriesOutput &ref, const element_type &key) {
                                    reinterpret_cast<TimeSeriesValueOutput<bool> &>(ref).set_value(
                                        reinterpret_cast<const TimeSeriesSetOutput_T<element_type> &>(ts).contains(key));
                                },
                                {}} {
        _is_empty_ref_output =
            dynamic_cast_ref<TimeSeriesValueOutput<bool>>(TimeSeriesValueOutputBuilder<bool>().make_instance(this));
    }

    template <typename T_Key> nb::object TimeSeriesSetOutput_T<T_Key>::py_value() const {
        if (_py_value.is_none()) {
            nb::set v{};
            for (const T_Key &item : _value) { v.add(item); }
            _py_value = v;
        }
        return _py_value;
    }

    template <typename T_Key> nb::object TimeSeriesSetOutput_T<T_Key>::py_delta_value() const {
        if (modified()) {
            if ((!_added.empty() || !_removed.empty()) && (_py_added.size() == 0 && _py_removed.size() == 0)) {
                for (const auto &item : _added) { _py_added.add(nb::cast(item)); }
                for (const auto &item : _removed) { _py_removed.add(nb::cast(item)); }
            }
            auto delta{make_set_delta<T_Key>(_added, _removed)};
            return nb::cast(delta);
        } else {
            auto delta{make_set_delta<T_Key>({}, {})};
            return nb::cast(delta);
        }
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::apply_result(nb::object value) {
        if (value.is_none()) { return; }
        _py_value = nb::none();
        _py_removed.clear();
        _py_added.clear();
        _added.clear();
        _removed.clear();
        try {
            if (nb::isinstance<SetDelta>(value)) {
                auto delta{nb::cast<SetDelta *>(value)};
                auto added   = delta->py_added();
                auto removed = delta->py_removed();

                if (added.is_valid() && !added.is_none()) {
                    for (const auto &e : nb::iter(added)) {
                        auto k{nb::cast<T_Key>(e)};
                        if (!_value.contains(k)) { _add(k); }
                    }
                }

                for (const auto &e : nb::iter(removed)) {
                    auto k{nb::cast<T_Key>(e)};
                    if (_added.contains(k)) { throw std::runtime_error("Cannot remove and add the same element"); }
                    if (_value.contains(k)) { _remove(k); }
                }
            } else {
                auto removed{get_removed()};
                auto v = nb::set(value);
                for (const auto &r : v) {
                    if (!nb::isinstance(r, removed)) {
                        auto k{nb::cast<T_Key>(r)};
                        if (!_value.contains(k)) { _add(k); }
                    } else {
                        auto item{nb::cast<T_Key>(r.attr("item"))};
                        if (_value.contains(item)) {
                            if (_added.contains(item)) { throw std::runtime_error("Cannot remove and add the same element"); }
                            _remove(item);
                        }
                    }
                }
            }
        } catch (const std::exception &e) { throw std::runtime_error(std::string("Error in apply_result: ") + e.what()); }

        _post_modify();
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::_add(const element_type &item) {
        _value.emplace(item);
        _added.emplace(item);
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::_remove(const element_type &item) {
        _value.erase(item);
        _removed.emplace(item);
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::_post_modify() {
        // Match Python: if self._added or self._removed or not self.valid:
        //     self.mark_modified()
        // Note: has_added() returns true if _added is NOT empty
        auto has_additions{has_added()};
        auto has_removals{has_removed()};
        auto is_empty{empty()};
        if (has_additions || has_removals || !valid()) {
            mark_modified();
            if (has_additions && _is_empty_ref_output->valid() && _is_empty_ref_output->value()) {
                _is_empty_ref_output->set_value(false);
            } else if (has_removals && is_empty) {
                _is_empty_ref_output->set_value(true);
            }
            _contains_ref_outputs.update_all(_added.begin(), _added.end());
            _contains_ref_outputs.update_all(_removed.begin(), _removed.end());
        }
        owning_graph().evaluation_engine_api().add_after_evaluation_notification([this]() {
            _added.clear();
            _removed.clear();
            _py_added.clear();
            _py_removed.clear();
        });
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::clear() {
        _added.clear();
        _removed.clear();
        _value.clear();
        _is_empty_ref_output->clear();
        _py_value = nb::none();
        _py_added.clear();
        _py_removed.clear();
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::copy_from_output(const TimeSeriesOutput &output) {
        auto &output_obj = dynamic_cast<const TimeSeriesSetOutput_T<T_Key> &>(output);

        _added.clear();
        _removed.clear();
        _is_empty_ref_output->clear();
        _py_value = nb::none();
        _py_added.clear();
        _py_removed.clear();

        // Calculate added elements (elements in output but not in current value)
        for (const auto &item : output_obj._value) {
            if (!_value.contains(item)) { _add(item); }
        }

        // Calculate removed elements (elements in current value but not in output)
        for (const auto &item : _value) {
            if (!output_obj._value.contains(item)) { _remove(item); }
        }

        if (_added.size() > 0 || _removed.size() > 0) {
            _value = collection_type(output_obj._value);
            mark_modified();
        }
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::copy_from_input(const TimeSeriesInput &input) {
        auto &input_obj = dynamic_cast<const TimeSeriesSetInput_T<T_Key> &>(input);

        _added.clear();
        _removed.clear();
        _is_empty_ref_output->clear();
        _py_value = nb::none();
        _py_added.clear();
        _py_removed.clear();

        auto input_value{input_obj.value()};
        // Calculate added elements (elements in input but not in current value)
        for (const auto &item : input_value) {
            if (!_value.contains(item)) { _add(item); }
        }

        for (const auto &item : _value) {
            if (!input_value.contains(item)) { _remove(item); }
        }

        if (_added.size() > 0 || _removed.size() > 0) { mark_modified(); }
    }

    template <typename T_Key> bool TimeSeriesSetOutput_T<T_Key>::py_contains(const nb::object &item) const {
        return contains(nb::cast<element_type>(item));
    }

    template <typename T_Key> bool TimeSeriesSetOutput_T<T_Key>::contains(const element_type &item) const {
        return _value.contains(item);
    }

    template <typename T_Key> size_t TimeSeriesSetOutput_T<T_Key>::size() const { return _value.size(); }

    template <typename T_Key> const nb::object TimeSeriesSetOutput_T<T_Key>::py_values() const { return py_value(); }

    template <typename T_Key> const nb::object TimeSeriesSetOutput_T<T_Key>::py_added() const {
        if (_py_added.size() == 0 && !_added.empty()) {
            for (const auto &item : _added) { _py_added.add(nb::cast(item)); }
        }
        return _py_added;
    }

    template <typename T_Key>
    const typename TimeSeriesSetOutput_T<T_Key>::collection_type &TimeSeriesSetOutput_T<T_Key>::added() const {
        return _added;
    }

    template <typename T_Key> bool TimeSeriesSetOutput_T<T_Key>::has_added() const { return !_added.empty(); }

    template <typename T_Key> bool TimeSeriesSetOutput_T<T_Key>::py_was_added(const nb::object &item) const {
        return was_added(nb::cast<element_type>(item));
    }

    template <typename T_Key> bool TimeSeriesSetOutput_T<T_Key>::was_added(const element_type &item) const {
        return _added.contains(item);
    }

    template <typename T_Key> const nb::object TimeSeriesSetOutput_T<T_Key>::py_removed() const {
        if (_py_removed.size() == 0 && !_removed.empty()) {
            for (const auto &item : _removed) { _py_removed.add(nb::cast(item)); }
        }
        return _py_removed;
    }

    template <typename T_Key>
    const typename TimeSeriesSetOutput_T<T_Key>::collection_type &TimeSeriesSetOutput_T<T_Key>::removed() const {
        return _removed;
    }

    template <typename T_Key> bool TimeSeriesSetOutput_T<T_Key>::has_removed() const { return !_removed.empty(); }

    template <typename T_Key> bool TimeSeriesSetOutput_T<T_Key>::py_was_removed(const nb::object &item) const {
        return was_removed(nb::cast<element_type>(item));
    }

    template <typename T_Key> bool TimeSeriesSetOutput_T<T_Key>::was_removed(const element_type &item) const {
        return _removed.contains(item);
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::py_remove(const nb::object &key) {
        if (key.is_none()) { return; }
        remove(nb::cast<element_type>(key));
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::remove(const element_type &key) {
        if (contains(key)) {
            _remove(key);
            mark_modified();
        }
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::py_add(const nb::object &key) {
        if (key.is_none()) { return; }
        add(nb::cast<element_type>(key));
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::add(const element_type &key) {
        if (!contains(key)) {
            _add(key);
            mark_modified();
        }
    }

    template <typename T_Key> bool TimeSeriesSetOutput_T<T_Key>::empty() const { return _value.empty(); }

    template <typename T_Key>
    TimeSeriesValueOutput<bool>::ptr TimeSeriesSetOutput_T<T_Key>::get_contains_output(const nb::object &item,
                                                                                       const nb::object &requester) {
        return _contains_ref_outputs.create_or_increment(nb::cast<element_type>(item), static_cast<void *>(requester.ptr()));
    }

    template <typename T_Key>
    void TimeSeriesSetOutput_T<T_Key>::release_contains_output(const nb::object &item, const nb::object &requester) {
        _contains_ref_outputs.release(nb::cast<element_type>(item), static_cast<void *>(requester.ptr()));
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::post_modify() { _post_modify(); }

    nb::object TimeSeriesSetInput::py_value() const { return output()->py_value(); }

    nb::object TimeSeriesSetInput::py_delta_value() const { return output()->py_delta_value(); }

    bool TimeSeriesSetInput::py_contains(const nb::object &item) const { return set_output().py_contains(item); }

    size_t TimeSeriesSetInput::size() const { return set_output().size(); }

    const nb::object TimeSeriesSetInput::py_values() const { return set_output().py_values(); }

    const nb::object TimeSeriesSetInput::py_added() const {
        if (has_prev_output()) {
            // Get current values as a set
            auto current_values = nb::set(py_values());
            // Get previous state (old values + removed - added)
            auto prev_values  = nb::set(prev_output().py_values());
            auto prev_removed = nb::set(prev_output().py_removed());
            auto prev_added   = nb::set(prev_output().py_added());
            auto old_state    = (prev_values | prev_removed) - prev_added;
            // Added items are current values minus old_state
            return current_values - old_state;
        } else {
            return sampled() ? py_values() : set_output().py_added();
        }
    }

    bool TimeSeriesSetInput::py_was_added(const nb::object &item) const {
        if (has_prev_output()) {
            return set_output().py_was_added(item) && !prev_output().py_contains(item);
        } else if (sampled()) {
            return py_contains(item);
        } else {
            return set_output().py_was_added(item);
        }
    }

    const nb::object TimeSeriesSetInput::py_removed() const {
        if (has_prev_output()) {
            auto prev_values    = nb::set(prev_output().py_values());
            auto prev_removed   = nb::set(prev_output().py_removed());
            auto prev_added     = nb::set(prev_output().py_added());
            auto current_values = nb::set(py_values());

            return (prev_values | prev_removed) - prev_added - current_values;
        } else if (sampled()) {
            return nb::set();
        } else if (has_output()) {
            return set_output().py_removed();
        } else {
            return nb::set();
        }
    }

    bool TimeSeriesSetInput::py_was_removed(const nb::object &item) const {
        if (has_prev_output()) {
            return prev_output().py_contains(item) && !py_contains(item);
        } else if (sampled()) {
            return false;
        } else {
            return set_output().py_was_removed(item);
        }
    }

    const TimeSeriesSetOutput &TimeSeriesSetInput::prev_output() const { return *_prev_output; }

    bool TimeSeriesSetInput::has_prev_output() const { return _prev_output != nullptr; }

    void TimeSeriesSetInput::reset_prev() { _prev_output = nullptr; }

    bool TimeSeriesSetInput::do_bind_output(TimeSeriesOutput::ptr output) {
        if (has_output()) {
            _prev_output = &set_output();
            // Clean up after the engine cycle is complete
            owning_graph().evaluation_engine_api().add_after_evaluation_notification([this]() { reset_prev(); });
        }
        return TimeSeriesInput::do_bind_output(output);
    }

    void TimeSeriesSetInput::do_un_bind_output() {
        if (has_output()) {
            _prev_output = &set_output();
            owning_graph().evaluation_engine_api().add_after_evaluation_notification([this]() { reset_prev(); });
        }
        TimeSeriesInput::do_un_bind_output();
    }

    nb::object SetDelta_Object::py_added() const { return _added; }

    template struct TimeSeriesSetInput_T<bool>;
    template struct TimeSeriesSetInput_T<int64_t>;
    template struct TimeSeriesSetInput_T<double>;
    template struct TimeSeriesSetInput_T<engine_date_t>;
    template struct TimeSeriesSetInput_T<engine_time_t>;
    template struct TimeSeriesSetInput_T<engine_time_delta_t>;
    template struct TimeSeriesSetInput_T<nb::object>;

    template struct TimeSeriesSetOutput_T<bool>;
    template struct TimeSeriesSetOutput_T<int64_t>;
    template struct TimeSeriesSetOutput_T<double>;
    template struct TimeSeriesSetOutput_T<engine_date_t>;
    template struct TimeSeriesSetOutput_T<engine_time_t>;
    template struct TimeSeriesSetOutput_T<engine_time_delta_t>;
    template struct TimeSeriesSetOutput_T<nb::object>;

    void tss_register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSetInput, TimeSeriesInput>(m, "TimeSeriesSetInput")
            .def("__contains__", &TimeSeriesSetInput::py_contains)
            .def("__len__", &TimeSeriesSetInput::size)
            .def("values", &TimeSeriesSetInput::py_values)
            .def("added", &TimeSeriesSetInput::py_added)
            .def("removed", &TimeSeriesSetInput::py_removed)
            .def("was_added", &TimeSeriesSetInput::py_was_added)
            .def("was_removed", &TimeSeriesSetInput::py_was_removed);

        nb::class_<TimeSeriesSetInput_T<bool>, TimeSeriesSetInput>(m, "TimeSeriesSetInput_Bool");
        nb::class_<TimeSeriesSetInput_T<int64_t>, TimeSeriesSetInput>(m, "TimeSeriesSetInput_Int");
        nb::class_<TimeSeriesSetInput_T<double>, TimeSeriesSetInput>(m, "TimeSeriesSetInput_Float");
        nb::class_<TimeSeriesSetInput_T<engine_date_t>, TimeSeriesSetInput>(m, "TimeSeriesSetInput_Date");
        nb::class_<TimeSeriesSetInput_T<engine_time_t>, TimeSeriesSetInput>(m, "TimeSeriesSetInput_DateTime");
        nb::class_<TimeSeriesSetInput_T<engine_time_delta_t>, TimeSeriesSetInput>(m, "TimeSeriesSetInput_TimeDelta");
        nb::class_<TimeSeriesSetInput_T<nb::object>, TimeSeriesSetInput>(m, "TimeSeriesSetInput_object");

        nb::class_<TimeSeriesSetOutput, TimeSeriesOutput>(m, "TimeSeriesSetOutput")
            .def("__contains__", &TimeSeriesSetOutput::py_contains)
            .def("__len__", &TimeSeriesSetOutput::size)
            .def("values", &TimeSeriesSetOutput::py_values)
            .def("added", &TimeSeriesSetOutput::py_added)
            .def("removed", &TimeSeriesSetOutput::py_removed)
            .def("was_added", &TimeSeriesSetOutput::py_was_added)
            .def("was_removed", &TimeSeriesSetOutput::py_was_removed)
            .def("get_contains_output", &TimeSeriesSetOutput::get_contains_output)
            .def("release_contains_output", &TimeSeriesSetOutput::release_contains_output);

        nb::class_<TimeSeriesSetOutput_T<bool>, TimeSeriesSetOutput>(m, "TimeSeriesSetOutput_Bool");
        nb::class_<TimeSeriesSetOutput_T<int64_t>, TimeSeriesSetOutput>(m, "TimeSeriesSetOutput_Int");
        nb::class_<TimeSeriesSetOutput_T<double>, TimeSeriesSetOutput>(m, "TimeSeriesSetOutput_Float");
        nb::class_<TimeSeriesSetOutput_T<engine_date_t>, TimeSeriesSetOutput>(m, "TimeSeriesSetOutput_Date");
        nb::class_<TimeSeriesSetOutput_T<engine_time_t>, TimeSeriesSetOutput>(m, "TimeSeriesSetOutput_DateTime");
        nb::class_<TimeSeriesSetOutput_T<engine_time_delta_t>, TimeSeriesSetOutput>(m, "TimeSeriesSetOutput_TimeDelta");
        nb::class_<TimeSeriesSetOutput_T<nb::object>, TimeSeriesSetOutput>(m, "TimeSeriesSetOutput_object");
    }
}  // namespace hgraph

/**
 * Runtime view structs handed to Python user nodes during evaluation:
 * the lazy TimeSeries/Output views, per-node python STATE, scheduler and
 * clock views, recordable state, and the guarded GlobalState view. See
 * docs/source/developer_guide/python_bridge.rst (GIL boundaries; the views
 * are call-scoped via PyTsGuard - python must not retain them).
 */
#ifndef HGRAPH_PYTHON_PY_RUNTIME_H
#define HGRAPH_PYTHON_PY_RUNTIME_H

#include "py_carriers.h"

namespace hgraph::python_bridge
{
    /** Applies a python node's return value to its output (REF whole-move,
        TSS frozenset replace semantics, canonical-delta apply). Defined in
        py_nodes.cpp. */
    void apply_py_result(nb::handle result, Out<TsVar<"O">> &out);

    /**
     * ONE compute/sink operator for ANY arity (Howard's review: per-arity
     * stubs do not scale): the argument ports pack into a STRUCTURAL
     * un-named TSB, wiring-time SCALARS ride a list-of-Any scalar, and the
     * LAYOUT string (part of node identity) maps the python call positions:
     * ``t`` = next ts field, ``s`` = next scalar, ``S`` = STATE namespace,
     * ``c`` = CLOCK, ``d`` = SCHEDULER, ``e`` = EvaluationEngineApi. All ts
     * fields must hold values before the python function is called (the
     * all-valid gate).
     */
    struct PyStateRef
    {
        PyObject *ns{nullptr};   ///< a SimpleNamespace, lazily created (per-node python STATE)
        friend bool operator==(const PyStateRef &, const PyStateRef &) noexcept = default;
    };

    struct PyEvalClock
    {
        DateTime evaluation_time{};
        DateTime now{};
        TimeDelta cycle_time{};
        DateTime next_cycle_evaluation_time{};

        explicit PyEvalClock(EvaluationClockView clock) noexcept
            : evaluation_time(clock.evaluation_time()), now(clock.now()), cycle_time(clock.cycle_time()),
              next_cycle_evaluation_time(clock.next_cycle_evaluation_time())
        {
        }
    };

    struct PyScheduler
    {
        NodeScheduler scheduler;
    };

    [[nodiscard]] inline nb::object py_state_namespace(State<PyStateRef> &state)
    {
        PyStateRef ref = state.get();
        if (ref.ns == nullptr)
        {
            nb::object ns = nb::module_::import_("types").attr("SimpleNamespace")();
            ref.ns        = ns.release().ptr();
            state.set(ref);
        }
        return nb::borrow(nb::handle(ref.ns));
    }

    inline void py_release_state(State<PyStateRef> &state)
    {
        PyStateRef ref = state.get();
        if (ref.ns != nullptr)
        {
            nb::gil_scoped_acquire gil;
            nb::steal(nb::handle(ref.ns));   // drop the held reference
            state.set(PyStateRef{});
        }
    }

    /** Call-scope lifetime guard: python must not use a view after its eval. */
    struct PyTsGuard
    {
        bool alive{true};
    };

    struct PyRuntimeGlobalState
    {
        GlobalStateView            state;
        std::shared_ptr<PyTsGuard> guard;

        [[nodiscard]] GlobalStateView checked() const
        {
            if (guard == nullptr || !guard->alive)
            {
                throw std::logic_error("a GlobalState view was accessed outside its node's evaluation");
            }
            return state;
        }
    };

    /** Call-scoped Python projection over the native engine-control view. */
    struct PyEvaluationEngineApi
    {
        EngineControlView           engine;
        std::shared_ptr<PyTsGuard> guard;

        [[nodiscard]] EngineControlView checked() const
        {
            if (guard == nullptr || !guard->alive)
            {
                throw std::logic_error(
                    "an EvaluationEngineApi view was accessed outside its node's evaluation");
            }
            if (!engine.valid())
            {
                throw std::logic_error("the active graph has no evaluation engine");
            }
            return engine;
        }
    };

    /**
     * The hgraph TimeSeries object handed to python user nodes: a LAZY,
     * C++-bound view over the node's live input - nothing converts unless
     * accessed. Kind-specific methods dispatch on the schema (TS/TSS/TSD/
     * TSL/TSB); child access returns child views sharing the same guard.
     */
    /** Mutable, call-scoped view of the node's own output (``_output``).

        All writes go through the native TSOutput mutation API. Child views
        share the callback guard, so Python cannot retain an output cursor
        beyond the evaluation that produced it. */
    struct PyOutput
    {
        TSOutputHandle             handle;
        DateTime                   now{};
        std::shared_ptr<PyTsGuard> guard;

        [[nodiscard]] TSOutputView checked() const
        {
            if (guard == nullptr || !guard->alive)
            {
                throw std::logic_error("an output view was accessed outside its node's evaluation");
            }
            return handle.view(now);
        }

        [[nodiscard]] bool valid() const
        {
            auto view = checked();
            return view.valid() && view.data_view().has_current_value();
        }

        [[nodiscard]] nb::object value() const
        {
            auto view = checked();
            if (!view.valid() || !view.data_view().has_current_value()) { return nb::none(); }
            return value_to_py(view.data_view().value());
        }

        void set_value(nb::object value) const
        {
            auto view = checked();
            if (value.is_none()) { return; }
            Out<TsVar<"O">> out{std::move(view), now};
            apply_py_result(value, out);
        }

        [[nodiscard]] PyOutput child(nb::handle key) const
        {
            auto view = checked();
            switch (view.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    Value key_value = py_to_value_as(key, view.schema()->key_type());
                    auto  dict      = view.as_dict();
                    if (!dict.contains(key_value.view()))
                    {
                        throw nb::key_error("output key not found");
                    }
                    return PyOutput{dict.at(key_value.view()).handle(), now, guard};
                }
                case TSTypeKind::TSL: {
                    auto list = view.as_list();
                    return PyOutput{list.at(nb::cast<std::size_t>(key)).handle(), now, guard};
                }
                case TSTypeKind::TSB: {
                    auto bundle = view.as_bundle();
                    TSOutputView result = nb::isinstance<nb::str>(key)
                                              ? bundle.field(nb::cast<std::string>(key))
                                              : bundle.at(nb::cast<std::size_t>(key));
                    return PyOutput{result.handle(), now, guard};
                }
                default: throw nb::type_error("this output kind has no children");
            }
        }

        [[nodiscard]] PyOutput get_or_create(nb::handle key) const
        {
            auto view = checked();
            if (view.schema()->kind != TSTypeKind::TSD)
            {
                throw nb::type_error("get_or_create: not a keyed output");
            }
            Value key_value = py_to_value_as(key, view.schema()->key_type());
            auto  mutation  = view.as_dict().begin_mutation(now);
            auto  child     = mutation.at(key_value.view());
            return PyOutput{TSOutputHandle{view.output(), child}, now, guard};
        }

        void erase(nb::handle key) const
        {
            auto view = checked();
            if (view.schema()->kind != TSTypeKind::TSD)
            {
                throw nb::type_error("item deletion: not a keyed output");
            }
            Value key_value = py_to_value_as(key, view.schema()->key_type());
            static_cast<void>(view.as_dict().begin_mutation(now).erase(key_value.view()));
        }

        void clear() const
        {
            auto view = checked();
            switch (view.schema()->kind)
            {
                case TSTypeKind::TSD: view.as_dict().begin_mutation(now).clear(); return;
                case TSTypeKind::TSS: view.as_set().begin_mutation(now).clear(); return;
                default: throw nb::type_error("clear: not a mutable collection output");
            }
        }

        [[nodiscard]] bool contains(nb::handle key) const
        {
            auto view = checked();
            switch (view.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    Value key_value = py_to_value_as(key, view.schema()->key_type());
                    return view.as_dict().contains(key_value.view());
                }
                case TSTypeKind::TSS: {
                    Value element = py_to_value_as(key, view.schema()->value_schema->element_type);
                    return view.as_set().contains(element.view());
                }
                default: throw nb::type_error("contains: not a keyed collection output");
            }
        }

        [[nodiscard]] std::size_t size() const
        {
            auto view = checked();
            switch (view.schema()->kind)
            {
                case TSTypeKind::TSD: return view.as_dict().size();
                case TSTypeKind::TSS: return view.as_set().size();
                case TSTypeKind::TSL: return view.as_list().size();
                case TSTypeKind::TSB: return view.as_bundle().size();
                default: throw nb::type_error("this output kind has no size");
            }
        }

        [[nodiscard]] nb::list removed_keys() const
        {
            nb::list result;
            auto     view = checked();
            auto     dict = view.as_dict();
            for (const ValueView &key : dict.removed_keys()) { result.append(value_to_py(key)); }
            return result;
        }

        [[nodiscard]] bool add(nb::handle value) const
        {
            auto view = checked();
            if (view.schema()->kind != TSTypeKind::TSS) { throw nb::type_error("add: not a set output"); }
            Value element = py_to_value_as(value, view.schema()->value_schema->element_type);
            return view.as_set().begin_mutation(now).add(element.view());
        }

        [[nodiscard]] bool remove(nb::handle value) const
        {
            auto view = checked();
            if (view.schema()->kind != TSTypeKind::TSS) { throw nb::type_error("remove: not a set output"); }
            Value element = py_to_value_as(value, view.schema()->value_schema->element_type);
            return view.as_set().begin_mutation(now).remove(element.view());
        }
    };

    /** Mutable, call-scoped view over a node's C++ recordable-state output. */
    struct PyRecordableState
    {
        TSOutputHandle             handle;
        DateTime                   now{};
        std::shared_ptr<PyTsGuard> guard;

        [[nodiscard]] TSOutputView checked() const
        {
            if (guard == nullptr || !guard->alive)
            {
                throw std::logic_error(
                    "a recordable-state view was accessed outside its node's evaluation");
            }
            return handle.view(now);
        }

        [[nodiscard]] bool valid() const
        {
            auto view = checked();
            return view.valid() && view.data_view().has_current_value();
        }

        [[nodiscard]] bool modified() const { return checked().modified(); }

        [[nodiscard]] nb::object value() const
        {
            auto view = checked();
            return view.valid() && view.data_view().has_current_value()
                       ? value_to_py(view.data_view().value())
                       : nb::none();
        }

        void set_value(nb::handle value) const
        {
            auto  view  = checked();
            Value delta = py_to_delta(value, view.schema());
            apply_delta(view, delta.view());
        }

        [[nodiscard]] PyRecordableState child(nb::handle key) const
        {
            auto view = checked();
            switch (view.schema()->kind)
            {
                case TSTypeKind::TSB: {
                    auto bundle = view.as_bundle();
                    TSOutputView child = nb::isinstance<nb::str>(key)
                                             ? bundle.field(nb::cast<std::string>(key))
                                             : bundle.at(nb::cast<std::size_t>(key));
                    return PyRecordableState{child.handle(), now, guard};
                }
                case TSTypeKind::TSL: {
                    auto list = view.as_list();
                    TSOutputView child = list.at(nb::cast<std::size_t>(key));
                    return PyRecordableState{child.handle(), now, guard};
                }
                default:
                    throw nb::type_error(
                        "recordable-state value has no statically addressable children");
            }
        }
    };

    struct PyTimeSeries
    {
        TSInputView                view;
        std::shared_ptr<PyTsGuard> guard;

        /** Throws when the view outlived its node's evaluation. */
        void require_alive() const
        {
            if (guard == nullptr || !guard->alive)
            {
                throw std::logic_error("a TimeSeries view was accessed outside its node's evaluation");
            }
        }

        [[nodiscard]] const TSInputView &checked() const
        {
            require_alive();
            return view;
        }

        [[nodiscard]] TSTypeKind kind() const { return checked().schema()->kind; }

        [[nodiscard]] nb::object value() const
        {
            const auto &v = checked();
            if (!v.valid()) { return nb::none(); }   // hgraph: invalid reads as None
            if (v.schema() != nullptr && v.schema()->kind == TSTypeKind::TSL)
            {
                // hgraph parity: a TSL's value is a tuple of child values
                // (invalid children read as None).
                auto list = const_cast<TSInputView &>(v).as_list();
                nb::list out;
                for (auto &&[index, child] : list.items())
                {
                    static_cast<void>(index);
                    out.append(child.valid() ? value_to_py(child.value()) : nb::none());
                }
                return nb::tuple(out);
            }
            if (v.schema() != nullptr && v.schema()->kind == TSTypeKind::REF)
            {
                // A REF input's value is the REFERENCE - TSInputView::
                // reference() reads the to-REF alternative's populated value
                // (peered at the true upstream output).
                return nb::cast(python_bridge::PyOpaqueRef{Value{v.reference()}});
            }
            nb::object result = value_to_py(v.value());
            if (v.schema() != nullptr && v.schema()->kind == TSTypeKind::TS && PySet_CheckExact(result.ptr()))
            {
                // hgraph parity: a scalar set is a FROZENSET (TSS values stay
                // mutable sets) - returning it to a TSS output means replace.
                return nb::steal(PyFrozenSet_New(result.ptr()));
            }
            return result;
        }

        [[nodiscard]] nb::object delta_value() const
        {
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TS: {
                    nb::object result = value_to_py(ts.value());
                    if (PySet_CheckExact(result.ptr()))
                    {
                        // hgraph parity: scalar sets are frozensets.
                        return nb::steal(PyFrozenSet_New(result.ptr()));
                    }
                    return result;
                }
                case TSTypeKind::TSD: {
                    // hgraph's friendly shape: {key: child delta, removed: REMOVED}
                    nb::dict result;
                    auto dict = ts.as_dict();
                    for (auto &&[key, child] : dict.modified_items())
                    {
                        result[value_to_py(key)] = PyTimeSeries{std::move(child), guard}.delta_value();
                    }
                    for (const ValueView &key : dict.removed_keys()) { result[value_to_py(key)] = removed_sentinel(); }
                    return result;
                }
                case TSTypeKind::TSS: {
                    // hgraph's SetDelta shape: added items plain, removals
                    // wrapped in Removed(...). Built as the registered
                    // SetDelta class so a node returning it applies as a
                    // DELTA (a plain frozenset return replaces the value).
                    auto     set = ts.as_set();
                    nb::list items;
                    for (const ValueView &element : set.added()) { items.append(value_to_py(element)); }
                    nb::object &removed_cls = python_bridge::removed_class_slot();
                    for (const ValueView &element : set.removed())
                    {
                        items.append(removed_cls.is_valid() ? removed_cls(value_to_py(element))
                                                            : value_to_py(element));
                    }
                    nb::object result = nb::steal(PyFrozenSet_New(nb::list(items).ptr()));
                    nb::object &set_delta_cls = python_bridge::set_delta_class_slot();
                    return set_delta_cls.is_valid() ? set_delta_cls(result) : result;
                }
                default: {
                    Value delta = capture_delta(ts);
                    return delta.has_value() ? value_to_py(delta.view()) : nb::none();
                }
            }
        }

        [[nodiscard]] bool modified() const { return checked().modified(); }
        [[nodiscard]] bool valid() const { return checked().valid(); }
        [[nodiscard]] bool all_valid() const { return checked().all_valid(); }
        [[nodiscard]] DateTime last_modified_time() const { return checked().last_modified_time(); }

        // --- TSS ---
        [[nodiscard]] nb::object added() const
        {
            nb::list items;
            auto set = checked().as_set();
            for (const ValueView &element : set.added()) { items.append(value_to_py(element)); }
            return nb::steal(PyFrozenSet_New(nb::list(items).ptr()));
        }

        [[nodiscard]] nb::object removed() const
        {
            nb::list items;
            auto set = checked().as_set();
            for (const ValueView &element : set.removed()) { items.append(value_to_py(element)); }
            return nb::steal(PyFrozenSet_New(nb::list(items).ptr()));
        }

        // --- TSD / TSL / TSB children (share the guard) ---
        [[nodiscard]] PyTimeSeries child_at(nb::handle key) const
        {
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    Value key_value = py_to_value_as(key, ts.schema()->key_type());
                    return PyTimeSeries{ts.as_dict().at(key_value.view()), guard};
                }
                case TSTypeKind::TSL: {
                    auto list = ts.as_list();
                    return PyTimeSeries{list[nb::cast<std::size_t>(key)], guard};
                }
                case TSTypeKind::TSB: {
                    auto bundle = ts.as_bundle();
                    if (nb::isinstance<nb::str>(key))
                    {
                        return PyTimeSeries{bundle.field(nb::cast<std::string>(key)), guard};
                    }
                    return PyTimeSeries{bundle[nb::cast<std::size_t>(key)], guard};
                }
                default: throw nb::type_error("this time-series kind has no children");
            }
        }

        [[nodiscard]] std::size_t size() const
        {
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TSD: return ts.as_dict().size();
                case TSTypeKind::TSL: return ts.as_list().size();
                case TSTypeKind::TSB: return ts.as_bundle().size();
                case TSTypeKind::TSS: return ts.as_set().size();
                default: throw nb::type_error("this time-series kind has no size");
            }
        }

        [[nodiscard]] nb::list keys() const
        {
            nb::list result;
            auto dict = checked().as_dict();
            for (const ValueView &key : dict.keys()) { result.append(value_to_py(key)); }
            return result;
        }

        [[nodiscard]] nb::list modified_keys() const
        {
            nb::list result;
            auto dict = checked().as_dict();
            for (const auto &[key, child] : dict.modified_items()) { result.append(value_to_py(key)); }
            return result;
        }

        [[nodiscard]] nb::list modified_items() const
        {
            nb::list result;
            auto dict = checked().as_dict();
            for (auto &&[key, child] : dict.modified_items())
            {
                result.append(nb::make_tuple(value_to_py(key), PyTimeSeries{std::move(child), guard}));
            }
            return result;
        }

        [[nodiscard]] nb::list modified_values() const
        {
            nb::list result;
            auto dict = checked().as_dict();
            for (auto &&[key, child] : dict.modified_items())
            {
                static_cast<void>(key);
                result.append(PyTimeSeries{std::move(child), guard});
            }
            return result;
        }

        /** Child views in order (TSB fields / TSD entries / TSL elements). */
        [[nodiscard]] nb::list values() const
        {
            nb::list    result;
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    auto dict = ts.as_dict();
                    for (const ValueView &key : dict.keys())
                    {
                        result.append(nb::cast(PyTimeSeries{dict.at(key), guard}));
                    }
                    return result;
                }
                case TSTypeKind::TSB: {
                    auto bundle = ts.as_bundle();
                    for (std::size_t index = 0; index < bundle.size(); ++index)
                    {
                        result.append(nb::cast(PyTimeSeries{bundle[index], guard}));
                    }
                    return result;
                }
                case TSTypeKind::TSL: {
                    auto list = ts.as_list();
                    for (std::size_t index = 0; index < list.size(); ++index)
                    {
                        result.append(nb::cast(PyTimeSeries{list[index], guard}));
                    }
                    return result;
                }
                default: throw nb::type_error("values(): not a container time-series");
            }
        }

        [[nodiscard]] nb::list removed_keys() const
        {
            nb::list result;
            auto dict = checked().as_dict();
            for (const ValueView &key : dict.removed_keys()) { result.append(value_to_py(key)); }
            return result;
        }

        [[nodiscard]] bool contains(nb::handle key) const
        {
            const auto &ts = checked();
            if (ts.schema()->kind == TSTypeKind::TSD)
            {
                Value key_value = py_to_value_as(key, ts.schema()->key_type());
                return ts.as_dict().contains(key_value.view());
            }
            throw nb::type_error("contains: not a keyed time-series");
        }

        /** The python REMOVED sentinel, registered by the hgraph package at import. */
        [[nodiscard]] static nb::object &removed_slot() { return removed_sentinel_slot(); }

        [[nodiscard]] static nb::object removed_sentinel()
        {
            nb::object &slot = removed_slot();
            return slot.is_valid() ? slot : nb::none();
        }
    };
}  // namespace hgraph::python_bridge

template <>
struct std::hash<hgraph::python_bridge::PyStateRef>
{
    [[nodiscard]] std::size_t operator()(const hgraph::python_bridge::PyStateRef &ref) const noexcept
    {
        return std::hash<const void *>{}(ref.ns);
    }
};

namespace hgraph::static_schema_detail
{
    using python_bridge::PyStateRef;

    template <>
    struct scalar_name<PyStateRef>
    {
        static constexpr std::string_view value{"py_state"};
    };

}  // namespace hgraph::static_schema_detail

#endif  // HGRAPH_PYTHON_PY_RUNTIME_H

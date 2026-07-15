/**
 * Python user nodes (@compute_node / @generator / @sink_node).
 * Ruling: graph-thread only, both modes; the GIL is RELEASED on
 * entering the run loop and ACQUIRED around each python call; values
 * cross the boundary through the module converters.
 *
 * Everything here is TU-local by design (the node/op structs' typeid IS
 * node identity, but registration happens only in this file through
 * register_python_overloads()).
 */
#include "py_runtime.h"
#include "py_bindings.h"

namespace nb = nanobind;
using namespace hgraph;
using namespace hgraph::python_bridge;

namespace hgraph::python_bridge
{
    void apply_py_result(nb::handle result, Out<TsVar<"O">> &out)
    {
        if (result.is_none()) { return; }
        const auto &erased = static_cast<const TSOutputView &>(out);
        if (erased.schema() != nullptr && erased.schema()->kind == TSTypeKind::REF)
        {
            // REF outputs carry OPAQUE reference values (Howard's ruling):
            // move the reference in whole - REF data has no delta ops. The
            // Value is rebuilt from the raw TimeSeriesReference (the same
            // construction the C++ runtime uses) so its binding matches the
            // output plan regardless of which side produced the reference.
            Value raw = py_to_value(result);
            TimeSeriesReference reference = raw.view().checked_as<TimeSeriesReference>();
            // SAME-REFERENCE dedup (the recurring rule): a re-evaluation
            // returning the unchanged reference must not re-tick consumers -
            // every re-publish samples the whole target on rebind.
            if (erased.data_view().has_current_value() &&
                erased.data_view().value().checked_as<TimeSeriesReference>() == reference)
            {
                return;
            }
            auto mutation = erased.begin_mutation(erased.evaluation_time());
            if (!mutation.move_value_from(Value{std::move(reference)}))
            {
                throw std::logic_error("REF output failed to move the reference value");
            }
            return;
        }
        nb::object shaped    = nb::borrow(result);
        const bool tss       = erased.schema()->kind == TSTypeKind::TSS;
        const bool has_value = tss && erased.data_view().has_current_value();
        nb::object current   = tss && has_value ? value_to_py(erased.data_view().value())
                                                : nb::steal(PyFrozenSet_New(nullptr));
        if (tss && PyFrozenSet_CheckExact(result.ptr()))
        {
            // hgraph parity (PythonTimeSeriesSetOutput.value setter): an
            // exact frozenset return REPLACES the whole set - removals are
            // computed against the current value. Any other shape (set with
            // Removed markers, set_delta, dict) stays a delta.
            nb::dict spec;
            spec["added"]   = result.attr("difference")(current);
            spec["removed"] = current.attr("difference")(result);
            shaped = spec;
        }
        Value delta = py_to_delta(shaped, erased.schema());
        if (tss && has_value)
        {
            // hgraph parity (_post_modify): a delta that nets to no change
            // on a valid output does not tick. Membership is checked on the
            // PYTHON values (delta and value plans bind differently).
            BundleView bundle{delta.view()};
            const auto in_current = [&](const ValueView &element) {
                return PySequence_Contains(current.ptr(), value_to_py(element).ptr()) == 1;
            };
            bool net_empty = true;
            for (const ValueView &element : SetView{bundle.at("added")})
            {
                if (!in_current(element)) { net_empty = false; break; }
            }
            if (net_empty)
            {
                for (const ValueView &element : SetView{bundle.at("removed")})
                {
                    if (in_current(element)) { net_empty = false; break; }
                }
            }
            if (net_empty) { return; }
        }
        // The python return is a CANONICAL DELTA (py_to_delta) - apply it as
        // one (atomic TS deltas coincide with values; compound kinds do not).
        apply_delta(out, delta.view());
    }
}  // namespace hgraph::python_bridge

namespace
{
    /**
     * Python inputs share one packed structural port, so per-child activity is
     * applied before acquiring the GIL and invoking user code. Uppercase TS
     * layout markers are passive. Runtime scheduler events remain independent
     * of input activity, including for ``active=()`` nodes.
     */
    /**
     * The node CONFIG string: the layout markers, optionally followed by
     * ``|name,name,...`` — the trailing layout entries called BY NAME
     * (python params after ``*args``: keyword-only, injectables declared
     * after the tail, and ``**kwargs`` expansions).
     */
    struct PyCallShape
    {
        std::string_view              layout;
        std::vector<std::string_view> kw_names;
    };

    [[nodiscard]] PyCallShape parse_py_call_shape(std::string_view config)
    {
        PyCallShape shape;
        const auto  separator = config.find('|');
        if (separator == std::string_view::npos)
        {
            shape.layout = config;
            return shape;
        }
        shape.layout = config.substr(0, separator);
        std::string_view names = config.substr(separator + 1);
        while (!names.empty())
        {
            const auto comma = names.find(',');
            shape.kw_names.push_back(names.substr(0, comma));
            if (comma == std::string_view::npos) { break; }
            names.remove_prefix(comma + 1);
        }
        return shape;
    }

    /**
     * Make python-node input activity REAL at the per-child link level.
     *
     * The packed ``args`` port is declaratively passive. At start we activate
     * each child per its layout marker, so per-child activity — including
     * runtime changes from python code — is the single subscription model.
     * Activity only controls subscription; it never schedules an evaluation.
     */
    void py_apply_input_activity(std::string_view layout, const TSInputView &args_view)
    {
        auto       &args     = const_cast<TSInputView &>(args_view);
        auto        bundle   = args.as_bundle();
        std::size_t ts_index = 0;
        for (const char kind : layout)
        {
            switch (kind)
            {
                case 't':
                case 'u':
                case 'C': bundle[ts_index++].make_active(); break;
                case 'T':
                case 'U':
                case 'P': bundle[ts_index++].make_passive(); break;
                default: break;
            }
        }
        args.make_passive();   // retain the invariant if a caller changed the root link
    }

    /**
     * A REF carries binding topology rather than target-value ticks. A directly
     * bound active REF can therefore be valid before graph start without ever
     * notifying its consumer. Request one explicit startup sample for that
     * case; this is independent of make_active() and does not mark the input
     * modified. Required invalid inputs remain guarded by py_assemble_args().
     */
    void py_schedule_initial_reference_sample(std::string_view layout, const TSInputView &args,
                                              SingleShotScheduler scheduler)
    {
        auto        bundle   = args.as_bundle();
        std::size_t ts_index = 0;
        for (const char kind : layout)
        {
            switch (kind)
            {
                case 't':
                case 'u':
                case 'C': {
                    auto child = bundle[ts_index++];
                    if (child.valid() && TypeRegistry::contains_ref(child.schema()))
                    {
                        scheduler.schedule_now();
                        return;
                    }
                    break;
                }
                case 'T':
                case 'U':
                case 'P': ++ts_index; break;
                default: break;
            }
        }
    }

    /** The stop-side mirror: every child link goes passive (including any the
        python code re-activated at runtime). */
    void py_clear_input_activity(std::string_view layout, const TSInputView &args_view)
    {
        auto       &args     = const_cast<TSInputView &>(args_view);
        auto        bundle   = args.as_bundle();
        std::size_t ts_index = 0;
        for (const char kind : layout)
        {
            switch (kind)
            {
                case 't':
                case 'u':
                case 'C':
                case 'T':
                case 'U':
                case 'P': bundle[ts_index++].make_passive(); break;
                default: break;
            }
        }
        args.make_passive();
    }

    struct PyInvocationState
    {
        State<PyStateRef> *local{nullptr};
        TSOutputView      *recordable{nullptr};
    };

    /** Assemble the python call args per the layout; false = a ts arg is not yet valid. */
    [[nodiscard]] bool py_assemble_args(std::string_view layout, const TSInputView &args, const ValueView &scalars,
                                        PyInvocationState state, NodeScheduler scheduler, DateTime now,
                                        nb::list &call_args, nb::list &context_values,
                                        const std::shared_ptr<PyTsGuard> &guard,
                                        const nb::object &runtime_global_state,
                                        const TSOutputView *output = nullptr)   // borrowed for the call only
    {
        auto        bundle       = args.as_bundle();
        std::size_t ts_index     = 0;
        std::size_t scalar_index = 0;
        auto        scalar_list  = scalars.valid() ? std::optional{scalars.as_list()} : std::nullopt;
        for (const char kind : layout)
        {
            switch (kind)
            {
                case 't':
                case 'u':
                case 'C':
                case 'T':
                case 'U':
                case 'P': {
                    auto child = bundle[ts_index++];
                    // 'u'/'U' = UNCHECKED (hgraph's valid=(...) opt-out): the
                    // python fn sees the view and guards itself.
                    if (kind != 'u' && kind != 'U' && !child.valid()) { return false; }
                    // The LAZY C++ TimeSeries view: nothing converts unless
                    // the python code touches it. Guard-invalidated after
                    // the call (a view must not outlive its evaluation).
                    nb::object ts_obj = nb::cast(PyTimeSeries{std::move(child), guard});
                    call_args.append(ts_obj);
                    if (kind == 'C' || kind == 'P')
                    {
                        // A context input is ALSO entered (python
                        // context-manager protocol) around the call - the
                        // value converts here because entering needs it.
                        context_values.append(nb::cast<PyTimeSeries &>(ts_obj).value());
                    }
                    break;
                }
                case 's': {
                    if (!scalar_list.has_value()) { throw std::logic_error("python node: missing scalars value"); }
                    call_args.append(value_to_py((*scalar_list)[scalar_index++].as_any().get()));
                    break;
                }
                case 'S':
                    if (state.local == nullptr)
                    {
                        throw std::logic_error(
                            "python node: local STATE is unavailable on a recordable-state node");
                    }
                    call_args.append(py_state_namespace(*state.local));
                    break;
                case 'R':
                    if (state.recordable == nullptr)
                    {
                        throw std::logic_error(
                            "python node: RECORDABLE_STATE is unavailable on this node");
                    }
                    call_args.append(nb::cast(PyRecordableState{
                        state.recordable->handle(), now, guard}));
                    break;
                case 'o': {
                    if (output == nullptr)
                    {
                        throw std::logic_error("_output injection requires a compute node");
                    }
                    call_args.append(nb::cast(PyOutput{output->handle(), now, guard}));
                    break;
                }
                case 'c': call_args.append(nb::cast(PyEvalClock{now})); break;
                case 'd': call_args.append(nb::cast(PyScheduler{scheduler})); break;
                case 'g': call_args.append(runtime_global_state); break;
                default: throw std::logic_error("python node: unknown layout marker");
            }
        }
        return true;
    }

    void py_assemble_lifecycle_args(std::string_view layout, const ValueView &scalars,
                                    State<PyStateRef> &state, NodeScheduler scheduler, DateTime now,
                                    const nb::object &runtime_global_state, nb::list &call_args)
    {
        std::size_t scalar_index = 0;
        auto scalar_list = scalars.valid() ? std::optional{scalars.as_list()} : std::nullopt;
        for (const char kind : layout)
        {
            switch (kind)
            {
                case 's':
                    if (!scalar_list.has_value())
                    {
                        throw std::logic_error("python lifecycle callback: missing scalars value");
                    }
                    call_args.append(value_to_py((*scalar_list)[scalar_index++].as_any().get()));
                    break;
                case 'S': call_args.append(py_state_namespace(state)); break;
                case 'c': call_args.append(nb::cast(PyEvalClock{now})); break;
                case 'd': call_args.append(nb::cast(PyScheduler{scheduler})); break;
                case 'g': call_args.append(runtime_global_state); break;
                default: throw std::logic_error("python lifecycle callback: unsupported layout marker");
            }
        }
    }

    /** Peel the trailing keyword-called entries off ``call_args`` (python
        params after ``*args`` fill BY NAME). */
    [[nodiscard]] nb::dict py_peel_kwargs(nb::list &call_args, std::span<const std::string_view> kw_names)
    {
        nb::dict kwargs;
        if (kw_names.empty()) { return kwargs; }
        const std::size_t total = nb::len(call_args);
        if (total < kw_names.size()) { throw std::logic_error("python node: call shape shorter than its kw names"); }
        const std::size_t first = total - kw_names.size();
        for (std::size_t index = 0; index < kw_names.size(); ++index)
        {
            kwargs[nb::str(std::string{kw_names[index]}.c_str())] = call_args[first + index];
        }
        nb::list positional;
        for (std::size_t index = 0; index < first; ++index) { positional.append(call_args[index]); }
        call_args = std::move(positional);
        return kwargs;
    }

    /** Enter context-manager values (hgraph's context semantics), call, exit in reverse. */
    [[nodiscard]] nb::object py_call_with_contexts(const nb::object &fn, nb::list &call_args,
                                                   nb::list &context_values,
                                                   const nb::object &runtime_global_state,
                                                   nb::dict call_kwargs = {})
    {
        nb::object runtime = nb::module_::import_("hgraph._wiring._state");
        runtime.attr("_push_runtime_global_state")(runtime_global_state);
        std::vector<nb::object> entered;
        entered.reserve(nb::len(context_values));
        auto unwind = UnwindCleanupGuard([&] {
            for (auto it = entered.rbegin(); it != entered.rend(); ++it)
            {
                (*it).attr("__exit__")(nb::none(), nb::none(), nb::none());
            }
            runtime.attr("_pop_runtime_global_state")();
        });
        for (nb::handle value : context_values)
        {
            if (nb::hasattr(value, "__enter__"))
            {
                nb::object holder = nb::borrow(value);
                holder.attr("__enter__")();
                entered.push_back(std::move(holder));
            }
        }
        nb::object result = fn(*nb::tuple(call_args), **call_kwargs);
        while (!entered.empty())
        {
            nb::object holder = std::move(entered.back());
            entered.pop_back();
            holder.attr("__exit__")(nb::none(), nb::none(), nb::none());
        }
        runtime.attr("_pop_runtime_global_state")();
        unwind.release();
        return result;
    }

    void py_call_lifecycle(const PyNodeRef &fn, bool enabled, std::string_view config, const ValueView &scalars,
                           State<PyStateRef> &state, NodeScheduler scheduler, DateTime now,
                           GlobalStateView global_state)
    {
        if (!enabled) { return; }
        nb::gil_scoped_acquire gil;
        nb::list call_args;
        nb::list context_values;
        auto guard = std::make_shared<PyTsGuard>();
        auto invalid = UnwindCleanupGuard([&] { guard->alive = false; });
        nb::object runtime_state = nb::cast(PyRuntimeGlobalState{global_state, guard});
        py_assemble_lifecycle_args(config, scalars, state, scheduler, now, runtime_state, call_args);
        (void)py_call_with_contexts(fn.record->fn, call_args, context_values, runtime_state);
        invalid.release();
        guard->alive = false;
    }

    struct py_compute_node
    {
        static constexpr auto name = "__py_compute";
        static constexpr std::string_view implementation_label = "hgraph.python.compute";

        static void start(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive> args,
                          Scalar<"config", Str> eval_config,
                          Scalar<"start_fn", PyNodeRef> fn, Scalar<"start_enabled", Bool> enabled,
                          Scalar<"start_config", Str> config,
                          Scalar<"start_scalars", ScalarVar<"SSV">> scalars,
                          State<PyStateRef> state, NodeScheduler scheduler, SingleShotScheduler initial_sample,
                          DateTime now,
                          GlobalStateView global_state)
        {
            const auto layout = parse_py_call_shape(eval_config.value()).layout;
            py_apply_input_activity(layout, args.base());
            py_schedule_initial_reference_sample(layout, args.base(), initial_sample);
            py_call_lifecycle(fn.value(), enabled.value(), config.value(), scalars.value(), state, scheduler, now,
                              global_state);
        }

        static void eval(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive> args,
                         Scalar<"fn", PyNodeRef> fn,
                         Scalar<"config", Str> config, Scalar<"scalars", ScalarVar<"SV">> scalars,
                         Scalar<"start_fn", PyNodeRef> start_fn, Scalar<"start_enabled", Bool> start_enabled,
                         Scalar<"start_config", Str> start_config,
                         Scalar<"start_scalars", ScalarVar<"SSV">> start_scalars,
                         Scalar<"stop_fn", PyNodeRef> stop_fn, Scalar<"stop_enabled", Bool> stop_enabled,
                         Scalar<"stop_config", Str> stop_config,
                         Scalar<"stop_scalars", ScalarVar<"XSV">> stop_scalars,
                         State<PyStateRef> state, NodeScheduler scheduler, DateTime now,
                         GlobalStateView global_state, Out<TsVar<"O">> out)
        {
            static_cast<void>(start_fn);
            static_cast<void>(start_enabled);
            static_cast<void>(start_config);
            static_cast<void>(start_scalars);
            static_cast<void>(stop_fn);
            static_cast<void>(stop_enabled);
            static_cast<void>(stop_config);
            static_cast<void>(stop_scalars);
            const PyCallShape shape = parse_py_call_shape(config.value());
            nb::gil_scoped_acquire gil;
            nb::list call_args;
            nb::list context_values;
            auto     guard   = std::make_shared<PyTsGuard>();
            auto     invalid = UnwindCleanupGuard([&] { guard->alive = false; });
            const auto &out_view = static_cast<const TSOutputView &>(out);
            nb::object runtime_state = nb::cast(PyRuntimeGlobalState{global_state, guard});
            if (!py_assemble_args(shape.layout, args.base(), scalars.value(),
                                  PyInvocationState{.local = &state}, scheduler, now, call_args,
                                  context_values, guard, runtime_state, &out_view))
            {
                return;
            }
            nb::dict call_kwargs = py_peel_kwargs(call_args, shape.kw_names);
            apply_py_result(
                py_call_with_contexts(fn.value().record->fn, call_args, context_values, runtime_state,
                                      std::move(call_kwargs)),
                out);
            invalid.release();
            guard->alive = false;
        }

        static void stop(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive> args,
                         Scalar<"config", Str> eval_config,
                         Scalar<"stop_fn", PyNodeRef> fn, Scalar<"stop_enabled", Bool> enabled,
                         Scalar<"stop_config", Str> config,
                         Scalar<"stop_scalars", ScalarVar<"XSV">> scalars,
                         State<PyStateRef> state, NodeScheduler scheduler, DateTime now,
                         GlobalStateView global_state)
        {
            // Mirror the start hook: drop the per-child link subscriptions so a
            // stopped node (e.g. a removed map_ child) can never be re-scheduled
            // by a lingering active input.
            py_clear_input_activity(parse_py_call_shape(eval_config.value()).layout, args.base());
            auto release = UnwindCleanupGuard([&] { py_release_state(state); });
            py_call_lifecycle(fn.value(), enabled.value(), config.value(), scalars.value(), state, scheduler, now,
                              global_state);
            release.release();
            py_release_state(state);
        }
    };

    struct py_compute_recordable_node
    {
        static constexpr auto name = "__py_compute_recordable";
        static constexpr std::string_view implementation_label =
            "hgraph.python.compute_recordable";

        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context)
        {
            const auto *schema = context.scalar_as<PyTsMetaRef>(
                "recordable_state_schema");
            if (schema == nullptr || schema->meta == nullptr)
            {
                throw std::invalid_argument(
                    "python recordable-state node requires a concrete state schema");
            }
            resolution.bind_ts("RS", schema->meta);
        }

        static void start(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive> args,
                          Scalar<"config", Str> eval_config, SingleShotScheduler initial_sample)
        {
            const auto layout = parse_py_call_shape(eval_config.value()).layout;
            py_apply_input_activity(layout, args.base());
            py_schedule_initial_reference_sample(layout, args.base(), initial_sample);
        }

        static void eval(
            In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive> args,
            Scalar<"fn", PyNodeRef> fn, Scalar<"config", Str> config,
            Scalar<"scalars", ScalarVar<"SV">> scalars,
            Scalar<"recordable_state_schema", PyTsMetaRef> recordable_state_schema,
            Scalar<"start_fn", PyNodeRef> start_fn,
            Scalar<"start_enabled", Bool> start_enabled,
            Scalar<"start_config", Str> start_config,
            Scalar<"start_scalars", ScalarVar<"SSV">> start_scalars,
            Scalar<"stop_fn", PyNodeRef> stop_fn,
            Scalar<"stop_enabled", Bool> stop_enabled,
            Scalar<"stop_config", Str> stop_config,
            Scalar<"stop_scalars", ScalarVar<"XSV">> stop_scalars,
            RecordableState<TsVar<"RS">> state, NodeScheduler scheduler,
            DateTime now, GlobalStateView global_state, Out<TsVar<"O">> out)
        {
            static_cast<void>(recordable_state_schema);
            static_cast<void>(start_fn);
            static_cast<void>(start_enabled);
            static_cast<void>(start_config);
            static_cast<void>(start_scalars);
            static_cast<void>(stop_fn);
            static_cast<void>(stop_enabled);
            static_cast<void>(stop_config);
            static_cast<void>(stop_scalars);
            const PyCallShape shape = parse_py_call_shape(config.value());
            nb::gil_scoped_acquire gil;
            nb::list call_args;
            nb::list context_values;
            auto guard = std::make_shared<PyTsGuard>();
            auto invalid = UnwindCleanupGuard([&] { guard->alive = false; });
            const auto &out_view = static_cast<const TSOutputView &>(out);
            TSOutputView state_view =
                static_cast<const TSOutputView &>(state).borrowed_ref();
            nb::object runtime_state = nb::cast(
                PyRuntimeGlobalState{global_state, guard});
            if (!py_assemble_args(
                    shape.layout, args.base(), scalars.value(),
                    PyInvocationState{.recordable = &state_view}, scheduler, now,
                    call_args, context_values, guard, runtime_state, &out_view))
            {
                return;
            }
            nb::dict call_kwargs = py_peel_kwargs(call_args, shape.kw_names);
            apply_py_result(
                py_call_with_contexts(fn.value().record->fn, call_args,
                                      context_values, runtime_state,
                                      std::move(call_kwargs)),
                out);
            invalid.release();
            guard->alive = false;
        }

        static void stop(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive> args,
                         Scalar<"config", Str> eval_config)
        {
            py_clear_input_activity(
                parse_py_call_shape(eval_config.value()).layout, args.base());
        }
    };

    struct py_sink_node
    {
        static constexpr auto name = "__py_sink";
        static constexpr std::string_view implementation_label = "hgraph.python.sink";

        static void start(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive> args,
                          Scalar<"config", Str> eval_config,
                          Scalar<"start_fn", PyNodeRef> fn, Scalar<"start_enabled", Bool> enabled,
                          Scalar<"start_config", Str> config,
                          Scalar<"start_scalars", ScalarVar<"SSV">> scalars,
                          State<PyStateRef> state, NodeScheduler scheduler, SingleShotScheduler initial_sample,
                          DateTime now,
                          GlobalStateView global_state)
        {
            const auto layout = parse_py_call_shape(eval_config.value()).layout;
            py_apply_input_activity(layout, args.base());
            py_schedule_initial_reference_sample(layout, args.base(), initial_sample);
            py_call_lifecycle(fn.value(), enabled.value(), config.value(), scalars.value(), state, scheduler, now,
                              global_state);
        }

        static void eval(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive> args,
                         Scalar<"fn", PyNodeRef> fn,
                         Scalar<"config", Str> config, Scalar<"scalars", ScalarVar<"SV">> scalars,
                         Scalar<"start_fn", PyNodeRef> start_fn, Scalar<"start_enabled", Bool> start_enabled,
                         Scalar<"start_config", Str> start_config,
                         Scalar<"start_scalars", ScalarVar<"SSV">> start_scalars,
                         Scalar<"stop_fn", PyNodeRef> stop_fn, Scalar<"stop_enabled", Bool> stop_enabled,
                         Scalar<"stop_config", Str> stop_config,
                         Scalar<"stop_scalars", ScalarVar<"XSV">> stop_scalars,
                         State<PyStateRef> state, NodeScheduler scheduler, DateTime now,
                         GlobalStateView global_state)
        {
            static_cast<void>(start_fn);
            static_cast<void>(start_enabled);
            static_cast<void>(start_config);
            static_cast<void>(start_scalars);
            static_cast<void>(stop_fn);
            static_cast<void>(stop_enabled);
            static_cast<void>(stop_config);
            static_cast<void>(stop_scalars);
            const PyCallShape shape = parse_py_call_shape(config.value());
            nb::gil_scoped_acquire gil;
            nb::list call_args;
            nb::list context_values;
            auto     guard   = std::make_shared<PyTsGuard>();
            auto     invalid = UnwindCleanupGuard([&] { guard->alive = false; });
            nb::object runtime_state = nb::cast(PyRuntimeGlobalState{global_state, guard});
            if (!py_assemble_args(shape.layout, args.base(), scalars.value(),
                                  PyInvocationState{.local = &state}, scheduler, now, call_args,
                                  context_values, guard, runtime_state))
            {
                return;
            }
            nb::dict call_kwargs = py_peel_kwargs(call_args, shape.kw_names);
            (void)py_call_with_contexts(fn.value().record->fn, call_args, context_values, runtime_state,
                                        std::move(call_kwargs));
            invalid.release();
            guard->alive = false;
        }

        static void stop(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive> args,
                         Scalar<"config", Str> eval_config,
                         Scalar<"stop_fn", PyNodeRef> fn, Scalar<"stop_enabled", Bool> enabled,
                         Scalar<"stop_config", Str> config,
                         Scalar<"stop_scalars", ScalarVar<"XSV">> scalars,
                         State<PyStateRef> state, NodeScheduler scheduler, DateTime now,
                         GlobalStateView global_state)
        {
            // Mirror the start hook: drop the per-child link subscriptions so a
            // stopped node (e.g. a removed map_ child) can never be re-scheduled
            // by a lingering active input.
            py_clear_input_activity(parse_py_call_shape(eval_config.value()).layout, args.base());
            auto release = UnwindCleanupGuard([&] { py_release_state(state); });
            py_call_lifecycle(fn.value(), enabled.value(), config.value(), scalars.value(), state, scheduler, now,
                              global_state);
            release.release();
            py_release_state(state);
        }
    };

    /** Heap iterator state (pointer-in-State, the frame-backend pattern). */
    struct PyGenHandle
    {
        nb::object iterator;
        nb::object pending;      ///< the value yielded for the SCHEDULED time
        std::optional<DateTime> last_time{};
        bool       exhausted{false};
    };

    struct PyGenStateRef
    {
        PyGenHandle *handle{nullptr};
        friend bool operator==(const PyGenStateRef &, const PyGenStateRef &) noexcept = default;
    };

    /** Pull the next (datetime, value) pair; schedules it or marks exhaustion. */
    template <typename Scheduler>
    void py_gen_advance(PyGenHandle &handle, Scheduler &sched)
    {
        nb::object next = nb::steal(PyIter_Next(handle.iterator.ptr()));
        if (!next.is_valid())
        {
            if (PyErr_Occurred() != nullptr) { throw nb::python_error(); }
            handle.exhausted = true;
            handle.pending   = nb::object{};
            return;
        }
        auto pair = nb::cast<nb::tuple>(next);
        if (nb::len(pair) != 2)
        {
            throw nb::value_error("a Python generator must yield (datetime, value) pairs");
        }
        DateTime when;
        if (!nb::try_cast<DateTime>(pair[0], when))
        {
            TimeDelta delay;
            if (!nb::try_cast<TimeDelta>(pair[0], delay))
            {
                throw nb::type_error("a Python generator time must be a datetime or timedelta");
            }
            when = sched.now() + delay;
        }
        if (handle.last_time.has_value() && when <= *handle.last_time)
        {
            throw std::invalid_argument("Python generator output times must be strictly increasing");
        }
        handle.last_time = when;
        handle.pending = nb::object(pair[1]);
        sched.schedule(when);
    }

    struct py_generator_node
    {
        static constexpr auto name = "__py_generator";
        static constexpr std::string_view implementation_label = "hgraph.python.generator";

        static void start(Scalar<"fn", PyNodeRef> fn, State<PyGenStateRef> state, SingleShotScheduler sched)
        {
            nb::gil_scoped_acquire gil;
            auto handle      = std::make_unique<PyGenHandle>();
            handle->iterator = nb::steal(PyObject_GetIter(fn.value().record->fn().ptr()));
            if (!handle->iterator.is_valid()) { throw nb::python_error(); }
            py_gen_advance(*handle, sched);
            state.set(PyGenStateRef{handle.release()});   // owned by node State until stop
        }

        static void eval(Scalar<"fn", PyNodeRef> fn, State<PyGenStateRef> state, NodeScheduler sched,
                         Out<TsVar<"O">> out)
        {
            static_cast<void>(fn);
            nb::gil_scoped_acquire gil;
            PyGenHandle *handle = state.get().handle;
            if (handle == nullptr || handle->exhausted) { return; }
            apply_py_result(handle->pending, out);
            py_gen_advance(*handle, sched);
        }

        static void stop(State<PyGenStateRef> state)
        {
            nb::gil_scoped_acquire gil;
            std::unique_ptr<PyGenHandle> handle{state.get().handle};
            state.set(PyGenStateRef{});
        }
    };

    struct op_py_compute : Operator<"__py_compute", In<"args", TsVar<"A">>, Scalar<"fn", PyNodeRef>,
                                     Scalar<"config", Str>, Scalar<"scalars", ScalarVar<"SV">>,
                                     Scalar<"start_fn", PyNodeRef>, Scalar<"start_enabled", Bool>,
                                     Scalar<"start_config", Str>,
                                     Scalar<"start_scalars", ScalarVar<"SSV">>,
                                     Scalar<"stop_fn", PyNodeRef>, Scalar<"stop_enabled", Bool>,
                                     Scalar<"stop_config", Str>,
                                     Scalar<"stop_scalars", ScalarVar<"XSV">>, Out<TsVar<"O">>> {};
    struct op_py_compute_recordable
        : Operator<"__py_compute_recordable", In<"args", TsVar<"A">>,
                   Scalar<"fn", PyNodeRef>, Scalar<"config", Str>,
                   Scalar<"scalars", ScalarVar<"SV">>,
                   Scalar<"recordable_state_schema", PyTsMetaRef>,
                   Scalar<"start_fn", PyNodeRef>, Scalar<"start_enabled", Bool>,
                   Scalar<"start_config", Str>,
                   Scalar<"start_scalars", ScalarVar<"SSV">>,
                   Scalar<"stop_fn", PyNodeRef>, Scalar<"stop_enabled", Bool>,
                   Scalar<"stop_config", Str>,
                   Scalar<"stop_scalars", ScalarVar<"XSV">>, Out<TsVar<"O">>> {};
    struct op_py_sink : Operator<"__py_sink", In<"args", TsVar<"A">>, Scalar<"fn", PyNodeRef>,
                                 Scalar<"config", Str>, Scalar<"scalars", ScalarVar<"SV">>,
                                 Scalar<"start_fn", PyNodeRef>, Scalar<"start_enabled", Bool>,
                                 Scalar<"start_config", Str>,
                                 Scalar<"start_scalars", ScalarVar<"SSV">>,
                                 Scalar<"stop_fn", PyNodeRef>, Scalar<"stop_enabled", Bool>,
                                 Scalar<"stop_config", Str>,
                                 Scalar<"stop_scalars", ScalarVar<"XSV">>> {};
    struct op_py_generator : Operator<"__py_generator", Scalar<"fn", PyNodeRef>, Out<TsVar<"O">>> {};
    struct harness_replay
    {
        static constexpr auto name              = "__harness_replay";
        static constexpr bool schedule_on_start = true;

        static void eval(Scalar<"key", std::string> key, GlobalStateView gs, NodeScheduler sched, State<Int> index,
                         Out<TsVar<"S">> out)
        {
            testing::replay::eval(std::move(key), std::move(gs), std::move(sched), std::move(index), std::move(out));
        }
    };

    struct harness_record
    {
        static constexpr auto name = "__harness_record";

        static auto defaults() { return std::tuple{arg<"sparse">(Bool{false})}; }

        static void start(Scalar<"key", std::string> key, Scalar<"sparse", Bool> sparse, GlobalStateView gs)
        {
            testing::record::start(std::move(key), std::move(sparse), std::move(gs));
        }

        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts, Scalar<"key", std::string> key,
                         Scalar<"sparse", Bool> sparse, GlobalStateView gs, DateTime now)
        {
            testing::record::eval(std::move(ts), std::move(key), std::move(sparse), std::move(gs), now);
        }
    };

    struct op_harness_replay
        : Operator<"__harness_replay", Scalar<"key", Str>, Out<TsVar<"S">>> {};
    struct op_harness_record
        : Operator<"__harness_record", In<"ts", TsVar<"S">>, Scalar<"key", Str>, Scalar<"sparse", Bool>> {};

    /** Materialize a STRUCTURAL port through a real node output (child
        sub-graph outputs must be node outputs - a python function returning
        combine[TSB[...]](...) produces a structural source). Canonical
        delta capture/apply keeps every kind's granularity. */
    struct materialize_node
    {
        static constexpr auto name = "__materialize";

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"S">> out)
        {
            const Value delta = capture_delta(ts.base());
            apply_delta(static_cast<const TSOutputView &>(out), delta.view());
        }
    };

    struct op_materialize : Operator<"__materialize", In<"ts", TsVar<"S">>, Out<TsVar<"S">>> {};

    /** hgraph's ``until_true(predicate, ts)`` with a plain python callable:
        the predicate rides a PyObj SCALAR and the kernel holds it, so
        passivating ``ts`` also stops the calls (upstream's
        until_true_default). Dispatch is the registry's — this overload is
        selected by the object-scalar first argument. */
    struct until_true_callable_node
    {
        static constexpr auto name = "until_true_callable";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.args.size() == 2 && context.scalar_as<PyObj>("predicate") != nullptr &&
                   context.args[1].kind == WiringArg::Kind::TimeSeries;
        }

        static void eval(Scalar<"predicate", PyObj> predicate, In<"ts", TsVar<"S">> ts, Out<TS<Bool>> out)
        {
            nb::gil_scoped_acquire gil;
            const bool stop =
                nb::cast<bool>(nb::bool_(predicate.value().get()(value_to_py(ts.value()))));
            out.set(stop);
            if (stop) { ts.make_passive(); }
        }
    };

    /** ``type_(ts)`` — the python TYPE of each tick's value (hgraph's
        TS[type]; the type object is a py-object scalar). */
    struct type_py_node
    {
        static constexpr auto name = "type_py";

        static void eval(In<"ts", TsVar<"S">> ts, Out<TS<PyObj>> out)
        {
            nb::gil_scoped_acquire gil;
            nb::object value = value_to_py(ts.value());
            out.set(PyObj{nb::borrow(value.type())});
        }
    };

    /** ``convert[TS[object]](ts)`` — box any TS payload into the python-
        object scalar (py parity: TS[object] widens over any value). */
    struct convert_to_py_object_node
    {
        static constexpr auto name = "convert_to_py_object";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            using namespace hgraph::operator_type_resolution;
            // The concrete TS[object] output is gated by the dispatcher's
            // requested-output match; already-object inputs use identity.
            const auto *in = time_series_schema_at(context, 0);
            return in != nullptr && in->kind == TSTypeKind::TS &&
                   in->value_schema != TypeRegistry::instance().value_type("object");
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TS<PyObj>> out)
        {
            nb::gil_scoped_acquire gil;
            out.set(PyObj{value_to_py(ts.base().value())});
        }
    };

    /** ``getattr_(TS[type], "name" | "__name__")`` — the type's __name__
        (upstream's getattr_type_name). */
    struct getattr_type_name_node
    {
        static constexpr auto name = "getattr_type_name";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            using namespace hgraph::operator_type_resolution;
            const auto *schema = time_series_schema_at(context, 0);
            const Str  *attr   = context.scalar_as<Str>("attr");
            return schema != nullptr && schema->kind == TSTypeKind::TS && attr != nullptr &&
                   schema->value_schema == TypeRegistry::instance().value_type("object") &&
                   (*attr == "name" || *attr == "__name__");
        }

        static void eval(In<"ts", TS<PyObj>> ts, Scalar<"attr", Str> attr, Out<TS<Str>> out)
        {
            static_cast<void>(attr);
            nb::gil_scoped_acquire gil;
            nb::handle value{ts.value().get()};
            out.set(Str{nb::cast<std::string>(value.attr("__name__"))});
        }
    };

    /** ``call(fn, ts)`` — hgraph's side-effect sink: invoke the python
        callable with each ticked value. */
    struct call_callable_node
    {
        static constexpr auto name = "call_callable";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.args.size() == 2 && context.scalar_as<PyObj>("fn") != nullptr &&
                   context.args[1].kind == WiringArg::Kind::TimeSeries;
        }

        static void eval(Scalar<"fn", PyObj> fn, In<"ts", TsVar<"S">> ts)
        {
            nb::gil_scoped_acquire gil;
            fn.value().get()(value_to_py(ts.value()));
        }
    };

    /** ``freeze(predicate, ts)`` with a callable: upstream's freeze_predicate
        — freeze once until_true(predicate, ts) fires. */
    struct freeze_callable_compose
    {
        static constexpr auto name = "freeze_callable";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.args.size() == 2 && context.scalar_as<PyObj>("predicate") != nullptr &&
                   context.args[1].kind == WiringArg::Kind::TimeSeries;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (operator_type_resolution::output_bound(resolution)) { return; }
            if (context.args.size() < 2 || context.args[1].kind != WiringArg::Kind::TimeSeries) { return; }
            operator_type_resolution::bind_output(resolution, context.args[1].port.schema);
        }

        static auto compose(Wiring &w, Scalar<"predicate", PyObj> predicate, NamedPort<"ts", TsVar<"S">> ts)
        {
            auto flag = wire<stdlib::until_true>(w, predicate.value(), ts);
            return wire<stdlib::freeze>(w, flag, ts);
        }
    };

    struct op_recover_pt
        : Operator<"__recovering_pass_through", In<"ts", TsVar<"S">>, Scalar<"fq_key", Str>, Out<TsVar<"S">>> {};

}  // namespace

template <>
struct std::hash<PyGenStateRef>
{
    [[nodiscard]] std::size_t operator()(const PyGenStateRef &ref) const noexcept
    {
        return std::hash<const void *>{}(ref.handle);
    }
};

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<PyGenStateRef>
    {
        static constexpr std::string_view value{"py_gen_state"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::python_bridge
{
    /** The python-node operator registrations, shared by NB_MODULE init and
        reset_registries (module.cpp) - keep this the ONLY copy. */
    void register_python_overloads()
    {
        (void)scalar_descriptor<PyObj>::value_meta();   // the python-object scalar
        register_overload<op_materialize, materialize_node>();
        register_overload<op_py_compute, py_compute_node>();
        register_overload<op_py_compute_recordable, py_compute_recordable_node>();
        register_overload<op_py_sink, py_sink_node>();
        register_overload<op_py_generator, py_generator_node>();
        register_overload<op_recover_pt, stdlib::component_detail::recovering_pass_through>();
        register_overload<op_harness_replay, harness_replay>();
        register_overload<op_harness_record, harness_record>();
        register_overload<stdlib::until_true, until_true_callable_node>();
        register_overload<stdlib::type_, type_py_node>();
        register_overload<stdlib::convert, convert_to_py_object_node>();
        register_overload<stdlib::getattr_, getattr_type_name_node>();
        register_graph_overload<stdlib::freeze, freeze_callable_compose>();
        register_overload<stdlib::call_op, call_callable_node>();

    }
}  // namespace hgraph::python_bridge

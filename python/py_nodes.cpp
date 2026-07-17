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
        if (erased.schema() != nullptr &&
            (erased.schema()->kind == TSTypeKind::TS || erased.schema()->kind == TSTypeKind::TSB) &&
            erased.data_view().ops().from_python_impl !=
                &ts_data_detail::missing_from_python)
        {
            static_cast<void>(
                erased.begin_mutation(erased.evaluation_time()).from_python(result));
            return;
        }
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
            // move_value_from returns NEWLY-MODIFIED, not success: a publish
            // landing as no new modification is a legitimate no-op (same rule
            // as the structural REF node in graph_wiring.cpp).
            static_cast<void>(mutation.move_value_from(Value{std::move(reference)}));
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

        if (erased.schema()->kind == TSTypeKind::TSD &&
            erased.schema()->element_ts() != nullptr &&
            erased.schema()->element_ts()->kind == TSTypeKind::TS &&
            nb::isinstance<nb::dict>(shaped))
        {
            const auto *key_meta = erased.schema()->key_type();
            const auto *child = erased.schema()->element_ts();
            auto python_delta = nb::cast<nb::dict>(shaped);
            auto mutation = erased.as_dict().begin_mutation(erased.evaluation_time());
            if (mutation.empty()) { mutation.reserve(nb::len(python_delta)); }

            const bool fixed_atomic =
                key_meta->value_kind() == ValueTypeKind::Atomic &&
                child->value_schema->value_kind() == ValueTypeKind::Atomic;
            if (fixed_atomic)
            {
                Value key_value{ValuePlanFactory::instance().type_for(key_meta)};
                Value child_delta{ValuePlanFactory::instance().type_for(child->value_schema)};
                for (auto [key, item] : python_delta)
                {
                    key_value.view().assign_from_python(key);
                    const bool strict_remove =
                        removed_sentinel_slot().is_valid() && item.is(removed_sentinel_slot());
                    const bool lenient_remove =
                        item.is_none() ||
                        (remove_if_exists_sentinel_slot().is_valid() &&
                         item.is(remove_if_exists_sentinel_slot()));
                    if (strict_remove || lenient_remove)
                    {
                        // Membership is pre-checked: an ineffective erase would
                        // touch-validate the output (the replay empty-tick rule),
                        // but a skipped REMOVE_IF_EXISTS must not tick (hgraph
                        // parity), and a strict REMOVE must raise.
                        if (!mutation.view().contains(key_value.view()))
                        {
                            if (strict_remove)
                            {
                                throw std::runtime_error(
                                    "REMOVE: key not present in TSD (use REMOVE_IF_EXISTS to remove-if-present)");
                            }
                            continue;
                        }
                        static_cast<void>(mutation.erase(key_value.view()));
                        continue;
                    }
                    child_delta.view().assign_from_python(item);
                    mutation.set(key_value.view(), child_delta.view());
                }
                return;
            }

            for (auto [key, item] : python_delta)
            {
                Value key_value = py_to_value_as(key, key_meta);
                const bool strict_remove =
                    removed_sentinel_slot().is_valid() && item.is(removed_sentinel_slot());
                const bool lenient_remove =
                    item.is_none() ||
                    (remove_if_exists_sentinel_slot().is_valid() &&
                     item.is(remove_if_exists_sentinel_slot()));
                if (strict_remove || lenient_remove)
                {
                    // See above: pre-check membership so a skipped lenient
                    // removal produces no tick and a strict one raises.
                    if (!mutation.view().contains(key_value.view()))
                    {
                        if (strict_remove)
                        {
                            throw std::runtime_error(
                                "REMOVE: key not present in TSD (use REMOVE_IF_EXISTS to remove-if-present)");
                        }
                    }
                    else { static_cast<void>(mutation.erase(key_value.view())); }
                }
                else
                {
                    Value child_delta = py_to_value_as(item, child->value_schema);
                    mutation.set(key_value.view(), child_delta.view());
                }
            }
            return;
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
                case 'a':
                case 'C': bundle[ts_index++].make_active(); break;
                case 'T':
                case 'U':
                case 'A':
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
                case 'a':
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
                case 'A':
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
                case 'a':
                case 'C':
                case 'T':
                case 'U':
                case 'A':
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

    struct PyFastComputeCache
    {
        PyFastComputeCache(const PyNodeRecord *record_, PyCallShape shape_,
                           TSInputView input_, ValueView scalars_, TSOutputHandle output_,
                           GlobalStateView global_state_)
            : record(record_), shape(std::move(shape_)), input(std::move(input_)),
              scalars(std::move(scalars_)), output(std::move(output_)),
              global_state(std::move(global_state_))
        {
        }

        const PyNodeRecord *record{nullptr};
        PyCallShape         shape{};
        TSInputView         input{};
        ValueView           scalars{};
        TSOutputHandle      output{};
        GlobalStateView     global_state{};
        PyObject           *input_object{nullptr};
        PyTimeSeries       *input_wrapper{nullptr};

        [[nodiscard]] bool direct() const noexcept
        {
            return shape.kw_names.empty() && shape.layout.size() == 1 &&
                   (shape.layout.front() == 't' || shape.layout.front() == 'u' ||
                    shape.layout.front() == 'T' || shape.layout.front() == 'U' ||
                    shape.layout.front() == 'a' || shape.layout.front() == 'A');
        }
    };

    struct PyFastComputeStateRef
    {
        PyFastComputeCache *cache{nullptr};
        friend bool operator==(const PyFastComputeStateRef &,
                               const PyFastComputeStateRef &) noexcept = default;
    };

    [[nodiscard]] bool py_make_ts_arg(char kind, TSInputView child,
                                      const PyTsLease &lease, nb::object &result)
    {
        const auto &evaluation_data = child.data_view();
        // 'u'/'U' = UNCHECKED (hgraph's valid=(...) opt-out): the
        // python fn sees the view and guards itself.
        if (kind != 'u' && kind != 'U' &&
            (!evaluation_data.valid() || !evaluation_data.has_current_value()))
        {
            return false;
        }
        if ((kind == 'a' || kind == 'A') && !evaluation_data.all_valid()) { return false; }
        // The LAZY C++ TimeSeries view: nothing converts unless the python
        // code touches it. The lease expires after the callback.
        const auto evaluation_storage = evaluation_data.valid()
                                            ? evaluation_data.storage_ref()
                                            : TSDataStorageRef<>{};
        result = nb::cast(PyTimeSeries{
            std::move(child), lease, evaluation_storage});
        return true;
    }

    [[nodiscard]] bool py_make_direct_ts_arg(PyFastComputeCache &cache, DateTime now,
                                             const PyTsLease &lease, nb::object &result)
    {
        TSInputView child = cache.input.borrowed_ref(now);
        const auto &evaluation_data = child.data_view();
        const char kind = cache.shape.layout.front();
        if (kind != 'u' && kind != 'U' &&
            (!evaluation_data.valid() || !evaluation_data.has_current_value()))
        {
            return false;
        }
        if ((kind == 'a' || kind == 'A') && !evaluation_data.all_valid()) { return false; }
        const auto evaluation_storage = evaluation_data.valid()
                                            ? evaluation_data.storage_ref()
                                            : TSDataStorageRef<>{};
        PyTimeSeries wrapped{std::move(child), lease, evaluation_storage};

        // Repoint only the cache's sole reference. If Python retained the
        // previous argument, leave that expired object untouched and replace
        // the cache entry with a fresh wrapper.
        if (cache.input_object != nullptr && Py_REFCNT(cache.input_object) == 1)
        {
            *cache.input_wrapper = std::move(wrapped);
            result = nb::borrow<nb::object>(nb::handle(cache.input_object));
            return true;
        }

        if (cache.input_object != nullptr)
        {
            nb::handle(cache.input_object).dec_ref();
            cache.input_object = nullptr;
            cache.input_wrapper = nullptr;
        }
        result = nb::cast(std::move(wrapped));
        cache.input_object = result.ptr();
        cache.input_wrapper = std::addressof(nb::cast<PyTimeSeries &>(result));
        nb::handle(cache.input_object).inc_ref();
        return true;
    }

    /** Assemble the python call args per the layout; false = a ts arg is not yet valid. */
    [[nodiscard]] bool py_assemble_args(std::string_view layout, const TSInputView &args, const ValueView &scalars,
                                        PyInvocationState state, NodeScheduler scheduler, DateTime now,
                                        nb::list &call_args, std::optional<nb::list> &context_values,
                                        const PyTsLease &lease,
                                        const nb::object &runtime_global_state, EngineControlView engine,
                                        const NodeView &node,
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
                case 'a':
                case 'C':
                case 'T':
                case 'U':
                case 'A':
                case 'P': {
                    auto child = bundle[ts_index++];
                    nb::object ts_obj;
                    if (!py_make_ts_arg(kind, std::move(child), lease, ts_obj)) { return false; }
                    call_args.append(ts_obj);
                    if (kind == 'C' || kind == 'P')
                    {
                        // A context input is ALSO entered (python
                        // context-manager protocol) around the call - the
                        // value converts here because entering needs it.
                        if (!context_values.has_value()) { context_values.emplace(); }
                        context_values->append(nb::cast<PyTimeSeries &>(ts_obj).value());
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
                        state.recordable->handle(), now, lease}));
                    break;
                case 'o': {
                    if (output == nullptr)
                    {
                        throw std::logic_error("_output injection requires a compute node");
                    }
                    call_args.append(nb::cast(PyOutput{output->handle(), now, lease}));
                    break;
                }
                case 'c': call_args.append(nb::cast(PyEvalClock{engine.evaluation_clock()})); break;
                case 'd': call_args.append(nb::cast(PyScheduler{scheduler})); break;
                case 'e': call_args.append(nb::cast(PyEvaluationEngineApi{engine, lease})); break;
                case 'g': call_args.append(runtime_global_state); break;
                case 'n': call_args.append(nb::cast(PyNode{node.pointer(), scheduler, lease})); break;
                default: throw std::logic_error("python node: unknown layout marker");
            }
        }
        return true;
    }

    [[nodiscard]] bool py_fast_compute_eligible(OperatorCallContext context)
    {
        const auto *config = context.scalar_as<Str>("config");
        const auto *start_enabled = context.scalar_as<Bool>("start_enabled");
        const auto *stop_enabled = context.scalar_as<Bool>("stop_enabled");
        if (config == nullptr || start_enabled == nullptr || stop_enabled == nullptr ||
            *start_enabled || *stop_enabled)
        {
            return false;
        }
        for (const char kind : parse_py_call_shape(*config).layout)
        {
            switch (kind)
            {
                case 't':
                case 'u':
                case 'T':
                case 'U':
                case 'a':
                case 'A':
                case 's': break;
                default: return false;
            }
        }
        return true;
    }

    [[nodiscard]] bool py_assemble_fast_args(std::string_view layout, const TSInputView &args,
                                             const ValueView &scalars, const PyTsLease &lease,
                                             nb::list &call_args)
    {
        auto bundle = args.as_bundle();
        std::size_t ts_index = 0;
        std::size_t scalar_index = 0;
        auto scalar_list = scalars.valid() ? std::optional{scalars.as_list()} : std::nullopt;
        for (const char kind : layout)
        {
            switch (kind)
            {
                case 't':
                case 'u':
                case 'T':
                case 'U':
                case 'a':
                case 'A': {
                    nb::object ts_obj;
                    if (!py_make_ts_arg(kind, bundle[ts_index++], lease, ts_obj)) { return false; }
                    call_args.append(ts_obj);
                    break;
                }
                case 's': {
                    if (!scalar_list.has_value())
                    {
                        throw std::logic_error("fast python node: missing scalars value");
                    }
                    call_args.append(value_to_py((*scalar_list)[scalar_index++].as_any().get()));
                    break;
                }
                default: throw std::logic_error("fast python node: unsupported layout marker");
            }
        }
        return true;
    }

    void py_assemble_lifecycle_args(std::string_view layout, const ValueView &scalars,
                                    State<PyStateRef> &state, NodeScheduler scheduler,
                                    const nb::object &runtime_global_state, EngineControlView engine,
                                    const PyTsLease &lease, const NodeView &node,
                                    nb::list &call_args)
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
                case 'c': call_args.append(nb::cast(PyEvalClock{engine.evaluation_clock()})); break;
                case 'd': call_args.append(nb::cast(PyScheduler{scheduler})); break;
                case 'e': call_args.append(nb::cast(PyEvaluationEngineApi{engine, lease})); break;
                case 'g': call_args.append(runtime_global_state); break;
                case 'n': call_args.append(nb::cast(PyNode{node.pointer(), scheduler, lease})); break;
                default: throw std::logic_error("python lifecycle callback: unsupported layout marker");
            }
        }
    }

    /** Peel the trailing keyword-called entries off ``call_args`` (python
        params after ``*args`` fill BY NAME). */
    [[nodiscard]] std::optional<nb::dict>
    py_peel_kwargs(nb::list &call_args, std::span<const std::string_view> kw_names)
    {
        if (kw_names.empty()) { return std::nullopt; }
        nb::dict kwargs;
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

    [[nodiscard]] nb::object py_invoke(const nb::object &fn, const nb::list &call_args,
                                       const std::optional<nb::dict> &call_kwargs)
    {
        if (call_kwargs.has_value())
        {
            return fn(*nb::tuple(call_args), **call_kwargs.value());
        }
        switch (nb::len(call_args))
        {
            case 0: return fn();
            case 1: return fn(call_args[0]);
            default: return fn(*nb::tuple(call_args));
        }
    }

    /** Enter context-manager values (hgraph's context semantics), call, exit in reverse. */
    [[nodiscard]] nb::object py_call_with_contexts(const nb::object &fn, nb::list &call_args,
                                                   const std::optional<nb::list> &context_values,
                                                   const nb::object &runtime_global_state,
                                                   std::optional<nb::dict> call_kwargs = std::nullopt)
    {
        const bool publish_runtime_state = !py_has_active_runtime_global_state();
        if (!publish_runtime_state && !context_values.has_value())
        {
            return py_invoke(fn, call_args, call_kwargs);
        }
        nb::object runtime;
        if (publish_runtime_state)
        {
            runtime = nb::module_::import_("hgraph._wiring._state");
            runtime.attr("_push_runtime_global_state")(runtime_global_state);
        }
        std::vector<nb::object> entered;
        entered.reserve(context_values.has_value() ? nb::len(*context_values) : 0);
        auto unwind = UnwindCleanupGuard([&] {
            for (auto it = entered.rbegin(); it != entered.rend(); ++it)
            {
                (*it).attr("__exit__")(nb::none(), nb::none(), nb::none());
            }
            if (publish_runtime_state) { runtime.attr("_pop_runtime_global_state")(); }
        });
        if (context_values.has_value())
        {
            for (nb::handle value : *context_values)
            {
                if (nb::hasattr(value, "__enter__"))
                {
                    nb::object holder = nb::borrow(value);
                    holder.attr("__enter__")();
                    entered.push_back(std::move(holder));
                }
            }
        }
        nb::object result = py_invoke(fn, call_args, call_kwargs);
        while (!entered.empty())
        {
            nb::object holder = std::move(entered.back());
            entered.pop_back();
            holder.attr("__exit__")(nb::none(), nb::none(), nb::none());
        }
        if (publish_runtime_state) { runtime.attr("_pop_runtime_global_state")(); }
        unwind.release();
        return result;
    }

    void py_call_lifecycle(const PyNodeRef &fn, bool enabled, std::string_view config, const ValueView &scalars,
                           State<PyStateRef> &state, NodeScheduler scheduler,
                           GlobalStateView global_state, EngineControlView engine, const NodeView &node)
    {
        if (!enabled) { return; }
        nb::gil_scoped_acquire gil;
        nb::list call_args;
        std::optional<nb::list> context_values;
        auto lease = py_ts_lease_for_call();
        auto invalid = UnwindCleanupGuard([&] { lease.invalidate(); });
        nb::object runtime_state = py_runtime_global_state_for_call(global_state, lease.guard);
        py_assemble_lifecycle_args(config, scalars, state, scheduler, runtime_state, engine, lease, node,
                                   call_args);
        (void)py_call_with_contexts(fn.record->fn, call_args, context_values, runtime_state);
        invalid.release();
        lease.invalidate();
    }

    struct py_compute_node
    {
        static constexpr auto name = "__py_compute";
        static constexpr std::string_view implementation_label = "hgraph.python.compute";
        using signature_args = std::tuple<
            In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>,
            Scalar<"fn", PyNodeRef>, Scalar<"config", Str>,
            Scalar<"scalars", ScalarVar<"SV">>, Scalar<"start_fn", PyNodeRef>,
            Scalar<"start_enabled", Bool>, Scalar<"start_config", Str>,
            Scalar<"start_scalars", ScalarVar<"SSV">>, Scalar<"stop_fn", PyNodeRef>,
            Scalar<"stop_enabled", Bool>, Scalar<"stop_config", Str>,
            Scalar<"stop_scalars", ScalarVar<"XSV">>, State<PyStateRef>, NodeScheduler,
            DateTime, GlobalStateView, EngineControlView, NodeView, Out<TsVar<"O">>>;

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return !py_fast_compute_eligible(context);
        }

        static void start(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive> args,
                          Scalar<"config", Str> eval_config,
                          Scalar<"start_fn", PyNodeRef> fn, Scalar<"start_enabled", Bool> enabled,
                          Scalar<"start_config", Str> config,
                          Scalar<"start_scalars", ScalarVar<"SSV">> scalars,
                          State<PyStateRef> state, NodeScheduler scheduler, SingleShotScheduler initial_sample,
                          GlobalStateView global_state, EngineControlView engine, NodeView node)
        {
            const auto layout = parse_py_call_shape(eval_config.value()).layout;
            py_apply_input_activity(layout, args.base());
            py_schedule_initial_reference_sample(layout, args.base(), initial_sample);
            py_call_lifecycle(fn.value(), enabled.value(), config.value(), scalars.value(), state, scheduler,
                              global_state, engine, node);
        }

        static void eval(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive> args,
                         Scalar<"fn", PyNodeRef> fn,
                         Scalar<"config", Str> config, Scalar<"scalars", ScalarVar<"SV">> scalars,
                         State<PyStateRef> state, NodeScheduler scheduler, DateTime now,
                         GlobalStateView global_state, EngineControlView engine, NodeView node,
                         Out<TsVar<"O">> out)
        {
            const PyCallShape shape = parse_py_call_shape(config.value());
            nb::gil_scoped_acquire gil;
            nb::list call_args;
            std::optional<nb::list> context_values;
            auto     lease   = py_ts_lease_for_call();
            auto     invalid = UnwindCleanupGuard([&] { lease.invalidate(); });
            const auto &out_view = static_cast<const TSOutputView &>(out);
            nb::object runtime_state =
                py_runtime_global_state_for_call(global_state, lease.guard);
            if (!py_assemble_args(shape.layout, args.base(), scalars.value(),
                                  PyInvocationState{.local = &state}, scheduler, now, call_args,
                                  context_values, lease, runtime_state, engine, node, &out_view))
            {
                return;
            }
            auto call_kwargs = py_peel_kwargs(call_args, shape.kw_names);
            apply_py_result(
                py_call_with_contexts(fn.value().record->fn, call_args, context_values, runtime_state,
                                      std::move(call_kwargs)),
                out);
            invalid.release();
            lease.invalidate();
        }

        static void stop(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive> args,
                         Scalar<"config", Str> eval_config,
                         Scalar<"stop_fn", PyNodeRef> fn, Scalar<"stop_enabled", Bool> enabled,
                         Scalar<"stop_config", Str> config,
                         Scalar<"stop_scalars", ScalarVar<"XSV">> scalars,
                         State<PyStateRef> state, NodeScheduler scheduler,
                         GlobalStateView global_state, EngineControlView engine, NodeView node)
        {
            // Mirror the start hook: drop the per-child link subscriptions so a
            // stopped node (e.g. a removed map_ child) can never be re-scheduled
            // by a lingering active input.
            py_clear_input_activity(parse_py_call_shape(eval_config.value()).layout, args.base());
            auto release = UnwindCleanupGuard([&] { py_release_state(state); });
            py_call_lifecycle(fn.value(), enabled.value(), config.value(), scalars.value(), state, scheduler,
                              global_state, engine, node);
            release.release();
            py_release_state(state);
        }
    };

    struct py_fast_compute_node
    {
        static constexpr auto name = "__py_compute";
        static constexpr std::string_view implementation_label =
            "hgraph.python.compute.fast";
        using signature_args = std::tuple<
            In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>,
            Scalar<"fn", PyNodeRef>, Scalar<"config", Str>,
            Scalar<"scalars", ScalarVar<"SV">>, Scalar<"start_fn", PyNodeRef>,
            Scalar<"start_enabled", Bool>, Scalar<"start_config", Str>,
            Scalar<"start_scalars", ScalarVar<"SSV">>, Scalar<"stop_fn", PyNodeRef>,
            Scalar<"stop_enabled", Bool>, Scalar<"stop_config", Str>,
            Scalar<"stop_scalars", ScalarVar<"XSV">>, State<PyFastComputeStateRef>,
            Out<TsVar<"O">>>;

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return py_fast_compute_eligible(context);
        }

        static void start(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive> args,
                          Scalar<"fn", PyNodeRef> fn, Scalar<"config", Str> config,
                          Scalar<"scalars", ScalarVar<"SV">> scalars,
                          State<PyFastComputeStateRef> state, SingleShotScheduler initial_sample,
                          GlobalStateView global_state, Out<TsVar<"O">> out)
        {
            PyCallShape shape = parse_py_call_shape(config.value());
            py_apply_input_activity(shape.layout, args.base());
            py_schedule_initial_reference_sample(shape.layout, args.base(), initial_sample);

            const bool direct = shape.kw_names.empty() && shape.layout.size() == 1 &&
                                (shape.layout.front() == 't' || shape.layout.front() == 'u' ||
                                 shape.layout.front() == 'T' || shape.layout.front() == 'U' ||
                                 shape.layout.front() == 'a' || shape.layout.front() == 'A');
            TSInputView cached_input = args.base().borrowed_ref();
            if (direct)
            {
                auto bundle = cached_input.as_bundle();
                cached_input = bundle[0];
            }
            auto cache = std::make_unique<PyFastComputeCache>(
                fn.value().record, std::move(shape), std::move(cached_input), scalars.value(),
                out.handle(), global_state);
            state.set(PyFastComputeStateRef{cache.get()});
            static_cast<void>(cache.release());
        }

        static void eval(State<PyFastComputeStateRef> state, DateTime now)
        {
            PyFastComputeCache *cache = state.get().cache;
            if (cache == nullptr) { throw std::logic_error("fast python node has no runtime cache"); }

            nb::gil_scoped_acquire gil;
            auto lease = py_ts_lease_for_call();
            auto invalid = UnwindCleanupGuard([&] { lease.invalidate(); });
            nb::object result;
            if (cache->direct())
            {
                nb::object ts_obj;
                if (!py_make_direct_ts_arg(*cache, now, lease, ts_obj)) { return; }
                if (py_has_active_runtime_global_state())
                {
                    result = cache->record->fn(ts_obj);
                }
                else
                {
                    nb::list call_args;
                    call_args.append(ts_obj);
                    std::optional<nb::list> context_values;
                    nb::object runtime_state =
                        py_runtime_global_state_for_call(cache->global_state, lease.guard);
                    result = py_call_with_contexts(cache->record->fn, call_args, context_values,
                                                   runtime_state);
                }
            }
            else
            {
                TSInputView input = cache->input.borrowed_ref(now);
                nb::list call_args;
                if (!py_assemble_fast_args(cache->shape.layout, input, cache->scalars,
                                           lease, call_args))
                {
                    return;
                }
                auto call_kwargs = py_peel_kwargs(call_args, cache->shape.kw_names);
                std::optional<nb::list> context_values;
                nb::object runtime_state =
                    py_runtime_global_state_for_call(cache->global_state, lease.guard);
                result = py_call_with_contexts(cache->record->fn, call_args, context_values,
                                               runtime_state, std::move(call_kwargs));
            }

            auto output_view = cache->output.view(now);
            Out<TsVar<"O">> out{std::move(output_view), now};
            apply_py_result(result, out);
            invalid.release();
            lease.invalidate();
        }

        static void stop(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive> args,
                         Scalar<"config", Str> config, State<PyFastComputeStateRef> state)
        {
            nb::gil_scoped_acquire gil;
            std::unique_ptr<PyFastComputeCache> cache{state.get().cache};
            state.set(PyFastComputeStateRef{});
            if (cache != nullptr && cache->input_object != nullptr)
            {
                nb::handle(cache->input_object).dec_ref();
                cache->input_object = nullptr;
                cache->input_wrapper = nullptr;
            }
            py_clear_input_activity(parse_py_call_shape(config.value()).layout, args.base());
        }
    };

    struct py_compute_recordable_node
    {
        static constexpr auto name = "__py_compute_recordable";
        static constexpr std::string_view implementation_label =
            "hgraph.python.compute_recordable";
        using signature_args = std::tuple<
            In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>,
            Scalar<"fn", PyNodeRef>, Scalar<"config", Str>,
            Scalar<"scalars", ScalarVar<"SV">>,
            Scalar<"recordable_state_schema", PyTsMetaRef>, Scalar<"start_fn", PyNodeRef>,
            Scalar<"start_enabled", Bool>, Scalar<"start_config", Str>,
            Scalar<"start_scalars", ScalarVar<"SSV">>, Scalar<"stop_fn", PyNodeRef>,
            Scalar<"stop_enabled", Bool>, Scalar<"stop_config", Str>,
            Scalar<"stop_scalars", ScalarVar<"XSV">>, RecordableState<TsVar<"RS">>,
            NodeScheduler, DateTime, GlobalStateView, EngineControlView, NodeView,
            Out<TsVar<"O">>>;

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
            RecordableState<TsVar<"RS">> state, NodeScheduler scheduler,
            DateTime now, GlobalStateView global_state, EngineControlView engine, NodeView node,
            Out<TsVar<"O">> out)
        {
            const PyCallShape shape = parse_py_call_shape(config.value());
            nb::gil_scoped_acquire gil;
            nb::list call_args;
            std::optional<nb::list> context_values;
            auto lease = py_ts_lease_for_call();
            auto invalid = UnwindCleanupGuard([&] { lease.invalidate(); });
            const auto &out_view = static_cast<const TSOutputView &>(out);
            TSOutputView state_view =
                static_cast<const TSOutputView &>(state).borrowed_ref();
            nb::object runtime_state =
                py_runtime_global_state_for_call(global_state, lease.guard);
            if (!py_assemble_args(
                    shape.layout, args.base(), scalars.value(),
                    PyInvocationState{.recordable = &state_view}, scheduler, now,
                    call_args, context_values, lease, runtime_state, engine, node, &out_view))
            {
                return;
            }
            auto call_kwargs = py_peel_kwargs(call_args, shape.kw_names);
            apply_py_result(
                py_call_with_contexts(fn.value().record->fn, call_args,
                                      context_values, runtime_state,
                                      std::move(call_kwargs)),
                out);
            invalid.release();
            lease.invalidate();
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
        using signature_args = std::tuple<
            In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>,
            Scalar<"fn", PyNodeRef>, Scalar<"config", Str>,
            Scalar<"scalars", ScalarVar<"SV">>, Scalar<"start_fn", PyNodeRef>,
            Scalar<"start_enabled", Bool>, Scalar<"start_config", Str>,
            Scalar<"start_scalars", ScalarVar<"SSV">>, Scalar<"stop_fn", PyNodeRef>,
            Scalar<"stop_enabled", Bool>, Scalar<"stop_config", Str>,
            Scalar<"stop_scalars", ScalarVar<"XSV">>, State<PyStateRef>, NodeScheduler,
            DateTime, GlobalStateView, EngineControlView, NodeView>;

        static void start(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive> args,
                          Scalar<"config", Str> eval_config,
                          Scalar<"start_fn", PyNodeRef> fn, Scalar<"start_enabled", Bool> enabled,
                          Scalar<"start_config", Str> config,
                          Scalar<"start_scalars", ScalarVar<"SSV">> scalars,
                          State<PyStateRef> state, NodeScheduler scheduler, SingleShotScheduler initial_sample,
                          GlobalStateView global_state, EngineControlView engine, NodeView node)
        {
            const auto layout = parse_py_call_shape(eval_config.value()).layout;
            py_apply_input_activity(layout, args.base());
            py_schedule_initial_reference_sample(layout, args.base(), initial_sample);
            py_call_lifecycle(fn.value(), enabled.value(), config.value(), scalars.value(), state, scheduler,
                              global_state, engine, node);
        }

        static void eval(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive> args,
                         Scalar<"fn", PyNodeRef> fn,
                         Scalar<"config", Str> config, Scalar<"scalars", ScalarVar<"SV">> scalars,
                         State<PyStateRef> state, NodeScheduler scheduler, DateTime now,
                         GlobalStateView global_state, EngineControlView engine, NodeView node)
        {
            const PyCallShape shape = parse_py_call_shape(config.value());
            nb::gil_scoped_acquire gil;
            nb::list call_args;
            std::optional<nb::list> context_values;
            auto     lease   = py_ts_lease_for_call();
            auto     invalid = UnwindCleanupGuard([&] { lease.invalidate(); });
            nb::object runtime_state =
                py_runtime_global_state_for_call(global_state, lease.guard);
            if (!py_assemble_args(shape.layout, args.base(), scalars.value(),
                                  PyInvocationState{.local = &state}, scheduler, now, call_args,
                                  context_values, lease, runtime_state, engine, node))
            {
                return;
            }
            auto call_kwargs = py_peel_kwargs(call_args, shape.kw_names);
            (void)py_call_with_contexts(fn.value().record->fn, call_args, context_values, runtime_state,
                                        std::move(call_kwargs));
            invalid.release();
            lease.invalidate();
        }

        static void stop(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive> args,
                         Scalar<"config", Str> eval_config,
                         Scalar<"stop_fn", PyNodeRef> fn, Scalar<"stop_enabled", Bool> enabled,
                         Scalar<"stop_config", Str> config,
                         Scalar<"stop_scalars", ScalarVar<"XSV">> scalars,
                         State<PyStateRef> state, NodeScheduler scheduler,
                         GlobalStateView global_state, EngineControlView engine, NodeView node)
        {
            // Mirror the start hook: drop the per-child link subscriptions so a
            // stopped node (e.g. a removed map_ child) can never be re-scheduled
            // by a lingering active input.
            py_clear_input_activity(parse_py_call_shape(eval_config.value()).layout, args.base());
            auto release = UnwindCleanupGuard([&] { py_release_state(state); });
            py_call_lifecycle(fn.value(), enabled.value(), config.value(), scalars.value(), state, scheduler,
                              global_state, engine, node);
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

template <>
struct std::hash<PyFastComputeStateRef>
{
    [[nodiscard]] std::size_t operator()(const PyFastComputeStateRef &ref) const noexcept
    {
        return std::hash<const void *>{}(ref.cache);
    }
};

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<PyGenStateRef>
    {
        static constexpr std::string_view value{"py_gen_state"};
    };

    template <>
    struct scalar_name<PyFastComputeStateRef>
    {
        static constexpr std::string_view value{"py_fast_compute_state"};
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
        register_overload<op_py_compute, py_fast_compute_node>();
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

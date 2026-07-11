/**
 * Optional Python bridge for the C++ runtime.
 *
 * This module exposes wiring, graph execution, services/adaptors, Python
 * user-authored nodes, value conversion, and the record/replay test harness
 * through nanobind. The runtime, schemas, dispatch, and value storage remain
 * C++-owned; this file is the compatibility/binding surface.
 */
#include "module_internal.h"

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/types/time_series/ts_output/alternative.h>
#include <hgraph/types/wired_fn.h>
#include <hgraph/lib/std/component.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/comparison.h>
#include <hgraph/lib/std/operators/control.h>
#include <hgraph/lib/std/operators/json.h>
#include <hgraph/lib/std/operators/impl/io_impl.h>   // io_write_slot (sys.stdout routing)
#include <hgraph/lib/std/operators/impl/table_impl.h>   // ts_table_layout (table_schema_info)
#include <hgraph/types/value/json_codec.h>          // to_json_string / from_json_string (builders)
#include <hgraph/types/context_wiring.h>
#include <hgraph/types/service_runtime.h>
#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/lib/std/operators/impl/higher_order_impl.h>
#include <hgraph/lib/std/operators/convert_target.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/push_source_node.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/util/scope.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/python/chrono.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/unique_ptr.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace nb = nanobind;
using namespace hgraph;
using namespace hgraph::python_bridge;

namespace
{
    // ---------------------------------------------------------------
    // Wiring surface
    // ---------------------------------------------------------------

    struct PyTsType
    {
        const TSValueTypeMetaData *meta{nullptr};
    };

    struct PyValueType
    {
        const ValueTypeMetaData *meta{nullptr};
    };

    struct PyScalarPattern
    {
        ScalarPattern pattern{};
    };

    struct PySizePattern
    {
        bool        variable{false};
        std::string name{};
        std::size_t value{0};
    };

    struct PyTypePattern
    {
        TypePattern pattern{};
    };

    struct PyWiredFn
    {
        WiredFn fn{};
    };

    struct PyNodeRecord;

    /** The user-node callable scalar (immortal record; identity by pointer). */
    struct PyNodeRef
    {
        const PyNodeRecord *record{nullptr};
        friend bool operator==(const PyNodeRef &, const PyNodeRef &) noexcept = default;
    };

    struct PyNodeHandle
    {
        const PyNodeRecord *record{nullptr};
    };

    /**
     * The python push-source sender: the slot is filled by the node's
     * on_start (graph thread); python threads convert and send through it.
     * shared_ptr is sanctioned here - this IS the cross-thread boundary.
     */
    struct PySenderSlot
    {
        PushSourceSender           sender{};
        const TSValueTypeMetaData *schema{nullptr};
    };

    struct PySender
    {
        std::shared_ptr<PySenderSlot> slot;

        void send(nb::handle object) const
        {
            if (slot == nullptr || !slot->sender.valid())
            {
                throw std::logic_error("push sender is not started yet (the graph must be running)");
            }
            Value value = py_to_delta(object, slot->schema);
            nb::gil_scoped_release release;
            slot->sender.send(std::move(value));
        }
    };

    /** An opaque pre-converted scalar Value (e.g. the user-node scalars list). */
    struct PyServiceDesc
    {
        const RuntimeServiceDescriptor *descriptor{nullptr};
    };

    struct PyScalarValue
    {
        Value value{};
    };

    // ---------------------------------------------------------------
    // Python graph callables as WiredFn values (Howard's ruling: the
    // type-erased context+ops pattern, so Python and C++ backends coexist;
    // identity = the user function object).
    // ---------------------------------------------------------------

    struct PyGraphFnRecord
    {
        nb::object                    wrapper;   ///< package-side: wrapper(borrowed_wiring, ports) -> port|None
        nb::object                    user_fn;   ///< identity anchor + keepalive
        std::vector<std::string>      name_storage;
        std::vector<std::string_view> names;
        std::size_t                   arity{0};
        bool                          has_output{true};
        /** The annotated output schema, when known (mesh_ needs the element
            type ahead of compilation). Null = resolve at compile. */
        const TSValueTypeMetaData    *output_schema{nullptr};
    };

    /** Immortal per-function records (stable context pointers; keyed by the
        user function object per the identity ruling). */
    [[nodiscard]] std::unordered_map<PyObject *, PyGraphFnRecord *> &py_graph_fn_registry()
    {
        static auto *registry = new std::unordered_map<PyObject *, PyGraphFnRecord *>{};
        return *registry;
    }

    // fn<X>() erases at a template instantiation point, so runtime names
    // resolve through a pre-instantiated table of the stdlib markers usable
    // as higher-order callables.
    [[nodiscard]] const std::unordered_map<std::string_view, WiredFn> &wired_fn_table()
    {
        static const auto *table = new std::unordered_map<std::string_view, WiredFn>{
            {"add_", fn<stdlib::add_>()},   {"sub_", fn<stdlib::sub_>()},
            {"mul_", fn<stdlib::mul_>()},   {"div_", fn<stdlib::div_>()},
            {"min_", fn<stdlib::min_>()},   {"max_", fn<stdlib::max_>()},
            {"bit_and", fn<stdlib::bit_and>()}, {"bit_or", fn<stdlib::bit_or>()},
            {"bit_xor", fn<stdlib::bit_xor>()}, {"union", fn<stdlib::union_>()},
            {"merge", fn<stdlib::merge>()}, {"eq_", fn<stdlib::eq_>()},
            {"not_", fn<stdlib::not_>()},   {"neg_", fn<stdlib::neg_>()},
            {"abs_", fn<stdlib::abs_>()},   {"str_", fn<stdlib::str_>()},
        };
        return *table;
    }

    struct PyPort
    {
        WiringPortRef ref{};
    };

    struct PyRun;

    struct PySwitchCases
    {
        stdlib::SwitchCases cases{};
    };

    /** hgraph's feedback: an unbound source port bound later to close a cycle. */
    struct PyFeedback
    {
        Wiring                    *wiring{nullptr};
        WiringPortRef              delegate{};
        const TSValueTypeMetaData *schema{nullptr};
        bool                       bound{false};
    };

    [[nodiscard]] std::vector<WiringArg> build_args(nb::tuple args, nb::dict kwargs)
    {
        std::vector<WiringArg> out;
        out.reserve(nb::len(args) + nb::len(kwargs));
        const auto push = [&](nb::handle object, std::string name) {
            WiringArg arg;
            arg.name = std::move(name);
            if (nb::isinstance<PyPort>(object))
            {
                arg.kind = WiringArg::Kind::TimeSeries;
                arg.port = nb::cast<PyPort &>(object).ref;
            }
            else if (nb::isinstance<PyWiredFn>(object))
            {
                arg.kind         = WiringArg::Kind::Scalar;
                arg.scalar_value = Value{nb::cast<PyWiredFn &>(object).fn};
                arg.scalar_meta  = arg.scalar_value.schema();
            }
            else if (nb::isinstance<PyScalarValue>(object))
            {
                arg.kind         = WiringArg::Kind::Scalar;
                arg.scalar_value = nb::cast<PyScalarValue &>(object).value;
                arg.scalar_meta  = arg.scalar_value.schema();
            }
            else if (nb::isinstance<PyNodeHandle>(object))
            {
                arg.kind         = WiringArg::Kind::Scalar;
                arg.scalar_value = Value{PyNodeRef{nb::cast<PyNodeHandle &>(object).record}};
                arg.scalar_meta  = arg.scalar_value.schema();
            }
            else if (nb::isinstance<PySwitchCases>(object))
            {
                arg.kind         = WiringArg::Kind::Scalar;
                arg.scalar_value = Value{nb::cast<PySwitchCases &>(object).cases};
                arg.scalar_meta  = arg.scalar_value.schema();
            }
            else
            {
                arg.kind         = WiringArg::Kind::Scalar;
                arg.scalar_value = py_to_value(object);
                arg.scalar_meta  = arg.scalar_value.schema();
            }
            out.push_back(std::move(arg));
        };
        for (nb::handle object : args) { push(object, {}); }
        for (auto [key, object] : kwargs) { push(object, nb::cast<std::string>(key)); }
        return out;
    }

    struct PyRun
    {
        GraphExecutorValue executor;

        /** Recorded read-back. DENSE (default): per-cycle values, None = no
            tick. SPARSE (recordings made with sparse=True, the harness's
            __elide__): (cycle_offset, delta) pairs in time order. */
        [[nodiscard]] nb::list recorded(const std::string &key, bool sparse)
        {
            nb::list result;
            if (sparse)
            {
                for (const auto &[offset, delta] :
                     testing::get_recorded_sparse(executor.view().graph().global_state(), key))
                {
                    result.append(nb::make_tuple(offset, value_to_py(delta.view())));
                }
                return result;
            }
            for (const auto &delta : testing::get_recorded_deltas(executor.view().graph().global_state(), key))
            {
                result.append(delta.has_value() ? value_to_py(delta->view()) : nb::none());
            }
            return result;
        }
    };

    struct PyWiring
    {
        // Owned by default; a BORROWED PyWiring (python graph callables run
        // against a Wiring the C++ side owns - e.g. a sub-graph compile)
        // aliases without ownership and cannot be run/finished.
        std::unique_ptr<GlobalContext> seed_context{};
        std::unique_ptr<Wiring>        owned{};
        Wiring                        *raw{nullptr};
        GlobalState                   *python_state{nullptr};
        bool                           finished{false};

        PyWiring()
            : owned(std::make_unique<Wiring>()), raw(owned.get())
        {
        }

        explicit PyWiring(GlobalState &state)
            : seed_context(std::make_unique<GlobalContext>(state)),
              owned(std::make_unique<Wiring>()),
              raw(owned.get()),
              python_state(&state)
        {
        }

        [[nodiscard]] static PyWiring borrow(Wiring &target)
        {
            PyWiring result;
            result.owned.reset();
            result.raw = &target;
            return result;
        }

        [[nodiscard]] Wiring &wiring_ref()
        {
            if (raw == nullptr) { throw std::logic_error("Wiring is no longer available"); }
            return *raw;
        }

        [[nodiscard]] nb::object wire(const std::string &name, nb::tuple args, nb::dict kwargs,
                                      std::optional<PyTsType> output_type,
                                      std::optional<std::vector<std::size_t>> sizes = std::nullopt)
        {
            ensure_open();
            auto wiring_args = build_args(args, kwargs);
            // Target-directed scalar conversion: with an explicit output
            // type, a leading plain-python scalar converts AT the target's
            // value schema (const((1,2,3), tp=TS[tuple[int,...]]) builds
            // the variadic tuple, not a generic mutable list).
            if (name == "const" && output_type.has_value() && output_type->meta != nullptr &&
                output_type->meta->value_schema != nullptr && nb::len(args) >= 1 &&
                !wiring_args.empty() && wiring_args[0].kind == WiringArg::Kind::Scalar)
            {
                try
                {
                    Value converted = py_to_value_as(args[0], output_type->meta->value_schema);
                    wiring_args[0].scalar_value = std::move(converted);
                    wiring_args[0].scalar_meta  = wiring_args[0].scalar_value.schema();
                }
                catch (const std::exception &)
                {
                    // keep the generic conversion; resolution reports mismatches
                }
            }
            const std::vector<std::size_t> size_hints = sizes.value_or(std::vector<std::size_t>{});
            ResolvedOperatorCall resolved = OperatorRegistry::instance().resolve(
                name, std::span<const WiringArg>{wiring_args.data(), wiring_args.size()}, std::nullopt,
                output_type.has_value() ? output_type->meta : nullptr,
                std::span<const std::size_t>{size_hints.data(), size_hints.size()},
                wiring_ref().operator_state(), &wiring_ref());
            OperatorWireResult result =
                resolved.impl->wire(wiring_ref(), resolved.map, resolved.args, resolved.kwargs);
            if (!result.has_output) { return nb::none(); }
            return nb::cast(PyPort{result.output.erased()});
        }

        void set_replay(const std::string &key, nb::list values, std::optional<PyTsType> ts_type)
        {
            ensure_open();
            std::vector<std::optional<Value>> deltas;
            deltas.reserve(nb::len(values));
            for (nb::handle object : values)
            {
                if (object.is_none()) { deltas.emplace_back(std::nullopt); }
                else if (ts_type.has_value()) { deltas.emplace_back(py_to_delta(object, ts_type->meta)); }
                else { deltas.emplace_back(py_to_value(object)); }
            }
            testing::set_replay_deltas(wiring_ref().global_state(), key, deltas);
        }

        [[nodiscard]] std::unique_ptr<PyRun> run(std::optional<DateTime> start_time, std::optional<DateTime> end_time,
                                                 bool realtime)
        {
            ensure_open();
            if (owned == nullptr) { throw std::logic_error("a borrowed Wiring cannot be run"); }
            finished = true;
            GraphBuilder builder = std::move(*owned).finish();

            GraphExecutorBuilder eb;
            eb.graph_builder(std::move(builder))
                .start_time(start_time.value_or(MIN_ST))
                .end_time(end_time.value_or(MAX_ET))
                .mode(realtime ? GraphExecutorMode::RealTime : GraphExecutorMode::Simulation);
            auto run = std::make_unique<PyRun>(PyRun{eb.make_executor()});
            {
                // Ruling: the GIL is released the instant we enter the run
                // loop; python user nodes re-acquire it per call.
                nb::gil_scoped_release release;
                run->executor.view().run();
            }
            if (python_state != nullptr)
            {
                python_state->view().copy_from(run->executor.view().graph().global_state());
            }
            return run;
        }

        [[nodiscard]] nb::tuple push_source(PyTsType ts_type, bool conflate, nb::object on_start)
        {
            ensure_open();
            auto slot     = std::make_shared<PySenderSlot>();
            slot->schema  = ts_type.meta;
            auto policy   = conflate ? make_push_source_conflating_policy(*ts_type.meta->value_schema)
                                     : make_push_source_queue_policy(*ts_type.meta->value_schema);
            NodeBuilder builder = make_push_source_node(
                *ts_type.meta, std::move(policy), [slot, on_start](PushSourceSender sender) {
                    slot->sender = std::move(sender);
                    if (!on_start.is_none())
                    {
                        // hgraph's @push_queue contract: the wrapped function
                        // IS the start lifecycle hook, invoked with the sender.
                        nb::gil_scoped_acquire gil;
                        on_start(nb::cast(PySender{slot}));
                    }
                });
            struct py_push_source_tag
            {
            };
            WiringPortRef ref = wiring_ref().add_unique_node(std::type_index(typeid(py_push_source_tag)),
                                                             std::move(builder), std::span<const WiringPortRef>{},
                                                             Value{});
            return nb::make_tuple(PyPort{std::move(ref)}, PySender{std::move(slot)});
        }

        [[nodiscard]] PyFeedback feedback(PyTsType ts_type, nb::handle initial)
        {
            ensure_open();
            const auto *schema = stdlib::feedback_detail::require_feedback_schema(ts_type.meta);
            Value       initial_delta;
            const bool  has_initial = !initial.is_none();
            if (has_initial)
            {
                initial_delta = py_to_delta(initial, schema);
                stdlib::feedback_detail::validate_initial_delta(*schema, initial_delta);
            }
            NodeBuilder builder = make_feedback_source_node(*schema, has_initial);
            WiringPortRef ref   = wiring_ref().add_unique_node(
                std::type_index(typeid(stdlib::feedback_detail::feedback_source_node_tag)), std::move(builder),
                std::span<const WiringPortRef>{}, std::move(initial_delta));
            return PyFeedback{&wiring_ref(), std::move(ref), schema, false};
        }

        void feedback_bind(PyFeedback &fb, const PyPort &port)
        {
            ensure_open();
            if (fb.bound) { throw std::logic_error("feedback is already bound"); }
            WiringPortRef ts_source =
                graph_wiring_detail::adapt_source_for_input(wiring_ref(), fb.schema, port.ref);
            WiringPortRef self_source =
                graph_wiring_detail::adapt_source_for_input(wiring_ref(), fb.schema, fb.delegate);
            std::array<WiringPortRef, 2> sources{std::move(ts_source), std::move(self_source)};

            NodeBuilder builder = make_feedback_sink_node(*fb.schema);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.binding().type_meta != nullptr ? builder.binding().type_meta->input_schema : nullptr,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));
            (void)wiring_ref().add_node(
                std::type_index(typeid(stdlib::feedback_detail::feedback_sink_node_tag)), std::move(builder),
                std::span<const WiringPortRef>{sources.data(), sources.size()}, Value{});
            fb.bound = true;
        }

        void release_seed_context() noexcept { seed_context.reset(); }

      private:
        void ensure_open() const
        {
            if (finished || raw == nullptr) { throw std::logic_error("this Wiring has already been finished/run"); }
        }
    };
    [[nodiscard]] WiringPortRef py_graph_fn_wire(const void *context, Wiring &w,
                                                 std::span<const WiringPortRef> args)
    {
        const auto &record = *static_cast<const PyGraphFnRecord *>(context);
        nb::gil_scoped_acquire gil;
        nb::list ports;
        for (const WiringPortRef &arg : args) { ports.append(nb::cast(PyPort{arg})); }
        nb::object borrowed = nb::cast(PyWiring::borrow(w));
        nb::object result   = record.wrapper(borrowed, nb::tuple(ports));
        if (result.is_none()) { return {}; }
        return nb::cast<PyPort &>(result).ref;
    }

    [[nodiscard]] CompiledSubGraph py_graph_fn_compile(const void *context,
                                                       std::span<const TSValueTypeMetaData *const> input_schemas)
    {
        const auto &record = *static_cast<const PyGraphFnRecord *>(context);
        if (input_schemas.size() != record.arity)
        {
            throw std::invalid_argument("python graph fn: compiled input schema count does not match its inputs");
        }
        Wiring child{WiringKind::SubGraph};
        std::vector<const TSValueTypeMetaData *> schemas{input_schemas.begin(), input_schemas.end()};
        std::vector<WiringPortRef> boundary;
        boundary.reserve(input_schemas.size());
        for (std::size_t index = 0; index < input_schemas.size(); ++index)
        {
            boundary.push_back(WiringPortRef::boundary_source(index, {}, input_schemas[index]));
        }
        WiringPortRef out =
            py_graph_fn_wire(context, child, {boundary.data(), boundary.size()});
        if (record.has_output)
        {
            return std::move(child).finish_subgraph(out, std::move(schemas));
        }
        return std::move(child).finish_subgraph(std::nullopt, std::move(schemas));
    }

    [[nodiscard]] const WiredFnOps &py_graph_fn_ops()
    {
        static constexpr WiredFnOps ops{
            &py_graph_fn_wire,
            &py_graph_fn_compile,
            [](const void *context) {
                const auto &record = *static_cast<const PyGraphFnRecord *>(context);
                return std::span<const std::string_view>{record.names.data(), record.names.size()};
            },
            [](const void *context) {
                // Known when the python fn carries a TS return annotation
                // (mesh_ learns its element type this way); else null.
                return static_cast<const PyGraphFnRecord *>(context)->output_schema;
            },
        };
        return ops;
    }


    // ---------------------------------------------------------------
    // Python user nodes (@compute_node / @generator / @sink_node).
    // Ruling: graph-thread only, both modes; the GIL is RELEASED on
    // entering the run loop and ACQUIRED around each python call; values
    // cross the boundary through the module converters.
    // ---------------------------------------------------------------

    struct PyNodeRecord
    {
        nb::object fn;
    };

    /** Immortal callable records (stable scalar identity by pointer). */
    [[nodiscard]] std::unordered_map<PyObject *, PyNodeRecord *> &py_node_registry()
    {
        static auto *registry = new std::unordered_map<PyObject *, PyNodeRecord *>{};
        return *registry;
    }

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
        Value delta = py_to_delta(result, erased.schema());
        // The python return is a CANONICAL DELTA (py_to_delta) - apply it as
        // one (atomic TS deltas coincide with values; compound kinds do not).
        apply_delta(out, delta.view());
    }

    /**
     * ONE compute/sink operator for ANY arity (Howard's review: per-arity
     * stubs do not scale): the argument ports pack into a STRUCTURAL
     * un-named TSB, wiring-time SCALARS ride a list-of-Any scalar, and the
     * LAYOUT string (part of node identity) maps the python call positions:
     * ``t`` = next ts field, ``s`` = next scalar, ``S`` = STATE namespace,
     * ``c`` = CLOCK, ``d`` = SCHEDULER. All ts fields must hold values
     * before the python function is called (the all-valid gate).
     */
    struct PyStateRef
    {
        PyObject *ns{nullptr};   ///< a SimpleNamespace, lazily created (per-node python STATE)
        friend bool operator==(const PyStateRef &, const PyStateRef &) noexcept = default;
    };

    struct PyEvalClock
    {
        DateTime evaluation_time{};
    };

    struct PyScheduler
    {
        NodeScheduler scheduler;
    };

    [[nodiscard]] nb::object py_state_namespace(State<PyStateRef> &state)
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

    void py_release_state(State<PyStateRef> &state)
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

    /**
     * The hgraph TimeSeries object handed to python user nodes: a LAZY,
     * C++-bound view over the node's live input - nothing converts unless
     * accessed. Kind-specific methods dispatch on the schema (TS/TSS/TSD/
     * TSL/TSB); child access returns child views sharing the same guard.
     */
    /** Read-only view of the node's OWN OUTPUT (hgraph's ``_output``
        injection): ``valid`` / ``value`` for delta-computing nodes. */
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
            if (v.schema() != nullptr && v.schema()->kind == TSTypeKind::REF)
            {
                // A REF input's value is the REFERENCE - TSInputView::
                // reference() reads the to-REF alternative's populated value
                // (peered at the true upstream output).
                return nb::cast(python_bridge::PyOpaqueRef{Value{v.reference()}});
            }
            return value_to_py(v.value());
        }

        [[nodiscard]] nb::object delta_value() const
        {
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TS: return value_to_py(ts.value());
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
                    auto     set = ts.as_set();
                    nb::list added_items;
                    for (const ValueView &element : set.added()) { added_items.append(value_to_py(element)); }
                    nb::object added = nb::steal(PyFrozenSet_New(nb::list(added_items).ptr()));
                    nb::list removed_items;
                    for (const ValueView &element : set.removed()) { removed_items.append(value_to_py(element)); }
                    if (nb::len(removed_items) == 0) { return added; }
                    nb::dict result;
                    result["added"]   = added;
                    result["removed"] = nb::steal(PyFrozenSet_New(nb::list(removed_items).ptr()));
                    return result;
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

    /**
     * Python inputs share one packed structural port, so per-child activity is
     * applied before acquiring the GIL and invoking user code. Uppercase TS
     * layout markers are passive. Runtime scheduler events remain independent
     * of input activity, including for ``active=()`` nodes.
     */
    [[nodiscard]] bool py_node_should_evaluate(std::string_view layout, const TSInputView &args,
                                               const NodeScheduler &scheduler)
    {
        if (scheduler.is_scheduled_now()) { return true; }
        auto        bundle   = args.as_bundle();
        std::size_t ts_index = 0;
        for (const char kind : layout)
        {
            switch (kind)
            {
                // Activity is REAL and runtime-mutable (make_active /
                // make_passive from python) — consult the link, not the
                // static layout marker.
                case 't':
                case 'u':
                case 'C':
                case 'T':
                case 'U':
                case 'P': {
                    auto child = bundle[ts_index++];
                    if (child.active() && child.modified()) { return true; }
                    break;
                }
                default: break;
            }
        }
        return false;
    }

    /**
     * Make python-node input activity REAL at the per-child link level.
     *
     * The framework activates the packed ``args`` port as ONE root
     * subscription; runtime ``make_passive()``/``make_active()`` from user
     * code operates on the CHILD links, which that root subscription would
     * bypass (a passivated child still notified through the bundle root).
     * At start we activate each child per its layout marker and drop the
     * root subscription, so per-child activity — including runtime changes
     * from python code — is the single subscription model.
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
        args.make_passive();   // the framework's root subscription
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

    /** Assemble the python call args per the layout; false = a ts arg is not yet valid. */
    [[nodiscard]] bool py_assemble_args(std::string_view layout, const TSInputView &args, const ValueView &scalars,
                                        State<PyStateRef> &state, NodeScheduler scheduler, DateTime now,
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
                case 'S': call_args.append(py_state_namespace(state)); break;
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

    /** Enter context-manager values (hgraph's context semantics), call, exit in reverse. */
    [[nodiscard]] nb::object py_call_with_contexts(const nb::object &fn, nb::list &call_args,
                                                   nb::list &context_values,
                                                   const nb::object &runtime_global_state)
    {
        nb::object runtime = nb::module_::import_("hgraph._runtime");
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
        nb::object result = fn(*nb::tuple(call_args));
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

        static void start(In<"args", TsVar<"A">, InputValidity::Unchecked> args,
                          Scalar<"config", Str> eval_config,
                          Scalar<"start_fn", PyNodeRef> fn, Scalar<"start_enabled", Bool> enabled,
                          Scalar<"start_config", Str> config,
                          Scalar<"start_scalars", ScalarVar<"SSV">> scalars,
                          State<PyStateRef> state, NodeScheduler scheduler, DateTime now,
                          GlobalStateView global_state)
        {
            py_apply_input_activity(eval_config.value(), args.base());
            py_call_lifecycle(fn.value(), enabled.value(), config.value(), scalars.value(), state, scheduler, now,
                              global_state);
        }

        static void eval(In<"args", TsVar<"A">, InputValidity::Unchecked> args, Scalar<"fn", PyNodeRef> fn,
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
            if (!py_node_should_evaluate(config.value(), args.base(), scheduler)) { return; }
            nb::gil_scoped_acquire gil;
            nb::list call_args;
            nb::list context_values;
            auto     guard   = std::make_shared<PyTsGuard>();
            auto     invalid = UnwindCleanupGuard([&] { guard->alive = false; });
            const auto &out_view = static_cast<const TSOutputView &>(out);
            nb::object runtime_state = nb::cast(PyRuntimeGlobalState{global_state, guard});
            if (!py_assemble_args(config.value(), args.base(), scalars.value(), state, scheduler, now, call_args,
                                  context_values, guard, runtime_state, &out_view))
            {
                return;
            }
            apply_py_result(
                py_call_with_contexts(fn.value().record->fn, call_args, context_values, runtime_state), out);
            invalid.release();
            guard->alive = false;
        }

        static void stop(In<"args", TsVar<"A">, InputValidity::Unchecked> args,
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
            py_clear_input_activity(eval_config.value(), args.base());
            auto release = UnwindCleanupGuard([&] { py_release_state(state); });
            py_call_lifecycle(fn.value(), enabled.value(), config.value(), scalars.value(), state, scheduler, now,
                              global_state);
            release.release();
            py_release_state(state);
        }
    };

    struct py_sink_node
    {
        static constexpr auto name = "__py_sink";

        static void start(In<"args", TsVar<"A">, InputValidity::Unchecked> args,
                          Scalar<"config", Str> eval_config,
                          Scalar<"start_fn", PyNodeRef> fn, Scalar<"start_enabled", Bool> enabled,
                          Scalar<"start_config", Str> config,
                          Scalar<"start_scalars", ScalarVar<"SSV">> scalars,
                          State<PyStateRef> state, NodeScheduler scheduler, DateTime now,
                          GlobalStateView global_state)
        {
            py_apply_input_activity(eval_config.value(), args.base());
            py_call_lifecycle(fn.value(), enabled.value(), config.value(), scalars.value(), state, scheduler, now,
                              global_state);
        }

        static void eval(In<"args", TsVar<"A">, InputValidity::Unchecked> args, Scalar<"fn", PyNodeRef> fn,
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
            if (!py_node_should_evaluate(config.value(), args.base(), scheduler)) { return; }
            nb::gil_scoped_acquire gil;
            nb::list call_args;
            nb::list context_values;
            auto     guard   = std::make_shared<PyTsGuard>();
            auto     invalid = UnwindCleanupGuard([&] { guard->alive = false; });
            nb::object runtime_state = nb::cast(PyRuntimeGlobalState{global_state, guard});
            if (!py_assemble_args(config.value(), args.base(), scalars.value(), state, scheduler, now, call_args,
                                  context_values, guard, runtime_state))
            {
                return;
            }
            (void)py_call_with_contexts(fn.value().record->fn, call_args, context_values, runtime_state);
            invalid.release();
            guard->alive = false;
        }

        static void stop(In<"args", TsVar<"A">, InputValidity::Unchecked> args,
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
            py_clear_input_activity(eval_config.value(), args.base());
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
        const DateTime when = nb::cast<DateTime>(pair[0]);
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
struct std::hash<PyNodeRef>
{
    [[nodiscard]] std::size_t operator()(const PyNodeRef &ref) const noexcept
    {
        return std::hash<const void *>{}(ref.record);
    }
};

template <>
struct std::hash<PyStateRef>
{
    [[nodiscard]] std::size_t operator()(const PyStateRef &ref) const noexcept
    {
        return std::hash<const void *>{}(ref.ns);
    }
};

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
    struct scalar_name<PyNodeRef>
    {
        static constexpr std::string_view value{"py_node_ref"};
    };

    template <>
    struct scalar_name<PyStateRef>
    {
        static constexpr std::string_view value{"py_state"};
    };

    template <>
    struct scalar_name<PyGenStateRef>
    {
        static constexpr std::string_view value{"py_gen_state"};
    };
}  // namespace hgraph::static_schema_detail

NB_MODULE(_hgraph, m)
{
    m.doc() = "hgraph C++ runtime bridge (slice 1: wire and run graphs by operator name)";

    // The graph-callable records are immortal by design (WiredFn contexts
    // must outlive every value referencing them), so their held Python
    // objects survive interpreter teardown - not a refcount bug.
    nb::set_leak_warnings(false);

    stdlib::register_standard_operators();
    python_bridge::py_infer_value_slot() =
        reinterpret_cast<python_bridge::PyInferValueFn>(&python_bridge::py_to_value);
    // The enum slots read the meta -> python-Enum-class registry (an
    // immortal map; lazily constructed by its accessor, cleared with the
    // registries).
    enum_to_python_slot() = [](const ValueTypeMetaData *meta, long long value) -> nb::object {
        auto &registry = python_bridge::enum_class_registry();
        if (const auto it = registry.find(meta); it != registry.end()) { return it->second(value); }
        throw std::logic_error(std::string{"enum '"} +
                               (meta->display_name ? meta->display_name : "?") +
                               "' has no registered python class");
    };
    enum_from_python_slot() = [](const ValueTypeMetaData *, nb::handle source) -> long long {
        return nb::cast<long long>(source.attr("value"));
    };

    // Route the diagnostic sinks (debug_print / print_) through python's
    // sys.stdout/sys.stderr so redirection and pytest capture behave like
    // hgraph's python prints (the run loop releases the GIL - acquire).
    stdlib::io_write_slot() = [](std::string_view line, bool to_stdout) {
        nb::gil_scoped_acquire gil;
        nb::object stream = nb::module_::import_("sys").attr(to_stdout ? "stdout" : "stderr");
        stream.attr("write")(nb::str((std::string{line} + "\n").c_str()));
    };
    (void)scalar_descriptor<PyObj>::value_meta();   // the python-object scalar
    register_overload<op_materialize, materialize_node>();
    register_overload<op_py_compute, py_compute_node>();
    register_overload<op_py_sink, py_sink_node>();
    register_overload<op_py_generator, py_generator_node>();
    register_overload<op_recover_pt, stdlib::component_detail::recovering_pass_through>();
    register_overload<op_harness_replay, harness_replay>();
    register_overload<op_harness_record, harness_record>();
    register_overload<stdlib::until_true, until_true_callable_node>();
    register_graph_overload<stdlib::freeze, freeze_callable_compose>();
    register_overload<stdlib::call_op, call_callable_node>();

    nb::class_<PyTsType>(m, "TsType")
        .def("__eq__", [](const PyTsType &self, nb::handle other) {
            return nb::isinstance<PyTsType>(other) && nb::cast<PyTsType &>(other).meta == self.meta;
        })
        .def("__hash__", [](const PyTsType &self) { return std::hash<const void *>{}(self.meta); })
        .def_prop_ro("kind", [](const PyTsType &self) { return static_cast<int>(self.meta->kind); })
        .def_prop_ro("value_kind",
                     [](const PyTsType &self) {
                         return self.meta->value_schema != nullptr
                                    ? static_cast<int>(self.meta->value_schema->kind)
                                    : -1;
                     })
        .def_prop_ro("fixed_size", [](const PyTsType &self) {
            return self.meta->kind == TSTypeKind::TSL ? self.meta->fixed_size() : 0;
        })
        .def_prop_ro("is_ts", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TS;
        })
        .def_prop_ro("is_tsd", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TSD;
        })
        .def_prop_ro("is_tsl", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TSL;
        })
        .def_prop_ro("is_tsb", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TSB;
        })
        .def_prop_ro("is_ref", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::REF;
        })
        .def_prop_ro("is_fixed_tsl", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TSL && self.meta->fixed_size() > 0;
        })
        .def_prop_ro("is_ts_bundle", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TS &&
                   self.meta->value_schema != nullptr && self.meta->value_schema->kind == ValueTypeKind::Bundle;
        })
        .def_prop_ro("is_ts_mapping", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TS &&
                   self.meta->value_schema != nullptr && self.meta->value_schema->kind == ValueTypeKind::Map;
        })
        .def_prop_ro("is_tss", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TSS;
        })
        .def_prop_ro("is_ts_sequence", [](const PyTsType &self) {
            return self.meta != nullptr && self.meta->kind == TSTypeKind::TS &&
                   self.meta->value_schema != nullptr &&
                   (self.meta->value_schema->kind == ValueTypeKind::Tuple ||
                    self.meta->value_schema->kind == ValueTypeKind::List);
        })
        .def_prop_ro("is_ts_json", [](const PyTsType &self) {
            return stdlib::json_tree::is_json_ts(self.meta);
        })
        .def("__repr__", [](const PyTsType &self) {
            return std::string{self.meta != nullptr && self.meta->display_name != nullptr ? self.meta->display_name
                                                                                          : "<ts?>"};
        });
    nb::class_<PyScalarPattern>(m, "ScalarPattern")
        .def("__repr__", [](const PyScalarPattern &self) {
            return scalar_pattern_to_string(self.pattern);
        });
    nb::class_<PySizePattern>(m, "SizePattern")
        .def("__repr__", [](const PySizePattern &self) {
            return self.variable ? "~" + self.name : std::to_string(self.value);
        });
    nb::class_<PyTypePattern>(m, "TypePattern")
        .def("__repr__", [](const PyTypePattern &self) {
            return ts_pattern_to_string(self.pattern);
        });
    nb::class_<PyPort>(m, "Port")
        .def_prop_ro("ts_type", [](const PyPort &self) { return PyTsType{self.ref.schema}; })
        .def_prop_ro("is_structural", [](const PyPort &self) { return self.ref.is_structural_source(); })
        // True for a CHILD projection of a node output (a non-empty peered
        // path) - sub-graph terminals need whole node outputs.
        .def_prop_ro("has_path", [](const PyPort &self) { return !self.ref.peered_path_or_empty().empty(); })
        .def_prop_ro("dereferenced", [](const PyPort &self) {
            // The descriptive-schema patch (the Port::as / reference-service
            // pattern): present a REF output as its value schema - input
            // binding inserts the REF adaptation. Applied RECURSIVELY so a
            // TSB with REF fields records its fields dereferenced
            // (eval_node parity: REF outputs record dereferenced values).
            if (self.ref.schema == nullptr) { return self; }
            if (self.ref.schema->kind == TSTypeKind::REF)
            {
                PyPort patched = self;
                patched.ref.schema = self.ref.schema->referenced_ts();
                return patched;
            }
            if (self.ref.schema->kind == TSTypeKind::TSD)
            {
                // DEEP dereference: REF elements at ANY nesting level patch
                // to their referenced shape (the elementwise from-REF
                // alternative recurses through nested dictionaries).
                const std::function<const TSValueTypeMetaData *(const TSValueTypeMetaData *)> deep =
                    [&](const TSValueTypeMetaData *schema) -> const TSValueTypeMetaData * {
                    auto &registry = TypeRegistry::instance();
                    const auto *current = registry.dereference(schema);
                    if (current != nullptr && current->kind == TSTypeKind::TSD)
                    {
                        const auto *element = deep(current->element_ts());
                        if (element != current->element_ts())
                        {
                            return registry.tsd(current->key_type(), element);
                        }
                    }
                    return current;
                };
                const auto *patched_schema = deep(self.ref.schema);
                if (patched_schema != self.ref.schema)
                {
                    PyPort patched  = self;
                    patched.ref.schema = patched_schema;
                    return patched;   // the from-REF dict alternative resolves it
                }
            }
            if (self.ref.schema->kind == TSTypeKind::TSB)
            {
                const auto *fields = self.ref.schema->fields();
                const auto  count  = self.ref.schema->field_count();
                bool        has_ref = false;
                for (std::size_t index = 0; index < count; ++index)
                {
                    const auto *type = fields[index].type;
                    if (type != nullptr && type->kind == TSTypeKind::REF) { has_ref = true; break; }
                }
                if (has_ref)
                {
                    auto &registry = TypeRegistry::instance();
                    std::vector<std::pair<std::string, const TSValueTypeMetaData *>> patched_fields;
                    patched_fields.reserve(count);
                    for (std::size_t index = 0; index < count; ++index)
                    {
                        const auto *type = fields[index].type;
                        while (type != nullptr && type->kind == TSTypeKind::REF) { type = type->referenced_ts(); }
                        patched_fields.emplace_back(std::string{fields[index].name}, type);
                    }
                    PyPort patched = self;
                    patched.ref.schema = registry.un_named_tsb(patched_fields);
                    return patched;
                }
            }
            return self;
        });
    nb::class_<PyRun>(m, "Run").def("recorded", &PyRun::recorded, nb::arg("key"), nb::arg("sparse") = false);

    m.def("ts_type", [](const std::string &name) {
        const auto *meta = TypeRegistry::instance().time_series_type(name);
        if (meta == nullptr) { throw nb::value_error(("unknown time-series type: " + name).c_str()); }
        return PyTsType{meta};
    });

    // --- time-series type CONSTRUCTION (the Python type layer builds
    // TS[...]/TSS[...]/TSD[...]/TSL[...]/TSB[...] through these) ---
    nb::class_<PyValueType>(m, "ValueType");
    m.def("value_type", [](const std::string &name) {
        const auto *meta = TypeRegistry::instance().value_type(name);
        if (meta == nullptr) { throw nb::value_error(("unknown value type: " + name).c_str()); }
        return PyValueType{meta};
    });
    m.def("ts", [](PyValueType v) { return PyTsType{TypeRegistry::instance().ts(v.meta)}; });
    m.def("ref_ts", [](PyTsType target) { return PyTsType{TypeRegistry::instance().ref(target.meta)}; });
    m.def("ref_target", [](PyTsType ref) { return PyTsType{TypeRegistry::instance().dereference(ref.meta)}; });
    m.def("set_vt", [](PyValueType e) { return PyValueType{TypeRegistry::instance().set(e.meta)}; });
    m.def("map_vt", [](PyValueType k, PyValueType v) {
        return PyValueType{TypeRegistry::instance().map(k.meta, v.meta)};
    });
    m.def("tuple_vt", [](PyValueType e) {
        // A homogeneous variadic tuple (python's tuple[X, ...]).
        return PyValueType{TypeRegistry::instance().list(e.meta, 0, true)};
    });
    m.def("nullable_tuple_vt", [](PyValueType e) {
        // A NULLABLE variadic tuple (elements may be None holes).
        return PyValueType{TypeRegistry::instance().nullable_tuple(e.meta)};
    });
    m.def("series_vt", [](PyValueType e) {
        return PyValueType{TypeRegistry::instance().series(e.meta)};
    });
    m.def("un_named_bundle_vt", [](nb::list fields) {
        // The structural (un-named) bundle - python's compound_scalar()
        // anonymous compounds (nominal-vs-structural rule, scalar.rst).
        std::vector<std::pair<std::string, const ValueTypeMetaData *>> field_metas;
        field_metas.reserve(nb::len(fields));
        for (nb::handle field : fields)
        {
            auto pair = nb::cast<nb::tuple>(field);
            field_metas.emplace_back(nb::cast<std::string>(pair[0]),
                                     nb::cast<PyValueType &>(pair[1]).meta);
        }
        return PyValueType{TypeRegistry::instance().un_named_bundle(field_metas)};
    });
    m.def("frame_vt", [](PyValueType schema) {
        // Frame[Schema]: the typed frame meta carrying its column bundle.
        return PyValueType{TypeRegistry::instance().frame(schema.meta)};
    });
    m.def("table_schema_info", [](PyTsType ts, const std::string &date_key, const std::string &as_of_key) {
        // TABLE layout introspection (design record step 6): the C++ layout
        // is the single source; python's TableSchema maps it declaratively.
        const auto &layout =
            hgraph::stdlib::table_ts_detail::ts_table_layout(ts.meta, date_key, as_of_key);
        nb::dict info;
        nb::list keys, types, partition_keys, removed_keys;
        for (std::size_t i = 0; i < layout.keys.size(); ++i)
        {
            keys.append(nb::str(layout.keys[i].c_str()));
            const auto *meta = layout.col_metas[i];
            types.append(
                nb::str(meta != nullptr && meta->display_name != nullptr ? meta->display_name : "?"));
        }
        for (const auto &name : layout.partition_keys) { partition_keys.append(nb::str(name.c_str())); }
        for (const auto &name : layout.removed_keys) { removed_keys.append(nb::str(name.c_str())); }
        info["keys"]           = keys;
        info["types"]          = types;
        info["partition_keys"] = partition_keys;
        info["removed_keys"]   = removed_keys;
        info["date_key"]       = nb::str(layout.date_key.c_str());
        info["as_of_key"]      = nb::str(layout.as_of_key.c_str());
        info["is_multi_row"]   = layout.is_multi_row;
        return info;
    });
    m.def("fixed_tuple_vt", [](nb::list elements) {
        std::vector<const ValueTypeMetaData *> metas;
        metas.reserve(nb::len(elements));
        for (nb::handle element : elements) { metas.push_back(nb::cast<PyValueType &>(element).meta); }
        return PyValueType{TypeRegistry::instance().tuple(metas)};
    });
    // Type INTROSPECTION for wiring-time target inference (py convert etc.).
    m.def("ts_value_vt", [](PyTsType t) { return PyValueType{t.meta->value_schema}; });
    m.def("vt_kind", [](PyValueType v) { return static_cast<int>(v.meta->kind); });
    m.def("vt_element", [](PyValueType v) { return PyValueType{v.meta->element_type}; });
    m.def("vt_key", [](PyValueType v) { return PyValueType{v.meta->key_type}; });
    m.def("tsd_key_vt", [](PyTsType t) { return PyValueType{t.meta->key_type()}; });
    m.def("tsb_value_vt", [](PyTsType t) { return PyValueType{t.meta->value_schema}; });
    m.def("tsl_element_ts", [](PyTsType t) { return PyTsType{t.meta->element_ts()}; });
    m.def("tss", [](PyValueType v) { return PyTsType{TypeRegistry::instance().tss(v.meta)}; });
    m.def("tsd", [](PyValueType k, PyTsType v) { return PyTsType{TypeRegistry::instance().tsd(k.meta, v.meta)}; });
    m.def("tsw", [](PyValueType v, std::size_t period, std::size_t min_period) {
        return PyTsType{TypeRegistry::instance().tsw(v.meta, period, min_period)};
    }, nb::arg("value"), nb::arg("period"), nb::arg("min_period") = 0);
    // The scalar-level JSON builders (hgraph's to_json_builder /
    // from_json_builder): serialize/parse a plain value by its schema.
    m.def("value_to_json", [](PyValueType meta, nb::handle value) {
        const Value owned = python_bridge::py_to_value_as(value, meta.meta);
        return to_json_string(owned.view());
    });
    m.def("value_from_json", [](PyValueType meta, const std::string &text) {
        const Value parsed = from_json_string(meta.meta, text);
        return python_bridge::value_to_py(parsed.view());
    });
    m.def("enum_vt", [](const std::string &name, nb::list members, nb::object cls) {
        std::vector<std::pair<std::string, long long>> table;
        table.reserve(nb::len(members));
        for (nb::handle item : members)
        {
            auto pair = nb::cast<nb::tuple>(item);
            table.emplace_back(nb::cast<std::string>(pair[0]), nb::cast<long long>(pair[1]));
        }
        const auto *meta = TypeRegistry::instance().enum_type(name, table);
        python_bridge::enum_class_registry()[meta] = std::move(cls);
        return PyValueType{meta};
    });
    m.def("tsw_duration", [](PyValueType v, TimeDelta time_range, TimeDelta min_time_range) {
        return PyTsType{TypeRegistry::instance().tsw_duration(v.meta, time_range, min_time_range)};
    }, nb::arg("value"), nb::arg("time_range"), nb::arg("min_time_range") = TimeDelta{0});
    m.def("tsl", [](PyTsType e, std::size_t size) { return PyTsType{TypeRegistry::instance().tsl(e.meta, size)}; },
          nb::arg("element"), nb::arg("size") = 0);
    m.def("tsb", [](const std::string &name, nb::list fields) {
        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> entries;
        entries.reserve(nb::len(fields));
        for (nb::handle field : fields)
        {
            auto pair = nb::cast<nb::tuple>(field);
            entries.emplace_back(nb::cast<std::string>(pair[0]), nb::cast<PyTsType &>(pair[1]).meta);
        }
        return PyTsType{TypeRegistry::instance().tsb(name, entries)};
    });

    m.def("scalar_pattern_var", [](const std::string &name) {
        return PyScalarPattern{ScalarPattern::var(name)};
    });
    m.def("scalar_pattern_value", [](PyValueType value) {
        return PyScalarPattern{ScalarPattern::concrete(value.meta)};
    });
    m.def("scalar_pattern_unknown_tuple", [] {
        return PyScalarPattern{ScalarPattern::unknown_tuple()};
    });
    m.def("scalar_pattern_unknown_tuple", [](PyScalarPattern element) {
        return PyScalarPattern{ScalarPattern::unknown_tuple(std::move(element.pattern))};
    });
    m.def("scalar_pattern_homogeneous_tuple", [](PyScalarPattern element) {
        return PyScalarPattern{ScalarPattern::homogeneous_tuple(std::move(element.pattern))};
    });
    m.def("scalar_pattern_fixed_tuple", [](nb::list elements) {
        std::vector<ScalarPattern> patterns;
        patterns.reserve(nb::len(elements));
        for (nb::handle element : elements) { patterns.push_back(nb::cast<PyScalarPattern>(element).pattern); }
        return PyScalarPattern{ScalarPattern::fixed_tuple(std::move(patterns))};
    });
    m.def("scalar_pattern_set", [](PyScalarPattern element) {
        return PyScalarPattern{ScalarPattern::set(std::move(element.pattern))};
    });
    m.def("scalar_pattern_map", [](PyScalarPattern key, PyScalarPattern value) {
        return PyScalarPattern{ScalarPattern::map(std::move(key.pattern), std::move(value.pattern))};
    });
    m.def("scalar_pattern_bundle", [] {
        return PyScalarPattern{ScalarPattern::bundle()};
    });
    m.def("scalar_pattern_bundle", [](const std::string &schema_variable) {
        return PyScalarPattern{ScalarPattern::bundle_var(schema_variable)};
    });

    m.def("size_pattern_var", [](const std::string &name) {
        return PySizePattern{true, name, 0};
    });
    m.def("size_pattern_value", [](std::size_t value) {
        return PySizePattern{false, {}, value};
    });

    m.def("type_pattern_var", [](const std::string &name) {
        return PyTypePattern{TypePattern::var(name)};
    });
    m.def("type_pattern_concrete", [](PyTsType type) {
        return PyTypePattern{TypePattern::concrete(type.meta)};
    });
    m.def("type_pattern_ts", [](PyScalarPattern value) {
        return PyTypePattern{TypePattern::ts(std::move(value.pattern))};
    });
    m.def("type_pattern_tss", [] {
        return PyTypePattern{TypePattern::tss(ScalarPattern::var("T"))};
    });
    m.def("type_pattern_tss", [](PyScalarPattern element) {
        return PyTypePattern{TypePattern::tss(std::move(element.pattern))};
    });
    m.def("type_pattern_tsd", [] {
        return PyTypePattern{TypePattern::tsd(ScalarPattern::var("K"), TypePattern::var("V"))};
    });
    m.def("type_pattern_tsd", [](PyScalarPattern key, PyTypePattern value) {
        return PyTypePattern{TypePattern::tsd(std::move(key.pattern), std::move(value.pattern))};
    });
    m.def("type_pattern_tsl", [] {
        return PyTypePattern{TypePattern::tsl_var(TypePattern::var("V"), "SIZE")};
    });
    m.def("type_pattern_tsl", [](PyTypePattern element, PySizePattern size) {
        TypePattern pattern = size.variable
                                  ? TypePattern::tsl_var(std::move(element.pattern), size.name)
                                  : TypePattern::tsl(std::move(element.pattern), size.value);
        return PyTypePattern{std::move(pattern)};
    });
    m.def("type_pattern_tsw", [] {
        return PyTypePattern{TypePattern::tsw_any(ScalarPattern::var("T"))};
    });
    m.def("type_pattern_tsw", [](PyScalarPattern element) {
        return PyTypePattern{TypePattern::tsw_any(std::move(element.pattern))};
    });
    m.def("type_pattern_tsw", [](PyScalarPattern element, std::size_t period, std::size_t min_period) {
        return PyTypePattern{TypePattern::tsw(std::move(element.pattern), period, min_period)};
    }, nb::arg("element"), nb::arg("period"), nb::arg("min_period") = 0);
    m.def("type_pattern_tsb", [] {
        return PyTypePattern{TypePattern::tsb_var("SCHEMA")};
    });
    m.def("type_pattern_tsb", [](const std::string &schema_variable) {
        return PyTypePattern{TypePattern::tsb_var(schema_variable)};
    });
    m.def("type_pattern_ref", [](PyTypePattern target) {
        return PyTypePattern{TypePattern::ref(std::move(target.pattern))};
    });
    m.def("type_pattern_signal", [] {
        return PyTypePattern{TypePattern::signal()};
    });
    const auto target_input_schemas = [](nb::tuple inputs) {
        std::vector<const TSValueTypeMetaData *> schemas;
        schemas.reserve(nb::len(inputs));
        for (nb::handle input : inputs)
        {
            if (nb::isinstance<PyPort>(input))
            {
                schemas.push_back(nb::cast<PyPort>(input).ref.schema);
            }
            else if (nb::isinstance<PyTsType>(input))
            {
                schemas.push_back(nb::cast<PyTsType>(input).meta);
            }
            else
            {
                throw nb::type_error("type target resolution inputs must be Port or TsType objects");
            }
        }
        return schemas;
    };

    const auto selected_key_names = [](nb::object keys) {
        std::vector<std::string> selected;
        if (keys.is_none()) { return selected; }
        for (nb::handle key : keys) { selected.push_back(nb::cast<std::string>(key)); }
        return selected;
    };

    m.def("resolve_convert_target",
          [target_input_schemas, selected_key_names](PyTypePattern pattern, nb::tuple inputs, nb::object keys) {
              std::vector<const TSValueTypeMetaData *> schemas = target_input_schemas(inputs);
              std::vector<std::string> selected = selected_key_names(std::move(keys));
              return PyTsType{stdlib::resolve_convert_target(
                  pattern.pattern,
                  std::span<const TSValueTypeMetaData *const>{schemas.data(), schemas.size()},
                  std::span<const std::string>{selected.data(), selected.size()})};
          },
          nb::arg("pattern"), nb::arg("inputs"), nb::arg("keys") = nb::none());

    m.def("resolve_collect_target",
          [target_input_schemas](PyTypePattern pattern, nb::tuple inputs) {
              std::vector<const TSValueTypeMetaData *> schemas = target_input_schemas(inputs);
              return PyTsType{stdlib::resolve_collect_target(
                  pattern.pattern,
                  std::span<const TSValueTypeMetaData *const>{schemas.data(), schemas.size()})};
          },
          nb::arg("pattern"), nb::arg("inputs"));

    m.def("resolve_combine_target",
          [target_input_schemas](PyTypePattern pattern, nb::tuple inputs) {
              std::vector<const TSValueTypeMetaData *> schemas = target_input_schemas(inputs);
              return PyTsType{stdlib::resolve_combine_target(
                  pattern.pattern,
                  std::span<const TSValueTypeMetaData *const>{schemas.data(), schemas.size()})};
          },
          nb::arg("pattern"), nb::arg("inputs"));

    m.def("resolve_emit_target",
          [target_input_schemas](PyTsType value_ts, nb::tuple inputs) {
              std::vector<const TSValueTypeMetaData *> schemas = target_input_schemas(inputs);
              return PyTsType{stdlib::resolve_emit_target(
                  value_ts.meta,
                  std::span<const TSValueTypeMetaData *const>{schemas.data(), schemas.size()})};
          },
          nb::arg("value_ts"), nb::arg("inputs"));

    m.def("operator_output_is_selective", [](const std::string &name) {
        return OperatorRegistry::instance().output_is_selective(name);
    });

    nb::class_<GlobalState>(m, "_GlobalState")
        .def(nb::init<>())
        .def("__len__", [](GlobalState &self) { return self.view().size(); })
        .def("__contains__", [](GlobalState &self, const std::string &key) { return self.view().contains(key); })
        .def("__getitem__",
             [](GlobalState &self, const std::string &key) -> nb::object {
                 const GlobalStateView state = self.view();
                 if (!state.contains(key)) { throw nb::key_error(key.c_str()); }
                 return value_to_py(state.get(key));
             })
        .def("get",
             [](GlobalState &self, const std::string &key, nb::object fallback) -> nb::object {
                 const GlobalStateView state = self.view();
                 return state.contains(key) ? value_to_py(state.get(key)) : fallback;
             },
             nb::arg("key"), nb::arg("default") = nb::none())
        .def("__setitem__",
             [](GlobalState &self, const std::string &key, nb::handle value) {
                 self.view().set(key, py_to_value(value));
             })
        .def("__delitem__",
             [](GlobalState &self, const std::string &key) {
                 if (!self.view().erase(key)) { throw nb::key_error(key.c_str()); }
             })
        .def("keys", [](GlobalState &self) {
            nb::list result;
            for (const ValueView key : self.view().as_value().view().as_map().keys())
            {
                result.append(value_to_py(key));
            }
            return result;
        });

    // Record/replay configuration is copied with the Python thread's seed.
    m.def("_set_record_replay_config", [](GlobalState &state, const std::string &model) {
        auto config = record_replay::config(state.view());
        config.model = model;
        record_replay::set_config(state.view(), std::move(config));
    });
    m.def("_set_as_of", [](GlobalState &state, nb::object value) {
        auto config = record_replay::config(state.view());
        config.as_of = value.is_none() ? std::optional<DateTime>{}
                                        : std::optional<DateTime>{nb::cast<DateTime>(value)};
        record_replay::set_config(state.view(), std::move(config));
    }, nb::arg("state"), nb::arg("value").none());
    m.def("_set_table_schema_date_key", [](GlobalState &state, const std::string &key) {
        auto config = record_replay::config(state.view());
        config.date_key = key;
        record_replay::set_config(state.view(), std::move(config));
    });
    m.def("_set_table_schema_as_of_key", [](GlobalState &state, const std::string &key) {
        auto config = record_replay::config(state.view());
        config.as_of_key = key;
        record_replay::set_config(state.view(), std::move(config));
    });
    m.def("_table_schema_keys", [](GlobalState &state) {
        const auto config = record_replay::config(state.view());
        return nb::make_tuple(config.date_key, config.as_of_key);
    });
    m.attr("MODE_NONE")          = static_cast<unsigned>(record_replay::Mode::None);
    m.attr("MODE_RECORD")        = static_cast<unsigned>(record_replay::Mode::Record);
    m.attr("MODE_REPLAY")        = static_cast<unsigned>(record_replay::Mode::Replay);
    m.attr("MODE_COMPARE")       = static_cast<unsigned>(record_replay::Mode::Compare);
    m.attr("MODE_REPLAY_OUTPUT") = static_cast<unsigned>(record_replay::Mode::ReplayOutput);
    m.attr("MODE_RECOVER")       = static_cast<unsigned>(record_replay::Mode::Recover);
    nb::class_<record_replay::scope>(m, "RecordReplayScope")
        .def(nb::init<record_replay::Mode, std::string>(), nb::arg("mode"), nb::arg("recordable_id") = std::string{});
    m.def("record_replay_scope", [](unsigned mode, const std::string &recordable_id) {
        return new record_replay::scope{static_cast<record_replay::Mode>(mode), recordable_id};
    }, nb::arg("mode"), nb::arg("recordable_id") = std::string{}, nb::rv_policy::take_ownership);
    m.def("current_record_replay_mode", [] {
        const auto &state = record_replay::current_scope();
        return nb::make_tuple(static_cast<unsigned>(state.mode), state.recordable_id);
    });
    m.def("_comparison_summary", [](GlobalState &state, const std::string &fq_key) {
        const auto summary = record_replay::comparison_summary(state.view(), fq_key);
        return nb::make_tuple(summary.compared, summary.mismatches);
    });
    m.def("frame_store_contains", [](const std::string &key) { return record_replay::store_contains(key); });
    m.def("frame_store_read", [](const std::string &key) { return frame_to_py(record_replay::store_read(key)); });

    // Context publishing (same-wiring; the C++ design record's semantics).
    // --- services (runtime identity; services.rst rulings 2026-07-05) ---
    nb::class_<PyServiceDesc>(m, "ServiceDescriptor")
        .def_prop_ro("flavour", [](const PyServiceDesc &self) {
            switch (self.descriptor->flavour)
            {
                case ServiceFlavour::Reference: return "reference";
                case ServiceFlavour::Subscription: return "subscription";
                case ServiceFlavour::RequestReply: return "request_reply";
                case ServiceFlavour::Adaptor: return "adaptor";
            }
            return "unknown";
        })
        .def_prop_ro("name", [](const PyServiceDesc &self) { return self.descriptor->name; });
    // (TsType kind/size introspection for the python sequence protocol)
    m.def("service_descriptor",
          [](const std::string &name, const std::string &flavour, std::optional<PyTsType> output,
             std::optional<PyTsType> key_ts, std::optional<PyTsType> value, std::optional<PyTsType> request,
             std::optional<PyTsType> response, const std::string &default_path) {
              RuntimeServiceDescriptor descriptor;
              descriptor.name         = name;
              descriptor.default_path = default_path;
              if (flavour == "reference")
              {
                  descriptor.flavour       = ServiceFlavour::Reference;
                  descriptor.output_schema = output.value().meta;
              }
              else if (flavour == "subscription")
              {
                  descriptor.flavour      = ServiceFlavour::Subscription;
                  descriptor.key_type     = key_ts.value().meta->value_schema;
                  descriptor.value_schema = value.value().meta;
              }
              else if (flavour == "request_reply")
              {
                  descriptor.flavour         = ServiceFlavour::RequestReply;
                  descriptor.request_schema  = request.value().meta;
                  descriptor.response_schema = response.value().meta;
              }
              else if (flavour == "adaptor")
              {
                  descriptor.flavour = ServiceFlavour::Adaptor;
                  if (request.has_value()) { descriptor.input_schema = request->meta; }   // adaptor input
                  if (output.has_value()) { descriptor.output_schema = output->meta; }
              }
              else { throw nb::value_error("unknown service flavour"); }
              return PyServiceDesc{&intern_service_descriptor(std::move(descriptor))};
          },
          nb::arg("name"), nb::arg("flavour"), nb::arg("output") = nb::none(), nb::arg("key_ts") = nb::none(),
          nb::arg("value") = nb::none(), nb::arg("request") = nb::none(), nb::arg("response") = nb::none(),
          nb::arg("default_path") = std::string{});
    m.def("find_service", [](const std::string &name) -> nb::object {
        const auto *descriptor = find_service_descriptor(name);
        return descriptor != nullptr ? nb::cast(PyServiceDesc{descriptor}) : nb::none();
    });
    m.def("service_client", [](PyWiring &w, const PyServiceDesc &desc, const std::string &path,
                               std::optional<PyPort> ts) {
        switch (desc.descriptor->flavour)
        {
            case ServiceFlavour::Reference:
                return PyPort{reference_service_client(w.wiring_ref(), *desc.descriptor, path)};
            case ServiceFlavour::Subscription:
                return PyPort{subscription_service_subscribe(w.wiring_ref(), *desc.descriptor, path,
                                                             ts.value().ref)};
            case ServiceFlavour::RequestReply:
                return PyPort{request_reply_service_call(w.wiring_ref(), *desc.descriptor, path, ts.value().ref)};
            case ServiceFlavour::Adaptor:
                throw std::logic_error("service_client does not accept adaptor descriptors");
        }
        throw std::logic_error("unreachable");
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{}, nb::arg("ts") = nb::none());
    m.def("register_service_impl", [](PyWiring &w, const PyServiceDesc &desc, const std::string &path,
                                      const PyWiredFn &impl) {
        switch (desc.descriptor->flavour)
        {
            case ServiceFlavour::Reference:
                register_reference_service_impl(w.wiring_ref(), *desc.descriptor, path, impl.fn);
                return;
            case ServiceFlavour::Subscription:
                register_subscription_service_impl(w.wiring_ref(), *desc.descriptor, path, impl.fn);
                return;
            case ServiceFlavour::RequestReply:
                register_request_reply_service_impl(w.wiring_ref(), *desc.descriptor, path, impl.fn);
                return;
            case ServiceFlavour::Adaptor:
                throw std::logic_error("register_service_impl does not accept adaptor descriptors");
        }
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{}, nb::arg("impl"));

    // mesh_(func)[k] cross-instance access, called inside a mesh function.
    m.def("mesh_ref", [](PyWiring &w, const PyPort &key, const std::string &name) {
        const TSValueTypeMetaData *out_schema = OperatorRegistry::instance().resolve_mesh_scope(name);
        if (out_schema == nullptr)
        {
            throw std::logic_error("mesh_ref used outside a mesh scope (no enclosing mesh is being wired)");
        }
        WiringPortRef placeholder =
            wire_operator(w.wiring_ref(), "nothing", std::span<const WiringArg>{}, true, out_schema)
                .output.erased();
        return PyPort{stdlib::higher_order_impl_detail::mesh_ref_erased(w.wiring_ref(), key.ref, placeholder, name)};
    }, nb::arg("w"), nb::arg("key"), nb::arg("name") = std::string{});

    m.def("service_impl_input", [](PyWiring &w, const PyServiceDesc &desc, const std::string &path) {
        return PyPort{service_impl_input(w.wiring_ref(), *desc.descriptor, path)};
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{});
    m.def("service_impl_output", [](PyWiring &w, const PyServiceDesc &desc, const std::string &path,
                                    const PyPort &out) {
        service_impl_output(w.wiring_ref(), *desc.descriptor, path, out.ref);
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{}, nb::arg("out"));
    m.def("register_multi_service_impl", [](PyWiring &w, nb::list descs, const std::string &path,
                                            const PyWiredFn &impl) {
        std::vector<const RuntimeServiceDescriptor *> descriptors;
        descriptors.reserve(nb::len(descs));
        for (nb::handle desc : descs) { descriptors.push_back(nb::cast<PyServiceDesc &>(desc).descriptor); }
        register_multi_service_impl(w.wiring_ref(),
                                    std::span<const RuntimeServiceDescriptor *const>{descriptors.data(),
                                                                                     descriptors.size()},
                                    path, impl.fn);
    }, nb::arg("w"), nb::arg("descs"), nb::arg("path") = std::string{}, nb::arg("impl"));

    m.def("adaptor_client", [](PyWiring &w, const PyServiceDesc &desc, const std::string &path,
                               std::optional<PyPort> in) -> nb::object {
        const WiringPortRef *in_ref = in.has_value() ? &in->ref : nullptr;
        WiringPortRef        out    = adaptor_client(w.wiring_ref(), *desc.descriptor, path, in_ref);
        if (out.schema == nullptr) { return nb::none(); }
        return nb::cast(PyPort{std::move(out)});
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{}, nb::arg("in") = nb::none());
    m.def("adaptor_from_graph", [](PyWiring &w, const PyServiceDesc &desc, const std::string &path) {
        return PyPort{adaptor_from_graph(w.wiring_ref(), *desc.descriptor, path)};
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{});
    m.def("adaptor_to_graph", [](PyWiring &w, const PyServiceDesc &desc, const std::string &path,
                                 const PyPort &out) {
        adaptor_to_graph(w.wiring_ref(), *desc.descriptor, path, out.ref);
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{}, nb::arg("out"));
    m.def("register_adaptor_impl", [](PyWiring &w, const PyServiceDesc &desc, const std::string &path,
                                      const PyWiredFn &impl) {
        register_adaptor_impl(w.wiring_ref(), *desc.descriptor, path, impl.fn);
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{}, nb::arg("impl"));

    m.def("push_context", [](PyWiring &w, const std::string &name, const PyPort &port) {
        if (name.empty()) { throw nb::value_error("context requires a non-empty name"); }
        graph_wiring_detail::push_context_source(w.wiring_ref(), name, port.ref);
    });
    m.def("pop_context", [] { graph_wiring_detail::pop_context_source(); });
    m.def("get_context", [](PyWiring &w, const std::string &name) {
        return PyPort{graph_wiring_detail::resolve_context_source(w.wiring_ref(), name)};
    });
    m.def("has_context", [](PyWiring &w, const std::string &name) {
        return graph_wiring_detail::has_context_source(w.wiring_ref(), name);
    });
    m.attr("IN_MEMORY")  = std::string{record_replay::IN_MEMORY};
    m.attr("DATA_FRAME") = std::string{record_replay::DATA_FRAME};
    m.attr("MIN_ST")     = nb::cast(MIN_ST);
    m.attr("MIN_TD")     = nb::cast(MIN_TD);

    m.def("operator_names", [] { return OperatorRegistry::instance().registered_names(); });

    // The passive marker: a tagged COPY of the port (passivity applies to
    // the tagged usage only - Python's passive(ts)).
    m.def("passive", [](const PyPort &port) {
        return PyPort{port.ref.with_arg_tag(WiringPortRef::ArgTag::Passive)};
    });

    nb::class_<PyWiredFn>(m, "WiredFn");
    nb::class_<PyNodeHandle>(m, "NodeRef");
    nb::class_<PyScalarValue>(m, "ScalarValue");
    nb::class_<PySender>(m, "Sender").def("send", &PySender::send, nb::arg("value"));
    nb::class_<PyOutput>(m, "OutputView")
        .def_prop_ro("valid", &PyOutput::valid)
        .def_prop_ro("value", &PyOutput::value);
    nb::class_<PyRuntimeGlobalState>(m, "RuntimeGlobalState")
        .def("__len__", [](const PyRuntimeGlobalState &self) { return self.checked().size(); })
        .def("__contains__", [](const PyRuntimeGlobalState &self, const std::string &key) {
            return self.checked().contains(key);
        })
        .def("__getitem__", [](const PyRuntimeGlobalState &self, const std::string &key) -> nb::object {
            const GlobalStateView state = self.checked();
            if (!state.contains(key)) { throw nb::key_error(key.c_str()); }
            return value_to_py(state.get(key));
        })
        .def("get", [](const PyRuntimeGlobalState &self, const std::string &key,
                       nb::object fallback) -> nb::object {
            const GlobalStateView state = self.checked();
            return state.contains(key) ? value_to_py(state.get(key)) : fallback;
        }, nb::arg("key"), nb::arg("default") = nb::none())
        .def("__setitem__", [](const PyRuntimeGlobalState &self, const std::string &key, nb::handle value) {
            self.checked().set(key, py_to_value(value));
        })
        .def("__delitem__", [](const PyRuntimeGlobalState &self, const std::string &key) {
            if (!self.checked().erase(key)) { throw nb::key_error(key.c_str()); }
        })
        .def("keys", [](const PyRuntimeGlobalState &self) {
            nb::list result;
            for (const ValueView key : self.checked().as_value().view().as_map().keys())
            {
                result.append(value_to_py(key));
            }
            return result;
        });
    nb::class_<PyTimeSeries>(m, "TimeSeries")
        .def_prop_ro("value", &PyTimeSeries::value)
        .def_prop_ro("_kind", [](const PyTimeSeries &self) { return static_cast<int>(self.kind()); })
        // hgraph's runtime activity control: a node may passivate/reactivate
        // its own input subscription (the C++ In views expose the same).
        .def("make_passive",
             [](PyTimeSeries &self) {
                 self.require_alive();
                 self.view.make_passive();
             })
        .def("make_active",
             [](PyTimeSeries &self) {
                 self.require_alive();
                 self.view.make_active();
             })
        .def_prop_ro("delta_value", &PyTimeSeries::delta_value)
        .def_prop_ro("modified", &PyTimeSeries::modified)
        .def_prop_ro("valid", &PyTimeSeries::valid)
        .def_prop_ro("all_valid", &PyTimeSeries::all_valid)
        // TSW eviction surface (hgraph's removed_value pair).
        .def_prop_ro("has_removed_value",
                     [](const PyTimeSeries &self) {
                         const auto &view = self.checked();
                         if (view.schema()->kind != TSTypeKind::TSW)
                         {
                             throw nb::attribute_error("has_removed_value");
                         }
                         return view.as_window().has_removed_value();
                     })
        .def_prop_ro("removed_value",
                     [](const PyTimeSeries &self) {
                         const auto &view = self.checked();
                         if (view.schema()->kind != TSTypeKind::TSW)
                         {
                             throw nb::attribute_error("removed_value");
                         }
                         return value_to_py(view.as_window().removed_value());
                     })
        .def_prop_ro("last_modified_time", &PyTimeSeries::last_modified_time)
        .def("added", &PyTimeSeries::added)
        .def("removed", &PyTimeSeries::removed)
        .def("keys", &PyTimeSeries::keys)
        .def("modified_keys", &PyTimeSeries::modified_keys)
        .def("modified_items", &PyTimeSeries::modified_items)
        .def("removed_keys", &PyTimeSeries::removed_keys)
        .def("__getitem__", &PyTimeSeries::child_at)
        .def("__getattr__", [](const PyTimeSeries &self, const std::string &name) {
            if (self.kind() != TSTypeKind::TSB) { throw nb::attribute_error(name.c_str()); }
            return self.child_at(nb::cast(name));
        })
        .def("__contains__", &PyTimeSeries::contains)
        .def("__len__", &PyTimeSeries::size)
        .def("__str__", [](const PyTimeSeries &self) { return nb::str(self.value()); })
        .def("__repr__", [](const PyTimeSeries &self) { return nb::str("TimeSeries({})").format(self.value()); });
    m.def("_set_cmp_result_enum", [](nb::object enum_class) { cmp_result_enum_slot() = std::move(enum_class); });
    m.def("_set_divide_by_zero_enum",
          [](nb::object enum_class) { divide_by_zero_enum_slot() = std::move(enum_class); });
    m.def("_set_removed_sentinel", [](nb::object sentinel) { PyTimeSeries::removed_slot() = std::move(sentinel); });
    nb::class_<PyArrowStream>(m, "ArrowStream")
        .def("__arrow_c_stream__",
             [](const PyArrowStream &self, nb::handle) { return self.capsule(); },
             nb::arg("requested_schema") = nb::none());
    nb::class_<PySeriesArray>(m, "ArrowSeriesArray")
        .def("__arrow_c_array__",
             [](const PySeriesArray &self, nb::handle) { return self.arrow_c_array(); },
             nb::arg("requested_schema") = nb::none());
    install_arrow_conversion_hooks();   // bind Frame/Series conversion onto the type-erased ops
    // Wiring-time scalar values as one list-of-Any (part of node identity).
    m.def("any_list", [](nb::list values) {
        auto &registry = TypeRegistry::instance();
        const auto *schema  = registry.mutable_list(registry.any());
        const auto *binding = ValuePlanFactory::instance().binding_for(schema);
        Value       result{*binding};
        MutableListView list{result.begin_mutation()};
        for (nb::handle item : values)
        {
            Value boxed_value{*ValuePlanFactory::instance().binding_for(registry.any())};
            MutableAnyView{boxed_value.begin_mutation()}.set(py_to_value(item));
            list.push_back(boxed_value.view());
        }
        return PyScalarValue{std::move(result)};
    });

    nb::class_<PyEvalClock>(m, "EvaluationClock")
        .def_prop_ro("evaluation_time", [](const PyEvalClock &clock) { return clock.evaluation_time; });
    nb::class_<PyScheduler>(m, "Scheduler")
        .def("schedule", [](const PyScheduler &self, DateTime when) { self.scheduler.schedule(when); })
        .def("schedule_delta", [](const PyScheduler &self, TimeDelta delta) { self.scheduler.schedule(delta); });

    // Pack argument ports into a STRUCTURAL un-named TSB (the dict/list
    // passing model for python user nodes - any arity, one operator).
    m.def("ts_field_types", [](PyTsType ts_type) {
        nb::list result;
        if (ts_type.meta == nullptr || ts_type.meta->kind != TSTypeKind::TSB) { return result; }
        for (std::size_t index = 0; index < ts_type.meta->field_count(); ++index)
        {
            const auto &field = ts_type.meta->fields()[index];
            result.append(nb::make_tuple(std::string{field.name != nullptr ? field.name : ""},
                                         PyTsType{field.type}));
        }
        return result;
    });

    m.def("tsb_port", [](PyTsType ts_type, nb::dict ports) {
        if (ts_type.meta == nullptr || ts_type.meta->kind != TSTypeKind::TSB)
        {
            throw nb::value_error("TSB.from_ts requires a TSB type");
        }
        if (nb::len(ports) != ts_type.meta->field_count())
        {
            throw nb::value_error("TSB.from_ts requires every field exactly once");
        }
        std::vector<WiringPortRef> children;
        children.reserve(ts_type.meta->field_count());
        for (std::size_t index = 0; index < ts_type.meta->field_count(); ++index)
        {
            const auto &field = ts_type.meta->fields()[index];
            nb::object  port  = ports.attr("get")(field.name);
            if (port.is_none())
            {
                throw nb::value_error(("TSB.from_ts is missing field '" + std::string{field.name} + "'").c_str());
            }
            children.push_back(nb::cast<PyPort &>(port).ref);
        }
        return PyPort{WiringPortRef::structural_source(ts_type.meta, std::move(children))};
    });

    m.def("structural_has_ref_children", [](const PyPort &port) {
        if (!port.ref.is_structural_source()) { return false; }
        for (const WiringPortRef &child : port.ref.structural_children())
        {
            if (child.schema != nullptr && child.schema->kind == TSTypeKind::REF) { return true; }
        }
        return false;
    });

    m.def("tsb_has_ref_fields", [](PyTsType ts_type) {
        if (ts_type.meta == nullptr || ts_type.meta->kind != TSTypeKind::TSB) { return false; }
        for (std::size_t index = 0; index < ts_type.meta->field_count(); ++index)
        {
            if (ts_type.meta->fields()[index].type != nullptr &&
                ts_type.meta->fields()[index].type->kind == TSTypeKind::REF)
            {
                return true;
            }
        }
        return false;
    });

    m.def("ref_port", [](PyWiring &wiring, const PyPort &port) {
        // Materialize a STRUCTURAL port as a REFERENCE output (the
        // structural-REF node): the child output becomes REF<S> without
        // copying - hgraph's combine-of-references shape.
        auto       &registry = TypeRegistry::instance();
        const auto *target   = registry.dereference(port.ref.schema);
        const auto *ref_schema = registry.ref(target);
        return PyPort{graph_wiring_detail::adapt_source_for_input(*wiring.raw, ref_schema, port.ref)};
    });

    m.def("un_named_tsb_type", [](nb::list fields) {
        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> field_metas;
        field_metas.reserve(nb::len(fields));
        for (nb::handle item : fields)
        {
            auto pair = nb::cast<nb::tuple>(item);
            field_metas.emplace_back(nb::cast<std::string>(pair[0]), nb::cast<PyTsType &>(pair[1]).meta);
        }
        return PyTsType{TypeRegistry::instance().un_named_tsb(field_metas)};
    });

    m.def("bundle_vt", [](const std::string &name, nb::list fields) {
        std::vector<std::pair<std::string, const ValueTypeMetaData *>> field_metas;
        field_metas.reserve(nb::len(fields));
        for (nb::handle item : fields)
        {
            auto pair = nb::cast<nb::tuple>(item);
            field_metas.emplace_back(nb::cast<std::string>(pair[0]), nb::cast<PyValueType &>(pair[1]).meta);
        }
        return PyValueType{TypeRegistry::instance().bundle(name, field_metas)};
    });

    m.def("register_bundle_class", [](const std::string &name, nb::object cls) {
        bundle_class_registry()[nb::str(name.c_str())] = std::move(cls);
    });

    m.def("tsl_element", [](const PyPort &port, std::size_t index) {
        // Fixed-TSL scalar indexing: the structural element projection
        // (zero-copy; no node) - the intended mechanism for tsl[i].
        const auto *element_schema =
            port.ref.schema != nullptr ? port.ref.schema->element_ts() : nullptr;
        return PyPort{subgraph_wiring_detail::tsl_element_ref(port.ref, index, element_schema)};
    });

    m.def("tsl_port", [](nb::list ports) {
        if (nb::len(ports) == 0) { throw nb::value_error("tsl_port requires at least one port"); }
        std::vector<WiringPortRef> children;
        children.reserve(nb::len(ports));
        const TSValueTypeMetaData *element = nullptr;
        for (nb::handle port : ports)
        {
            const WiringPortRef &ref = nb::cast<PyPort &>(port).ref;
            if (element == nullptr) { element = ref.schema; }
            else if (element != ref.schema)
            {
                throw nb::value_error("TSL.from_ts requires ports of one element type");
            }
            children.push_back(ref);
        }
        const auto *schema = TypeRegistry::instance().tsl(element, children.size());
        return PyPort{WiringPortRef::structural_source(schema, std::move(children))};
    });

    m.def("bundle_port", [](nb::list ports, nb::list keep_ref) {
        if (nb::len(ports) == 0) { throw nb::value_error("bundle_port requires at least one port"); }
        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
        std::vector<WiringPortRef> children;
        fields.reserve(nb::len(ports));
        children.reserve(nb::len(ports));
        std::size_t index = 0;
        auto &registry = TypeRegistry::instance();
        for (nb::handle port : ports)
        {
            const WiringPortRef &ref = nb::cast<PyPort &>(port).ref;
            // Howard's REF ruling: a non-REF parameter bound to a REF source
            // accepts the DEREFERENCED value (binding inserts the from-REF
            // adaptation); a REF parameter receives the reference itself as
            // an opaque value.
            const bool as_ref = index < nb::len(keep_ref) && nb::cast<bool>(keep_ref[index]);
            const auto *field_schema =
                as_ref ? (ref.schema != nullptr && ref.schema->kind == TSTypeKind::REF
                              ? ref.schema
                              : registry.ref(registry.dereference(ref.schema)))
                       : registry.dereference(ref.schema);
            fields.emplace_back("_" + std::to_string(index++), field_schema);
            children.push_back(ref);
        }
        const auto *schema = registry.un_named_tsb(fields);
        return PyPort{WiringPortRef::structural_source(schema, std::move(children))};
    });

    nb::class_<PyOpaqueRef>(m, "TimeSeriesRef")
        .def("__eq__",
             [](const PyOpaqueRef &self, nb::handle other) {
                 return nb::isinstance<PyOpaqueRef>(other) &&
                        nb::cast<PyOpaqueRef &>(other).value.view().equals(self.value.view());
             })
        .def("__repr__", [](const PyOpaqueRef &self) {
            return std::string{self.value.view().checked_as<TimeSeriesReference>().is_empty() ? "<ref: empty>"
                                                                                              : "<ref>"};
        });
    m.def("empty_time_series_reference", [] {
        // The registry's interned reference meta - taken from a REF schema
        // directly (every REF plan shares it; a Value built any other way
        // would fail the binding-identity checks).
        auto       &registry = TypeRegistry::instance();
        const auto *ref_meta = registry.ref(registry.ts(scalar_descriptor<Int>::value_meta()));
        const auto *binding  = ValuePlanFactory::instance().binding_for(ref_meta->value_schema);
        if (binding == nullptr) { throw std::logic_error("TimeSeriesReference meta has no plan binding"); }
        return PyOpaqueRef{Value{*binding}};
    });

    m.def("node_ref", [](nb::object fn) {
        auto &registry = py_node_registry();
        auto  found    = registry.find(fn.ptr());
        if (found == registry.end())
        {
            auto *record = new PyNodeRecord{fn};   // immortal: scalar identity by pointer
            found        = registry.emplace(fn.ptr(), record).first;
        }
        return PyNodeHandle{found->second};
    });

    m.def("graph_fn", [](nb::object wrapper, nb::object user_fn, nb::list param_names, bool has_output,
                         std::optional<PyTsType> output_type) {
        auto &registry = py_graph_fn_registry();
        auto  found    = registry.find(user_fn.ptr());
        if (found == registry.end())
        {
            auto *record = new PyGraphFnRecord{};   // immortal: WiredFn contexts must outlive every value
            record->wrapper    = wrapper;
            record->user_fn    = user_fn;
            record->has_output = has_output;
            record->arity      = nb::len(param_names);
            if (output_type.has_value()) { record->output_schema = output_type->meta; }
            record->name_storage.reserve(record->arity);
            for (nb::handle name : param_names) { record->name_storage.push_back(nb::cast<std::string>(name)); }
            for (const auto &name : record->name_storage) { record->names.emplace_back(name); }
            found = registry.emplace(user_fn.ptr(), record).first;
        }
        const PyGraphFnRecord *record = found->second;
        return PyWiredFn{WiredFn{
            .ops        = &py_graph_fn_ops(),
            .context    = record,
            .identity   = &typeid(PyGraphFnRecord),
            .arity      = record->arity,
            .has_output = record->has_output,
        }};
    }, nb::arg("wrapper"), nb::arg("user_fn"), nb::arg("param_names"), nb::arg("has_output"),
       nb::arg("output_type") = nb::none());
    nb::class_<PySwitchCases>(m, "SwitchCases");
    nb::class_<PyFeedback>(m, "Feedback")
        .def_prop_ro("port", [](const PyFeedback &fb) { return PyPort{fb.delegate}; })
        .def_prop_ro("bound", [](const PyFeedback &fb) { return fb.bound; });

    m.def("switch_cases", [](nb::dict cases, bool reload) {
        stdlib::SwitchCases result;
        result.reload_on_ticked = reload;
        for (auto [key, branch] : cases)
        {
            WiredFn fn;
            if (nb::isinstance<PyWiredFn>(branch)) { fn = nb::cast<PyWiredFn &>(branch).fn; }
            else
            {
                const auto &table = wired_fn_table();
                const auto  found = table.find(nb::cast<std::string>(branch));
                if (found == table.end()) { throw nb::value_error("no wired-fn erasure for switch branch"); }
                fn = found->second;
            }
            if (key.is_none()) { result.default_branch = fn; }
            else { result.cases.push_back(stdlib::SwitchCase{py_to_value(key), fn}); }
        }
        return PySwitchCases{std::move(result)};
    }, nb::arg("cases"), nb::arg("reload") = false);
    m.def("wired_op", [](const std::string &name) {
        const auto &table = wired_fn_table();
        const auto  found = table.find(name);
        if (found == table.end())
        {
            throw nb::value_error(
                ("no wired-fn erasure for operator '" + name + "' (the bridge pre-instantiates a fixed set)").c_str());
        }
        return PyWiredFn{found->second};
    });

    // Conversion-layer round trip (test/debug aid): Python -> Value -> Python.
    m.def("_roundtrip_value", [](nb::handle object) { return value_to_py(py_to_value(object).view()); });

    nb::class_<PyWiring>(m, "Wiring")
        .def(nb::init<>())
        .def(nb::init<GlobalState &>(), nb::arg("state"))
        .def("wire", &PyWiring::wire, nb::arg("name"), nb::arg("args") = nb::tuple(),
             nb::arg("kwargs") = nb::dict(), nb::arg("output_type") = nb::none(),
             nb::arg("sizes") = nb::none())
        .def("set_replay", &PyWiring::set_replay, nb::arg("key"), nb::arg("values"),
             nb::arg("ts_type") = nb::none())
        .def("feedback", &PyWiring::feedback, nb::arg("ts_type"), nb::arg("initial") = nb::none())
        .def("feedback_bind", &PyWiring::feedback_bind, nb::arg("feedback"), nb::arg("port"))
        .def("_release_seed_context", &PyWiring::release_seed_context)
        .def("run", &PyWiring::run, nb::arg("start_time") = nb::none(), nb::arg("end_time") = nb::none(),
             nb::arg("realtime") = false)
        .def("push_source", &PyWiring::push_source, nb::arg("ts_type"), nb::arg("conflate") = false,
             nb::arg("on_start") = nb::none());

    m.def(
        "_evaluate_const",
        [](GlobalState &state, const std::string &name, nb::tuple args, nb::dict kwargs,
           std::optional<PyTsType> output_type) {
            const auto wiring_args = build_args(args, kwargs);
            Value      value       = OperatorRegistry::instance().evaluate_const(
                name, std::span<const WiringArg>{wiring_args.data(), wiring_args.size()},
                output_type.has_value() ? output_type->meta : nullptr, state.view());
            return value.has_value() ? value_to_py(value.view()) : nb::none();
        },
        nb::arg("state"), nb::arg("name"), nb::arg("args") = nb::tuple(), nb::arg("kwargs") = nb::dict(),
        nb::arg("output_type") = nb::none());

    m.def("reset_registries", [] {
        python_bridge::enum_class_registry().clear();   // meta pointers are re-interned
        reset_all_registries();
        stdlib::register_standard_operators();
    (void)scalar_descriptor<PyObj>::value_meta();   // the python-object scalar
    register_overload<op_materialize, materialize_node>();
    register_overload<op_py_compute, py_compute_node>();
    register_overload<op_py_sink, py_sink_node>();
    register_overload<op_py_generator, py_generator_node>();
    register_overload<op_recover_pt, stdlib::component_detail::recovering_pass_through>();
    register_overload<op_harness_replay, harness_replay>();
    register_overload<op_harness_record, harness_record>();
    register_overload<stdlib::until_true, until_true_callable_node>();
    register_graph_overload<stdlib::freeze, freeze_callable_compose>();
    register_overload<stdlib::call_op, call_callable_node>();
    });
}

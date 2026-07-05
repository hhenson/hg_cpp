/**
 * The Python bridge (roadmap P4, slice 1): wiring and running graphs from
 * Python through the runtime-schema dispatch contract — the exact three
 * calls proven template-free by tests/cpp/test_erased_wiring.cpp (build
 * WiringArgs -> OperatorRegistry::resolve -> impl->wire), plus the eager
 * const-evaluable entry (P1) and the replay/record test harness.
 *
 * Scalar coverage in this slice: bool/int/float/str plus the chrono types
 * (via the vendored nanobind casters). Containers, bundles, Frame and the
 * remaining atoms follow with the type-conversion layer.
 */
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/types/wired_fn.h>
#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/operators/control.h>
#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/push_source_node.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/python/chrono.h>

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <hgraph/types/frame.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/unique_ptr.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace nb = nanobind;
using namespace hgraph;

namespace
{
    // ---------------------------------------------------------------
    // Python <-> Value scalar conversion (slice 1: the core atoms)
    // ---------------------------------------------------------------

    [[nodiscard]] Value py_to_value(nb::handle object);
    [[nodiscard]] nb::object value_to_py(const ValueView &view);
    [[nodiscard]] Value py_arrow_to_frame(nb::handle object);

    // datetime.date / datetime.time have no chrono caster; convert by attribute.
    [[nodiscard]] bool is_py_date(nb::handle object)
    {
        return nb::hasattr(object, "year") && nb::hasattr(object, "month") && nb::hasattr(object, "day") &&
               !nb::hasattr(object, "hour");
    }

    [[nodiscard]] bool is_py_time(nb::handle object)
    {
        return nb::hasattr(object, "hour") && nb::hasattr(object, "minute") && !nb::hasattr(object, "year");
    }

    [[nodiscard]] Value py_container_to_value(nb::handle object)
    {
        auto &registry = TypeRegistry::instance();
        const auto build = [&](const ValueTypeMetaData *schema, auto &&fill) {
            const auto *binding = ValuePlanFactory::instance().binding_for(schema);
            Value       value{*binding};
            fill(value.begin_mutation());
            return value;
        };
        if (nb::isinstance<nb::frozenset>(object) || nb::isinstance<nb::set>(object))
        {
            std::vector<Value> elements;
            for (nb::handle item : object) { elements.push_back(py_to_value(item)); }
            if (elements.empty()) { throw nb::type_error("cannot infer the element type of an empty set"); }
            return build(registry.mutable_set(elements.front().schema()), [&](ValueView view) {
                MutableSetView set{std::move(view)};
                for (const auto &element : elements) { set.add(element.view()); }
            });
        }
        if (nb::isinstance<nb::dict>(object))
        {
            std::vector<std::pair<Value, Value>> entries;
            for (auto [key, item] : nb::cast<nb::dict>(object))
            {
                entries.emplace_back(py_to_value(key), py_to_value(item));
            }
            if (entries.empty()) { throw nb::type_error("cannot infer the key/value types of an empty dict"); }
            return build(registry.mutable_map(entries.front().first.schema(), entries.front().second.schema()),
                         [&](ValueView view) {
                             MutableMapView map{std::move(view)};
                             for (const auto &[key, item] : entries) { map.set_item(key.view(), item.view()); }
                         });
        }
        if (nb::isinstance<nb::tuple>(object) || nb::isinstance<nb::list>(object))
        {
            std::vector<Value> elements;
            for (nb::handle item : object) { elements.push_back(py_to_value(item)); }
            if (elements.empty()) { throw nb::type_error("cannot infer the element type of an empty sequence"); }
            return build(registry.mutable_list(elements.front().schema()), [&](ValueView view) {
                MutableListView list{std::move(view)};
                for (const auto &element : elements) { list.push_back(element.view()); }
            });
        }
        throw nb::type_error("unsupported Python value for hgraph");
    }

    Value py_to_value(nb::handle object)
    {
        if (nb::isinstance<nb::bool_>(object)) { return Value{Bool{nb::cast<bool>(object)}}; }
        if (nb::isinstance<nb::int_>(object)) { return Value{Int{nb::cast<Int>(object)}}; }
        if (nb::isinstance<nb::float_>(object)) { return Value{Float{nb::cast<Float>(object)}}; }
        if (nb::isinstance<nb::str>(object)) { return Value{Str{nb::cast<std::string>(object)}}; }
        if (nb::isinstance<nb::bytes>(object))
        {
            auto raw = nb::cast<nb::bytes>(object);
            return Value{Bytes{std::string{raw.c_str(), raw.size()}}};
        }
        // Order matters: the chrono caster happily converts datetime.date to a
        // midnight time_point, so the date/time attribute checks come FIRST.
        if (is_py_date(object))
        {
            return Value{Date{std::chrono::year{nb::cast<int>(object.attr("year"))},
                              std::chrono::month{nb::cast<unsigned>(object.attr("month"))},
                              std::chrono::day{nb::cast<unsigned>(object.attr("day"))}}};
        }
        if (is_py_time(object))
        {
            const std::int64_t micros = nb::cast<std::int64_t>(object.attr("hour")) * 3'600'000'000LL +
                                        nb::cast<std::int64_t>(object.attr("minute")) * 60'000'000LL +
                                        nb::cast<std::int64_t>(object.attr("second")) * 1'000'000LL +
                                        nb::cast<std::int64_t>(object.attr("microsecond"));
            return Value{Time{micros}};
        }
        DateTime when;
        if (nb::try_cast<DateTime>(object, when)) { return Value{when}; }
        TimeDelta delta;
        if (nb::try_cast<TimeDelta>(object, delta)) { return Value{delta}; }
        if (nb::hasattr(object, "__arrow_c_stream__")) { return py_arrow_to_frame(object); }
        return py_container_to_value(object);
    }

    struct PyArrowStream
    {
        Frame frame;

        [[nodiscard]] nb::object capsule() const
        {
            auto reader = std::make_shared<arrow::TableBatchReader>(*frame.table);
            auto *stream = new ArrowArrayStream{};
            const auto status = arrow::ExportRecordBatchReader(std::move(reader), stream);
            if (!status.ok())
            {
                delete stream;
                throw std::runtime_error("arrow export failed: " + status.ToString());
            }
            return nb::steal(PyCapsule_New(stream, "arrow_array_stream", [](PyObject *object) {
                auto *raw = static_cast<ArrowArrayStream *>(
                    PyCapsule_GetPointer(object, "arrow_array_stream"));
                if (raw != nullptr)
                {
                    if (raw->release != nullptr) { raw->release(raw); }
                    delete raw;
                }
            }));
        }
    };

    [[nodiscard]] nb::object frame_to_py(const Frame &frame)
    {
        if (!frame.has_value()) { return nb::none(); }
        // TableBatchReader borrows; PyArrowStream keeps the Frame alive for
        // the export call, and the exported stream owns batch references.
        nb::object stream = nb::cast(PyArrowStream{frame});
        return nb::module_::import_("pyarrow").attr("table")(stream);
    }

    [[nodiscard]] Value py_arrow_to_frame(nb::handle object)
    {
        nb::object capsule = object.attr("__arrow_c_stream__")();
        auto *stream = static_cast<ArrowArrayStream *>(
            PyCapsule_GetPointer(capsule.ptr(), "arrow_array_stream"));
        if (stream == nullptr) { throw nb::type_error("expected an arrow_array_stream capsule"); }
        auto reader = arrow::ImportRecordBatchReader(stream);
        if (!reader.ok()) { throw std::runtime_error("arrow import failed: " + reader.status().ToString()); }
        auto table = arrow::Table::FromRecordBatchReader(reader->get());
        if (!table.ok()) { throw std::runtime_error("arrow read failed: " + table.status().ToString()); }
        return Value{Frame{.table = std::move(*table)}};
    }

    [[nodiscard]] nb::object atomic_to_py(const ValueView &view)
    {
        const auto *meta = view.schema();
        if (meta == scalar_descriptor<Bool>::value_meta()) { return nb::cast(view.checked_as<Bool>()); }
        if (meta == scalar_descriptor<Int>::value_meta()) { return nb::cast(view.checked_as<Int>()); }
        if (meta == scalar_descriptor<Float>::value_meta()) { return nb::cast(view.checked_as<Float>()); }
        if (meta == scalar_descriptor<Str>::value_meta()) { return nb::cast(view.checked_as<Str>()); }
        if (meta == scalar_descriptor<DateTime>::value_meta()) { return nb::cast(view.checked_as<DateTime>()); }
        if (meta == scalar_descriptor<TimeDelta>::value_meta()) { return nb::cast(view.checked_as<TimeDelta>()); }
        if (meta == scalar_descriptor<Date>::value_meta())
        {
            const auto &date = view.checked_as<Date>();
            return nb::module_::import_("datetime").attr("date")(static_cast<int>(date.year()),
                                                                 static_cast<unsigned>(date.month()),
                                                                 static_cast<unsigned>(date.day()));
        }
        if (meta == scalar_descriptor<Time>::value_meta())
        {
            const auto micros = view.checked_as<Time>().microseconds;
            return nb::module_::import_("datetime").attr("time")(
                static_cast<int>(micros / 3'600'000'000LL), static_cast<int>(micros / 60'000'000LL % 60),
                static_cast<int>(micros / 1'000'000LL % 60), static_cast<int>(micros % 1'000'000LL));
        }
        if (meta == scalar_descriptor<Frame>::value_meta()) { return frame_to_py(view.checked_as<Frame>()); }
        if (meta == scalar_descriptor<Bytes>::value_meta())
        {
            const auto &bytes = view.checked_as<Bytes>();
            return nb::cast(nb::bytes(bytes.data.data(), bytes.data.size()));
        }
        throw nb::type_error((std::string{"unsupported hgraph atomic for Python: "} +
                              (meta != nullptr && meta->display_name ? meta->display_name : "?"))
                                 .c_str());
    }

    nb::object value_to_py(const ValueView &view)
    {
        if (!view.valid()) { return nb::none(); }
        const auto *meta = view.schema();
        switch (meta->kind)
        {
            case ValueTypeKind::Atomic: return atomic_to_py(view);
            case ValueTypeKind::Tuple: {
                auto      tuple = view.as_tuple();
                nb::list  items;
                for (std::size_t index = 0; index < tuple.size(); ++index) { items.append(value_to_py(tuple.at(index))); }
                return nb::tuple(items);
            }
            case ValueTypeKind::Bundle: {
                auto     bundle = view.as_bundle();
                nb::dict result;
                for (std::size_t index = 0; index < meta->field_count; ++index)
                {
                    result[meta->fields[index].name] = value_to_py(bundle.at(index));
                }
                return result;
            }
            case ValueTypeKind::List: {
                nb::list result;
                for (const ValueView &element : view.as_list()) { result.append(value_to_py(element)); }
                return result;
            }
            case ValueTypeKind::Set: {
                nb::list items;
                for (const ValueView &element : view.as_set().values()) { items.append(value_to_py(element)); }
                return nb::steal(PyFrozenSet_New(nb::list(items).ptr()));
            }
            case ValueTypeKind::Map: {
                nb::dict result;
                for (const auto [key, item] : view.as_map()) { result[value_to_py(key)] = value_to_py(item); }
                return result;
            }
            case ValueTypeKind::Any: return value_to_py(view.as_any().get());
            default:
                throw nb::type_error("unsupported hgraph value kind for Python");
        }
    }

    // ---------------------------------------------------------------
    // Schema-directed conversion: Python test-vector entries to CANONICAL
    // delta values for a target ts type (what replay applies per cycle).
    // ---------------------------------------------------------------

    [[nodiscard]] const ValueTypeBinding &delta_binding(const ValueTypeMetaData *meta)
    {
        const auto *binding = ValuePlanFactory::instance().binding_for(meta);
        if (binding == nullptr) { throw nb::type_error("schema has no canonical binding"); }
        return *binding;
    }

    [[nodiscard]] Value py_to_value_as(nb::handle object, const ValueTypeMetaData *meta)
    {
        // Atomic coercions the generic converter cannot do (int -> float).
        if (meta == scalar_descriptor<Float>::value_meta() && nb::isinstance<nb::int_>(object) &&
            !nb::isinstance<nb::bool_>(object))
        {
            return Value{Float{static_cast<Float>(nb::cast<Int>(object))}};
        }
        Value value = py_to_value(object);
        if (meta->kind == ValueTypeKind::Any && value.schema() != meta)
        {
            Value boxed{delta_binding(meta)};
            MutableAnyView{boxed.begin_mutation()}.set(std::move(value));
            return boxed;
        }
        return value;
    }

    [[nodiscard]] Value py_to_delta(nb::handle object, const TSValueTypeMetaData *ts)
    {
        switch (ts->kind)
        {
            case TSTypeKind::TS: return py_to_value_as(object, ts->value_schema);
            case TSTypeKind::TSS: {
                // A set/frozenset adds; {"added": ..., "removed": ...} is explicit.
                const auto *elem = ts->value_schema->element_type;
                SetBuilder  added{delta_binding(elem)};
                SetBuilder  removed{delta_binding(elem)};
                nb::handle  add_from = object, remove_from = nb::handle{};
                if (nb::isinstance<nb::dict>(object))
                {
                    auto spec = nb::cast<nb::dict>(object);
                    add_from    = spec.contains("added") ? spec["added"] : nb::handle{};
                    remove_from = spec.contains("removed") ? spec["removed"] : nb::handle{};
                }
                if (add_from.is_valid())
                {
                    for (nb::handle item : add_from) { (void)added.insert_copy(py_to_value_as(item, elem).view().data()); }
                }
                if (remove_from.is_valid())
                {
                    for (nb::handle item : remove_from) { (void)removed.insert_copy(py_to_value_as(item, elem).view().data()); }
                }
                BundleBuilder bundle{delta_binding(ts->delta_value_schema)};
                bundle.set("added", added.build());
                bundle.set("removed", removed.build());
                return bundle.build();
            }
            case TSTypeKind::TSD: {
                // {key: child-delta}; a None value removes the key (hgraph's REMOVE).
                const auto *key_meta   = ts->key_type();
                const auto *child      = ts->element_ts();
                SetBuilder  removed{delta_binding(key_meta)};
                MapBuilder  modified{delta_binding(key_meta), delta_binding(child->delta_value_schema)};
                for (auto [key, item] : nb::cast<nb::dict>(object))
                {
                    Value key_value = py_to_value_as(key, key_meta);
                    if (item.is_none()) { (void)removed.insert_copy(key_value.view().data()); }
                    else
                    {
                        Value child_delta = py_to_delta(item, child);
                        modified.set_item_copy(key_value.view().data(), child_delta.view().data());
                    }
                }
                BundleBuilder bundle{delta_binding(ts->delta_value_schema)};
                bundle.set("removed", removed.build());
                bundle.set("modified", modified.build());
                return bundle.build();
            }
            case TSTypeKind::TSL: {
                // A list/tuple of per-index entries; None = index silent this cycle.
                const auto *map_meta = ts->delta_value_schema;
                MapBuilder  builder{delta_binding(map_meta->key_type), delta_binding(map_meta->element_type)};
                std::int64_t index = 0;
                for (nb::handle item : object)
                {
                    if (!item.is_none())
                    {
                        Value child_delta = py_to_delta(item, ts->element_ts());
                        builder.set_item_copy(std::addressof(index), child_delta.view().data());
                    }
                    ++index;
                }
                return builder.build();
            }
            default: return py_to_value_as(object, ts->delta_value_schema);
        }
    }

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

        [[nodiscard]] nb::list recorded(const std::string &key)
        {
            nb::list result;
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
        std::unique_ptr<Wiring> owned{std::make_unique<Wiring>()};
        Wiring                 *raw{owned.get()};
        bool                    finished{false};

        PyWiring() = default;

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
                                      std::optional<PyTsType> output_type)
        {
            ensure_open();
            const auto wiring_args = build_args(args, kwargs);
            ResolvedOperatorCall resolved = OperatorRegistry::instance().resolve(
                name, std::span<const WiringArg>{wiring_args.data(), wiring_args.size()}, std::nullopt,
                output_type.has_value() ? output_type->meta : nullptr);
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
        Wiring child;
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
            nullptr,   // output schema unknown ahead of compilation (like operator markers)
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

    [[nodiscard]] nb::object call_py_node(const PyNodeRef &ref, std::span<const nb::object> args)
    {
        nb::list call_args;
        for (const nb::object &arg : args) { call_args.append(arg); }
        return ref.record->fn(*nb::tuple(call_args));
    }

    void apply_py_result(nb::handle result, Out<TsVar<"O">> &out)
    {
        if (result.is_none()) { return; }
        const auto &erased = static_cast<const TSOutputView &>(out);
        Value       delta  = py_to_delta(result, erased.schema());
        out.apply(delta.view());
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

    /** Assemble the python call args per the layout; false = a ts arg is not yet valid. */
    [[nodiscard]] bool py_assemble_args(std::string_view layout, const TSInputView &args, const ValueView &scalars,
                                        State<PyStateRef> &state, NodeScheduler scheduler, DateTime now,
                                        nb::list &call_args)
    {
        auto        bundle       = args.as_bundle();
        std::size_t ts_index     = 0;
        std::size_t scalar_index = 0;
        auto        scalar_list  = scalars.valid() ? std::optional{scalars.as_list()} : std::nullopt;
        for (const char kind : layout)
        {
            switch (kind)
            {
                case 't': {
                    auto child = bundle[ts_index++];
                    if (!child.valid()) { return false; }
                    call_args.append(value_to_py(child.value()));
                    break;
                }
                case 's': {
                    if (!scalar_list.has_value()) { throw std::logic_error("python node: missing scalars value"); }
                    call_args.append(value_to_py((*scalar_list)[scalar_index++].as_any().get()));
                    break;
                }
                case 'S': call_args.append(py_state_namespace(state)); break;
                case 'c': call_args.append(nb::cast(PyEvalClock{now})); break;
                case 'd': call_args.append(nb::cast(PyScheduler{scheduler})); break;
                default: throw std::logic_error("python node: unknown layout marker");
            }
        }
        return true;
    }

    struct py_compute_node
    {
        static constexpr auto name = "__py_compute";

        static void eval(In<"args", TsVar<"A">, InputValidity::Unchecked> args, Scalar<"fn", PyNodeRef> fn,
                         Scalar<"config", Str> config, Scalar<"scalars", ScalarVar<"SV">> scalars,
                         State<PyStateRef> state, NodeScheduler scheduler, DateTime now, Out<TsVar<"O">> out)
        {
            nb::gil_scoped_acquire gil;
            nb::list call_args;
            if (!py_assemble_args(config.value(), args.base(), scalars.value(), state, scheduler, now, call_args))
            {
                return;
            }
            apply_py_result(fn.value().record->fn(*nb::tuple(call_args)), out);
        }

        static void stop(State<PyStateRef> state) { py_release_state(state); }
    };

    struct py_sink_node
    {
        static constexpr auto name = "__py_sink";

        static void eval(In<"args", TsVar<"A">, InputValidity::Unchecked> args, Scalar<"fn", PyNodeRef> fn,
                         Scalar<"config", Str> config, Scalar<"scalars", ScalarVar<"SV">> scalars,
                         State<PyStateRef> state, NodeScheduler scheduler, DateTime now)
        {
            nb::gil_scoped_acquire gil;
            nb::list call_args;
            if (!py_assemble_args(config.value(), args.base(), scalars.value(), state, scheduler, now, call_args))
            {
                return;
            }
            (void)fn.value().record->fn(*nb::tuple(call_args));
        }

        static void stop(State<PyStateRef> state) { py_release_state(state); }
    };

    /** Heap iterator state (pointer-in-State, the frame-backend pattern). */
    struct PyGenHandle
    {
        nb::object iterator;
        nb::object pending;      ///< the value yielded for the SCHEDULED time
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
        handle.pending = nb::object(pair[1]);
        sched.schedule(nb::cast<DateTime>(pair[0]));
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
                                     Scalar<"config", Str>, Scalar<"scalars", ScalarVar<"SV">>, Out<TsVar<"O">>> {};
    struct op_py_sink : Operator<"__py_sink", In<"args", TsVar<"A">>, Scalar<"fn", PyNodeRef>,
                                 Scalar<"config", Str>, Scalar<"scalars", ScalarVar<"SV">>> {};
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

        static void start(Scalar<"key", std::string> key, GlobalStateView gs)
        {
            testing::record::start(std::move(key), std::move(gs));
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"key", std::string> key, GlobalStateView gs, DateTime now)
        {
            testing::record::eval(std::move(ts), std::move(key), std::move(gs), now);
        }
    };

    struct op_harness_replay
        : Operator<"__harness_replay", Scalar<"key", Str>, Out<TsVar<"S">>> {};
    struct op_harness_record : Operator<"__harness_record", In<"ts", TsVar<"S">>, Scalar<"key", Str>> {};

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
    register_overload<op_py_compute, py_compute_node>();
    register_overload<op_py_sink, py_sink_node>();
    register_overload<op_py_generator, py_generator_node>();
    register_overload<op_recover_pt, stdlib::component_detail::recovering_pass_through>();
    register_overload<op_harness_replay, harness_replay>();
    register_overload<op_harness_record, harness_record>();

    nb::class_<PyTsType>(m, "TsType");
    nb::class_<PyPort>(m, "Port");
    nb::class_<PyRun>(m, "Run").def("recorded", &PyRun::recorded, nb::arg("key"));

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
    m.def("tss", [](PyValueType v) { return PyTsType{TypeRegistry::instance().tss(v.meta)}; });
    m.def("tsd", [](PyValueType k, PyTsType v) { return PyTsType{TypeRegistry::instance().tsd(k.meta, v.meta)}; });
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

    // record/replay configuration (the model is explicit wiring-time config).
    m.def("set_record_replay_config", [](const std::string &model) {
        record_replay::set_config(record_replay::Config{.model = model});
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
    m.def("comparison_summary", [](const std::string &fq_key) {
        const auto summary = record_replay::comparison_summary(fq_key);
        return nb::make_tuple(summary.compared, summary.mismatches);
    });
    m.def("frame_store_contains", [](const std::string &key) { return record_replay::store_contains(key); });
    m.def("frame_store_read", [](const std::string &key) { return frame_to_py(record_replay::store_read(key)); });
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
    nb::class_<PyArrowStream>(m, "ArrowStream")
        .def("__arrow_c_stream__",
             [](const PyArrowStream &self, nb::handle) { return self.capsule(); },
             nb::arg("requested_schema") = nb::none());
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
    m.def("bundle_port", [](nb::list ports) {
        if (nb::len(ports) == 0) { throw nb::value_error("bundle_port requires at least one port"); }
        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
        std::vector<WiringPortRef> children;
        fields.reserve(nb::len(ports));
        children.reserve(nb::len(ports));
        std::size_t index = 0;
        for (nb::handle port : ports)
        {
            const WiringPortRef &ref = nb::cast<PyPort &>(port).ref;
            fields.emplace_back("_" + std::to_string(index++), ref.schema);
            children.push_back(ref);
        }
        const auto *schema = TypeRegistry::instance().un_named_tsb(fields);
        return PyPort{WiringPortRef::structural_source(schema, std::move(children))};
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

    m.def("graph_fn", [](nb::object wrapper, nb::object user_fn, nb::list param_names, bool has_output) {
        auto &registry = py_graph_fn_registry();
        auto  found    = registry.find(user_fn.ptr());
        if (found == registry.end())
        {
            auto *record = new PyGraphFnRecord{};   // immortal: WiredFn contexts must outlive every value
            record->wrapper    = wrapper;
            record->user_fn    = user_fn;
            record->has_output = has_output;
            record->arity      = nb::len(param_names);
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
    });
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
        .def("wire", &PyWiring::wire, nb::arg("name"), nb::arg("args") = nb::tuple(),
             nb::arg("kwargs") = nb::dict(), nb::arg("output_type") = nb::none())
        .def("set_replay", &PyWiring::set_replay, nb::arg("key"), nb::arg("values"),
             nb::arg("ts_type") = nb::none())
        .def("feedback", &PyWiring::feedback, nb::arg("ts_type"), nb::arg("initial") = nb::none())
        .def("feedback_bind", &PyWiring::feedback_bind, nb::arg("feedback"), nb::arg("port"))
        .def("run", &PyWiring::run, nb::arg("start_time") = nb::none(), nb::arg("end_time") = nb::none(),
             nb::arg("realtime") = false)
        .def("push_source", &PyWiring::push_source, nb::arg("ts_type"), nb::arg("conflate") = false,
             nb::arg("on_start") = nb::none());

    m.def(
        "evaluate_const",
        [](const std::string &name, nb::tuple args, nb::dict kwargs, std::optional<PyTsType> output_type) {
            const auto wiring_args = build_args(args, kwargs);
            Value      value       = OperatorRegistry::instance().evaluate_const(
                name, std::span<const WiringArg>{wiring_args.data(), wiring_args.size()},
                output_type.has_value() ? output_type->meta : nullptr);
            return value.has_value() ? value_to_py(value.view()) : nb::none();
        },
        nb::arg("name"), nb::arg("args") = nb::tuple(), nb::arg("kwargs") = nb::dict(),
        nb::arg("output_type") = nb::none());

    m.def("reset_registries", [] {
        reset_all_registries();
        stdlib::register_standard_operators();
    register_overload<op_py_compute, py_compute_node>();
    register_overload<op_py_sink, py_sink_node>();
    register_overload<op_py_generator, py_generator_node>();
    register_overload<op_recover_pt, stdlib::component_detail::recovering_pass_through>();
    register_overload<op_harness_replay, harness_replay>();
    register_overload<op_harness_record, harness_record>();
    });
}

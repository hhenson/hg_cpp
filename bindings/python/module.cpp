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
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/python/chrono.h>

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
        return py_container_to_value(object);
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
                auto     tuple = view.as_tuple();
                nb::dict result;
                for (std::size_t index = 0; index < tuple.size(); ++index)
                {
                    result[meta->fields[index].name] = value_to_py(tuple.at(index));
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

    struct PyPort
    {
        WiringPortRef ref{};
    };

    struct PyRun;

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
        Wiring wiring{};
        bool   finished{false};

        [[nodiscard]] nb::object wire(const std::string &name, nb::tuple args, nb::dict kwargs,
                                      std::optional<PyTsType> output_type)
        {
            ensure_open();
            const auto wiring_args = build_args(args, kwargs);
            ResolvedOperatorCall resolved = OperatorRegistry::instance().resolve(
                name, std::span<const WiringArg>{wiring_args.data(), wiring_args.size()}, std::nullopt,
                output_type.has_value() ? output_type->meta : nullptr);
            OperatorWireResult result = resolved.impl->wire(wiring, resolved.map, resolved.args, resolved.kwargs);
            if (!result.has_output) { return nb::none(); }
            return nb::cast(PyPort{result.output.erased()});
        }

        void set_replay(const std::string &key, nb::list values)
        {
            ensure_open();
            std::vector<std::optional<Value>> deltas;
            deltas.reserve(nb::len(values));
            for (nb::handle object : values)
            {
                if (object.is_none()) { deltas.emplace_back(std::nullopt); }
                else { deltas.emplace_back(py_to_value(object)); }
            }
            testing::set_replay_deltas(wiring.global_state(), key, deltas);
        }

        [[nodiscard]] std::unique_ptr<PyRun> run()
        {
            ensure_open();
            finished = true;
            GraphBuilder builder = std::move(wiring).finish();

            GraphExecutorBuilder eb;
            eb.graph_builder(std::move(builder)).start_time(MIN_ST).end_time(MAX_ET);
            auto run = std::make_unique<PyRun>(PyRun{eb.make_executor()});
            run->executor.view().run();
            return run;
        }

      private:
        void ensure_open() const
        {
            if (finished) { throw std::logic_error("this Wiring has already been finished/run"); }
        }
    };
}  // namespace

NB_MODULE(_hgraph, m)
{
    m.doc() = "hgraph C++ runtime bridge (slice 1: wire and run graphs by operator name)";

    stdlib::register_standard_operators();

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
    m.attr("IN_MEMORY")  = std::string{record_replay::IN_MEMORY};
    m.attr("DATA_FRAME") = std::string{record_replay::DATA_FRAME};

    m.def("operator_names", [] { return OperatorRegistry::instance().registered_names(); });

    // Conversion-layer round trip (test/debug aid): Python -> Value -> Python.
    m.def("_roundtrip_value", [](nb::handle object) { return value_to_py(py_to_value(object).view()); });

    nb::class_<PyWiring>(m, "Wiring")
        .def(nb::init<>())
        .def("wire", &PyWiring::wire, nb::arg("name"), nb::arg("args") = nb::tuple(),
             nb::arg("kwargs") = nb::dict(), nb::arg("output_type") = nb::none())
        .def("set_replay", &PyWiring::set_replay, nb::arg("key"), nb::arg("values"))
        .def("run", &PyWiring::run);

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
    });
}

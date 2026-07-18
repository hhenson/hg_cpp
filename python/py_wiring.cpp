/**
 * Wiring-domain implementation + bindings: the leaked graph-fn/WiredFn/node
 * registries, erased WiringArg assembly, the python graph-fn WiredFn ops,
 * and bind_wiring() (Wiring/Run, node_ref/graph_fn, switch/dispatch cases,
 * feedback, component, _evaluate_const).
 */
#include "py_wiring.h"
#include "py_bindings.h"

namespace nb = nanobind;
using namespace hgraph;
using namespace hgraph::python_bridge;

namespace
{
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

    struct RuntimeOperatorRecord
    {
        std::string name;
        std::vector<std::string> names;
        std::vector<std::string_view> name_views;
        std::size_t arity{0};
        bool variadic{false};
        bool has_output{false};
    };

    [[nodiscard]] std::unordered_map<std::string, RuntimeOperatorRecord *> &runtime_operator_registry()
    {
        static auto *registry = new std::unordered_map<std::string, RuntimeOperatorRecord *>{};
        return *registry;
    }

    [[nodiscard]] WiringPortRef runtime_operator_wire(const void *context, Wiring &w,
                                                      std::span<const WiringPortRef> args)
    {
        const auto &record = *static_cast<const RuntimeOperatorRecord *>(context);
        return wire_erased_operator(w, record.name, args, record.has_output);
    }

    [[nodiscard]] CompiledSubGraph runtime_operator_compile(
        const void *context, std::span<const TSValueTypeMetaData *const> input_schemas)
    {
        Wiring child{WiringKind::SubGraph};
        std::vector<const TSValueTypeMetaData *> schemas{input_schemas.begin(), input_schemas.end()};
        std::vector<WiringPortRef> boundary;
        boundary.reserve(input_schemas.size());
        for (std::size_t index = 0; index < input_schemas.size(); ++index)
        {
            boundary.push_back(WiringPortRef::boundary_source(index, {}, input_schemas[index]));
        }
        WiringPortRef out = runtime_operator_wire(context, child, boundary);
        const auto &record = *static_cast<const RuntimeOperatorRecord *>(context);
        if (record.has_output)
        {
            return std::move(child).finish_subgraph(out, std::move(schemas));
        }
        return std::move(child).finish_subgraph(std::nullopt, std::move(schemas));
    }

    [[nodiscard]] const WiredFnOps &runtime_operator_ops()
    {
        static constexpr WiredFnOps ops{
            &runtime_operator_wire,
            &runtime_operator_compile,
            [](const void *context) {
                const auto &record = *static_cast<const RuntimeOperatorRecord *>(context);
                return std::span<const std::string_view>{
                    record.name_views.data(), record.name_views.size()};
            },
            [](const void *) -> const TSValueTypeMetaData * { return nullptr; },
        };
        return ops;
    }

    [[nodiscard]] WiredFn runtime_operator_fn(const std::string &name)
    {
        auto &registry = runtime_operator_registry();
        auto  found    = registry.find(name);
        if (found == registry.end())
        {
            const auto shape = OperatorRegistry::instance().callable_shape(name);
            if (!shape.has_value())
            {
                throw std::invalid_argument(
                    "operator '" + name +
                    "' has no single time-series-only callable shape for higher-order use");
            }
            auto *record = new RuntimeOperatorRecord{};
            record->name = name;
            record->names = shape->parameter_names;
            record->name_views.reserve(record->names.size());
            for (const std::string &parameter : record->names)
            {
                record->name_views.push_back(parameter);
            }
            record->arity = shape->arity;
            record->variadic = shape->variadic;
            record->has_output = shape->has_output;
            found = registry.emplace(name, record).first;
        }
        return WiredFn{
            .ops           = &runtime_operator_ops(),
            .context       = found->second,
            .identity      = &typeid(RuntimeOperatorRecord),
            .operator_name = found->second->name,
            .arity         = found->second->arity,
            .variadic      = found->second->variadic,
            .has_output    = found->second->has_output,
        };
    }

    /** Immortal callable records (stable scalar identity by pointer). */
    [[nodiscard]] std::unordered_map<PyObject *, PyNodeRecord *> &py_node_registry()
    {
        static auto *registry = new std::unordered_map<PyObject *, PyNodeRecord *>{};
        return *registry;
    }

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
        // The call result is authoritative. An unannotated lambda is
        // provisionally output-producing for generic inference, but may
        // compile to an actual sink.
        if (out.schema != nullptr)
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
}  // namespace

namespace hgraph::python_bridge
{
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
            else if (nb::isinstance<PyTsType>(object))
            {
                arg.kind         = WiringArg::Kind::Scalar;
                arg.scalar_value = Value{PyTsMetaRef{nb::cast<PyTsType &>(object).meta}};
                arg.scalar_meta  = arg.scalar_value.schema();
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
            else if (nb::isinstance<PyDispatchCases>(object))
            {
                arg.kind         = WiringArg::Kind::Scalar;
                arg.scalar_value = Value{nb::cast<PyDispatchCases &>(object).cases};
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

    void bind_wiring(nb::module_ &m)
    {
    python_conversion_traits<WiredFn>::to_python_hook = [](const WiredFn &value) {
        return nb::cast(PyWiredFn{value});
    };
    python_conversion_traits<WiredFn>::from_python_hook = [](nb::handle source) {
        if (!nb::isinstance<PyWiredFn>(source))
        {
            throw nb::type_error("expected a WiredFn value");
        }
        return nb::cast<PyWiredFn &>(source).fn;
    };

    nb::class_<PyRun>(m, "Run").def("recorded", &PyRun::recorded, nb::arg("key"), nb::arg("sparse") = false);
    m.def("operator_names", [] { return OperatorRegistry::instance().registered_names(); });

    nb::class_<EvaluationTrace>(m, "EvaluationTrace")
        .def(nb::init<std::optional<std::string>, bool, bool, bool, bool, bool>(),
             nb::arg("filter").none() = nb::none(), nb::arg("start") = true,
             nb::arg("eval") = true, nb::arg("stop") = true,
             nb::arg("node") = true, nb::arg("graph") = true)
        .def_static("set_print_all_values", &EvaluationTrace::set_print_all_values,
                    nb::arg("value"))
        .def_static("set_use_logger", &EvaluationTrace::set_use_logger,
                    nb::arg("value"));

    nb::class_<PyWiredFn>(m, "WiredFn")
        .def_prop_ro("arity", [](const PyWiredFn &self) { return self.fn.arity; })
        .def_prop_ro("variadic", [](const PyWiredFn &self) { return self.fn.variadic; })
        .def_prop_ro("has_output", [](const PyWiredFn &self) { return self.fn.has_output; })
        .def_prop_ro("_python_callable", [](const PyWiredFn &self) -> nb::object {
            if (self.fn.identity != nullptr && *self.fn.identity == typeid(PyGraphFnRecord) &&
                self.fn.context != nullptr)
            {
                return static_cast<const PyGraphFnRecord *>(self.fn.context)->user_fn;
            }
            return nb::none();
        });
    nb::class_<PyNodeHandle>(m, "NodeRef");
    nb::class_<PyScalarValue>(m, "ScalarValue");
    nb::class_<PySender>(m, "Sender").def("send", &PySender::send, nb::arg("value"));

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
    nb::class_<PyDispatchCases>(m, "DispatchCases");
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
    m.def("dispatch_cases", [](nb::list entries, nb::list on, nb::object default_branch) {
        const auto as_wired_fn = [](nb::handle branch) {
            if (nb::isinstance<PyWiredFn>(branch))
            {
                return nb::cast<PyWiredFn &>(branch).fn;
            }
            const auto &table = wired_fn_table();
            const auto found = table.find(nb::cast<std::string>(branch));
            if (found == table.end())
            {
                throw nb::value_error("no wired-fn erasure for dispatch branch");
            }
            return found->second;
        };

        stdlib::DispatchCases result;
        result.dispatch_args.clear();
        for (nb::handle index : on)
        {
            result.dispatch_args.push_back(nb::cast<std::size_t>(index));
        }
        for (nb::handle item : entries)
        {
            nb::tuple pair = nb::cast<nb::tuple>(item);
            nb::tuple types = nb::cast<nb::tuple>(pair[0]);
            stdlib::DispatchCase entry;
            entry.types.reserve(nb::len(types));
            for (nb::handle type : types)
            {
                entry.types.push_back(nb::cast<PyValueType &>(type).meta);
            }
            entry.branch = as_wired_fn(pair[1]);
            result.cases.push_back(std::move(entry));
        }
        if (!default_branch.is_none()) { result.default_branch = as_wired_fn(default_branch); }
        return PyDispatchCases{std::move(result)};
    }, nb::arg("entries"), nb::arg("on"), nb::arg("default_branch").none() = nb::none());
    m.def("wired_op", [](const std::string &name) {
        const auto &table = wired_fn_table();
        const auto  found = table.find(name);
        return PyWiredFn{found != table.end() ? found->second : runtime_operator_fn(name)};
    });

    // Conversion-layer round trip (test/debug aid): Python -> Value -> Python.
    m.def("_roundtrip_value", [](nb::handle object) { return value_to_py(py_to_value(object).view()); });

    nb::class_<PyWiring>(m, "Wiring")
        .def(nb::init<>())
        .def(nb::init<GlobalState &>(), nb::arg("state"))
        .def("exception_time_series", &PyWiring::exception_time_series,
             nb::arg("port"), nb::arg("trace_back_depth") = 1,
             nb::arg("capture_values") = false)
        .def("wire", &PyWiring::wire, nb::arg("name"), nb::arg("args") = nb::tuple(),
             nb::arg("kwargs") = nb::dict(), nb::arg("output_type") = nb::none(),
             nb::arg("sizes") = nb::none())
        .def("set_replay", &PyWiring::set_replay, nb::arg("key"), nb::arg("values"),
             nb::arg("ts_type") = nb::none())
        .def("feedback", &PyWiring::feedback, nb::arg("ts_type"), nb::arg("initial") = nb::none())
        .def("feedback_bind", &PyWiring::feedback_bind, nb::arg("feedback"), nb::arg("port"))
        .def("_release_seed_context", &PyWiring::release_seed_context)
        .def("run", &PyWiring::run, nb::arg("start_time") = nb::none(), nb::arg("end_time") = nb::none(),
             nb::arg("realtime") = false, nb::arg("trace").none() = nb::none())
        .def("push_source", &PyWiring::push_source, nb::arg("ts_type"), nb::arg("conflate") = false,
             nb::arg("on_start") = nb::none());

    m.def(
        "component",
        [](PyWiring &wiring, const std::string &recordable_id,
           nb::list names, nb::list ports, nb::object compose) -> nb::object {
            if (nb::len(names) != nb::len(ports))
            {
                throw nb::value_error("component names and ports must have the same length");
            }

            std::vector<WiringNamedPortRef> inputs;
            inputs.reserve(nb::len(names));
            for (std::size_t index = 0; index < nb::len(names); ++index)
            {
                inputs.emplace_back(
                    nb::cast<std::string>(names[index]),
                    nb::cast<PyPort &>(ports[index]).ref);
            }

            try
            {
                WiringPortRef out = stdlib::component(
                    wiring.wiring_ref(), recordable_id,
                    std::span<const WiringNamedPortRef>{inputs.data(), inputs.size()},
                    [&](std::span<const WiringPortRef> wrapped) {
                        nb::list args;
                        for (const WiringPortRef &port : wrapped)
                        {
                            args.append(nb::cast(PyPort{port}));
                        }
                        nb::object result = compose(nb::tuple(args));
                        return result.is_none() ? WiringPortRef{}
                                                : nb::cast<PyPort &>(result).ref;
                    });
                return out.is_unbound_source() ? nb::none()
                                               : nb::cast(PyPort{std::move(out)});
            }
            catch (const std::invalid_argument &error)
            {
                if (std::string_view{error.what()}.starts_with(
                        "component: duplicate recordable id"))
                {
                    throw std::runtime_error(error.what());
                }
                throw;
            }
        },
        nb::arg("wiring"), nb::arg("recordable_id"), nb::arg("names"),
        nb::arg("ports"), nb::arg("compose"));

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
    }
}  // namespace hgraph::python_bridge

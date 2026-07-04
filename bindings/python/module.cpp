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

    [[nodiscard]] Value py_to_value(nb::handle object)
    {
        if (nb::isinstance<nb::bool_>(object)) { return Value{Bool{nb::cast<bool>(object)}}; }
        if (nb::isinstance<nb::int_>(object)) { return Value{Int{nb::cast<Int>(object)}}; }
        if (nb::isinstance<nb::float_>(object)) { return Value{Float{nb::cast<Float>(object)}}; }
        if (nb::isinstance<nb::str>(object)) { return Value{Str{nb::cast<std::string>(object)}}; }
        DateTime when;
        if (nb::try_cast<DateTime>(object, when)) { return Value{when}; }
        TimeDelta delta;
        if (nb::try_cast<TimeDelta>(object, delta)) { return Value{delta}; }
        throw nb::type_error("unsupported Python scalar for hgraph (slice 1: bool/int/float/str/datetime/timedelta)");
    }

    [[nodiscard]] nb::object value_to_py(const ValueView &view)
    {
        if (!view.valid()) { return nb::none(); }
        const auto *meta = view.schema();
        if (meta == scalar_descriptor<Bool>::value_meta()) { return nb::cast(view.checked_as<Bool>()); }
        if (meta == scalar_descriptor<Int>::value_meta()) { return nb::cast(view.checked_as<Int>()); }
        if (meta == scalar_descriptor<Float>::value_meta()) { return nb::cast(view.checked_as<Float>()); }
        if (meta == scalar_descriptor<Str>::value_meta()) { return nb::cast(view.checked_as<Str>()); }
        if (meta == scalar_descriptor<DateTime>::value_meta()) { return nb::cast(view.checked_as<DateTime>()); }
        if (meta == scalar_descriptor<TimeDelta>::value_meta()) { return nb::cast(view.checked_as<TimeDelta>()); }
        throw nb::type_error("unsupported hgraph scalar for Python (slice 1)");
    }

    // ---------------------------------------------------------------
    // Wiring surface
    // ---------------------------------------------------------------

    struct PyTsType
    {
        const TSValueTypeMetaData *meta{nullptr};
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

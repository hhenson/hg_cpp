#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/service_runtime.h>
#include <hgraph/types/service_wiring.h>

#include <catch2/catch_test_macros.hpp>

// Runtime service identity (services.rst, rulings 2026-07-05): the erased
// flavour flows over RuntimeServiceDescriptor share the role markers and
// name-qualified path grammar with the C++ templates, so an erased
// registration UNIFIES with a template client on the same path - the
// python-impl-for-C++-stub direction, proven structurally in C++.

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct RuntimeBaseValue
    {
        static constexpr std::string_view name{"runtime_base_value"};
        using output_schema = TS<Int>;
    };

    struct RuntimeConstGraph
    {
        [[maybe_unused]] static constexpr auto name = "runtime_const_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            return wire<stdlib::const_>(w, Int{41}).as<TS<Int>>();
        }
    };

    struct ErasedRegisterTemplateClient
    {
        [[maybe_unused]] static constexpr auto name = "erased_register_template_client";

        static Port<TS<Int>> compose(Wiring &w)
        {
            auto &registry = TypeRegistry::instance();
            RuntimeServiceDescriptor descriptor;
            descriptor.name          = std::string{RuntimeBaseValue::name};
            descriptor.flavour       = ServiceFlavour::Reference;
            descriptor.output_schema = registry.ts(scalar_descriptor<Int>::value_meta());
            const auto *interned     = &intern_service_descriptor(std::move(descriptor));

            // Register through the ERASED flow (a WiredFn impl - the python
            // shape) and consume through the TEMPLATE client on the same
            // path: identity must unify.
            register_reference_service_impl(w, *interned, "prices", fn<RuntimeConstGraph>());
            auto value = service::reference_service<RuntimeBaseValue>(w, service::path("prices"));
            return wire<stdlib::add_>(w, value, wire<stdlib::const_>(w, Int{1}).as<TS<Int>>()).as<TS<Int>>();
        }
    };

    struct RuntimeEchoServiceAdaptor : service_adaptor::interface
    {
        static constexpr std::string_view name{"runtime_echo_service_adaptor"};
        using input_schema  = TS<Int>;
        using output_schema = TS<Int>;
    };

    struct RuntimeEchoServiceAdaptorImpl
    {
        [[maybe_unused]] static constexpr auto name = "runtime_echo_service_adaptor_impl";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &, Port<TSD<Int, TS<Int>>> requests)
        {
            return requests;
        }
    };

    struct ErasedServiceAdaptorRegisterTemplateClients
    {
        [[maybe_unused]] static constexpr auto name = "erased_service_adaptor_register_template_clients";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            auto &registry = TypeRegistry::instance();
            RuntimeServiceDescriptor descriptor;
            descriptor.name          = std::string{RuntimeEchoServiceAdaptor::name};
            descriptor.flavour       = ServiceFlavour::ServiceAdaptor;
            descriptor.input_schema  = registry.ts(scalar_descriptor<Int>::value_meta());
            descriptor.output_schema = descriptor.input_schema;
            const auto *interned = &intern_service_descriptor(std::move(descriptor));

            register_service_adaptor_impl(
                w, *interned, "echo", fn<RuntimeEchoServiceAdaptorImpl>());
            const auto custom = service_adaptor::path("echo");
            auto lhs_reply = wire<RuntimeEchoServiceAdaptor>(w, custom, lhs);
            auto rhs_reply = wire<RuntimeEchoServiceAdaptor>(w, custom, rhs);
            return wire<stdlib::add_>(w, lhs_reply, rhs_reply).as<TS<Int>>();
        }
    };
}  // namespace

TEST_CASE("service runtime: an erased registration serves a template client")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ErasedRegisterTemplateClient>(), values<Int>(42));
}

TEST_CASE("service runtime: descriptor interning enforces schema match by name")
{
    RuntimeServiceDescriptor first;
    first.name          = "intern_check";
    first.flavour       = ServiceFlavour::Reference;
    first.output_schema = TypeRegistry::instance().ts(scalar_descriptor<Int>::value_meta());
    const auto *interned = &intern_service_descriptor(first);

    // Same name + schemas: the SAME record (python re-import tolerance).
    CHECK(&intern_service_descriptor(first) == interned);

    RuntimeServiceDescriptor conflicting = first;
    conflicting.output_schema = TypeRegistry::instance().ts(scalar_descriptor<Float>::value_meta());
    CHECK_THROWS_AS((void)intern_service_descriptor(conflicting), std::invalid_argument);

    CHECK(find_service_descriptor("intern_check") == interned);
}

TEST_CASE("service runtime: erased service-adaptor registration serves typed clients")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ErasedServiceAdaptorRegisterTemplateClients>(
                     values<Int>(1, none, 2), values<Int>(10, none, 20)),
                 values<Int>(none, 11, none, 22));
}

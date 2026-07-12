#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>

#include <cstdint>

using namespace hgraph;

Value fixture_atomic_value{};
Value fixture_bundle_value{};
AnyPtr fixture_atomic_pointer{};
AnyPtr fixture_bundle_pointer{};

#if defined(_MSC_VER)
#define HGRAPH_DEBUGGER_NOINLINE __declspec(noinline)
#else
#define HGRAPH_DEBUGGER_NOINLINE __attribute__((noinline))
#endif

extern "C" HGRAPH_DEBUGGER_NOINLINE void hgraph_debugger_fixture_stop()
{
#if defined(__GNUC__) || defined(__clang__)
    asm volatile("" ::: "memory");
#endif
}

int main()
{
    auto &registry = TypeRegistry::instance();
    const auto *int_schema = registry.register_scalar<std::int32_t>("int32");
    const auto *bool_schema = registry.register_scalar<bool>("bool");

    fixture_atomic_value = Value{std::int32_t{42}};
    auto atomic_view = fixture_atomic_value.view();
    fixture_atomic_pointer = AnyPtr::read_only(*atomic_view.record(), atomic_view.data());

    const auto *bundle_schema = registry.bundle("DebuggerFixture", {{"number", int_schema}, {"enabled", bool_schema}});
    const ValueTypeRef bundle_type = ValuePlanFactory::instance().type_for(bundle_schema);
    BundleBuilder builder{bundle_type};
    builder.set("number", atomic_view);
    fixture_bundle_value = builder.build();
    auto bundle_view = fixture_bundle_value.view();
    fixture_bundle_pointer = AnyPtr::read_only(*bundle_view.record(), bundle_view.data());

    hgraph_debugger_fixture_stop();
    return fixture_atomic_value.view().checked_as<std::int32_t>() == 42 ? 0 : 1;
}

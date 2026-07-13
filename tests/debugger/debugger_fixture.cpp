#include <hgraph/runtime/node.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>

#include <cstdint>
#include <utility>

using namespace hgraph;

Value fixture_atomic_value{};
Value fixture_bundle_value{};
Value fixture_list_value{};
Value fixture_map_value{};
NodeValue fixture_node_value{};
AnyPtr fixture_atomic_pointer{};
AnyPtr fixture_bundle_pointer{};
AnyPtr fixture_list_pointer{};
AnyPtr fixture_map_pointer{};
AnyPtr fixture_node_pointer{};

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

    ListBuilder list_builder{registry.scalar_type<std::int32_t>()};
    list_builder.push_back<std::int32_t>(3);
    list_builder.push_back<std::int32_t>(5);
    list_builder.push_back<std::int32_t>(8);
    fixture_list_value = list_builder.build();
    auto list_view = fixture_list_value.view();
    fixture_list_pointer = AnyPtr::read_only(*list_view.record(), list_view.data());

    const auto *map_schema = registry.mutable_map(int_schema, int_schema);
    fixture_map_value = Value{ValuePlanFactory::instance().type_for(map_schema)};
    auto map_mutation = fixture_map_value.as_map().begin_mutation();
    map_mutation.set_item(Value{std::int32_t{1}}.view(), Value{std::int32_t{10}}.view());
    map_mutation.set_item(Value{std::int32_t{2}}.view(), Value{std::int32_t{20}}.view());
    static_cast<void>(map_mutation.remove(Value{std::int32_t{2}}.view()));
    auto map_view = fixture_map_value.view();
    fixture_map_pointer = AnyPtr::read_only(*map_view.record(), map_view.data());

    NodeTypeMetaData node_schema;
    node_schema.display_name = "debugger_fixture_node";
    node_schema.state_schema = int_schema;
    NodeBuilder node_builder = NodeBuilder::native(std::move(node_schema));
    fixture_node_value = node_builder.make_node();
    fixture_node_pointer = fixture_node_value.view().pointer().to_any();

    hgraph_debugger_fixture_stop();
    return fixture_atomic_value.view().checked_as<std::int32_t>() == 42 ? 0 : 1;
}

#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node.h>
#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/types/metadata/debug_descriptor.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>

using namespace hgraph;

Value fixture_atomic_value{};
Value fixture_bundle_value{};
Value fixture_list_value{};
Value fixture_map_value{};
NodeValue fixture_node_value{};
std::unique_ptr<testing::MockRootGraph> fixture_root_graph{};
AnyPtr fixture_atomic_pointer{};
AnyPtr fixture_bundle_pointer{};
AnyPtr fixture_list_pointer{};
AnyPtr fixture_map_pointer{};
AnyPtr fixture_node_pointer{};
AnyPtr fixture_graph_pointer{};
AnyPtr fixture_typed_null_pointer{};
AnyPtr fixture_malformed_pointer{};
std::unique_ptr<TypeRecord> fixture_invalid_record_owner{};
TypeRecord *fixture_invalid_record{};
DebugDescriptor fixture_invalid_descriptor{};

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
    fixture_typed_null_pointer = AnyPtr::typed_null(*atomic_view.record());
    fixture_invalid_record_owner = std::make_unique<TypeRecord>(*atomic_view.record());
    fixture_invalid_record = fixture_invalid_record_owner.get();
    ++fixture_invalid_record->abi_version;
    fixture_invalid_descriptor = *atomic_view.record()->debug;
    ++fixture_invalid_descriptor.abi_version;

    const std::uintptr_t malformed_words[2]{0, reinterpret_cast<std::uintptr_t>(&fixture_atomic_value)};
    static_assert(sizeof(malformed_words) == sizeof(fixture_malformed_pointer));
    std::memcpy(&fixture_malformed_pointer, malformed_words, sizeof(malformed_words));

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

    NodeTypeMetaData graph_node_schema;
    graph_node_schema.display_name = "debugger_fixture_graph_node";
    GraphBuilder graph_builder;
    graph_builder.label("debugger_fixture_graph")
        .add_node(NodeBuilder::native(std::move(graph_node_schema)));
    fixture_root_graph = std::make_unique<testing::MockRootGraph>(graph_builder);
    fixture_graph_pointer = fixture_root_graph->graph().pointer().to_any();

    hgraph_debugger_fixture_stop();
    return fixture_atomic_value.view().checked_as<std::int32_t>() == 42 ? 0 : 1;
}

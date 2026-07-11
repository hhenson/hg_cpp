#include <hgraph/runtime/runtime.h>
#include <hgraph/types/metadata/type_meta_data.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <type_traits>

namespace
{
    template <typename Binding> constexpr void assert_binding_layout()
    {
        static_assert(std::is_standard_layout_v<Binding>);
        static_assert(std::is_trivially_copyable_v<Binding>);
        static_assert(sizeof(Binding) == sizeof(void *) * 3);
        static_assert(alignof(Binding) == alignof(void *));
    }

    template <typename Binding> constexpr void assert_storage_ref_layout()
    {
        using Ref = hgraph::MemoryUtils::StorageRef<Binding>;
        static_assert(std::is_standard_layout_v<Ref>);
        static_assert(std::is_trivially_copyable_v<Ref>);
        static_assert(sizeof(Ref) == sizeof(void *) * 2);
        static_assert(alignof(Ref) == alignof(void *));
    }

    template <typename Binding> constexpr void assert_storage_handle_layout()
    {
        using Handle = hgraph::MemoryUtils::StorageHandle<hgraph::MemoryUtils::InlineStoragePolicy<>, Binding>;
        static_assert(sizeof(Handle) == sizeof(void *) * 3);
        static_assert(alignof(Handle) == alignof(void *));
    }
}  // namespace

TEST_CASE("current type-erasure records retain their baseline layouts")
{
    using namespace hgraph;

    assert_binding_layout<ValueTypeBinding>();
    assert_binding_layout<TSDataBinding>();
    assert_binding_layout<NodeTypeBinding>();
    assert_binding_layout<GraphTypeBinding>();
    assert_binding_layout<GraphExecutorTypeBinding>();
    assert_binding_layout<EvaluationClockTypeBinding>();

    assert_storage_ref_layout<ValueTypeBinding>();
    assert_storage_ref_layout<TSDataBinding>();
    assert_storage_ref_layout<NodeTypeBinding>();
    assert_storage_ref_layout<GraphTypeBinding>();
    assert_storage_ref_layout<GraphExecutorTypeBinding>();
    assert_storage_ref_layout<EvaluationClockTypeBinding>();

    static_assert(sizeof(ValueView) == sizeof(void *) * 2);
    static_assert(sizeof(NodeView) == sizeof(void *) * 2);
    static_assert(sizeof(GraphView) == sizeof(void *) * 2);
    static_assert(sizeof(GraphExecutorView) == sizeof(void *) * 2);
    static_assert(sizeof(EvaluationClockView) == sizeof(void *) * 2);

    // TSData refs cache the selected ops pointer in addition to the generic
    // binding/data cursor. This is true for both generic and specialized refs.
    static_assert(sizeof(TSDataStorageRef<>) == sizeof(void *) * 3);
    static_assert(sizeof(IndexedTSDataStorageRef) == sizeof(void *) * 3);
    static_assert(sizeof(TSSDataStorageRef) == sizeof(void *) * 3);
    static_assert(sizeof(TSDDataStorageRef) == sizeof(void *) * 3);
    static_assert(sizeof(TSWDataStorageRef) == sizeof(void *) * 3);
    static_assert(sizeof(TSDataView) == sizeof(void *) * 3);

    using RawHandle = MemoryUtils::StorageHandle<>;
    static_assert(sizeof(RawHandle) == sizeof(void *) * 3);
    static_assert(alignof(RawHandle) == alignof(void *));

    assert_storage_handle_layout<ValueTypeBinding>();
    assert_storage_handle_layout<TSDataBinding>();
    assert_storage_handle_layout<NodeTypeBinding>();
    assert_storage_handle_layout<GraphTypeBinding>();
    assert_storage_handle_layout<GraphExecutorTypeBinding>();
    assert_storage_handle_layout<EvaluationClockTypeBinding>();

    SUCCEED("compile-time type-erasure layout assertions passed");
}

TEST_CASE("value schemas expose the unified schema header as their standard-layout prefix")
{
    using namespace hgraph;

    static_assert(std::is_standard_layout_v<ValueTypeMetaData>);
    static_assert(offsetof(ValueTypeMetaData, header) == 0);
    static_assert(offsetof(ValueTypeMetaData, flags) == sizeof(SchemaHeader));
    static_assert(!std::is_base_of_v<TypeMetaData, ValueTypeMetaData>);
    static_assert(!std::is_convertible_v<const ValueTypeMetaData *, const TypeMetaData *>);

    static_assert(std::is_base_of_v<TypeMetaData, TSValueTypeMetaData>);
    static_assert(std::is_convertible_v<const TSValueTypeMetaData *, const TypeMetaData *>);
    static_assert(sizeof(void *) != 8 || sizeof(TSValueTypeMetaData) == 80);

    static_assert(static_cast<std::uint8_t>(ValueTypeKind::Atomic) == 0);
    static_assert(static_cast<std::uint8_t>(ValueTypeKind::Tuple) == 1);
    static_assert(static_cast<std::uint8_t>(ValueTypeKind::Bundle) == 2);
    static_assert(static_cast<std::uint8_t>(ValueTypeKind::List) == 3);
    static_assert(static_cast<std::uint8_t>(ValueTypeKind::Set) == 4);
    static_assert(static_cast<std::uint8_t>(ValueTypeKind::Map) == 5);
    static_assert(static_cast<std::uint8_t>(ValueTypeKind::CyclicBuffer) == 6);
    static_assert(static_cast<std::uint8_t>(ValueTypeKind::Queue) == 7);
    static_assert(static_cast<std::uint8_t>(ValueTypeKind::Any) == 8);

    const auto *meta = TypeRegistry::instance().value_type("int");
    REQUIRE(meta != nullptr);
    REQUIRE(&meta->schema_header() == reinterpret_cast<const SchemaHeader *>(meta));
    REQUIRE(meta->schema_header().valid());
    REQUIRE(meta->schema_header().family == TypeFamily::Value);
    REQUIRE(meta->schema_header().kind == static_cast<TypeKind>(meta->value_kind()));

    ValueTypeMetaData invalid{ValueTypeKind::Atomic, ValueTypeFlags::None, "invalid"};
    invalid.header.kind = static_cast<TypeKind>(ValueTypeKind::Any) + 1;
    REQUIRE_FALSE(invalid.try_value_kind().has_value());
    REQUIRE_FALSE(try_value_type_kind(invalid.header.kind).has_value());
    REQUIRE_THROWS_AS(invalid.value_kind(), std::invalid_argument);
}

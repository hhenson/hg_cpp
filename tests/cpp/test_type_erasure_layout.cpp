#include <hgraph/runtime/runtime.h>
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

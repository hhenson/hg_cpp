#include <hgraph/types/value/any_ops.h>

#include <hgraph/config.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value.h>

#include <compare>
#include <cstddef>
#include <string>

namespace hgraph
{
    namespace
    {
        // The Any storage is an embedded owning Value; the ops below interpret
        // `memory` as that Value and delegate to it, with an explicit empty state.

        std::size_t any_hash(const void *, const void *memory)
        {
            const Value &value = *static_cast<const Value *>(memory);
            return value.has_value() ? value.hash() : std::size_t{0};
        }

        bool any_equals(const void *, const void *lhs, const void *rhs) noexcept
        {
            const Value &a = *static_cast<const Value *>(lhs);
            const Value &b = *static_cast<const Value *>(rhs);
            if (a.has_value() != b.has_value()) { return false; }
            if (!a.has_value()) { return true; }
            return a.equals(b);
        }

        std::partial_ordering any_compare(const void *, const void *lhs, const void *rhs) noexcept
        {
            const Value &a = *static_cast<const Value *>(lhs);
            const Value &b = *static_cast<const Value *>(rhs);
            if (a.has_value() != b.has_value())
            {
                return a.has_value() ? std::partial_ordering::greater : std::partial_ordering::less;
            }
            if (!a.has_value()) { return std::partial_ordering::equivalent; }
            return a.compare(b);
        }

        std::string any_to_string(const void *, const void *memory)
        {
            const Value &value = *static_cast<const Value *>(memory);
            return value.has_value() ? value.to_string() : std::string{"None"};
        }
    }  // namespace

    const ValueOps &any_ops() noexcept
    {
        static const ValueOps ops{
            nullptr,  // context
            true,     // allows_mutation
            &any_hash,
            &any_equals,
            &any_compare,
            &any_to_string,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
            nullptr,  // to_python (deferred)
            nullptr,  // from_python (deferred)
            nullptr,  // to_python_buffer (deferred)
#endif
            // copy_construct_view / copy_assign_view / owning_binding default to
            // nullptr: the storage plan's lifecycle copies the embedded Value.
        };
        return ops;
    }

    const ValueTypeBinding &any_binding()
    {
        const ValueTypeMetaData *meta = TypeRegistry::instance().any();
        return ValueTypeBinding::intern(*meta, MemoryUtils::plan_for<Value>(), any_ops());
    }
}  // namespace hgraph

#include <hgraph/types/value/any_ops.h>

#include <hgraph/config.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <nanobind/nanobind.h>
#include <hgraph/python/bridge_state.h>
namespace hgraph { namespace nb = nanobind; }
#endif

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

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        nb::object any_to_python(const void *, const void *memory)
        {
            const Value &value = *static_cast<const Value *>(memory);
            if (!value.has_value()) { return nb::none(); }
            // Type-erased delegation: the BOXED value's own binding converts.
            return value.view().binding().ops_ref().to_python(value.view().data());
        }

        void any_from_python(const void *, const ValueTypeRef &, void *memory, nb::handle source)
        {
            Value &boxed = *static_cast<Value *>(memory);
            if (source.is_none())
            {
                boxed = Value{};
                return;
            }
            // Schema-free INFERENCE lives in the module (a dispatch on
            // python types); it installs this hook at import.
            const auto slot = python_bridge::py_infer_value_slot();
            if (slot == nullptr)
            {
                throw std::logic_error("Any::from_python requires the python module's inference hook");
            }
            boxed = reinterpret_cast<Value (*)(nb::handle)>(slot)(source);
        }
#endif
    }  // namespace

    const ValueOps &any_ops() noexcept
    {
        static const ValueOps ops{
            ValueOpsKind::Base,
            nullptr,  // context
            true,     // allows_mutation
            &any_hash,
            &any_equals,
            &any_compare,
            &any_to_string,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
            &any_to_python,
            &any_from_python,
            nullptr,  // to_python_buffer
#endif
            // copy_construct_view / copy_assign_view / owning_binding default to
            // nullptr: the storage plan's lifecycle copies the embedded Value.
        };
        return ops;
    }

    ValueTypeRef any_type()
    {
        const ValueTypeMetaData *meta = TypeRegistry::instance().any();
        return intern_value_type(*meta, MemoryUtils::plan_for<Value>(), any_ops());
    }

    ValueTypeRef any_type(const ValueTypeMetaData &meta)
    {
        return intern_value_type(meta, MemoryUtils::plan_for<Value>(), any_ops());
    }
}  // namespace hgraph

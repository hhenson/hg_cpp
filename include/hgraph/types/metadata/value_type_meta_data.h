//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_VALUE_TYPE_META_DATA_H
#define HGRAPH_CPP_ROOT_VALUE_TYPE_META_DATA_H

#include <hgraph/util/date_time.h>
#include <hgraph/types/metadata/type_meta_data.h>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <type_traits>

namespace hgraph
{
    struct ValueTypeMetaData;

    /**
     * Eight value-layer kinds, matching the ``Value Kinds`` section of the
     * developer guide. Every ``ValueTypeMetaData`` carries one of these
     * tags; consumers dispatch on it to interpret the rest of the metadata
     * fields.
     */
    enum class ValueTypeKind : uint8_t
    {
        /** Single scalar (integer, floating-point, bool, string, engine time/date). */
        Atomic,
        /** Fixed-arity ordered fields, addressed by index. */
        Tuple,
        /** Named tuple; fields addressed by name with field-name metadata preserved. */
        Bundle,
        /** Ordered sequence of one element type; ``fixed_size`` distinguishes static vs dynamic. */
        List,
        /** Unordered set of unique elements of one type. */
        Set,
        /** Key-value mapping with one key type and one value type. */
        Map,
        /** Fixed-capacity ring buffer of one element type. */
        CyclicBuffer,
        /** FIFO queue with capacity and ordering. */
        Queue,
    };

    /**
     * Capability flags carried on each ``ValueTypeMetaData``.
     *
     * Trivially-* flags drive fast paths (memcpy moves, no-destroy on
     * teardown). The hashable / equatable / comparable bits indicate which
     * ``ValueOps`` hooks the registry should generate for the type.
     * ``BufferCompatible`` marks scalars that can be exposed as a
     * contiguous Arrow-style buffer. ``VariadicTuple`` marks tuple
     * metadata used as a variadic-element list.
     */
    enum class ValueTypeFlags : uint32_t
    {
        None = 0,
        TriviallyConstructible = 1u << 0,
        TriviallyDestructible = 1u << 1,
        TriviallyCopyable = 1u << 2,
        Hashable = 1u << 3,
        Comparable = 1u << 4,
        Equatable = 1u << 5,
        BufferCompatible = 1u << 6,
        VariadicTuple = 1u << 7,
    };

    /** Bitwise OR over ``ValueTypeFlags``. */
    constexpr ValueTypeFlags operator|(ValueTypeFlags lhs, ValueTypeFlags rhs) noexcept
    {
        return static_cast<ValueTypeFlags>(static_cast<uint32_t>(lhs) | static_cast<uint32_t>(rhs));
    }

    /** Bitwise AND over ``ValueTypeFlags``. */
    constexpr ValueTypeFlags operator&(ValueTypeFlags lhs, ValueTypeFlags rhs) noexcept
    {
        return static_cast<ValueTypeFlags>(static_cast<uint32_t>(lhs) & static_cast<uint32_t>(rhs));
    }

    /** Bitwise NOT over ``ValueTypeFlags``. */
    constexpr ValueTypeFlags operator~(ValueTypeFlags value) noexcept
    {
        return static_cast<ValueTypeFlags>(~static_cast<uint32_t>(value));
    }

    /** In-place OR-assign for ``ValueTypeFlags``. */
    constexpr ValueTypeFlags &operator|=(ValueTypeFlags &lhs, ValueTypeFlags rhs) noexcept
    {
        lhs = lhs | rhs;
        return lhs;
    }

    /** True when every bit in ``flag`` is also set in ``flags``. */
    constexpr bool has_flag(ValueTypeFlags flags, ValueTypeFlags flag) noexcept
    {
        return (static_cast<uint32_t>(flags) & static_cast<uint32_t>(flag)) != 0u;
    }

    /**
     * Field descriptor for ``Tuple`` and ``Bundle`` schemas.
     *
     * ``name`` is null for tuples and non-null (registry-interned) for
     * bundles. Layout information (byte offsets within the composite) is
     * not carried here; it lives on the corresponding
     * ``MemoryUtils::CompositeComponent`` of the bound storage plan.
     */
    struct ValueFieldMetaData
    {
        /** Field name; null for tuple fields, registry-interned for bundle fields. */
        const char *name{nullptr};
        /** Field index in declaration order. */
        size_t index{0};
        /** Schema for this field. */
        const ValueTypeMetaData *type{nullptr};
    };

    /**
     * Schema descriptor for value-layer types.
     *
     * Describes what a value *is*: kind, capability flags, and the
     * kind-specific child references (element/key types for collections,
     * field arrays for tuples and bundles, ``fixed_size`` for sized lists
     * and ring buffers). Memory-layout information — byte size, alignment,
     * field offsets, lifecycle hooks — belongs to the matching
     * ``MemoryUtils::StoragePlan`` and is not carried on the schema.
     *
     * Always interned by ``TypeRegistry``: equivalent descriptions resolve
     * to the same pointer.
     */
    struct ValueTypeMetaData final : TypeMetaData
    {
        /** Default-construct an empty atomic-category descriptor. */
        constexpr ValueTypeMetaData() noexcept
            : TypeMetaData(MetaCategory::Value)
        {
        }

        /** Construct a descriptor with kind, flags, and optional name. */
        constexpr ValueTypeMetaData(ValueTypeKind kind_,
                                    ValueTypeFlags flags_,
                                    const char *display_name_ = nullptr) noexcept
            : TypeMetaData(MetaCategory::Value, display_name_)
            , kind(kind_)
            , flags(flags_)
        {
        }

        /** Kind tag; see ``ValueTypeKind``. */
        ValueTypeKind kind{ValueTypeKind::Atomic};
        /** Capability flags. */
        ValueTypeFlags flags{ValueTypeFlags::None};
        /** Element type for ``List``, ``Set``, ``Map`` (value), ``CyclicBuffer``, ``Queue``. */
        const ValueTypeMetaData *element_type{nullptr};
        /** Key type for ``Map``. */
        const ValueTypeMetaData *key_type{nullptr};
        /** Field array for ``Tuple`` and ``Bundle`` (registry-owned, ``field_count`` entries). */
        const ValueFieldMetaData *fields{nullptr};
        /** Number of entries in ``fields``. */
        size_t field_count{0};
        /** Fixed size for static lists, ring buffer capacity for ``CyclicBuffer``, max queue size for ``Queue``. */
        size_t fixed_size{0};

        /** True when ``fixed_size`` is non-zero. Note: this is a semantic property (capacity / staticness), not a layout property. */
        [[nodiscard]] constexpr bool is_fixed_size() const noexcept { return fixed_size > 0; }
        /** True when ``flag`` is set in ``flags``. */
        [[nodiscard]] constexpr bool has(ValueTypeFlags flag) const noexcept { return has_flag(flags, flag); }
        /** True when the underlying C++ type is trivially default constructible. */
        [[nodiscard]] constexpr bool is_trivially_constructible() const noexcept
        {
            return has(ValueTypeFlags::TriviallyConstructible);
        }
        /** True when the underlying C++ type is trivially destructible. */
        [[nodiscard]] constexpr bool is_trivially_destructible() const noexcept
        {
            return has(ValueTypeFlags::TriviallyDestructible);
        }
        /** True when the underlying C++ type is trivially copyable. */
        [[nodiscard]] constexpr bool is_trivially_copyable() const noexcept
        {
            return has(ValueTypeFlags::TriviallyCopyable);
        }
        /** True when ``std::hash`` is available for the underlying C++ type. */
        [[nodiscard]] constexpr bool is_hashable() const noexcept { return has(ValueTypeFlags::Hashable); }
        /** True when the underlying C++ type supports ``operator<``. */
        [[nodiscard]] constexpr bool is_comparable() const noexcept { return has(ValueTypeFlags::Comparable); }
        /** True when the underlying C++ type supports ``operator==``. */
        [[nodiscard]] constexpr bool is_equatable() const noexcept { return has(ValueTypeFlags::Equatable); }
        /** True when the type can be exposed as a contiguous buffer (Arrow-compatible). */
        [[nodiscard]] constexpr bool is_buffer_compatible() const noexcept
        {
            return has(ValueTypeFlags::BufferCompatible);
        }
        /** True when this metadata represents a variadic-element tuple. */
        [[nodiscard]] constexpr bool is_variadic_tuple() const noexcept
        {
            return has(ValueTypeFlags::VariadicTuple);
        }
    };

    namespace detail
    {
        template <typename T>
        concept Hashable = requires(const T &value) {
            { std::hash<T>{}(value) } -> std::convertible_to<size_t>;
        };

        template <typename T>
        concept Equatable = requires(const T &lhs, const T &rhs) {
            { lhs == rhs } -> std::convertible_to<bool>;
        };

        template <typename T>
        concept Comparable = requires(const T &lhs, const T &rhs) {
            { lhs < rhs } -> std::convertible_to<bool>;
        };

        template <typename T>
        constexpr bool buffer_compatible_type =
            std::is_arithmetic_v<T> || std::is_same_v<T, engine_date_t> || std::is_same_v<T, engine_time_t> ||
            std::is_same_v<T, engine_time_delta_t>;
    }  // namespace detail

    /**
     * Compile-time scalar capability inference for ``T``.
     *
     * Combines trivially-* type traits with concept checks for
     * hashability, equatability, and comparability, plus the
     * ``BufferCompatible`` test for arithmetic / engine-time types.
     * Used by ``TypeRegistry::register_scalar<T>`` to derive the flags
     * for an atomic schema.
     */
    template <typename T>
    constexpr ValueTypeFlags compute_scalar_flags() noexcept
    {
        ValueTypeFlags flags = ValueTypeFlags::None;

        if constexpr (std::is_trivially_default_constructible_v<T>)
        {
            flags |= ValueTypeFlags::TriviallyConstructible;
        }
        if constexpr (std::is_trivially_destructible_v<T>)
        {
            flags |= ValueTypeFlags::TriviallyDestructible;
        }
        if constexpr (std::is_trivially_copyable_v<T>)
        {
            flags |= ValueTypeFlags::TriviallyCopyable;
        }
        if constexpr (detail::Hashable<T>)
        {
            flags |= ValueTypeFlags::Hashable;
        }
        if constexpr (detail::Equatable<T>)
        {
            flags |= ValueTypeFlags::Equatable;
        }
        if constexpr (detail::Comparable<T>)
        {
            flags |= ValueTypeFlags::Comparable;
        }
        if constexpr (detail::buffer_compatible_type<T>)
        {
            flags |= ValueTypeFlags::BufferCompatible;
        }

        return flags;
    }
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_TYPE_META_DATA_H

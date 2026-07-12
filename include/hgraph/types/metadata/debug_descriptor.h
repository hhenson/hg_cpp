#ifndef HGRAPH_TYPES_METADATA_DEBUG_DESCRIPTOR_H
#define HGRAPH_TYPES_METADATA_DEBUG_DESCRIPTOR_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/utils/memory_utils.h>

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace hgraph
{
    struct TypeRecord;
    struct ValueTypeMetaData;
    struct DebugDynamicLayout;

    inline constexpr std::uint32_t DEBUG_DESCRIPTOR_MAGIC = 0x48474444u;
    inline constexpr std::uint16_t DEBUG_DESCRIPTOR_ABI_VERSION = 1;

    enum class DebugLayoutKind : std::uint8_t
    {
        Opaque = 0,
        Atomic = 1,
        FixedComposite = 2,
        Sequence = 3,
        KeyedSlots = 4,
        Node = 5,
        Graph = 6,
    };

    enum class DebugAtomicKind : std::uint8_t
    {
        Opaque = 0,
        Boolean = 1,
        SignedInteger = 2,
        UnsignedInteger = 3,
        FloatingPoint = 4,
    };

    enum class DebugDescriptorFlags : std::uint32_t
    {
        None = 0,
        HasValidityBitmap = 1u << 0u,
    };

    enum class DebugFieldFlags : std::uint32_t
    {
        None = 0,
        Optional = 1u << 0u,
    };

    [[nodiscard]] constexpr DebugDescriptorFlags operator|(DebugDescriptorFlags lhs, DebugDescriptorFlags rhs) noexcept
    {
        return static_cast<DebugDescriptorFlags>(static_cast<std::uint32_t>(lhs) | static_cast<std::uint32_t>(rhs));
    }

    [[nodiscard]] constexpr bool has_flag(DebugDescriptorFlags flags, DebugDescriptorFlags requested) noexcept
    {
        return (static_cast<std::uint32_t>(flags) & static_cast<std::uint32_t>(requested)) ==
               static_cast<std::uint32_t>(requested);
    }

    [[nodiscard]] constexpr bool has_flag(DebugFieldFlags flags, DebugFieldFlags requested) noexcept
    {
        return (static_cast<std::uint32_t>(flags) & static_cast<std::uint32_t>(requested)) ==
               static_cast<std::uint32_t>(requested);
    }

    struct DebugField
    {
        const char *name{nullptr};
        std::size_t offset{0};
        const TypeRecord *type{nullptr};
        std::uint32_t validity_bit{0};
        DebugFieldFlags flags{DebugFieldFlags::None};
    };

    struct HGRAPH_EXPORT DebugDescriptor
    {
        std::uint32_t magic{0};
        std::uint16_t abi_version{0};
        DebugLayoutKind layout{DebugLayoutKind::Opaque};
        DebugAtomicKind atomic_kind{DebugAtomicKind::Opaque};
        DebugDescriptorFlags flags{DebugDescriptorFlags::None};
        std::uint32_t field_count{0};
        const DebugField *fields{nullptr};
        std::size_t validity_offset{0};
        std::uint32_t validity_word_size{0};
        std::uint32_t reserved0{0};
        const TypeRecord *key_type{nullptr};
        const TypeRecord *element_type{nullptr};
        const DebugDynamicLayout *dynamic_layout{nullptr};

        [[nodiscard]] bool valid() const noexcept;
    };

    template <typename T> [[nodiscard]] consteval DebugAtomicKind debug_atomic_kind_for() noexcept
    {
        using Value = std::remove_cv_t<T>;
        if constexpr (std::is_same_v<Value, bool>)
            return DebugAtomicKind::Boolean;
        else if constexpr (std::is_integral_v<Value> && std::is_signed_v<Value>)
            return DebugAtomicKind::SignedInteger;
        else if constexpr (std::is_integral_v<Value> && std::is_unsigned_v<Value>)
            return DebugAtomicKind::UnsignedInteger;
        else if constexpr (std::is_floating_point_v<Value>)
            return DebugAtomicKind::FloatingPoint;
        else
            return DebugAtomicKind::Opaque;
    }

    [[nodiscard]] HGRAPH_EXPORT const DebugDescriptor &intern_atomic_debug_descriptor(
        const ValueTypeMetaData &schema, const MemoryUtils::StoragePlan &plan, DebugAtomicKind atomic_kind);

    [[nodiscard]] HGRAPH_EXPORT const DebugDescriptor &intern_fixed_composite_debug_descriptor(
        const ValueTypeMetaData &schema, const MemoryUtils::StoragePlan &plan);

    [[nodiscard]] HGRAPH_EXPORT const DebugDescriptor *find_value_debug_descriptor(
        const ValueTypeMetaData &schema, const MemoryUtils::StoragePlan &plan) noexcept;

    HGRAPH_EXPORT void clear_value_debug_descriptors() noexcept;

    static_assert(sizeof(DebugField) == 4 * sizeof(void *));
    static_assert(alignof(DebugField) == alignof(void *));
    static_assert(std::is_standard_layout_v<DebugField>);
    static_assert(std::is_trivially_copyable_v<DebugField>);
    static_assert(offsetof(DebugField, name) == 0);
    static_assert(offsetof(DebugField, offset) == sizeof(void *));
    static_assert(offsetof(DebugField, type) == 2 * sizeof(void *));
    static_assert(offsetof(DebugField, validity_bit) == 3 * sizeof(void *));

    static_assert(sizeof(DebugDescriptor) == 8 * sizeof(void *));
    static_assert(alignof(DebugDescriptor) == alignof(void *));
    static_assert(std::is_standard_layout_v<DebugDescriptor>);
    static_assert(std::is_trivially_copyable_v<DebugDescriptor>);
    static_assert(offsetof(DebugDescriptor, magic) == 0);
    static_assert(offsetof(DebugDescriptor, fields) == 2 * sizeof(void *));
    static_assert(offsetof(DebugDescriptor, validity_offset) == 3 * sizeof(void *));
    static_assert(offsetof(DebugDescriptor, key_type) == 5 * sizeof(void *));
    static_assert(offsetof(DebugDescriptor, dynamic_layout) == 7 * sizeof(void *));
} // namespace hgraph

#endif // HGRAPH_TYPES_METADATA_DEBUG_DESCRIPTOR_H

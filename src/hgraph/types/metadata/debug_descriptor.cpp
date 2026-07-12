#include <hgraph/types/metadata/debug_descriptor.h>

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/value/value_type_ref.h>

#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        inline constexpr std::uint32_t KNOWN_DESCRIPTOR_FLAGS = 1u;
        inline constexpr std::uint32_t KNOWN_FIELD_FLAGS = 1u;
        inline constexpr std::uint32_t VALIDITY_WORD_SIZE = sizeof(std::uint64_t);

        struct DescriptorKey
        {
            const ValueTypeMetaData *schema{nullptr};
            const MemoryUtils::StoragePlan *plan{nullptr};

            [[nodiscard]] bool operator==(const DescriptorKey &) const noexcept = default;
        };

        struct DescriptorKeyHash
        {
            [[nodiscard]] std::size_t operator()(const DescriptorKey &key) const noexcept
            {
                const auto schema_hash = std::hash<const ValueTypeMetaData *>{}(key.schema);
                const auto plan_hash = std::hash<const MemoryUtils::StoragePlan *>{}(key.plan);
                return schema_hash ^ (plan_hash + 0x9e3779b9u + (schema_hash << 6u) + (schema_hash >> 2u));
            }
        };

        struct DescriptorEntry
        {
            std::vector<DebugField> fields{};
            DebugDescriptor descriptor{};

            void bind_fields() noexcept
            {
                descriptor.fields = fields.empty() ? nullptr : fields.data();
                descriptor.field_count = static_cast<std::uint32_t>(fields.size());
            }
        };

        class ValueDebugDescriptorRegistry
        {
          public:
            [[nodiscard]] const DebugDescriptor &intern_atomic(const ValueTypeMetaData &schema,
                                                               const MemoryUtils::StoragePlan &plan,
                                                               DebugAtomicKind atomic_kind)
            {
                if (schema.value_kind() != ValueTypeKind::Atomic)
                    throw std::invalid_argument("atomic debug descriptor requires an atomic value schema");

                auto entry = std::make_unique<DescriptorEntry>();
                entry->descriptor = DebugDescriptor{
                    .magic = DEBUG_DESCRIPTOR_MAGIC,
                    .abi_version = DEBUG_DESCRIPTOR_ABI_VERSION,
                    .layout = DebugLayoutKind::Atomic,
                    .atomic_kind = atomic_kind,
                };
                return intern(DescriptorKey{&schema, &plan}, std::move(entry));
            }

            [[nodiscard]] const DebugDescriptor &intern_fixed_composite(const ValueTypeMetaData &schema,
                                                                        const MemoryUtils::StoragePlan &plan)
            {
                if (schema.value_kind() != ValueTypeKind::Tuple && schema.value_kind() != ValueTypeKind::Bundle)
                    throw std::invalid_argument("fixed-composite debug descriptor requires tuple or bundle schema");
                if (schema.field_count > std::numeric_limits<std::uint32_t>::max())
                    throw std::length_error("fixed-composite debug descriptor exceeds the field-count ABI");
                if (!plan.is_composite() ||
                    plan.component_count() != schema.field_count + (schema.field_count == 0 ? 0 : 1))
                    throw std::invalid_argument("fixed-composite debug descriptor requires matching public fields "
                                                "and validity storage");

                auto entry = std::make_unique<DescriptorEntry>();
                entry->fields.reserve(schema.field_count);
                const auto components = plan.components();
                for (std::size_t index = 0; index < schema.field_count; ++index)
                {
                    const ValueTypeRef child = ValuePlanFactory::instance().type_for(schema.fields[index].type);
                    if (!child.valid())
                        throw std::logic_error("fixed-composite debug descriptor child has no "
                                               "canonical type record");
                    entry->fields.push_back(DebugField{
                        .name = schema.fields[index].name,
                        .offset = components[index].offset,
                        .type = child.record(),
                        .validity_bit = static_cast<std::uint32_t>(index),
                        .flags = DebugFieldFlags::Optional,
                    });
                }

                entry->descriptor = DebugDescriptor{
                    .magic = DEBUG_DESCRIPTOR_MAGIC,
                    .abi_version = DEBUG_DESCRIPTOR_ABI_VERSION,
                    .layout = DebugLayoutKind::FixedComposite,
                    .atomic_kind = DebugAtomicKind::Opaque,
                    .flags =
                        schema.field_count == 0 ? DebugDescriptorFlags::None : DebugDescriptorFlags::HasValidityBitmap,
                    .validity_offset = schema.field_count == 0 ? 0 : components[schema.field_count].offset,
                    .validity_word_size = schema.field_count == 0 ? 0 : VALIDITY_WORD_SIZE,
                };
                entry->bind_fields();
                return intern(DescriptorKey{&schema, &plan}, std::move(entry));
            }

            [[nodiscard]] const DebugDescriptor *find(const ValueTypeMetaData &schema,
                                                      const MemoryUtils::StoragePlan &plan) const noexcept
            {
                const std::lock_guard lock(mutex_);
                const auto found = entries_.find(DescriptorKey{&schema, &plan});
                return found == entries_.end() ? nullptr : &found->second->descriptor;
            }

            void clear() noexcept
            {
                const std::lock_guard lock(mutex_);
                entries_.clear();
            }

          private:
            [[nodiscard]] const DebugDescriptor &intern(DescriptorKey key, std::unique_ptr<DescriptorEntry> candidate)
            {
                if (!candidate->descriptor.valid())
                    throw std::invalid_argument("cannot intern an invalid debug descriptor");

                const std::lock_guard lock(mutex_);
                if (const auto found = entries_.find(key); found != entries_.end())
                {
                    const auto &existing = found->second->descriptor;
                    if (existing.layout != candidate->descriptor.layout ||
                        existing.atomic_kind != candidate->descriptor.atomic_kind)
                    {
                        throw std::logic_error("debug descriptor conflicts with the canonical "
                                               "schema/plan descriptor");
                    }
                    return existing;
                }
                const DebugDescriptor *result = &candidate->descriptor;
                entries_.emplace(key, std::move(candidate));
                return *result;
            }

            mutable std::mutex mutex_{};
            std::unordered_map<DescriptorKey, std::unique_ptr<DescriptorEntry>, DescriptorKeyHash> entries_{};
        };

        [[nodiscard]] ValueDebugDescriptorRegistry &descriptor_registry()
        {
            static auto *registry = new ValueDebugDescriptorRegistry();
            return *registry;
        }
    } // namespace

    bool DebugDescriptor::valid() const noexcept
    {
        const auto layout_value = static_cast<std::uint8_t>(layout);
        const auto atomic_value = static_cast<std::uint8_t>(atomic_kind);
        if (magic != DEBUG_DESCRIPTOR_MAGIC || abi_version != DEBUG_DESCRIPTOR_ABI_VERSION ||
            layout_value > static_cast<std::uint8_t>(DebugLayoutKind::Graph) ||
            atomic_value > static_cast<std::uint8_t>(DebugAtomicKind::FloatingPoint) || reserved0 != 0 ||
            (static_cast<std::uint32_t>(flags) & ~KNOWN_DESCRIPTOR_FLAGS) != 0 ||
            (field_count == 0) != (fields == nullptr))
        {
            return false;
        }
        if (has_flag(flags, DebugDescriptorFlags::HasValidityBitmap))
        {
            if (layout != DebugLayoutKind::FixedComposite || validity_word_size != VALIDITY_WORD_SIZE)
                return false;
        }
        else if (validity_offset != 0 || validity_word_size != 0)
        {
            return false;
        }
        for (std::uint32_t index = 0; index < field_count; ++index)
        {
            const DebugField &field = fields[index];
            if (field.type == nullptr || (static_cast<std::uint32_t>(field.flags) & ~KNOWN_FIELD_FLAGS) != 0)
                return false;
        }
        return true;
    }

    const DebugDescriptor &intern_atomic_debug_descriptor(const ValueTypeMetaData &schema,
                                                          const MemoryUtils::StoragePlan &plan,
                                                          DebugAtomicKind atomic_kind)
    {
        return descriptor_registry().intern_atomic(schema, plan, atomic_kind);
    }

    const DebugDescriptor &intern_fixed_composite_debug_descriptor(const ValueTypeMetaData &schema,
                                                                   const MemoryUtils::StoragePlan &plan)
    {
        return descriptor_registry().intern_fixed_composite(schema, plan);
    }

    const DebugDescriptor *find_value_debug_descriptor(const ValueTypeMetaData &schema,
                                                       const MemoryUtils::StoragePlan &plan) noexcept
    {
        return descriptor_registry().find(schema, plan);
    }

    void clear_value_debug_descriptors() noexcept
    {
        descriptor_registry().clear();
    }
} // namespace hgraph

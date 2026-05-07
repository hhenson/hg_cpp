#include <hgraph/types/metadata/value_plan_factory.h>

#include <hgraph/types/utils/intern_table.h>
#include <hgraph/types/value/compact_container_ops.h>

#include <compare>
#include <cstddef>
#include <fmt/format.h>
#include <iterator>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        struct CompositeIndexedContext
        {
            const ValueTypeMetaData              *schema{nullptr};
            std::vector<const ValueTypeBinding *> child_bindings{};
            std::vector<std::size_t>              offsets{};
        };

        struct ArrayIndexedContext
        {
            const ValueTypeMetaData *schema{nullptr};
            const ValueTypeBinding *element_binding{nullptr};
            std::size_t             size{0};
            std::size_t             stride{0};
        };

        [[nodiscard]] std::size_t combine_hash(std::size_t seed, std::size_t value) noexcept
        {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            return seed;
        }

        [[nodiscard]] std::size_t composite_indexed_size(const void *context, const void *) noexcept
        {
            return static_cast<const CompositeIndexedContext *>(context)->child_bindings.size();
        }

        [[nodiscard]] const void *composite_indexed_element_at(const void *context, const void *memory,
                                                               std::size_t index)
        {
            const auto *state = static_cast<const CompositeIndexedContext *>(context);
            return static_cast<const std::byte *>(memory) + state->offsets[index];
        }

        [[nodiscard]] const ValueTypeBinding *composite_indexed_element_binding(const void *context, const void *,
                                                                                std::size_t index) noexcept
        {
            return static_cast<const CompositeIndexedContext *>(context)->child_bindings[index];
        }

        [[nodiscard]] std::size_t composite_value_hash(const void *context, const void *memory) noexcept
        {
            if (memory == nullptr) { return 0; }
            try
            {
                const auto *state = static_cast<const CompositeIndexedContext *>(context);
                std::size_t seed  = 0;
                for (std::size_t index = 0; index < state->child_bindings.size(); ++index)
                {
                    const auto &ops = state->child_bindings[index]->checked_ops();
                    const auto *child = static_cast<const std::byte *>(memory) + state->offsets[index];
                    seed = combine_hash(seed, ops.hash(child));
                }
                return seed;
            }
            catch (...)
            {
                return 0;
            }
        }

        [[nodiscard]] bool composite_value_equals(const void *context, const void *lhs, const void *rhs) noexcept
        {
            if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
            try
            {
                const auto *state = static_cast<const CompositeIndexedContext *>(context);
                for (std::size_t index = 0; index < state->child_bindings.size(); ++index)
                {
                    const auto &ops = state->child_bindings[index]->checked_ops();
                    const auto *a = static_cast<const std::byte *>(lhs) + state->offsets[index];
                    const auto *b = static_cast<const std::byte *>(rhs) + state->offsets[index];
                    if (!ops.equals(a, b)) { return false; }
                }
                return true;
            }
            catch (...)
            {
                return false;
            }
        }

        [[nodiscard]] std::partial_ordering composite_value_compare(const void *context, const void *lhs,
                                                                    const void *rhs) noexcept
        {
            if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }
            try
            {
                const auto *state = static_cast<const CompositeIndexedContext *>(context);
                for (std::size_t index = 0; index < state->child_bindings.size(); ++index)
                {
                    const auto &ops = state->child_bindings[index]->checked_ops();
                    const auto *a = static_cast<const std::byte *>(lhs) + state->offsets[index];
                    const auto *b = static_cast<const std::byte *>(rhs) + state->offsets[index];
                    const auto  c = ops.compare(a, b);
                    if (c != 0) { return c; }
                }
                return std::partial_ordering::equivalent;
            }
            catch (...)
            {
                return std::partial_ordering::unordered;
            }
        }

        [[nodiscard]] std::string composite_value_to_string(const void *context, const void *memory)
        {
            if (memory == nullptr) { return {}; }
            const auto *state = static_cast<const CompositeIndexedContext *>(context);
            const bool  bundle = state->schema != nullptr && state->schema->kind == ValueTypeKind::Bundle;
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{}", bundle ? '{' : '(');
            for (std::size_t index = 0; index < state->child_bindings.size(); ++index)
            {
                if (index > 0) { fmt::format_to(std::back_inserter(out), ", "); }
                if (bundle)
                {
                    const char *name = state->schema->fields[index].name;
                    fmt::format_to(std::back_inserter(out), "{}: ", name != nullptr ? name : "");
                }
                const auto &ops = state->child_bindings[index]->checked_ops();
                const auto *child = static_cast<const std::byte *>(memory) + state->offsets[index];
                fmt::format_to(std::back_inserter(out), "{}", ops.to_string(child));
            }
            fmt::format_to(std::back_inserter(out), "{}", bundle ? '}' : ')');
            return fmt::to_string(out);
        }

        [[nodiscard]] std::size_t array_indexed_size(const void *context, const void *) noexcept
        {
            return static_cast<const ArrayIndexedContext *>(context)->size;
        }

        [[nodiscard]] const void *array_indexed_element_at(const void *context, const void *memory, std::size_t index)
        {
            const auto *state = static_cast<const ArrayIndexedContext *>(context);
            return static_cast<const std::byte *>(memory) + index * state->stride;
        }

        [[nodiscard]] const ValueTypeBinding *array_indexed_element_binding(const void *context, const void *,
                                                                            std::size_t) noexcept
        {
            return static_cast<const ArrayIndexedContext *>(context)->element_binding;
        }

        [[nodiscard]] std::size_t array_value_hash(const void *context, const void *memory) noexcept
        {
            if (memory == nullptr) { return 0; }
            try
            {
                const auto *state = static_cast<const ArrayIndexedContext *>(context);
                const auto &ops   = state->element_binding->checked_ops();
                std::size_t seed  = 0;
                for (std::size_t index = 0; index < state->size; ++index)
                {
                    const auto *child = static_cast<const std::byte *>(memory) + index * state->stride;
                    seed = combine_hash(seed, ops.hash(child));
                }
                return seed;
            }
            catch (...)
            {
                return 0;
            }
        }

        [[nodiscard]] bool array_value_equals(const void *context, const void *lhs, const void *rhs) noexcept
        {
            if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
            try
            {
                const auto *state = static_cast<const ArrayIndexedContext *>(context);
                const auto &ops   = state->element_binding->checked_ops();
                for (std::size_t index = 0; index < state->size; ++index)
                {
                    const auto *a = static_cast<const std::byte *>(lhs) + index * state->stride;
                    const auto *b = static_cast<const std::byte *>(rhs) + index * state->stride;
                    if (!ops.equals(a, b)) { return false; }
                }
                return true;
            }
            catch (...)
            {
                return false;
            }
        }

        [[nodiscard]] std::partial_ordering array_value_compare(const void *context, const void *lhs,
                                                                const void *rhs) noexcept
        {
            if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }
            try
            {
                const auto *state = static_cast<const ArrayIndexedContext *>(context);
                const auto &ops   = state->element_binding->checked_ops();
                for (std::size_t index = 0; index < state->size; ++index)
                {
                    const auto *a = static_cast<const std::byte *>(lhs) + index * state->stride;
                    const auto *b = static_cast<const std::byte *>(rhs) + index * state->stride;
                    const auto  c = ops.compare(a, b);
                    if (c != 0) { return c; }
                }
                return std::partial_ordering::equivalent;
            }
            catch (...)
            {
                return std::partial_ordering::unordered;
            }
        }

        [[nodiscard]] std::string array_value_to_string(const void *context, const void *memory)
        {
            if (memory == nullptr) { return "[]"; }
            const auto *state = static_cast<const ArrayIndexedContext *>(context);
            const auto &ops   = state->element_binding->checked_ops();
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "[");
            for (std::size_t index = 0; index < state->size; ++index)
            {
                if (index > 0) { fmt::format_to(std::back_inserter(out), ", "); }
                const auto *child = static_cast<const std::byte *>(memory) + index * state->stride;
                fmt::format_to(std::back_inserter(out), "{}", ops.to_string(child));
            }
            fmt::format_to(std::back_inserter(out), "]");
            return fmt::to_string(out);
        }

        struct CompositeIndexedOpsEntry
        {
            CompositeIndexedContext context{};
            IndexedValueOps         ops{};

            CompositeIndexedOpsEntry(const CompositeIndexedOpsEntry &other)
                : context{other.context}, ops{other.ops}
            {
                ops.context = &context;
            }

            CompositeIndexedOpsEntry(CompositeIndexedOpsEntry &&other) noexcept
                : context{std::move(other.context)}, ops{other.ops}
            {
                ops.context = &context;
            }

            CompositeIndexedOpsEntry(const ValueTypeMetaData &schema, const MemoryUtils::StoragePlan &plan)
            {
                if (!plan.is_composite() || plan.component_count() != schema.field_count)
                {
                    throw std::logic_error("ValuePlanFactory: composite indexed ops require matching composite plan");
                }

                context.child_bindings.reserve(schema.field_count);
                context.offsets.reserve(schema.field_count);
                const auto components = plan.components();
                for (std::size_t index = 0; index < schema.field_count; ++index)
                {
                    const auto *child_binding =
                        ValuePlanFactory::instance().binding_for(schema.fields[index].type);
                    if (child_binding == nullptr)
                    {
                        throw std::logic_error("ValuePlanFactory: composite field has no resolvable indexed binding");
                    }
                    context.child_bindings.push_back(child_binding);
                    context.offsets.push_back(components[index].offset);
                }

                context.schema = &schema;

                ops = IndexedValueOps{
                    {&context,
                     &composite_value_hash,
                     &composite_value_equals,
                     &composite_value_compare,
                     &composite_value_to_string},
                    &composite_indexed_size,
                    &composite_indexed_element_at,
                    &composite_indexed_element_binding,
                    nullptr,
                };
            }
        };

        struct ArrayIndexedOpsEntry
        {
            ArrayIndexedContext context{};
            IndexedValueOps     ops{};

            ArrayIndexedOpsEntry(const ArrayIndexedOpsEntry &other)
                : context{other.context}, ops{other.ops}
            {
                ops.context = &context;
            }

            ArrayIndexedOpsEntry(ArrayIndexedOpsEntry &&other) noexcept
                : context{other.context}, ops{other.ops}
            {
                ops.context = &context;
            }

            ArrayIndexedOpsEntry(const ValueTypeMetaData &schema, const MemoryUtils::StoragePlan &plan)
            {
                if (!plan.is_array())
                {
                    throw std::logic_error("ValuePlanFactory: array indexed ops require an array plan");
                }

                const auto *element_binding = ValuePlanFactory::instance().binding_for(schema.element_type);
                if (element_binding == nullptr)
                {
                    throw std::logic_error("ValuePlanFactory: fixed list element has no resolvable binding");
                }

                context = ArrayIndexedContext{
                    .schema          = &schema,
                    .element_binding = element_binding,
                    .size            = plan.array_count(),
                    .stride          = plan.array_stride(),
                };

                ops = IndexedValueOps{
                    {&context, &array_value_hash, &array_value_equals, &array_value_compare, &array_value_to_string},
                    &array_indexed_size,
                    &array_indexed_element_at,
                    &array_indexed_element_binding,
                    nullptr,
                };
            }
        };

        [[nodiscard]] InternTable<const ValueTypeMetaData *, CompositeIndexedOpsEntry> &
        composite_indexed_ops_cache() noexcept
        {
            static InternTable<const ValueTypeMetaData *, CompositeIndexedOpsEntry> cache;
            return cache;
        }

        [[nodiscard]] InternTable<const ValueTypeMetaData *, ArrayIndexedOpsEntry> &array_indexed_ops_cache() noexcept
        {
            static InternTable<const ValueTypeMetaData *, ArrayIndexedOpsEntry> cache;
            return cache;
        }

        [[nodiscard]] const IndexedValueOps &composite_indexed_ops(const ValueTypeMetaData &schema,
                                                                   const MemoryUtils::StoragePlan &plan)
        {
            return composite_indexed_ops_cache().emplace(&schema, schema, plan).ops;
        }

        [[nodiscard]] const IndexedValueOps &array_indexed_ops(const ValueTypeMetaData &schema,
                                                               const MemoryUtils::StoragePlan &plan)
        {
            return array_indexed_ops_cache().emplace(&schema, schema, plan).ops;
        }

        void clear_structured_indexed_ops() noexcept
        {
            composite_indexed_ops_cache().clear();
            array_indexed_ops_cache().clear();
        }
    }  // namespace

    ValuePlanFactory &ValuePlanFactory::instance()
    {
        static ValuePlanFactory factory;
        return factory;
    }

    void ValuePlanFactory::register_atomic(const ValueTypeMetaData *schema, const MemoryUtils::StoragePlan *plan)
    {
        if (schema == nullptr || plan == nullptr) { return; }

        std::lock_guard<std::mutex> lock(mutex_);
        if (const auto it = cache_.find(schema); it != cache_.end())
        {
            if (it->second == plan) { return; }
            throw std::logic_error("ValuePlanFactory: atomic schema already registered with a different plan");
        }
        cache_.emplace(schema, plan);
    }

    void ValuePlanFactory::register_binding(const ValueTypeBinding &binding)
    {
        if (!binding.valid()) { return; }

        std::lock_guard<std::mutex> lock(mutex_);
        if (const auto it = binding_cache_.find(binding.type_meta); it != binding_cache_.end())
        {
            if (it->second == &binding) { return; }
            throw std::logic_error("ValuePlanFactory: schema already registered with a different binding");
        }

        if (const auto it = cache_.find(binding.type_meta); it != cache_.end())
        {
            if (it->second != binding.plan())
            {
                throw std::logic_error("ValuePlanFactory: schema already registered with a different plan");
            }
        }
        else
        {
            cache_.emplace(binding.type_meta, binding.plan());
        }

        binding_cache_.emplace(binding.type_meta, &binding);
    }

    const MemoryUtils::StoragePlan *ValuePlanFactory::plan_for(const ValueTypeMetaData *schema)
    {
        if (schema == nullptr) { return nullptr; }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = cache_.find(schema); it != cache_.end()) { return it->second; }
        }

        return synthesise(schema);
    }

    const MemoryUtils::StoragePlan *ValuePlanFactory::find(const ValueTypeMetaData *schema) const
    {
        if (schema == nullptr) { return nullptr; }

        std::lock_guard<std::mutex> lock(mutex_);
        const auto                  it = cache_.find(schema);
        return it == cache_.end() ? nullptr : it->second;
    }

    const ValueTypeBinding *ValuePlanFactory::binding_for(const ValueTypeMetaData *schema)
    {
        if (schema == nullptr) { return nullptr; }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = binding_cache_.find(schema); it != binding_cache_.end()) { return it->second; }
        }

        return synthesise_binding(schema);
    }

    const ValueTypeBinding *ValuePlanFactory::find_binding(const ValueTypeMetaData *schema) const
    {
        if (schema == nullptr) { return nullptr; }

        std::lock_guard<std::mutex> lock(mutex_);
        const auto                  it = binding_cache_.find(schema);
        return it == binding_cache_.end() ? nullptr : it->second;
    }

    void ValuePlanFactory::reset() noexcept
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cache_.clear();
        binding_cache_.clear();
        clear_structured_indexed_ops();
    }

    const MemoryUtils::StoragePlan *ValuePlanFactory::synthesise(const ValueTypeMetaData *schema)
    {
        const MemoryUtils::StoragePlan *plan = nullptr;

        switch (schema->kind)
        {
            case ValueTypeKind::Atomic:
                throw std::logic_error(
                    "ValuePlanFactory: atomic schema has no canonical plan; register it via register_atomic "
                    "(typically through TypeRegistry::register_scalar<T>)");

            case ValueTypeKind::Tuple:
            {
                auto builder = MemoryUtils::tuple();
                builder.reserve(schema->field_count);
                for (size_t index = 0; index < schema->field_count; ++index)
                {
                    const ValueTypeMetaData *field_type = schema->fields[index].type;
                    const MemoryUtils::StoragePlan *field_plan = plan_for(field_type);
                    if (field_plan == nullptr)
                    {
                        throw std::logic_error("ValuePlanFactory: tuple field has no resolvable plan");
                    }
                    builder.add_plan(*field_plan);
                }
                plan = &builder.build();
                break;
            }

            case ValueTypeKind::Bundle:
            {
                auto builder = MemoryUtils::named_tuple();
                builder.reserve(schema->field_count);
                for (size_t index = 0; index < schema->field_count; ++index)
                {
                    const ValueFieldMetaData &field = schema->fields[index];
                    const MemoryUtils::StoragePlan *field_plan = plan_for(field.type);
                    if (field_plan == nullptr)
                    {
                        throw std::logic_error("ValuePlanFactory: bundle field has no resolvable plan");
                    }
                    builder.add_field(field.name != nullptr ? field.name : "", *field_plan);
                }
                plan = &builder.build();
                break;
            }

            case ValueTypeKind::List:
            {
                if (schema->fixed_size == 0)
                {
                    const ValueTypeBinding *binding = binding_for(schema);
                    plan = binding != nullptr ? binding->plan() : nullptr;
                    break;
                }
                const MemoryUtils::StoragePlan *element_plan = plan_for(schema->element_type);
                if (element_plan == nullptr)
                {
                    throw std::logic_error("ValuePlanFactory: fixed list has no element plan");
                }
                plan = &MemoryUtils::array_plan(*element_plan, schema->fixed_size);
                break;
            }

            case ValueTypeKind::Set:
            case ValueTypeKind::Map:
            case ValueTypeKind::CyclicBuffer:
            case ValueTypeKind::Queue:
            {
                const ValueTypeBinding *binding = binding_for(schema);
                plan = binding != nullptr ? binding->plan() : nullptr;
                break;
            }
        }

        if (plan == nullptr)
        {
            throw std::logic_error("ValuePlanFactory: unhandled ValueTypeKind");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        const auto                  it = cache_.find(schema);
        if (it != cache_.end()) { return it->second; }
        cache_.emplace(schema, plan);
        return plan;
    }

    const ValueTypeBinding *ValuePlanFactory::synthesise_binding(const ValueTypeMetaData *schema)
    {
        const ValueTypeBinding *binding = nullptr;

        switch (schema->kind)
        {
            case ValueTypeKind::Atomic:
                throw std::logic_error(
                    "ValuePlanFactory: atomic schema has no canonical binding; register it via TypeRegistry::register_scalar<T>");

            case ValueTypeKind::Tuple:
            case ValueTypeKind::Bundle:
            {
                for (size_t index = 0; index < schema->field_count; ++index)
                {
                    if (binding_for(schema->fields[index].type) == nullptr)
                    {
                        throw std::logic_error("ValuePlanFactory: composite field has no resolvable binding");
                    }
                }
                const MemoryUtils::StoragePlan *plan = plan_for(schema);
                if (plan == nullptr) { throw std::logic_error("ValuePlanFactory: composite has no resolvable plan"); }
                binding = &ValueTypeBinding::intern(*schema, *plan, composite_indexed_ops(*schema, *plan));
                break;
            }

            case ValueTypeKind::List:
            {
                const ValueTypeBinding *element_binding = binding_for(schema->element_type);
                if (element_binding == nullptr)
                {
                    throw std::logic_error("ValuePlanFactory: list element has no resolvable binding");
                }
                if (schema->fixed_size == 0)
                {
                    binding = &compact_list_binding(*element_binding);
                }
                else
                {
                    const MemoryUtils::StoragePlan *plan = plan_for(schema);
                    if (plan == nullptr) { throw std::logic_error("ValuePlanFactory: fixed list has no resolvable plan"); }
                    binding = &ValueTypeBinding::intern(*schema, *plan, array_indexed_ops(*schema, *plan));
                }
                break;
            }

            case ValueTypeKind::Set:
            {
                const ValueTypeBinding *element_binding = binding_for(schema->element_type);
                if (element_binding == nullptr)
                {
                    throw std::logic_error("ValuePlanFactory: set element has no resolvable binding");
                }
                binding = &compact_set_binding(*element_binding);
                break;
            }

            case ValueTypeKind::Map:
            {
                const ValueTypeBinding *key_binding = binding_for(schema->key_type);
                const ValueTypeBinding *value_binding = binding_for(schema->element_type);
                if (key_binding == nullptr || value_binding == nullptr)
                {
                    throw std::logic_error("ValuePlanFactory: map key/value has no resolvable binding");
                }
                binding = &compact_map_binding(*key_binding, *value_binding);
                break;
            }

            case ValueTypeKind::CyclicBuffer:
            {
                const ValueTypeBinding *element_binding = binding_for(schema->element_type);
                if (element_binding == nullptr)
                {
                    throw std::logic_error("ValuePlanFactory: cyclic buffer element has no resolvable binding");
                }
                binding = &compact_cyclic_buffer_binding(*element_binding, schema->fixed_size);
                break;
            }

            case ValueTypeKind::Queue:
            {
                const ValueTypeBinding *element_binding = binding_for(schema->element_type);
                if (element_binding == nullptr)
                {
                    throw std::logic_error("ValuePlanFactory: queue element has no resolvable binding");
                }
                binding = &compact_queue_binding(*element_binding, schema->fixed_size);
                break;
            }
        }

        if (binding == nullptr)
        {
            throw std::logic_error("ValuePlanFactory: unhandled ValueTypeKind while synthesising binding");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        const auto                  it = binding_cache_.find(schema);
        if (it != binding_cache_.end()) { return it->second; }

        if (const auto plan_it = cache_.find(schema); plan_it != cache_.end())
        {
            if (plan_it->second != binding->plan())
            {
                throw std::logic_error("ValuePlanFactory: synthesised binding does not match cached plan");
            }
        }
        else
        {
            cache_.emplace(schema, binding->plan());
        }

        binding_cache_.emplace(schema, binding);
        return binding;
    }
}  // namespace hgraph

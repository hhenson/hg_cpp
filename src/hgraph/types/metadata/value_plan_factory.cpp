#include <hgraph/types/metadata/value_plan_factory.h>

#include <stdexcept>

namespace hgraph
{
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
                    throw std::logic_error(
                        "ValuePlanFactory: dynamic list plans require value-layer support (not yet ported)");
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
                throw std::logic_error(
                    "ValuePlanFactory: container plans require value-layer support (not yet ported)");
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
}  // namespace hgraph

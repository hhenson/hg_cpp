#include <hgraph/types/time_series/ts_type_ref.h>

#include <hgraph/types/metadata/type_record_registry.h>
#include <hgraph/types/time_series/ts_data/ops.h>

#include <stdexcept>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] bool scalar_root_kind(TSTypeKind kind) noexcept
        {
            return kind == TSTypeKind::TS || kind == TSTypeKind::SIGNAL;
        }

        void validate_ts_record(const TypeRecord &record)
        {
            if (!record.valid() || record.schema->family != TypeFamily::TimeSeries ||
                (record.role != TypeRole::Data && record.role != TypeRole::Input && record.role != TypeRole::Output))
            {
                throw std::invalid_argument("TSRoleTypeRef requires a TimeSeries Data/Input/Output TypeRecord");
            }
            const auto *schema = reinterpret_cast<const TSValueTypeMetaData *>(record.schema);
            if (record.schema->kind != static_cast<TypeKind>(schema->kind))
            {
                throw std::invalid_argument("TSRoleTypeRef requires matching common and time-series schema kinds");
            }
            if (record.ops_abi_version != TS_DATA_OPS_ABI_VERSION)
            {
                throw std::invalid_argument("TSRoleTypeRef requires TSData ops ABI version 1");
            }
            const auto *ops = static_cast<const TSDataOps *>(record.ops);
            if (!scalar_root_kind(schema->kind) || ops == nullptr || ops->kind != schema->kind)
            {
                throw std::invalid_argument("TSRoleTypeRef requires matching scalar TSData ops");
            }
            if (record.capabilities != ts_type_capabilities(record.role, *record.plan, *ops))
            {
                throw std::invalid_argument("TSRoleTypeRef capabilities do not match its role, plan, and ops");
            }
        }
    }

    TypeCapabilities ts_type_capabilities(TypeRole role,
                                          const MemoryUtils::StoragePlan &plan,
                                          const TSDataOps &ops)
    {
        if (role != TypeRole::Data && role != TypeRole::Input && role != TypeRole::Output)
        {
            throw std::invalid_argument("time-series type capabilities require Data, Input, or Output role");
        }
        TypeCapabilities result = TypeCapabilities::Viewable;
        if (plan.can_default_construct()) result |= TypeCapabilities::Constructible;
        if (plan.trivially_destructible || plan.lifecycle.can_destroy()) result |= TypeCapabilities::Destructible;
        if (plan.can_copy_construct()) result |= TypeCapabilities::Copyable;
        if (plan.can_move_construct()) result |= TypeCapabilities::Movable;
        if ((role == TypeRole::Data || role == TypeRole::Output) && ops.allows_mutation)
            result |= TypeCapabilities::Mutable;
        return result;
    }

    TSRoleTypeRef intern_ts_type(const TSValueTypeMetaData &schema,
                                 TypeRole role,
                                 const MemoryUtils::StoragePlan &plan,
                                 const TSDataOps &ops)
    {
        if (!schema.header.valid() || schema.header.family != TypeFamily::TimeSeries ||
            schema.header.kind != static_cast<TypeKind>(schema.kind) ||
            !scalar_root_kind(schema.kind))
        {
            throw std::invalid_argument("intern_ts_type supports only canonical TS and SIGNAL schemas");
        }
        if (ops.kind != schema.kind)
        {
            throw std::invalid_argument("intern_ts_type requires ops kind matching the schema");
        }
        const TypeRecordDefinition definition{
            .key = TypeRecordKey{.schema = &schema.header, .role = role, .plan = &plan, .ops = &ops, .debug = nullptr},
            .ops_abi_version = TS_DATA_OPS_ABI_VERSION,
            .capabilities = ts_type_capabilities(role, plan, ops),
            .implementation_label = {},
        };
        return TSRoleTypeRef{&TypeRecordRegistry::instance().intern(definition)};
    }

    TSRoleTypeRef TSRoleTypeRef::checked(AnyPtr pointer)
    {
        if (pointer.is_unbound()) return {};
        if (!pointer.well_formed() || pointer.record() == nullptr)
            throw std::invalid_argument("TSRoleTypeRef requires a well-formed pointer");
        validate_ts_record(*pointer.record());
        if (pointer.writable_access() &&
            !has_capability(pointer.record()->capabilities, TypeCapabilities::Mutable))
            throw std::invalid_argument("TSRoleTypeRef writable pointer requires a mutable role");
        return TSRoleTypeRef{pointer.record()};
    }

    bool TSRoleTypeRef::valid() const noexcept
    {
        if (record_ == nullptr) return false;
        try { validate_ts_record(*record_); return true; }
        catch (...) { return false; }
    }

    const TSValueTypeMetaData *TSRoleTypeRef::schema() const noexcept
    {
        return record_ != nullptr ? reinterpret_cast<const TSValueTypeMetaData *>(record_->schema) : nullptr;
    }

    const MemoryUtils::StoragePlan &TSRoleTypeRef::checked_plan() const
    {
        if (plan() == nullptr) throw std::logic_error("TSRoleTypeRef is unbound");
        return *plan();
    }

    const TSDataOps *TSRoleTypeRef::ops() const noexcept
    {
        return record_ != nullptr ? static_cast<const TSDataOps *>(record_->ops) : nullptr;
    }

    const TSDataOps &TSRoleTypeRef::ops_ref() const
    {
        if (ops() == nullptr) throw std::logic_error("TSRoleTypeRef is unbound");
        return *ops();
    }
} // namespace hgraph

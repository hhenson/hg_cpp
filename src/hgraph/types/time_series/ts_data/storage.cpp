#include <hgraph/types/time_series/ts_data/storage.h>

#include "ownership.h"
#include "../ts_input/target_link_ops.h"

#include <hgraph/types/time_series/ts_input/target_link.h>
#include <hgraph/util/scope.h>

#include <stdexcept>
#include <utility>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] TSStorageTypeRef checked_legacy_root_type(const TSDataBinding &binding)
        {
            if (is_migrated_ts_root_schema(binding.type_meta))
            {
                throw std::invalid_argument(
                    "migrated TSData roots require a Data/Input/Output TypeRecord");
            }
            return TSStorageTypeRef{binding};
        }
    }

    namespace detail
    {
        [[nodiscard]] const TSDataOwnershipOps *ownership_ops_for(const TSDataOps *ops) noexcept
        {
            if (target_link_context_for_ops(ops) != nullptr)
            {
                static const TSDataOwnershipOps target_link_leaf{
                    .child_count = [](const void *, const void *) noexcept { return std::size_t{0}; },
                    .child_at = [](const void *, void *, std::size_t) noexcept { return TSDataOwnedChild{}; },
                    .stop = [](const void *context, void *memory) noexcept {
                        const auto *target_context = static_cast<const TSInputTargetLinkContext *>(context);
                        if (target_context == nullptr || memory == nullptr) { return; }
                        if (auto *link = target_link_storage_at(*target_context, memory); link != nullptr)
                            link->unbind_noexcept();
                    },
                };
                return &target_link_leaf;
            }
            if (const auto *composed = composed_input_ownership_ops_for(ops); composed != nullptr) { return composed; }
            if (const auto *proxy = proxy_ts_data_ownership_ops_for(ops); proxy != nullptr) { return proxy; }
            if (const auto *slot = slot_ts_data_ownership_ops_for(ops); slot != nullptr) { return slot; }
            if (const auto *dynamic = dynamic_list_ts_data_ownership_ops_for(ops); dynamic != nullptr)
                return dynamic;
            return fixed_ts_data_ownership_ops_for(ops);
        }

        void attach_owned_ts_data_parents(TSDataView root)
        {
            if (!root.valid()) { return; }
            const auto &ops = root.ops();
            const auto *ownership = ownership_ops_for(&ops);
            if (ownership == nullptr) { return; }
            const auto count = ownership->child_count(ops.context, root.data());
            for (std::size_t index = 0; index < count; ++index)
            {
                const auto owned = ownership->child_at(ops.context, const_cast<void *>(root.data()), index);
                if (!owned.type || owned.data == nullptr) { continue; }
                TSDataView child{owned.type, owned.data};
                if (owned.attach_parent)
                    child.bind_parent(root, owned.parent_child_id == TS_DATA_NO_CHILD_ID
                                                ? index
                                                : owned.parent_child_id);
                attach_owned_ts_data_parents(child.borrowed_ref());
            }
        }

        void attach_owned_ts_data_parent(TSDataView child, const TSDataView &parent, std::size_t child_id)
        {
            child.bind_parent(parent, child_id);
            attach_owned_ts_data_parents(child.borrowed_ref());
        }

        void stop_owned_ts_data_tree(TSDataView root) noexcept
        {
            if (!root.valid()) { return; }
            static_cast<void>(fallback_on_exception(false, [&] {
                const auto &ops = root.ops();
                const auto *ownership = ownership_ops_for(&ops);
                if (ownership == nullptr) { return true; }
                if (ownership->stop != nullptr)
                    ownership->stop(ops.context, const_cast<void *>(root.data()));
                const auto count = ownership->child_count(ops.context, root.data());
                for (std::size_t index = 0; index < count; ++index)
                {
                    const auto owned = ownership->child_at(ops.context, const_cast<void *>(root.data()), index);
                    if (!owned.type || owned.data == nullptr) { continue; }
                    stop_owned_ts_data_tree(TSDataView{owned.type, owned.data});
                }
                return true;
            }));
        }

        void invalidate_owned_ts_data_tree(TSDataView root) noexcept
        {
            if (!root.valid()) { return; }
            static_cast<void>(fallback_on_exception(false, [&] {
                auto &tracking = root.mutable_tracking();
                tracking.observers.invalidate(&tracking);
                const auto &ops = root.ops();
                const auto *ownership = ownership_ops_for(&ops);
                if (ownership == nullptr) { return true; }
                if (ownership->stop != nullptr)
                    ownership->stop(ops.context, const_cast<void *>(root.data()));
                const auto count = ownership->child_count(ops.context, root.data());
                for (std::size_t index = 0; index < count; ++index)
                {
                    const auto owned = ownership->child_at(ops.context, const_cast<void *>(root.data()), index);
                    if (!owned.type || owned.data == nullptr) { continue; }
                    TSDataView child{owned.type, owned.data};
                    invalidate_owned_ts_data_tree(std::move(child));
                }
                return true;
            }));
        }
    }

    TSDataOwnedStorage::TSDataOwnedStorage(TSStorageTypeRef type, const MemoryUtils::AllocatorOps &allocator)
    {
        construct_default(type, allocator);
    }

    TSDataOwnedStorage::TSDataOwnedStorage(const TSDataOwnedStorage &other)
    {
        construct_copy(other);
    }

    TSDataOwnedStorage &TSDataOwnedStorage::operator=(const TSDataOwnedStorage &other)
    {
        if (this != &other)
        {
            TSDataOwnedStorage replacement{other};
            *this = std::move(replacement);
        }
        return *this;
    }

    TSDataOwnedStorage::TSDataOwnedStorage(TSDataOwnedStorage &&other) noexcept
        : type_(std::exchange(other.type_, {})), allocator_(std::exchange(other.allocator_, nullptr)),
          data_(std::exchange(other.data_, nullptr))
    {
        if (data_ != nullptr) { detail::attach_owned_ts_data_parents(TSDataView{type_, data_}); }
    }

    TSDataOwnedStorage &TSDataOwnedStorage::operator=(TSDataOwnedStorage &&other) noexcept
    {
        if (this != &other)
        {
            reset();
            type_ = std::exchange(other.type_, {});
            allocator_ = std::exchange(other.allocator_, nullptr);
            data_ = std::exchange(other.data_, nullptr);
            if (data_ != nullptr) { detail::attach_owned_ts_data_parents(TSDataView{type_, data_}); }
        }
        return *this;
    }

    TSDataOwnedStorage::~TSDataOwnedStorage() noexcept
    {
        reset();
    }

    void TSDataOwnedStorage::construct_default(TSStorageTypeRef type, const MemoryUtils::AllocatorOps &allocator)
    {
        const auto *plan = type.plan();
        if (plan == nullptr || !plan->valid())
            throw std::invalid_argument("TSData storage requires a valid storage type");
        type_ = type;
        allocator_ = &allocator;
        data_ = allocator.allocate_storage(plan->layout);
        auto rollback = make_scope_exit([&]() noexcept {
            allocator.deallocate_storage(data_, plan->layout);
            type_ = {};
            allocator_ = nullptr;
            data_ = nullptr;
        });
        plan->default_construct(data_);
        detail::attach_owned_ts_data_parents(TSDataView{type_, data_});
        rollback.release();
    }

    void TSDataOwnedStorage::construct_copy(const TSDataOwnedStorage &other)
    {
        if (!other.has_value()) return;
        const auto *plan = other.type_.plan();
        if (plan == nullptr || !plan->can_copy_construct())
            throw std::logic_error("TSData storage is not copy constructible");
        type_ = other.type_;
        allocator_ = other.allocator_;
        data_ = allocator_->allocate_storage(plan->layout);
        auto rollback = make_scope_exit([&]() noexcept {
            allocator_->deallocate_storage(data_, plan->layout);
            type_ = {};
            allocator_ = nullptr;
            data_ = nullptr;
        });
        plan->copy_construct(data_, other.data_);
        detail::attach_owned_ts_data_parents(TSDataView{type_, data_});
        rollback.release();
    }

    void TSDataOwnedStorage::reset() noexcept
    {
        if (data_ != nullptr)
        {
            const auto *plan = type_.plan();
            detail::invalidate_owned_ts_data_tree(TSDataView{type_, data_});
            plan->destroy(data_);
            allocator_->deallocate_storage(data_, plan->layout);
        }
        type_ = {};
        allocator_ = nullptr;
        data_ = nullptr;
    }

    TSData::TSData() noexcept = default;

    TSData::TSData(const TSDataBinding &binding)
        : storage_(checked_legacy_root_type(binding))
    {}

    TSData::TSData(TSRoleTypeRef type)
        : storage_(TSStorageTypeRef{type})
    {
    }

    bool TSData::has_value() const noexcept
    {
        return storage_.has_value();
    }

    const TSDataBinding *TSData::binding() const noexcept
    {
        return storage_.storage_type().legacy_binding();
    }

    TSStorageTypeRef TSData::storage_type_ref() const noexcept
    {
        return storage_.storage_type();
    }

    TSRoleTypeRef TSData::type_ref() const noexcept
    {
        return storage_.storage_type().type_ref();
    }

    const TSValueTypeMetaData *TSData::schema() const noexcept
    {
        return storage_.storage_type().schema();
    }

    TSDataView TSData::view()
    {
        return TSDataView{storage_.storage_type(), storage_.data()};
    }

    TSDataView TSData::view() const
    {
        return TSDataView{storage_.storage_type(), storage_.data()};
    }
}  // namespace hgraph

#include <hgraph/types/time_series/ts_input/target_link.h>

#include <hgraph/types/time_series/ts_input/detail.h>

#include <hgraph/util/scope.h>

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace hgraph::detail
{
    namespace
    {
        void unsubscribe_node(TSInputTargetActiveNode &node,
                              TSInputTargetLinkState::SchedulingNotifier &notifier) noexcept
        {
            if (!node.observed.bound()) { return; }
            [[maybe_unused]] auto reset_observed = make_scope_exit([&]() noexcept { node.observed.reset(); });
            [[maybe_unused]] auto unsubscribe_observer =
                make_scope_exit<true>([&] { node.observed.data_view().unsubscribe(&notifier); });
        }

        void unsubscribe_tree(TSInputTargetActiveNode &node,
                              TSInputTargetLinkState::SchedulingNotifier &notifier) noexcept
        {
            unsubscribe_node(node, notifier);
            for (auto &[slot, child] : node.children)
            {
                if (child) { unsubscribe_tree(*child, notifier); }
            }
        }

        void replace_observer(const TSOutputHandle &observed,
                              Notifiable           *previous,
                              Notifiable           *replacement) noexcept
        {
            if (!observed.bound() || previous == nullptr || replacement == nullptr) { return; }
            static_cast<void>(fallback_on_exception(false, [&] {
                observed.data_view().replace_observer(previous, replacement);
                return true;
            }));
        }

        void replace_tree_observers(TSInputTargetActiveNode                    &node,
                                    TSInputTargetLinkState::SchedulingNotifier &previous,
                                    TSInputTargetLinkState::SchedulingNotifier &replacement) noexcept
        {
            replace_observer(node.observed, &previous, &replacement);
            for (auto &[slot, child] : node.children)
            {
                if (child) { replace_tree_observers(*child, previous, replacement); }
            }
        }

        [[nodiscard]] bool project_target_path(TSDataView &current,
                                               const TSValueTypeMetaData *&current_schema,
                                               const TSInputTargetActiveNode *node)
        {
            if (node == nullptr || node->parent == nullptr) { return current.valid() && current_schema != nullptr; }
            if (!project_target_path(current, current_schema, node->parent)) { return false; }

            const auto &ops = input_endpoint_ops_for(current_schema);
            if (ops.target_child == nullptr || ops.child_schema == nullptr) { return false; }
            current = ops.target_child(current, node->slot);
            current_schema = ops.child_schema(current_schema, node->slot);
            return current.valid() && current_schema != nullptr;
        }

        [[nodiscard]] const TSValueTypeMetaData *target_schema_at_node(
            const TSValueTypeMetaData *current,
            const TSInputTargetActiveNode *node) noexcept
        {
            if (node == nullptr || node->parent == nullptr) { return current; }
            const auto *parent_schema = target_schema_at_node(current, node->parent);
            if (parent_schema == nullptr) { return nullptr; }
            return fallback_on_exception<const TSValueTypeMetaData *>(nullptr, [&] {
                const auto &ops = input_endpoint_ops_for(parent_schema);
                return ops.child_schema != nullptr ? ops.child_schema(parent_schema, node->slot) : nullptr;
            });
        }

        void resubscribe_tree(TSInputTargetLinkStorage &link,
                              const TSValueTypeMetaData &schema,
                              TSInputTargetActiveNode &node)
        {
            auto &state = link.state_;

            if (node.locally_active)
            {
                const auto observed = link.target_output_at_path(schema, &node);
                if (!node.observed.same_as(observed))
                {
                    unsubscribe_node(node, state.scheduling_notifier);
                    node.observed = observed;
                    if (node.observed.bound() && state.scheduling_notifier.target() != nullptr)
                    {
                        node.observed.data_view().subscribe(&state.scheduling_notifier);
                    }
                }
            }

            for (auto &[slot, child] : node.children)
            {
                if (!child) { continue; }
                static_cast<void>(slot);
                resubscribe_tree(link, schema, *child);
            }
        }
    }  // namespace

    TSInputTargetActiveNode *TSInputTargetActiveNode::child_at(std::size_t slot) const noexcept
    {
        if (const auto it = children.find(slot); it != children.end()) { return it->second.get(); }
        return nullptr;
    }

    bool TSInputTargetActiveNode::has_any_active() const noexcept
    {
        if (locally_active) { return true; }
        return std::ranges::any_of(children, [](const auto &entry) {
            return entry.second && entry.second->has_any_active();
        });
    }

    TSInputTargetActiveNode &TSInputTargetActiveNode::ensure_child(std::size_t slot)
    {
        auto &child = children[slot];
        if (!child)
        {
            child = std::make_unique<TSInputTargetActiveNode>();
            child->parent = this;
            child->slot = slot;
        }
        return *child;
    }

    bool TSInputTargetActiveNode::try_prune_child(std::size_t slot)
    {
        const auto it = children.find(slot);
        if (it == children.end()) { return false; }
        if (it->second && it->second->has_any_active()) { return false; }
        children.erase(it);
        return true;
    }

    void TSInputTargetActiveNode::clear_observed() noexcept
    {
        observed.reset();
        for (auto &[slot, child] : children)
        {
            if (child) { child->clear_observed(); }
        }
    }

    void TSInputTargetLinkState::SchedulingNotifier::set_target(Notifiable *target) noexcept
    {
        target_ = target;
    }

    Notifiable *TSInputTargetLinkState::SchedulingNotifier::target() const noexcept
    {
        return target_;
    }

    void TSInputTargetLinkState::SchedulingNotifier::notify(DateTime modified_time)
    {
        if (target_ != nullptr) { target_->notify(modified_time); }
    }

    TSInputTargetLinkState::TSInputTargetLinkState(TSInputTargetLinkStorage &owner) noexcept
        : owner(&owner)
    {
    }

    TSInputTargetLinkState::~TSInputTargetLinkState() noexcept
    {
        unsubscribe_active_tree();
    }

    void TSInputTargetLinkState::move_from(TSInputTargetLinkState &other) noexcept
    {
        unsubscribe_active_tree();
        target.reset();
        active_root_node.reset();

        scheduling_notifier.set_target(other.scheduling_notifier.target());
        other.scheduling_notifier.set_target(nullptr);

        target = other.target;
        if (target.bound())
        {
            replace_observer(target, &other, this);
            other.target.reset();
        }

        if (other.active_root_node)
        {
            if (scheduling_notifier.target() != nullptr)
            {
                replace_tree_observers(*other.active_root_node, other.scheduling_notifier, scheduling_notifier);
            }
            active_root_node = std::move(other.active_root_node);
        }
    }

    void TSInputTargetLinkState::notify(DateTime modified_time)
    {
        if (owner != nullptr) { owner->record_target_modified(modified_time); }
    }

    TSInputTargetActiveNode *TSInputTargetLinkState::active_root() const noexcept
    {
        return active_root_node.get();
    }

    TSInputTargetActiveNode &TSInputTargetLinkState::ensure_active_root()
    {
        if (!active_root_node) { active_root_node = std::make_unique<TSInputTargetActiveNode>(); }
        return *active_root_node;
    }

    void TSInputTargetLinkState::try_prune_active_root()
    {
        if (active_root_node && !active_root_node->has_any_active()) { active_root_node.reset(); }
    }

    void TSInputTargetLinkState::clear_active_observed() noexcept
    {
        if (active_root_node) { active_root_node->clear_observed(); }
    }

    void TSInputTargetLinkState::unsubscribe_active_tree() noexcept
    {
        if (active_root_node) { unsubscribe_tree(*active_root_node, scheduling_notifier); }
    }

    TSInputTargetLinkStorage::TSInputTargetLinkStorage() noexcept
        : state_(*this)
    {
    }

    TSInputTargetLinkStorage::TSInputTargetLinkStorage(const TSInputTargetLinkStorage &other)
        : tracking(other.tracking),
          state_(*this)
    {
    }

    TSInputTargetLinkStorage &TSInputTargetLinkStorage::operator=(const TSInputTargetLinkStorage &other)
    {
        if (this != &other)
        {
            unbind_noexcept();
            state_.active_root_node.reset();
            state_.scheduling_notifier.set_target(nullptr);
            tracking = other.tracking;
        }
        return *this;
    }

    TSInputTargetLinkStorage::TSInputTargetLinkStorage(TSInputTargetLinkStorage &&other) noexcept
        : tracking(std::move(other.tracking)),
          state_(*this)
    {
        state_.move_from(other.state_);
    }

    TSInputTargetLinkStorage &TSInputTargetLinkStorage::operator=(TSInputTargetLinkStorage &&other) noexcept
    {
        if (this != &other)
        {
            unbind_noexcept();
            state_.active_root_node.reset();
            state_.scheduling_notifier.set_target(nullptr);
            tracking = std::move(other.tracking);
            state_.move_from(other.state_);
        }
        return *this;
    }

    TSInputTargetLinkStorage::~TSInputTargetLinkStorage() noexcept
    {
        unbind_noexcept();
    }

    bool TSInputTargetLinkStorage::bound() const noexcept
    {
        return target_output().bound();
    }

    void TSInputTargetLinkStorage::bind(const TSValueTypeMetaData &schema, const TSOutputView &output)
    {
        if (!output_view_bound(output))
        {
            throw std::invalid_argument("TSInput target binding requires a bound output view");
        }

        auto target = schema.kind == TSTypeKind::SIGNAL ? output.handle() : output.binding_for(schema);
        if (schema.kind != TSTypeKind::SIGNAL && !time_series_schema_equivalent(target.schema(), &schema))
        {
            throw std::invalid_argument("TSInput target binding schema does not match the input slot schema");
        }

        unbind();
        auto &state = state_;
        state.target = target;
        auto rollback = make_scope_exit<true>([this] { unbind(); });
        state.target.data_view().subscribe(&state);
        if (state.target.data_view().last_modified_time() != MIN_DT)
        {
            record_target_modified(state.target.data_view().last_modified_time());
        }
        resubscribe_active_target(schema);
        rollback.release();
    }

    void TSInputTargetLinkStorage::unbind()
    {
        unsubscribe_active_target();
        if (state_.target.bound()) { state_.target.data_view().unsubscribe(&state_); }
        state_.target.reset();
    }

    void TSInputTargetLinkStorage::unbind_noexcept() noexcept
    {
        [[maybe_unused]] auto cleanup = make_scope_exit<true>([this] { unbind(); });
    }

    void TSInputTargetLinkStorage::record_target_modified(DateTime modified_time)
    {
        if (!tracking.record_modified(modified_time)) { return; }
        tracking.parent.notify_child_modified(modified_time);
    }

    TSInputTargetActiveNode &TSInputTargetLinkStorage::root_node()
    {
        return state_.ensure_active_root();
    }

    TSInputTargetActiveNode &TSInputTargetLinkStorage::child_node(TSInputTargetActiveNode *parent, std::size_t slot)
    {
        return (parent != nullptr ? *parent : root_node()).ensure_child(slot);
    }

    void TSInputTargetLinkStorage::make_active(TSInputTargetActiveNode *node,
                                               const TSDataView &observed,
                                               Notifiable *target_notifier)
    {
        auto &state = state_;
        state.scheduling_notifier.set_target(target_notifier);

        auto &active_node = node != nullptr ? *node : state.ensure_active_root();
        const auto observed_handle = observed.valid()
                                         ? TSOutputHandle{state.target.output(), observed.borrowed_ref()}
                                         : TSOutputHandle{};
        if (active_node.locally_active && active_node.observed.same_as(observed_handle)) { return; }

        if (active_node.locally_active) { unsubscribe_node(active_node, state.scheduling_notifier); }
        active_node.locally_active = true;
        active_node.observed = observed_handle;
        if (active_node.observed.bound() && target_notifier != nullptr)
        {
            active_node.observed.data_view().subscribe(&state.scheduling_notifier);
        }
    }

    void TSInputTargetLinkStorage::make_passive(TSInputTargetActiveNode *node)
    {
        if (node == nullptr) { node = state_.active_root(); }
        if (node == nullptr || !node->locally_active) { return; }

        unsubscribe_node(*node, state_.scheduling_notifier);
        node->locally_active = false;
    }

    bool TSInputTargetLinkStorage::active(const TSInputTargetActiveNode *node) const noexcept
    {
        if (node == nullptr) { node = state_.active_root(); }
        return node != nullptr && node->locally_active;
    }

    TSOutputHandle TSInputTargetLinkStorage::target_output_at_path(const TSValueTypeMetaData &schema,
                                                                   const TSInputTargetActiveNode *node) const
    {
        if (!bound()) { return {}; }
        TSDataView current = target_view();
        const auto *current_schema = &schema;
        if (!project_target_path(current, current_schema, node)) { return {}; }
        return TSOutputHandle{target_output().output(), current};
    }

    void TSInputTargetLinkStorage::resubscribe_active_target(const TSValueTypeMetaData &schema)
    {
        if (state_.active_root() == nullptr) { return; }
        resubscribe_tree(*this, schema, *state_.active_root());
    }

    void TSInputTargetLinkStorage::unsubscribe_active_target() noexcept
    {
        state_.unsubscribe_active_tree();
    }

    TSDataView TSInputTargetLinkStorage::target_view() const noexcept
    {
        return target_output().data_view();
    }

    const TSOutputHandle &TSInputTargetLinkStorage::target_output() const noexcept
    {
        return state_.target;
    }

    const TSInputTargetLinkState *TSInputTargetLinkStorage::state() const noexcept
    {
        return &state_;
    }

    bool is_target_link_view(const TSDataView &view) noexcept
    {
        return is_target_link_binding(view.binding()) && view.data() != nullptr;
    }

    bool target_link_bound(const TSDataView &view) noexcept
    {
        const auto *link = target_link_storage(view);
        return link != nullptr && link->bound();
    }

    TSDataView target_link_resolve(const TSDataView &view, const TSInputTargetActiveNode *node) noexcept
    {
        const auto *schema = target_link_schema(view);
        const auto *link = target_link_storage(view);
        if (schema == nullptr || link == nullptr || !link->bound()) { return {}; }
        return fallback_on_exception(TSDataView{}, [&] {
            return link->target_output_at_path(*schema, node).data_view();
        });
    }

    const TSValueTypeMetaData *target_path_schema(const TSDataView &target_link,
                                                  const TSInputTargetActiveNode *node) noexcept
    {
        const TSValueTypeMetaData *current = target_link_schema(target_link);
        return target_schema_at_node(current, node);
    }

    TSInputTargetActiveNode *target_link_child_node(const TSDataView &view,
                                                    TSInputTargetActiveNode *parent,
                                                    std::size_t slot)
    {
        auto *link = mutable_target_link_storage(view);
        if (link == nullptr) { throw std::logic_error("TSInput target navigation requires TargetLink storage"); }
        return &link->child_node(parent, slot);
    }

    void bind_target_link(const TSDataView &view, const TSOutputView &output)
    {
        auto *link = mutable_target_link_storage(view);
        const auto *schema = target_link_schema(view);
        if (link == nullptr || schema == nullptr)
        {
            throw std::logic_error("TSInput target binding requires TargetLink storage");
        }
        link->bind(*schema, output);
    }

    void unbind_target_link(const TSDataView &view)
    {
        auto *link = mutable_target_link_storage(view);
        if (link == nullptr) { throw std::logic_error("TSInput target unbinding requires TargetLink storage"); }
        link->unbind();
    }

    void make_target_link_active(const TSDataView &view,
                                 TSInputTargetActiveNode *node,
                                 const TSDataView &observed,
                                 Notifiable *target_notifier)
    {
        auto *link = mutable_target_link_storage(view);
        if (link == nullptr) { throw std::logic_error("TSInput target activation requires TargetLink storage"); }
        link->make_active(node, observed, target_notifier);
    }

    void make_target_link_passive(const TSDataView &view, TSInputTargetActiveNode *node)
    {
        auto *link = mutable_target_link_storage(view);
        if (link != nullptr) { link->make_passive(node); }
    }

    bool target_link_active(const TSDataView &view, const TSInputTargetActiveNode *node) noexcept
    {
        const auto *link = target_link_storage(view);
        return link != nullptr && link->active(node);
    }
}  // namespace hgraph::detail

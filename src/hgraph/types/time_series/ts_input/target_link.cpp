#include <hgraph/types/time_series/ts_input/target_link.h>

#include "target_link_ops.h"

#include <hgraph/types/time_series/ts_input/detail.h>

#include <hgraph/util/scope.h>

#include <algorithm>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph::detail
{
    struct TSInputTargetLinkStorage::StructuralTransition
    {
        TSOutputHandle previous_target{};
        DateTime       modified_time{MIN_DT};
        bool           sampled_current{false};

        void clear() noexcept
        {
            previous_target.reset();
            modified_time = MIN_DT;
            sampled_current = false;
        }
    };

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

        void unsubscribe_handle_noexcept(TSOutputHandle &observed, Notifiable *observer) noexcept
        {
            if (!observed.bound()) { return; }
            // Destruction can run after the observed output has already cleared observers;
            // normal unbind() remains strict for graph operation.
            if (observer != nullptr)
            {
                static_cast<void>(fallback_on_exception(false, [&] {
                    auto view = observed.data_view();
                    if (view.valid() && view.tracking().observers.contains(observer)) { view.unsubscribe(observer); }
                    return true;
                }));
            }
            observed.reset();
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

        void unsubscribe_tree_noexcept(TSInputTargetActiveNode &node,
                                       TSInputTargetLinkState::SchedulingNotifier &notifier) noexcept
        {
            unsubscribe_handle_noexcept(node.observed, &notifier);
            for (auto &[slot, child] : node.children)
            {
                if (child) { unsubscribe_tree_noexcept(*child, notifier); }
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

        [[nodiscard]] TSSDataView target_slot_set(const TSInputTargetLinkStorage &link)
        {
            auto target = link.target_view();
            if (!target.valid() || target.schema() == nullptr)
            {
                throw std::logic_error("Target-link slot observer requires a bound structural target");
            }
            if (target.schema()->kind == TSTypeKind::TSD) { return target.as_dict().key_set(); }
            if (target.schema()->kind == TSTypeKind::TSS) { return target.as_set(); }
            throw std::logic_error("Target-link slot observer requires a TSS or TSD target");
        }

        [[nodiscard]] bool has_published_structural_key(const TSDataView &target,
                                                        DateTime transition_time)
        {
            if (!target.valid() || target.schema() == nullptr) { return false; }
            const bool modified_now = target.modified(transition_time);
            if (target.schema()->kind == TSTypeKind::TSS)
            {
                auto set = target.as_set();
                for (std::size_t slot = 0; slot < set.slot_capacity(); ++slot)
                {
                    if (!set.slot_occupied(slot) || (modified_now && set.slot_added(slot))) { continue; }
                    if (set.slot_live(slot) || set.slot_removed(slot)) { return true; }
                }
                return false;
            }
            if (target.schema()->kind == TSTypeKind::TSD)
            {
                auto dict = target.as_dict();
                for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                {
                    if (!dict.slot_occupied(slot) || (modified_now && dict.slot_added(slot))) { continue; }
                    if (dict.slot_removed(slot) ||
                        (dict.slot_live(slot) && dict.at_slot(slot).has_current_value()))
                    {
                        return true;
                    }
                }
            }
            return false;
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

            for (auto &[slot_index, child] : node.children)
            {
                if (!child) { continue; }
                static_cast<void>(slot_index);
                resubscribe_tree(link, schema, *child);
            }
        }
    }  // namespace

    TSInputTargetActiveNode *TSInputTargetActiveNode::child_at(std::size_t slot_index) const noexcept
    {
        if (const auto it = children.find(slot_index); it != children.end()) { return it->second.get(); }
        return nullptr;
    }

    bool TSInputTargetActiveNode::has_any_active() const noexcept
    {
        if (locally_active) { return true; }
        return std::ranges::any_of(children, [](const auto &entry) {
            return entry.second && entry.second->has_any_active();
        });
    }

    TSInputTargetActiveNode &TSInputTargetActiveNode::ensure_child(std::size_t slot_index)
    {
        auto &child = children[slot_index];
        if (!child)
        {
            child = std::make_unique<TSInputTargetActiveNode>();
            child->parent = this;
            child->slot = slot_index;
        }
        return *child;
    }

    bool TSInputTargetActiveNode::try_prune_child(std::size_t slot_index)
    {
        const auto it = children.find(slot_index);
        if (it == children.end()) { return false; }
        if (it->second && it->second->has_any_active()) { return false; }
        children.erase(it);
        return true;
    }

    void TSInputTargetActiveNode::clear_observed() noexcept
    {
        observed.reset();
        for (auto &[slot_index, child] : children)
        {
            static_cast<void>(slot_index);
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

    void TSInputTargetLinkState::source_invalidated(const TSDataTracking *source) noexcept
    {
        if (owner != nullptr) { owner->source_invalidated(source); }
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
          state_(*this),
          slot_observers_(std::move(other.slot_observers_)),
          slot_observers_subscribed_(std::exchange(other.slot_observers_subscribed_, false)),
          structural_transition_(std::move(other.structural_transition_))
    {
        other.slot_observers_.clear();
        state_.move_from(other.state_);
    }

    TSInputTargetLinkStorage &TSInputTargetLinkStorage::operator=(TSInputTargetLinkStorage &&other) noexcept
    {
        if (this != &other)
        {
            unbind_noexcept();
            state_.active_root_node.reset();
            state_.scheduling_notifier.set_target(nullptr);
            // Published target links are stable and are not move-assigned.
            // Preserve any observers of this destination identity; observers
            // of the moved-from identity see that source disappear.
            if (!other.slot_observers_.empty())
            {
                static_cast<void>(fallback_on_exception(false, [&] {
                    other.slot_observers_.notify_clear();
                    return true;
                }));
                other.unsubscribe_slot_observers_noexcept();
                other.slot_observers_.clear();
            }
            tracking = std::move(other.tracking);
            state_.move_from(other.state_);
            structural_transition_ = std::move(other.structural_transition_);
            static_cast<void>(fallback_on_exception(false, [&] {
                subscribe_slot_observers();
                return true;
            }));
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
        bind_impl(schema, output, MIN_DT, false);
    }

    void TSInputTargetLinkStorage::bind_sampled(const TSValueTypeMetaData &schema,
                                                const TSOutputView &output,
                                                DateTime modified_time)
    {
        if (modified_time == MIN_DT)
        {
            throw std::invalid_argument("Sampled TSInput target binding requires an evaluation time");
        }
        bind_impl(schema, output, modified_time, true);
    }

    void TSInputTargetLinkStorage::bind_impl(const TSValueTypeMetaData &schema,
                                             const TSOutputView &output,
                                             DateTime modified_time,
                                             bool sampled)
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

        const bool previous_has_published_key =
            sampled && state_.target.bound() &&
            has_published_structural_key(state_.target.data_view(), modified_time);
        if (state_.target.bound()) { detach_target(sampled, modified_time); }
        else if (!sampled || structural_transition_time() != modified_time)
        {
            if (structural_transition_) { structural_transition_->clear(); }
        }

        auto &state = state_;
        state.target = target;
        auto rollback = make_scope_exit<true>([this] { unbind(); });
        state.target.data_view().subscribe(&state);
        if (state.target.data_view().last_modified_time() != MIN_DT)
        {
            record_target_modified(state.target.data_view().last_modified_time());
        }
        subscribe_slot_observers();
        resubscribe_active_target(schema);
        const bool publish_sampled_transition =
            sampled && (output.valid() || previous_has_published_key);
        if (publish_sampled_transition)
        {
            if (!structural_transition_)
            {
                structural_transition_ = std::make_unique<StructuralTransition>();
            }
            structural_transition_->modified_time = modified_time;
            structural_transition_->sampled_current = true;
            record_target_modified(modified_time);
        }
        else if (sampled && structural_transition_) { structural_transition_->clear(); }
        rollback.release();
    }

    void TSInputTargetLinkStorage::unbind()
    {
        detach_target(false, MIN_DT);
    }

    void TSInputTargetLinkStorage::unbind_structural(DateTime modified_time)
    {
        if (modified_time == MIN_DT)
        {
            throw std::invalid_argument("Structural TSInput target unbinding requires an evaluation time");
        }
        if (!state_.target.bound()) { return; }
        const bool has_published_key =
            has_published_structural_key(state_.target.data_view(), modified_time);
        detach_target(has_published_key, modified_time);
        if (!has_published_key) { return; }
        record_target_modified(modified_time);
    }

    void TSInputTargetLinkStorage::detach_target(bool retain_structural_target, DateTime modified_time)
    {
        if (state_.target.bound() && slot_observers_subscribed_)
        {
            slot_observers_.notify_clear();
            unsubscribe_slot_observers();
        }
        unsubscribe_active_target();
        if (state_.target.bound()) { state_.target.data_view().unsubscribe(&state_); }
        if (retain_structural_target)
        {
            if (!structural_transition_)
            {
                structural_transition_ = std::make_unique<StructuralTransition>();
            }
            structural_transition_->previous_target = state_.target;
            structural_transition_->modified_time = modified_time;
            structural_transition_->sampled_current = false;
        }
        else if (structural_transition_) { structural_transition_->clear(); }
        state_.target.reset();
    }

    void TSInputTargetLinkStorage::unbind_noexcept() noexcept
    {
        if (state_.target.bound() && slot_observers_subscribed_)
        {
            static_cast<void>(fallback_on_exception(false, [&] {
                slot_observers_.notify_clear();
                return true;
            }));
            unsubscribe_slot_observers_noexcept();
        }
        if (state_.active_root_node) { unsubscribe_tree_noexcept(*state_.active_root_node, state_.scheduling_notifier); }
        unsubscribe_handle_noexcept(state_.target, &state_);
        structural_transition_.reset();
    }

    void TSInputTargetLinkStorage::source_invalidated(const TSDataTracking *source) noexcept
    {
        static_cast<void>(source);
        const bool notify_slot_clear = slot_observers_subscribed_;
        slot_observers_subscribed_ = false;
        state_.clear_active_observed();
        state_.target.reset();
        structural_transition_.reset();
        if (notify_slot_clear)
        {
            static_cast<void>(fallback_on_exception(false, [&] {
                slot_observers_.notify_clear();
                return true;
            }));
        }
    }

    void TSInputTargetLinkStorage::add_slot_observer(SlotObserver *observer)
    {
        slot_observers_.add(observer);
        auto rollback = make_scope_exit<true>([&] { slot_observers_.remove(observer); });
        if (bound())
        {
            target_slot_set(*this).subscribe_slot_observer(observer);
        }
        if (bound()) { slot_observers_subscribed_ = true; }
        rollback.release();
    }

    void TSInputTargetLinkStorage::remove_slot_observer(SlotObserver *observer)
    {
        if (bound() && slot_observers_subscribed_)
        {
            target_slot_set(*this).unsubscribe_slot_observer(observer);
        }
        slot_observers_.remove(observer);
        if (slot_observers_.empty()) { slot_observers_subscribed_ = false; }
    }

    void TSInputTargetLinkStorage::subscribe_slot_observers()
    {
        if (!bound() || slot_observers_.empty()) { return; }

        auto set = target_slot_set(*this);
        std::vector<SlotObserver *> subscribed;
        subscribed.reserve(slot_observers_.entries().size());
        auto rollback = make_scope_exit<true>([&] {
            for (SlotObserver *observer : subscribed) { set.unsubscribe_slot_observer(observer); }
        });
        for (SlotObserver *observer : slot_observers_.entries())
        {
            if (observer == nullptr) { continue; }
            set.subscribe_slot_observer(observer);
            subscribed.push_back(observer);
        }
        slot_observers_subscribed_ = true;
        rollback.release();
    }

    void TSInputTargetLinkStorage::unsubscribe_slot_observers()
    {
        if (!bound() || !slot_observers_subscribed_) { return; }
        auto clear_subscribed = make_scope_exit([&] { slot_observers_subscribed_ = false; });
        auto set = target_slot_set(*this);
        for (SlotObserver *observer : slot_observers_.entries())
        {
            if (observer != nullptr) { set.unsubscribe_slot_observer(observer); }
        }
    }

    void TSInputTargetLinkStorage::unsubscribe_slot_observers_noexcept() noexcept
    {
        if (!bound() || !slot_observers_subscribed_) { return; }
        static_cast<void>(fallback_on_exception(false, [&] {
            unsubscribe_slot_observers();
            return true;
        }));
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

    TSDataView TSInputTargetLinkStorage::previous_target_view() const noexcept
    {
        return structural_transition_ != nullptr
                   ? structural_transition_->previous_target.data_view()
                   : TSDataView{};
    }

    const TSOutputHandle &TSInputTargetLinkStorage::target_output() const noexcept
    {
        return state_.target;
    }

    bool TSInputTargetLinkStorage::structural_transition_active() const noexcept
    {
        return structural_transition_ != nullptr &&
               structural_transition_->modified_time != MIN_DT &&
               tracking.last_modified_time == structural_transition_->modified_time;
    }

    bool TSInputTargetLinkStorage::sampled_structural_transition() const noexcept
    {
        return structural_transition_active() && structural_transition_->sampled_current;
    }

    DateTime TSInputTargetLinkStorage::structural_transition_time() const noexcept
    {
        return structural_transition_ != nullptr ? structural_transition_->modified_time : MIN_DT;
    }

    const TSInputTargetLinkState *TSInputTargetLinkStorage::state() const noexcept
    {
        return &state_;
    }

    bool is_target_link_view(const TSDataView &view) noexcept
    {
        return target_link_context_for_ops(view.storage_type().ops()) != nullptr && view.data() != nullptr;
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

    void bind_target_link_sampled(const TSDataView &view, const TSOutputView &output,
                                  DateTime modified_time)
    {
        auto *link = mutable_target_link_storage(view);
        const auto *schema = target_link_schema(view);
        if (link == nullptr || schema == nullptr)
        {
            throw std::logic_error("Sampled TSInput target binding requires TargetLink storage");
        }
        link->bind_sampled(*schema, output, modified_time);
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

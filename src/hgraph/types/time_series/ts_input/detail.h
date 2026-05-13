#ifndef HGRAPH_CPP_TS_INPUT_DETAIL_H
#define HGRAPH_CPP_TS_INPUT_DETAIL_H

#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/util/scope.h>

#include <map>
#include <memory>
#include <utility>
#include <vector>

namespace hgraph::detail
{
    struct TSInputEndpointOps;
    struct TSInputNode;

    [[nodiscard]] bool output_view_bound(const TSOutputView &output) noexcept;
    [[nodiscard]] const TSDataView &empty_ts_data_view() noexcept;
    void validate_input_view_kind(const TSValueTypeMetaData *schema, TSTypeKind expected, const char *what);
    [[nodiscard]] const TSInputEndpointOps &input_endpoint_ops_for(const TSValueTypeMetaData *schema);
    [[nodiscard]] const TSDataBinding *input_data_binding_for(const TSValueTypeMetaData *schema);

    struct TSInputEndpointOps
        {
            using child_count_fn = std::size_t (*)(const TSValueTypeMetaData *schema) noexcept;
            using key_at_fn = std::string_view (*)(const TSValueTypeMetaData *schema,
                                                   std::size_t                index) noexcept;
            using find_key_fn = std::size_t (*)(const TSValueTypeMetaData *schema,
                                                std::string_view              name) noexcept;
            using child_schema_fn = const TSValueTypeMetaData *(*)(const TSValueTypeMetaData *schema,
                                                                   std::size_t                index) noexcept;
            using bind_children_fn = void (*)(TSInputNode &node, TSOutputView output);
            using target_child_fn = TSDataView (*)(TSDataView parent, std::size_t index);

            const char      *name{nullptr};
            bool             supports_input_projection{false};
            bool             named_value_projection{false};
            char             value_open{'['};
            char             value_close{']'};
            child_count_fn   child_count{nullptr};
            key_at_fn        key_at{nullptr};
            find_key_fn      find_key{nullptr};
            child_schema_fn  child_schema{nullptr};
            bind_children_fn bind_children{nullptr};
            target_child_fn  target_child{nullptr};
        };

        struct TSInputActiveTarget;

        struct TSInputSchedulingNotifier final : Notifiable
        {
            TSInputActiveTarget *owner{nullptr};
            Notifiable          *target{nullptr};

            void notify(engine_time_t modified_time) override
            {
                if (target != nullptr) { target->notify(modified_time); }
            }

            void notify_invalidated() noexcept override;
        };

        struct TSInputActiveTarget
        {
            explicit TSInputActiveTarget(std::vector<std::size_t> path_)
                : path(std::move(path_))
            {
                notifier.owner = this;
            }

            TSInputActiveTarget(const TSInputActiveTarget &) = delete;
            TSInputActiveTarget &operator=(const TSInputActiveTarget &) = delete;

            ~TSInputActiveTarget() noexcept
            {
                unsubscribe();
            }

            void subscribe(TSDataView observed_, Notifiable *target)
            {
                if (observed_.valid() && observed_.data() == observed.data() && observed_.binding() == observed.binding())
                {
                    notifier.target = target;
                    return;
                }

                unsubscribe();
                notifier.target = target;
                if (target == nullptr || !observed_.valid()) { return; }
                observed = observed_;
                observed.subscribe(&notifier);
            }

            void unsubscribe() noexcept
            {
                if (!observed.valid()) { return; }
                [[maybe_unused]] auto reset_observed = make_scope_exit([this]() noexcept { observed = {}; });
                [[maybe_unused]] auto unsubscribe_observer =
                    make_scope_exit<true>([this] { observed.unsubscribe(&notifier); });
            }

            bool active{true};
            std::vector<std::size_t> path{};
            TSDataView observed{};
            TSInputSchedulingNotifier notifier{};
        };

        inline void TSInputSchedulingNotifier::notify_invalidated() noexcept
        {
            if (owner != nullptr) { owner->observed = {}; }
        }

        struct TSInputNode
        {
            struct TargetObserver final : Notifiable
            {
                explicit TargetObserver(TSInputNode *owner_) noexcept
                    : owner(owner_)
                {
                }

                void notify(engine_time_t modified_time) override
                {
                    if (owner != nullptr) { owner->record_modified(modified_time); }
                }

                void notify_invalidated() noexcept override
                {
                    if (owner != nullptr) { owner->target_invalidated(); }
                }

                TSInputNode *owner{nullptr};
            };

            TSInputNode(TSEndpointRole              role_,
                        const TSValueTypeMetaData  *schema_,
                        TSInputNode                *parent_,
                        std::size_t                 child_id_)
                : role(role_),
                  schema(schema_),
                  parent(parent_),
                  child_id(child_id_),
                  endpoint_ops(&input_endpoint_ops_for(schema_)),
                  data_binding(input_data_binding_for(schema_)),
                  target_observer(this)
            {
            }

            TSInputNode(const TSInputNode &) = delete;
            TSInputNode &operator=(const TSInputNode &) = delete;

            ~TSInputNode() noexcept
            {
                unbind_target_noexcept();
            }

            [[nodiscard]] std::unique_ptr<TSInputNode> deep_copy(TSInputNode *new_parent) const
            {
                auto copy = std::make_unique<TSInputNode>(role, schema, new_parent, child_id);
                copy->tracking.last_modified_time = tracking.last_modified_time;
                copy->children.reserve(children.size());
                for (const auto &child : children)
                {
                    copy->children.push_back(child ? child->deep_copy(copy.get()) : nullptr);
                }
                return copy;
            }

            void relink(TSInputNode *new_parent) noexcept
            {
                parent = new_parent;
                target_observer.owner = this;
                for (auto &child : children)
                {
                    if (child) { child->relink(this); }
                }
                for (auto &[path, active] : active_targets)
                {
                    if (active) { active->notifier.owner = active.get(); }
                }
            }

            bool record_modified(engine_time_t modified_time)
            {
                if (!tracking.record_modified(modified_time)) { return false; }
                if (parent != nullptr) { parent->record_modified(modified_time); }
                return true;
            }

            void bind_target(TSOutputView output)
            {
                if (role != TSEndpointRole::Peered)
                {
                    throw std::logic_error("TSInput target binding requires a peered terminal");
                }
                if (!output_view_bound(output))
                {
                    throw std::invalid_argument("TSInput target binding requires a bound output view");
                }
                if (!time_series_schema_equivalent(output.schema(), schema))
                {
                    throw std::invalid_argument("TSInput target binding schema does not match the input slot schema");
                }

                unbind_target();
                target = output;
                target.subscribe(&target_observer);
                if (target.last_modified_time() != MIN_DT) { record_modified(target.last_modified_time()); }
                resubscribe_active_targets();
            }

            void bind_output_tree(TSOutputView output)
            {
                if (role == TSEndpointRole::Peered)
                {
                    bind_target(output);
                    return;
                }
                if (role != TSEndpointRole::NonPeered)
                {
                    throw std::logic_error("TSInput binding requires a peered terminal or non-peered prefix");
                }
                if (!output_view_bound(output))
                {
                    throw std::invalid_argument("TSInput non-peered binding requires a bound output view");
                }
                if (!time_series_schema_equivalent(output.schema(), schema))
                {
                    throw std::invalid_argument("TSInput non-peered binding schema does not match the input prefix schema");
                }

                if (endpoint_ops == nullptr || endpoint_ops->bind_children == nullptr)
                {
                    throw std::logic_error("TSInput non-peered binding requires a TSB or fixed TSL endpoint ops");
                }
                endpoint_ops->bind_children(*this, output);
            }

            void unbind_target()
            {
                unsubscribe_active_targets();
                if (output_view_bound(target))
                {
                    target.unsubscribe(&target_observer);
                }
                target = {};
            }

            void unbind_output_tree()
            {
                if (role == TSEndpointRole::Peered)
                {
                    unbind_target();
                    return;
                }
                if (role != TSEndpointRole::NonPeered) { return; }
                for (auto &child : children)
                {
                    if (child) { child->unbind_output_tree(); }
                }
            }

            void unbind_target_noexcept() noexcept
            {
                [[maybe_unused]] auto cleanup = make_scope_exit<true>([this] { unbind_target(); });
            }

            void target_invalidated() noexcept
            {
                target = {};
                for (auto &[path, active] : active_targets)
                {
                    if (active) { active->observed = {}; }
                }
            }

            void make_local_active(Notifiable *target_notifier)
            {
                if (locally_active)
                {
                    if (local_notifier.target == nullptr && target_notifier != nullptr) { tracking.observers.subscribe(&local_notifier); }
                    if (local_notifier.target != nullptr && target_notifier == nullptr) { tracking.observers.unsubscribe(&local_notifier); }
                    local_notifier.target = target_notifier;
                    return;
                }
                locally_active = true;
                local_notifier.owner = nullptr;
                local_notifier.target = target_notifier;
                if (target_notifier != nullptr) { tracking.observers.subscribe(&local_notifier); }
            }

            void make_local_passive()
            {
                if (!locally_active) { return; }
                if (local_notifier.target != nullptr) { tracking.observers.unsubscribe(&local_notifier); }
                local_notifier.target = nullptr;
                locally_active = false;
            }

            [[nodiscard]] bool local_active() const noexcept
            {
                return locally_active;
            }

            void make_target_active(const std::vector<std::size_t> &path, TSDataView observed, Notifiable *target_notifier)
            {
                auto &active = active_targets[path];
                if (!active) { active = std::make_unique<TSInputActiveTarget>(path); }
                active->active = true;
                active->subscribe(observed, target_notifier);
            }

            void make_target_passive(const std::vector<std::size_t> &path)
            {
                const auto it = active_targets.find(path);
                if (it == active_targets.end()) { return; }
                it->second->unsubscribe();
                active_targets.erase(it);
            }

            [[nodiscard]] bool target_active(const std::vector<std::size_t> &path) const noexcept
            {
                return active_targets.find(path) != active_targets.end();
            }

            [[nodiscard]] TSDataView target_child_at_path(const std::vector<std::size_t> &path) const
            {
                if (!output_view_bound(target)) { return {}; }
                TSDataView current = target.data_view();
                const auto *current_schema = schema;
                for (const auto index : path)
                {
                    const auto &ops = input_endpoint_ops_for(current_schema);
                    if (ops.target_child == nullptr || ops.child_schema == nullptr) { return {}; }
                    current = ops.target_child(current, index);
                    current_schema = ops.child_schema(current_schema, index);
                    if (current_schema == nullptr || !current.valid()) { return {}; }
                }
                return current;
            }

            void resubscribe_active_targets()
            {
                for (auto &[path, active] : active_targets)
                {
                    if (!active) { continue; }
                    active->subscribe(target_child_at_path(path), active->notifier.target);
                }
            }

            void unsubscribe_active_targets() noexcept
            {
                for (auto &[path, active] : active_targets)
                {
                    if (active) { active->unsubscribe(); }
                }
            }

            TSEndpointRole role{TSEndpointRole::Peered};
            const TSValueTypeMetaData *schema{nullptr};
            TSInputNode *parent{nullptr};
            std::size_t child_id{TS_DATA_NO_CHILD_ID};
            std::vector<std::unique_ptr<TSInputNode>> children{};

            const TSInputEndpointOps *endpoint_ops{nullptr};
            const TSDataBinding *data_binding{nullptr};
            TSDataTracking       tracking{};

            TSOutputView target{};
            TargetObserver target_observer;

            bool locally_active{false};
            TSInputSchedulingNotifier local_notifier{};
            std::map<std::vector<std::size_t>, std::unique_ptr<TSInputActiveTarget>> active_targets{};
        };

        [[nodiscard]] inline std::unique_ptr<TSInputNode> make_node_from_endpoint_schema(
            const TSEndpointSchema &endpoint_schema,
            TSInputNode            *parent,
            std::size_t             child_id)
        {
            if (endpoint_schema.empty()) { return nullptr; }

            auto node = std::make_unique<TSInputNode>(
                endpoint_schema.role(),
                endpoint_schema.schema(),
                parent,
                child_id);
            if (endpoint_schema.is_non_peered())
            {
                node->children.reserve(endpoint_schema.children().size());
                for (std::size_t index = 0; index < endpoint_schema.children().size(); ++index)
                {
                    node->children.push_back(
                        make_node_from_endpoint_schema(endpoint_schema.children()[index], node.get(), index));
                }
            }
            return node;
        }
}  // namespace hgraph::detail

#endif  // HGRAPH_CPP_TS_INPUT_DETAIL_H

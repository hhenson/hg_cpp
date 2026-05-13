#include <hgraph/types/time_series/ts_input.h>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/container_ops.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <fmt/format.h>
#include <iterator>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace hgraph
{
    namespace detail
    {
        struct TSInputEndpointOps;
    }

    namespace
    {
        [[nodiscard]] constexpr std::size_t ts_kind_index(TSTypeKind kind) noexcept
        {
            return static_cast<std::size_t>(kind);
        }

        [[nodiscard]] bool output_view_bound(const TSOutputView &output) noexcept
        {
            return output.output() != nullptr && output.data_view().valid();
        }

        void validate_endpoint_kind(const TSValueTypeMetaData *schema, TSTypeKind expected, const char *what)
        {
            if (schema == nullptr || schema->kind != expected)
            {
                throw std::invalid_argument(std::string{what} + " requires a matching time-series shape");
            }
        }

        void validate_input_endpoint_schema(const TSEndpointSchema &endpoint_schema, bool root)
        {
            const auto *schema = endpoint_schema.schema();
            if (schema == nullptr) { throw std::invalid_argument("TSInput endpoint annotation requires a schema"); }

            if (root)
            {
                if (schema->kind != TSTypeKind::TSB || !endpoint_schema.is_non_peered())
                {
                    throw std::invalid_argument("TSInput root endpoint annotation must be a non-peered TSB");
                }
            }

            if (endpoint_schema.role() == TSEndpointRole::Peered)
            {
                return;
            }

            if (schema->kind != TSTypeKind::TSB && schema->kind != TSTypeKind::TSL)
            {
                throw std::invalid_argument(
                    "TSInput non-peered prefixes require TSB or fixed-size TSL schemas");
            }
            if (schema->kind == TSTypeKind::TSL && schema->fixed_size() == 0)
            {
                throw std::invalid_argument("TSInput non-peered TSL prefixes currently require a fixed size");
            }
            for (const auto &child : endpoint_schema.children())
            {
                validate_input_endpoint_schema(child, false);
            }
        }

        void append_endpoint_key(std::string &key, const TSEndpointSchema &endpoint_schema)
        {
            const auto role = static_cast<std::uint8_t>(endpoint_schema.role());
            key.append(reinterpret_cast<const char *>(&role), sizeof(role));

            const auto schema_bits = reinterpret_cast<std::uintptr_t>(endpoint_schema.schema());
            key.append(reinterpret_cast<const char *>(&schema_bits), sizeof(schema_bits));

            if (endpoint_schema.is_non_peered())
            {
                const auto &children = endpoint_schema.children();
                const auto  size     = children.size();
                key.append(reinterpret_cast<const char *>(&size), sizeof(size));
                for (const auto &child : children) { append_endpoint_key(key, child); }
            }
        }

        [[nodiscard]] std::string plan_cache_key(const TSInputConstructionPlan &plan)
        {
            std::string key;
            key.reserve(128);
            append_endpoint_key(key, plan.endpoint_schema());
            return key;
        }

        [[nodiscard]] const TSDataView &empty_ts_data_view() noexcept
        {
            static const TSDataView empty{};
            return empty;
        }

        [[nodiscard]] const TSDataBinding *input_data_binding_for(const TSValueTypeMetaData *schema);
        [[nodiscard]] const detail::TSInputEndpointOps &input_endpoint_ops_for(const TSValueTypeMetaData *schema);
    }  // namespace

    namespace detail
    {
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

        void TSInputSchedulingNotifier::notify_invalidated() noexcept
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

        [[nodiscard]] std::unique_ptr<TSInputNode> make_node_from_endpoint_schema(
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
    }  // namespace detail

    namespace
    {
        inline constexpr std::size_t input_npos = static_cast<std::size_t>(-1);

        [[nodiscard]] std::size_t no_endpoint_child_count(const TSValueTypeMetaData *) noexcept
        {
            return 0;
        }

        [[nodiscard]] std::string_view no_endpoint_key_at(const TSValueTypeMetaData *, std::size_t) noexcept
        {
            return {};
        }

        [[nodiscard]] std::size_t no_endpoint_find_key(const TSValueTypeMetaData *, std::string_view) noexcept
        {
            return input_npos;
        }

        [[nodiscard]] const TSValueTypeMetaData *no_endpoint_child_schema(const TSValueTypeMetaData *,
                                                                          std::size_t) noexcept
        {
            return nullptr;
        }

        [[nodiscard]] std::size_t tsb_endpoint_child_count(const TSValueTypeMetaData *schema) noexcept
        {
            return schema != nullptr ? schema->field_count() : 0;
        }

        [[nodiscard]] std::string_view tsb_endpoint_key_at(const TSValueTypeMetaData *schema,
                                                           std::size_t                index) noexcept
        {
            if (schema == nullptr || index >= schema->field_count()) { return {}; }
            const auto *name = schema->fields()[index].name;
            return name != nullptr ? std::string_view{name} : std::string_view{};
        }

        [[nodiscard]] std::size_t tsb_endpoint_find_key(const TSValueTypeMetaData *schema,
                                                        std::string_view          name) noexcept
        {
            if (schema == nullptr) { return input_npos; }
            for (std::size_t index = 0; index < schema->field_count(); ++index)
            {
                const auto *field_name = schema->fields()[index].name;
                if (field_name != nullptr && name == field_name) { return index; }
            }
            return input_npos;
        }

        [[nodiscard]] const TSValueTypeMetaData *tsb_endpoint_child_schema(const TSValueTypeMetaData *schema,
                                                                           std::size_t                index) noexcept
        {
            return schema != nullptr && index < schema->field_count() ? schema->fields()[index].type : nullptr;
        }

        [[nodiscard]] std::size_t tsl_endpoint_child_count(const TSValueTypeMetaData *schema) noexcept
        {
            return schema != nullptr ? schema->fixed_size() : 0;
        }

        [[nodiscard]] const TSValueTypeMetaData *tsl_endpoint_child_schema(const TSValueTypeMetaData *schema,
                                                                           std::size_t                index) noexcept
        {
            return schema != nullptr && index < schema->fixed_size() ? schema->element_ts() : nullptr;
        }

        [[nodiscard]] const TSValueTypeMetaData *tsd_endpoint_child_schema(const TSValueTypeMetaData *schema,
                                                                           std::size_t) noexcept
        {
            return schema != nullptr ? schema->element_ts() : nullptr;
        }

        void require_planned_children(const detail::TSInputNode &node, std::size_t actual_size, const char *what)
        {
            if (node.children.size() != actual_size)
            {
                throw std::logic_error(std::string{what} + " child count does not match its schema");
            }
        }

        detail::TSInputNode &checked_child_node(detail::TSInputNode &node, std::size_t index, const char *what)
        {
            auto *child = index < node.children.size() ? node.children[index].get() : nullptr;
            if (child == nullptr) { throw std::logic_error(std::string{what} + " contains an unplanned child"); }
            return *child;
        }

        void bind_tsb_endpoint_children(detail::TSInputNode &node, TSOutputView output)
        {
            auto data = output.data_view();
            auto bundle = data.as_bundle();
            require_planned_children(node, bundle.size(), "TSInput non-peered TSB");
            for (std::size_t index = 0; index < node.children.size(); ++index)
            {
                checked_child_node(node, index, "TSInput non-peered TSB")
                    .bind_output_tree(TSOutputView{output.output(), bundle.at(index), output.evaluation_time()});
            }
        }

        void bind_tsl_endpoint_children(detail::TSInputNode &node, TSOutputView output)
        {
            auto data = output.data_view();
            auto list = data.as_list();
            require_planned_children(node, list.size(), "TSInput non-peered TSL");
            for (std::size_t index = 0; index < node.children.size(); ++index)
            {
                checked_child_node(node, index, "TSInput non-peered TSL")
                    .bind_output_tree(TSOutputView{output.output(), list.at(index), output.evaluation_time()});
            }
        }

        [[nodiscard]] TSDataView tsb_target_child_at(TSDataView parent, std::size_t index)
        {
            auto bundle = parent.as_bundle();
            return bundle.at(index);
        }

        [[nodiscard]] TSDataView tsl_target_child_at(TSDataView parent, std::size_t index)
        {
            auto list = parent.as_list();
            return list.at(index);
        }

        [[nodiscard]] TSDataView tsd_target_child_at(TSDataView parent, std::size_t slot)
        {
            auto dict = parent.as_dict();
            return dict.at_slot(slot);
        }

        const detail::TSInputEndpointOps endpoint_ts_ops{
            .name = "TS",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
        };

        const detail::TSInputEndpointOps endpoint_tss_ops{
            .name = "TSS",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
        };

        const detail::TSInputEndpointOps endpoint_tsd_ops{
            .name = "TSD",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &tsd_endpoint_child_schema,
            .target_child = &tsd_target_child_at,
        };

        const detail::TSInputEndpointOps endpoint_tsl_ops{
            .name = "TSL",
            .supports_input_projection = true,
            .child_count = &tsl_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &tsl_endpoint_child_schema,
            .bind_children = &bind_tsl_endpoint_children,
            .target_child = &tsl_target_child_at,
        };

        const detail::TSInputEndpointOps endpoint_tsw_ops{
            .name = "TSW",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
        };

        const detail::TSInputEndpointOps endpoint_tsb_ops{
            .name = "TSB",
            .supports_input_projection = true,
            .named_value_projection = true,
            .value_open = '{',
            .value_close = '}',
            .child_count = &tsb_endpoint_child_count,
            .key_at = &tsb_endpoint_key_at,
            .find_key = &tsb_endpoint_find_key,
            .child_schema = &tsb_endpoint_child_schema,
            .bind_children = &bind_tsb_endpoint_children,
            .target_child = &tsb_target_child_at,
        };

        const detail::TSInputEndpointOps endpoint_ref_ops{
            .name = "REF",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
        };

        const detail::TSInputEndpointOps endpoint_signal_ops{
            .name = "SIGNAL",
            .child_count = &no_endpoint_child_count,
            .key_at = &no_endpoint_key_at,
            .find_key = &no_endpoint_find_key,
            .child_schema = &no_endpoint_child_schema,
        };

        [[nodiscard]] const detail::TSInputEndpointOps &input_endpoint_ops_for(const TSValueTypeMetaData *schema)
        {
            static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
            static const std::array<const detail::TSInputEndpointOps *, kind_count> table{
                &endpoint_ts_ops,
                &endpoint_tss_ops,
                &endpoint_tsd_ops,
                &endpoint_tsl_ops,
                &endpoint_tsw_ops,
                &endpoint_tsb_ops,
                &endpoint_ref_ops,
                &endpoint_signal_ops,
            };

            if (schema == nullptr) { throw std::logic_error("TSInput endpoint ops require a schema"); }
            const auto index = ts_kind_index(schema->kind);
            if (index >= table.size() || table[index] == nullptr)
            {
                throw std::logic_error("TSInput endpoint ops are not registered for the schema kind");
            }
            return *table[index];
        }

        struct InputStructuredOpsEntry
        {
            const TSValueTypeMetaData *schema{nullptr};
            const detail::TSInputEndpointOps *endpoint_ops{nullptr};
            TSDataLayout              layout{};
            IndexedValueOps           value_ops{};
            IndexedTSDataOps          ts_data_ops{};
            const ValueTypeBinding   *value_binding{nullptr};
            const ValueTypeBinding   *delta_binding{nullptr};
            const TSDataBinding      *ts_data_binding{nullptr};
        };

        [[nodiscard]] const detail::TSInputNode *input_node(const void *memory) noexcept
        {
            return static_cast<const detail::TSInputNode *>(memory);
        }

        [[nodiscard]] detail::TSInputNode *mutable_input_node(void *memory) noexcept
        {
            return static_cast<detail::TSInputNode *>(memory);
        }

        [[nodiscard]] bool input_node_has_current_value(const detail::TSInputNode *node)
        {
            if (node == nullptr) { return false; }
            if (node->role == TSEndpointRole::Peered)
            {
                return output_view_bound(node->target) && node->target.valid();
            }
            for (const auto &child : node->children)
            {
                if (input_node_has_current_value(child.get())) { return true; }
            }
            return false;
        }

        [[nodiscard]] bool input_node_all_valid(const detail::TSInputNode *node)
        {
            if (node == nullptr) { return false; }
            if (node->role == TSEndpointRole::Peered)
            {
                return output_view_bound(node->target) && node->target.all_valid();
            }
            for (const auto &child : node->children)
            {
                if (!input_node_all_valid(child.get())) { return false; }
            }
            return true;
        }

        [[nodiscard]] std::size_t input_child_count(const detail::TSInputNode *node) noexcept
        {
            return node != nullptr ? node->children.size() : 0;
        }

        [[nodiscard]] const detail::TSInputNode *input_child_at(const detail::TSInputNode *node,
                                                                std::size_t                index)
        {
            if (node == nullptr || index >= node->children.size()) { return nullptr; }
            return node->children[index].get();
        }

        [[nodiscard]] const TSDataBinding *regular_ts_data_binding_for(const TSValueTypeMetaData *schema)
        {
            return TSDataPlanFactory::instance().binding_for(schema);
        }

        [[nodiscard]] const ValueTypeBinding *regular_value_binding_for(const TSValueTypeMetaData *schema)
        {
            return schema != nullptr ? ValuePlanFactory::instance().binding_for(schema->value_schema) : nullptr;
        }

        [[nodiscard]] const ValueTypeBinding *value_binding_for_data_binding(const TSDataBinding *binding)
        {
            if (binding == nullptr) { return nullptr; }
            const auto &ops = binding->checked_ops();
            const auto *layout = ops.layout_impl(ops.context);
            return layout != nullptr ? layout->value_binding : nullptr;
        }

        [[nodiscard]] const TSDataBinding *child_data_binding(const detail::TSInputNode *child)
        {
            if (child == nullptr) { return nullptr; }
            if (child->role == TSEndpointRole::NonPeered) { return child->data_binding; }
            if (output_view_bound(child->target)) { return child->target.binding(); }
            return regular_ts_data_binding_for(child->schema);
        }

        [[nodiscard]] const void *child_data_memory(const detail::TSInputNode *child)
        {
            if (child == nullptr) { return nullptr; }
            if (child->role == TSEndpointRole::NonPeered) { return child; }
            return output_view_bound(child->target) ? child->target.data_view().data() : nullptr;
        }

        [[nodiscard]] const ValueTypeBinding *child_value_binding(const detail::TSInputNode *child)
        {
            if (child == nullptr) { return nullptr; }
            if (child->role == TSEndpointRole::NonPeered)
            {
                return value_binding_for_data_binding(child->data_binding);
            }
            if (output_view_bound(child->target)) { return child->target.value().binding(); }
            return regular_value_binding_for(child->schema);
        }

        [[nodiscard]] const void *child_value_memory(const detail::TSInputNode *child)
        {
            if (child == nullptr) { return nullptr; }
            if (child->role == TSEndpointRole::NonPeered) { return child; }
            return output_view_bound(child->target) ? child->target.value().data() : nullptr;
        }

        [[nodiscard]] const TSDataLayout *input_ts_data_layout(const void *context)
        {
            return &static_cast<const InputStructuredOpsEntry *>(context)->layout;
        }

        [[nodiscard]] const TSDataTracking *input_ts_data_tracking(const void *, const void *memory)
        {
            const auto *node = input_node(memory);
            if (node == nullptr) { throw std::logic_error("TSInput virtual TSData requires live node memory"); }
            return &node->tracking;
        }

        [[nodiscard]] TSDataTracking *input_ts_data_mutable_tracking(const void *, void *memory)
        {
            auto *node = mutable_input_node(memory);
            if (node == nullptr) { throw std::logic_error("TSInput virtual TSData requires live node memory"); }
            return &node->tracking;
        }

        [[nodiscard]] bool input_ts_data_has_current_value(const void *, const void *memory)
        {
            return input_node_has_current_value(input_node(memory));
        }

        [[nodiscard]] bool input_ts_data_all_valid(const void *, const void *memory)
        {
            return input_node_all_valid(input_node(memory));
        }

        [[nodiscard]] const void *input_ts_data_value_memory(const void *, const void *memory)
        {
            return memory;
        }

        [[nodiscard]] const void *input_ts_data_delta_memory(const void *, const void *)
        {
            return nullptr;
        }

        void input_ts_data_cleanup_delta(const void *, void *, engine_time_t)
        {
        }

        void input_ts_data_record_child_modified(const void *, void *memory, std::size_t, engine_time_t modified_time)
        {
            if (auto *node = mutable_input_node(memory); node != nullptr) { node->record_modified(modified_time); }
        }

        [[nodiscard]] std::size_t input_ts_data_indexed_size(const void *, const void *memory)
        {
            return input_child_count(input_node(memory));
        }

        [[nodiscard]] const TSDataBinding *input_ts_data_element_binding(const void *,
                                                                         const void *memory,
                                                                         std::size_t index)
        {
            return child_data_binding(input_child_at(input_node(memory), index));
        }

        [[nodiscard]] const void *input_ts_data_element_memory(const void *, const void *memory, std::size_t index)
        {
            return child_data_memory(input_child_at(input_node(memory), index));
        }

        [[nodiscard]] std::size_t input_value_size(const void *, const void *memory) noexcept
        {
            return input_child_count(input_node(memory));
        }

        [[nodiscard]] const void *input_value_element_at(const void *, const void *memory, std::size_t index)
        {
            return child_value_memory(input_child_at(input_node(memory), index));
        }

        [[nodiscard]] const ValueTypeBinding *input_value_element_binding(const void *,
                                                                          const void *memory,
                                                                          std::size_t index) noexcept
        {
            return fallback_on_exception<const ValueTypeBinding *>(nullptr, [&] {
                return child_value_binding(input_child_at(input_node(memory), index));
            });
        }

        [[nodiscard]] ValueView input_value_project_value(const void *context, const void *memory, std::size_t index)
        {
            return ValueView{input_value_element_binding(context, memory, index),
                             input_value_element_at(context, memory, index)};
        }

        [[nodiscard]] Range<ValueView> input_value_make_range(const void *context, const void *memory)
        {
            return Range<ValueView>{.context = context, .memory = memory, .limit = input_value_size(context, memory),
                                    .predicate = nullptr, .projector = &input_value_project_value};
        }

        [[nodiscard]] std::size_t input_value_hash(const void *context, const void *memory)
        {
            std::size_t seed = 0;
            const auto  size = input_value_size(context, memory);
            for (std::size_t index = 0; index < size; ++index)
            {
                const auto *binding = input_value_element_binding(context, memory, index);
                const auto *child   = input_value_element_at(context, memory, index);
                const auto  value   = child != nullptr && binding != nullptr ? binding->checked_ops().hash(child) : 0;
                seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            }
            return seed;
        }

        [[nodiscard]] bool input_value_equals(const void *context, const void *lhs, const void *rhs) noexcept
        {
            if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
            return fallback_on_exception(false, [&] {
                const auto size = input_value_size(context, lhs);
                if (input_value_size(context, rhs) != size) { return false; }
                for (std::size_t index = 0; index < size; ++index)
                {
                    const auto *binding = input_value_element_binding(context, lhs, index);
                    const auto *a       = input_value_element_at(context, lhs, index);
                    const auto *b       = input_value_element_at(context, rhs, index);
                    if (a == nullptr || b == nullptr)
                    {
                        if (a != b) { return false; }
                        continue;
                    }
                    if (binding == nullptr || !binding->checked_ops().equals(a, b)) { return false; }
                }
                return true;
            });
        }

        [[nodiscard]] std::partial_ordering input_value_compare(const void *context,
                                                                const void *lhs,
                                                                const void *rhs) noexcept
        {
            if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }
            return fallback_on_exception(std::partial_ordering::unordered, [&] {
                const auto size = std::min(input_value_size(context, lhs), input_value_size(context, rhs));
                for (std::size_t index = 0; index < size; ++index)
                {
                    const auto *binding = input_value_element_binding(context, lhs, index);
                    const auto *a       = input_value_element_at(context, lhs, index);
                    const auto *b       = input_value_element_at(context, rhs, index);
                    if (binding == nullptr)
                    {
                        if (a != b) { return std::partial_ordering::unordered; }
                        continue;
                    }
                    const auto order = binding->checked_ops().compare(a, b);
                    if (order != 0) { return order; }
                }
                const auto lhs_size = input_value_size(context, lhs);
                const auto rhs_size = input_value_size(context, rhs);
                if (lhs_size < rhs_size) { return std::partial_ordering::less; }
                if (lhs_size > rhs_size) { return std::partial_ordering::greater; }
                return std::partial_ordering::equivalent;
            });
        }

        [[nodiscard]] std::string input_value_to_string(const void *context, const void *memory)
        {
            if (memory == nullptr) { return {}; }
            const auto *entry = static_cast<const InputStructuredOpsEntry *>(context);
            const auto *node  = input_node(memory);
            const auto *endpoint_ops = entry != nullptr ? entry->endpoint_ops : nullptr;
            const bool  named = endpoint_ops != nullptr && endpoint_ops->named_value_projection;
            const char  open = endpoint_ops != nullptr ? endpoint_ops->value_open : '[';
            const char  close = endpoint_ops != nullptr ? endpoint_ops->value_close : ']';
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{}", open);
            const auto size = input_value_size(context, memory);
            for (std::size_t index = 0; index < size; ++index)
            {
                if (index > 0) { fmt::format_to(std::back_inserter(out), ", "); }
                if (named)
                {
                    const auto *schema = node != nullptr ? node->schema : nullptr;
                    const auto  key = endpoint_ops != nullptr && endpoint_ops->key_at != nullptr
                                          ? endpoint_ops->key_at(schema, index)
                                          : std::string_view{};
                    fmt::format_to(std::back_inserter(out), "{}: ", key);
                }
                const auto *binding = input_value_element_binding(context, memory, index);
                const auto *child   = input_value_element_at(context, memory, index);
                if (binding != nullptr && child != nullptr)
                {
                    fmt::format_to(std::back_inserter(out), "{}", binding->checked_ops().to_string(child));
                }
            }
            fmt::format_to(std::back_inserter(out), "{}", close);
            return fmt::to_string(out);
        }

        [[nodiscard]] const TSDataBinding *input_data_binding_for(const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr) { return nullptr; }
            const auto &endpoint_ops = input_endpoint_ops_for(schema);
            if (!endpoint_ops.supports_input_projection)
            {
                return regular_ts_data_binding_for(schema);
            }

            static std::mutex mutex;
            static std::unordered_map<const TSValueTypeMetaData *, std::unique_ptr<InputStructuredOpsEntry>> cache;

            std::lock_guard lock{mutex};
            if (const auto it = cache.find(schema); it != cache.end()) { return it->second->ts_data_binding; }

            auto entry = std::make_unique<InputStructuredOpsEntry>();
            entry->schema = schema;
            entry->endpoint_ops = &endpoint_ops;

            entry->value_ops.context        = entry.get();
            entry->value_ops.allows_mutation = false;
            entry->value_ops.hash_impl      = &input_value_hash;
            entry->value_ops.equals_impl    = &input_value_equals;
            entry->value_ops.compare_impl   = &input_value_compare;
            entry->value_ops.to_string_impl = &input_value_to_string;
            entry->value_ops.size           = &input_value_size;
            entry->value_ops.element_at     = &input_value_element_at;
            entry->value_ops.element_binding = &input_value_element_binding;
            entry->value_ops.make_range     = &input_value_make_range;

            const auto &node_plan = MemoryUtils::plan_for<detail::TSInputNode>();
            entry->value_binding =
                &ValueTypeBinding::intern(*schema->value_schema, node_plan, entry->value_ops);
            entry->delta_binding = ValuePlanFactory::instance().binding_for(schema->delta_value_schema);

            entry->layout.value_binding = entry->value_binding;
            entry->layout.delta_binding = entry->delta_binding;

            entry->ts_data_ops.context                  = entry.get();
            entry->ts_data_ops.allows_mutation          = false;
            entry->ts_data_ops.layout_impl              = &input_ts_data_layout;
            entry->ts_data_ops.tracking_impl            = &input_ts_data_tracking;
            entry->ts_data_ops.mutable_tracking_impl    = &input_ts_data_mutable_tracking;
            entry->ts_data_ops.has_current_value_impl   = &input_ts_data_has_current_value;
            entry->ts_data_ops.all_valid_impl           = &input_ts_data_all_valid;
            entry->ts_data_ops.value_memory_impl        = &input_ts_data_value_memory;
            entry->ts_data_ops.delta_memory_impl        = &input_ts_data_delta_memory;
            entry->ts_data_ops.cleanup_delta_impl       = &input_ts_data_cleanup_delta;
            entry->ts_data_ops.record_child_modified_impl = &input_ts_data_record_child_modified;
            entry->ts_data_ops.size_impl                = &input_ts_data_indexed_size;
            entry->ts_data_ops.element_binding_impl     = &input_ts_data_element_binding;
            entry->ts_data_ops.element_memory_impl      = &input_ts_data_element_memory;

            entry->ts_data_binding = &TSDataBinding::intern(*schema, node_plan, entry->ts_data_ops);
            const auto *result = entry->ts_data_binding;
            cache.emplace(schema, std::move(entry));
            return result;
        }

        template <typename T>
        [[nodiscard]] Range<T> empty_input_range() noexcept
        {
            return Range<T>{.context = nullptr, .memory = nullptr, .limit = 0, .predicate = nullptr,
                            .projector = nullptr};
        }

        template <typename K, typename V>
        [[nodiscard]] KeyValueRange<K, V> empty_input_kv_range() noexcept
        {
            return KeyValueRange<K, V>{.context = nullptr, .memory = nullptr, .limit = 0, .predicate = nullptr,
                                       .projector = nullptr};
        }

        [[nodiscard]] std::string_view tsb_input_project_key(const void *context, const void *, std::size_t index)
        {
            const auto *view = static_cast<const TSBInputView *>(context);
            const auto &ops = input_endpoint_ops_for(view->schema());
            return ops.key_at != nullptr ? ops.key_at(view->schema(), index) : std::string_view{};
        }

        [[nodiscard]] bool tsb_input_valid_child(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSBInputView *>(context)->at(index).valid();
        }

        [[nodiscard]] bool tsb_input_modified_child(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSBInputView *>(context)->at(index).modified();
        }

        [[nodiscard]] TSInputView tsb_input_project_value(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSBInputView *>(context)->at(index);
        }

        [[nodiscard]] std::pair<std::string_view, TSInputView> tsb_input_project_item(
            const void *context,
            const void *,
            std::size_t index)
        {
            const auto *view = static_cast<const TSBInputView *>(context);
            const auto &ops = input_endpoint_ops_for(view->schema());
            const auto key = ops.key_at != nullptr ? ops.key_at(view->schema(), index) : std::string_view{};
            return {key, view->at(index)};
        }

        [[nodiscard]] bool tsl_input_valid_child(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSLInputView *>(context)->at(index).valid();
        }

        [[nodiscard]] bool tsl_input_modified_child(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSLInputView *>(context)->at(index).modified();
        }

        [[nodiscard]] TSInputView tsl_input_project_value(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSLInputView *>(context)->at(index);
        }

        [[nodiscard]] std::pair<std::size_t, TSInputView> tsl_input_project_item(
            const void *context,
            const void *,
            std::size_t index)
        {
            return {index, static_cast<const TSLInputView *>(context)->at(index)};
        }

        [[nodiscard]] bool tsd_input_live_slot(const void *context, const void *, std::size_t slot)
        {
            return static_cast<const TSDInputView *>(context)->slot_live(slot);
        }

        [[nodiscard]] bool tsd_input_valid_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *view = static_cast<const TSDInputView *>(context);
            return view->slot_live(slot) && view->at_slot(slot).valid();
        }

        [[nodiscard]] bool tsd_input_modified_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *view = static_cast<const TSDInputView *>(context);
            return view->slot_live(slot) && view->slot_modified(slot);
        }

        [[nodiscard]] bool tsd_input_added_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *view = static_cast<const TSDInputView *>(context);
            return view->slot_occupied(slot) && view->slot_added(slot);
        }

        [[nodiscard]] bool tsd_input_removed_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *view = static_cast<const TSDInputView *>(context);
            return view->slot_occupied(slot) && view->slot_removed(slot);
        }

        [[nodiscard]] TSInputView tsd_input_project_value(const void *context, const void *, std::size_t slot)
        {
            return static_cast<const TSDInputView *>(context)->at_slot(slot);
        }

        [[nodiscard]] std::pair<ValueView, TSInputView> tsd_input_project_item(
            const void *context,
            const void *,
            std::size_t slot)
        {
            const auto *view = static_cast<const TSDInputView *>(context);
            return {view->key_at_slot(slot), view->at_slot(slot)};
        }
    }  // namespace

    TSInputConstructionPlan::TSInputConstructionPlan(const TSValueTypeMetaData &root_schema,
                                                     TSEndpointSchema           endpoint_schema)
        : schema_(&root_schema),
          endpoint_schema_(std::move(endpoint_schema))
    {
        if (!time_series_schema_equivalent(&root_schema, endpoint_schema_.schema()))
        {
            throw std::invalid_argument("TSInput construction annotation schema does not match the root schema");
        }
        validate_input_endpoint_schema(endpoint_schema_, true);
    }

    const TSValueTypeMetaData &TSInputConstructionPlan::schema() const noexcept
    {
        return *schema_;
    }

    const TSEndpointSchema &TSInputConstructionPlan::endpoint_schema() const noexcept
    {
        return endpoint_schema_;
    }

    TSInputConstructionPlan TSInputPlanFactory::compile(const TSValueTypeMetaData &root_schema,
                                                        const TSEndpointSchema    &endpoint_schema)
    {
        return TSInputConstructionPlan{root_schema, endpoint_schema};
    }

    TSInputBuilder::TSInputBuilder(TSInputConstructionPlan plan)
        : plan_(std::move(plan))
    {
    }

    const TSValueTypeMetaData &TSInputBuilder::schema() const noexcept
    {
        return plan_.schema();
    }

    TSInput TSInputBuilder::make_input() const
    {
        return TSInput{*this};
    }

    const TSInputBuilder *TSInputBuilderFactory::builder_for(const TSValueTypeMetaData &root_schema,
                                                            const TSEndpointSchema    &endpoint_schema)
    {
        return builder_for(TSInputPlanFactory::compile(root_schema, endpoint_schema));
    }

    const TSInputBuilder &TSInputBuilderFactory::checked_builder_for(const TSValueTypeMetaData &root_schema,
                                                                    const TSEndpointSchema    &endpoint_schema)
    {
        return checked_builder_for(TSInputPlanFactory::compile(root_schema, endpoint_schema));
    }

    const TSInputBuilder *TSInputBuilderFactory::builder_for(const TSInputConstructionPlan &plan)
    {
        if (plan.endpoint_schema().schema() == nullptr || plan.endpoint_schema().schema()->kind != TSTypeKind::TSB ||
            !plan.endpoint_schema().is_non_peered())
        {
            return nullptr;
        }

        static std::unordered_map<std::string, std::unique_ptr<TSInputBuilder>> cache;
        static std::mutex mutex;

        const auto key = plan_cache_key(plan);
        std::lock_guard lock{mutex};
        if (const auto it = cache.find(key); it != cache.end()) { return it->second.get(); }

        auto builder = std::unique_ptr<TSInputBuilder>(new TSInputBuilder(plan));
        const auto *result = builder.get();
        cache.emplace(key, std::move(builder));
        return result;
    }

    const TSInputBuilder &TSInputBuilderFactory::checked_builder_for(const TSInputConstructionPlan &plan)
    {
        if (const auto *builder = builder_for(plan); builder != nullptr) { return *builder; }
        throw std::invalid_argument("TSInputBuilderFactory requires a non-peered TSB root annotation");
    }

    TSInput::TSInput() noexcept = default;

    TSInput::TSInput(const TSInputBuilder &builder)
        : builder_(&builder)
    {
        rebuild_from_plan(builder.plan_);
    }

    TSInput::TSInput(const TSInputConstructionPlan &plan)
    {
        rebuild_from_plan(plan);
    }

    TSInput::TSInput(const TSInput &other)
        : builder_(other.builder_),
          schema_(other.schema_),
          root_(other.root_ ? other.root_->deep_copy(nullptr) : nullptr)
    {
    }

    TSInput &TSInput::operator=(const TSInput &other)
    {
        if (this == &other) { return *this; }
        TSInput replacement{other};
        return *this = std::move(replacement);
    }

    TSInput::TSInput(TSInput &&other) noexcept
        : builder_(std::exchange(other.builder_, nullptr)),
          schema_(std::exchange(other.schema_, nullptr)),
          root_(std::move(other.root_))
    {
        relink_nodes();
    }

    TSInput &TSInput::operator=(TSInput &&other) noexcept
    {
        if (this != &other)
        {
            root_.reset();
            builder_ = std::exchange(other.builder_, nullptr);
            schema_ = std::exchange(other.schema_, nullptr);
            root_ = std::move(other.root_);
            relink_nodes();
        }
        return *this;
    }

    TSInput::~TSInput() = default;

    bool TSInput::has_value() const noexcept
    {
        return root_ != nullptr;
    }

    const TSValueTypeMetaData *TSInput::schema() const noexcept
    {
        return schema_;
    }

    TSInputView TSInput::view(Notifiable *scheduling_notifier, engine_time_t evaluation_time)
    {
        return TSInputView{this, root_.get(), {}, {}, scheduling_notifier, evaluation_time};
    }

    TSInputView TSInput::view(Notifiable *scheduling_notifier, engine_time_t evaluation_time) const
    {
        return TSInputView{const_cast<TSInput *>(this), root_.get(), {}, {}, scheduling_notifier, evaluation_time};
    }

    void TSInput::rebuild_from_plan(const TSInputConstructionPlan &plan)
    {
        schema_ = &plan.schema();
        root_ = detail::make_node_from_endpoint_schema(plan.endpoint_schema(), nullptr, TS_DATA_NO_CHILD_ID);
        relink_nodes();
    }

    void TSInput::relink_nodes() noexcept
    {
        if (root_) { root_->relink(nullptr); }
    }

    TSInputView::TSInputView() noexcept = default;

    TSInputView::TSInputView(TSInput                  *input,
                             detail::TSInputNode     *node,
                             TSDataView               target_view,
                             std::vector<std::size_t> target_path,
                             Notifiable              *scheduling_notifier,
                             engine_time_t            evaluation_time) noexcept
        : input_(input),
          node_(node),
          data_view_(target_view),
          target_path_(std::move(target_path)),
          scheduling_notifier_(scheduling_notifier),
          evaluation_time_(evaluation_time)
    {
        if (!data_view_.valid() && node_ != nullptr && node_->role == TSEndpointRole::NonPeered)
        {
            data_view_ = TSDataView{node_->data_binding, node_};
        }
    }

    engine_time_t TSInputView::evaluation_time() const noexcept
    {
        return evaluation_time_;
    }

    const TSDataBinding *TSInputView::binding() const noexcept
    {
        if (target_view_live()) { return data_view_.binding(); }
        if (node_ != nullptr && node_->role == TSEndpointRole::Peered && node_->target.bound())
        {
            return node_->target.binding();
        }
        return node_ != nullptr ? node_->data_binding : nullptr;
    }

    const TSValueTypeMetaData *TSInputView::schema() const noexcept
    {
        if (target_view_live()) { return data_view_.schema(); }
        return node_ != nullptr ? node_->schema : nullptr;
    }

    const TSDataView &TSInputView::data_view() const noexcept
    {
        if (target_view_live()) { return data_view_; }
        if (node_ == nullptr) { return empty_ts_data_view(); }
        if (node_->role == TSEndpointRole::Peered)
        {
            return node_->target.bound() ? node_->target.data_view() : empty_ts_data_view();
        }
        return data_view_;
    }

    bool TSInputView::bound() const noexcept
    {
        if (node_ == nullptr) { return false; }
        if (!is_bindable()) { return true; }
        return node_->target.bound();
    }

    bool TSInputView::is_bindable() const noexcept
    {
        return node_ != nullptr && node_->role == TSEndpointRole::Peered;
    }

    bool TSInputView::valid() const
    {
        const auto &data = data_view();
        return data.valid() && data.has_current_value();
    }

    bool TSInputView::all_valid() const
    {
        const auto &data = data_view();
        return data.valid() && data.all_valid();
    }

    engine_time_t TSInputView::last_modified_time() const
    {
        const auto &data = data_view();
        return data.valid() ? data.last_modified_time() : MIN_DT;
    }

    bool TSInputView::modified() const
    {
        if (evaluation_time_ == MIN_DT) { return false; }
        const auto &data = data_view();
        return data.valid() && data.modified(evaluation_time_);
    }

    ValueView TSInputView::value() const
    {
        auto data = data_view();
        if (data.valid()) { return data.value(); }
        throw std::logic_error("TSInputView::value requires a live input view");
    }

    ValueView TSInputView::delta_value() const
    {
        auto data = data_view();
        if (data.valid()) { return data.delta_value(evaluation_time_); }
        throw std::logic_error("TSInputView::delta_value requires a live input view");
    }

    void TSInputView::bind_output(const TSOutputView &output)
    {
        if (node_ == nullptr) { throw std::logic_error("TSInputView::bind_output requires a live input view"); }
        if (!is_bindable())
        {
            throw std::logic_error("TSInputView::bind_output requires a peered target-link input view");
        }
        node_->bind_target(output);
        data_view_ = node_->target.bound()
                         ? (target_path_.empty() ? node_->target.data_view()
                                                 : node_->target_child_at_path(target_path_))
                         : TSDataView{};
        if (scheduling_notifier_ != nullptr && evaluation_time_ != MIN_DT && modified())
        {
            scheduling_notifier_->notify(evaluation_time_);
        }
    }

    void TSInputView::unbind_output()
    {
        if (node_ == nullptr) { return; }
        if (!is_bindable())
        {
            throw std::logic_error("TSInputView::unbind_output requires a peered target-link input view");
        }
        const bool was_valid = valid();
        node_->unbind_target();
        data_view_ = {};
        if (was_valid && scheduling_notifier_ != nullptr && evaluation_time_ != MIN_DT)
        {
            scheduling_notifier_->notify(evaluation_time_);
        }
    }

    void TSInputView::make_active()
    {
        if (node_ == nullptr) { return; }
        if (!target_path_.empty())
        {
            node_->make_target_active(target_path_, target_view_live() ? data_view_ : TSDataView{}, scheduling_notifier_);
            if (scheduling_notifier_ != nullptr && evaluation_time_ != MIN_DT && modified())
            {
                scheduling_notifier_->notify(evaluation_time_);
            }
            return;
        }
        node_->make_local_active(scheduling_notifier_);
        if (scheduling_notifier_ != nullptr && evaluation_time_ != MIN_DT && modified())
        {
            scheduling_notifier_->notify(evaluation_time_);
        }
    }

    void TSInputView::make_passive()
    {
        if (node_ == nullptr) { return; }
        if (!target_path_.empty())
        {
            node_->make_target_passive(target_path_);
            return;
        }
        node_->make_local_passive();
    }

    bool TSInputView::active() const
    {
        if (node_ == nullptr) { return false; }
        if (!target_path_.empty()) { return node_->target_active(target_path_); }
        return node_->local_active();
    }

    TSSInputView TSInputView::as_set() &
    {
        return TSSInputView{*this};
    }

    TSSInputView TSInputView::as_set() const &
    {
        return TSSInputView{*this};
    }

    TSDInputView TSInputView::as_dict() &
    {
        return TSDInputView{*this};
    }

    TSDInputView TSInputView::as_dict() const &
    {
        return TSDInputView{*this};
    }

    TSBInputView TSInputView::as_bundle() &
    {
        return TSBInputView{*this};
    }

    TSBInputView TSInputView::as_bundle() const &
    {
        return TSBInputView{*this};
    }

    TSLInputView TSInputView::as_list() &
    {
        return TSLInputView{*this};
    }

    TSLInputView TSInputView::as_list() const &
    {
        return TSLInputView{*this};
    }

    TSWInputView TSInputView::as_window() &
    {
        return TSWInputView{*this};
    }

    TSWInputView TSInputView::as_window() const &
    {
        return TSWInputView{*this};
    }

    bool TSInputView::is_target_position() const noexcept
    {
        return target_view_live();
    }

    bool TSInputView::target_view_live() const noexcept
    {
        return node_ != nullptr && node_->role == TSEndpointRole::Peered && node_->target.bound() &&
               data_view_.valid();
    }

    TSDataView &TSInputView::checked_target_data_view(const char *what) const
    {
        if (target_view_live()) { return const_cast<TSDataView &>(data_view_); }
        if (node_ != nullptr && node_->role == TSEndpointRole::Peered && node_->target.bound())
        {
            return node_->target.data_view();
        }
        throw std::logic_error(std::string{what} + " requires a bound peered input view");
    }

    TSInputView TSInputView::child_from_target(TSDataView child, std::size_t index) const
    {
        auto path = target_path_;
        path.push_back(index);
        return TSInputView{input_, node_, child, std::move(path), scheduling_notifier_, evaluation_time_};
    }

    TSInputView TSInputView::child_from_node(detail::TSInputNode *child) const noexcept
    {
        TSDataView target_view{};
        if (child != nullptr && child->role == TSEndpointRole::Peered && child->target.bound())
        {
            target_view = child->target.data_view();
        }
        return TSInputView{input_, child, target_view, {}, scheduling_notifier_, evaluation_time_};
    }

    TSBInputView::TSBInputView(TSInputView view)
        : TSInputTypedView<TSBInputView>(std::move(view))
    {
        validate_endpoint_kind(schema(), TSTypeKind::TSB, "TSBInputView");
    }

    std::size_t TSBInputView::size() const
    {
        const auto &ops = input_endpoint_ops_for(schema());
        return ops.child_count != nullptr ? ops.child_count(schema()) : 0;
    }

    bool TSBInputView::empty() const
    {
        return size() == 0;
    }

    bool TSBInputView::has_field(std::string_view name) const noexcept
    {
        return find_field_index(name) != npos;
    }

    TSBDataView TSBInputView::data_view() const
    {
        return view_.data_view().as_bundle();
    }

    Range<std::string_view> TSBInputView::keys() const
    {
        return Range<std::string_view>{.context = this, .memory = nullptr, .limit = size(), .predicate = nullptr,
                                       .projector = &tsb_input_project_key};
    }

    Range<TSInputView> TSBInputView::values() const
    {
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = size(), .predicate = nullptr,
                                  .projector = &tsb_input_project_value};
    }

    Range<TSInputView> TSBInputView::valid_values() const
    {
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = size(),
                                  .predicate = &tsb_input_valid_child,
                                  .projector = &tsb_input_project_value};
    }

    Range<TSInputView> TSBInputView::modified_values() const
    {
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = size(),
                                  .predicate = &tsb_input_modified_child,
                                  .projector = &tsb_input_project_value};
    }

    KeyValueRange<std::string_view, TSInputView> TSBInputView::items() const
    {
        return KeyValueRange<std::string_view, TSInputView>{.context = this,
                                                            .memory = nullptr,
                                                            .limit = size(),
                                                            .predicate = nullptr,
                                                            .projector = &tsb_input_project_item};
    }

    KeyValueRange<std::string_view, TSInputView> TSBInputView::valid_items() const
    {
        return KeyValueRange<std::string_view, TSInputView>{.context = this,
                                                            .memory = nullptr,
                                                            .limit = size(),
                                                            .predicate = &tsb_input_valid_child,
                                                            .projector = &tsb_input_project_item};
    }

    KeyValueRange<std::string_view, TSInputView> TSBInputView::modified_items() const
    {
        return KeyValueRange<std::string_view, TSInputView>{.context = this,
                                                            .memory = nullptr,
                                                            .limit = size(),
                                                            .predicate = &tsb_input_modified_child,
                                                            .projector = &tsb_input_project_item};
    }

    TSInputView TSBInputView::at(std::size_t index) &
    {
        if (index >= size()) { throw std::out_of_range("TSBInputView::at index out of range"); }
        if (view_.is_target_position())
        {
            auto bundle = view_.data_view_.as_bundle();
            return view_.child_from_target(bundle.at(index), index);
        }
        if (view_.node_ == nullptr || view_.node_->role != TSEndpointRole::NonPeered)
        {
            throw std::logic_error("TSBInputView::at requires a non-peered bundle or bound target");
        }
        auto *child = index < view_.node_->children.size() ? view_.node_->children[index].get() : nullptr;
        if (child == nullptr) { throw std::logic_error("TSBInputView::at selected an unplanned input slot"); }
        return view_.child_from_node(child);
    }

    TSInputView TSBInputView::at(std::size_t index) const &
    {
        return const_cast<TSBInputView *>(this)->at(index);
    }

    TSInputView TSBInputView::operator[](std::size_t index) &
    {
        return at(index);
    }

    TSInputView TSBInputView::operator[](std::size_t index) const &
    {
        return at(index);
    }

    TSInputView TSBInputView::at(std::string_view name) &
    {
        return at(field_index(name));
    }

    TSInputView TSBInputView::at(std::string_view name) const &
    {
        return const_cast<TSBInputView *>(this)->at(name);
    }

    TSInputView TSBInputView::field(std::string_view name) &
    {
        return at(name);
    }

    TSInputView TSBInputView::field(std::string_view name) const &
    {
        return at(name);
    }

    TSInputView TSBInputView::operator[](std::string_view name) &
    {
        return at(name);
    }

    TSInputView TSBInputView::operator[](std::string_view name) const &
    {
        return at(name);
    }

    std::size_t TSBInputView::field_index(std::string_view name) const
    {
        const auto index = find_field_index(name);
        if (index == npos) { throw std::out_of_range("TSBInputView field not found"); }
        return index;
    }

    std::size_t TSBInputView::find_field_index(std::string_view name) const noexcept
    {
        return fallback_on_exception(npos, [&] {
            const auto &ops = input_endpoint_ops_for(schema());
            return ops.find_key != nullptr ? ops.find_key(schema(), name) : npos;
        });
    }

    std::string_view TSBInputView::key_at(std::size_t index) const noexcept
    {
        return fallback_on_exception(std::string_view{}, [&] {
            const auto &ops = input_endpoint_ops_for(schema());
            return ops.key_at != nullptr ? ops.key_at(schema(), index) : std::string_view{};
        });
    }

    TSLInputView::TSLInputView(TSInputView view)
        : TSInputTypedView<TSLInputView>(std::move(view))
    {
        validate_endpoint_kind(schema(), TSTypeKind::TSL, "TSLInputView");
    }

    std::size_t TSLInputView::size() const
    {
        const auto &ops = input_endpoint_ops_for(schema());
        return ops.child_count != nullptr ? ops.child_count(schema()) : 0;
    }

    bool TSLInputView::empty() const
    {
        return size() == 0;
    }

    TSLDataView TSLInputView::data_view() const
    {
        return view_.data_view().as_list();
    }

    Range<TSInputView> TSLInputView::values() const
    {
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = size(), .predicate = nullptr,
                                  .projector = &tsl_input_project_value};
    }

    Range<TSInputView> TSLInputView::valid_values() const
    {
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = size(),
                                  .predicate = &tsl_input_valid_child,
                                  .projector = &tsl_input_project_value};
    }

    Range<TSInputView> TSLInputView::modified_values() const
    {
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = size(),
                                  .predicate = &tsl_input_modified_child,
                                  .projector = &tsl_input_project_value};
    }

    KeyValueRange<std::size_t, TSInputView> TSLInputView::items() const
    {
        return KeyValueRange<std::size_t, TSInputView>{.context = this,
                                                       .memory = nullptr,
                                                       .limit = size(),
                                                       .predicate = nullptr,
                                                       .projector = &tsl_input_project_item};
    }

    KeyValueRange<std::size_t, TSInputView> TSLInputView::valid_items() const
    {
        return KeyValueRange<std::size_t, TSInputView>{.context = this,
                                                       .memory = nullptr,
                                                       .limit = size(),
                                                       .predicate = &tsl_input_valid_child,
                                                       .projector = &tsl_input_project_item};
    }

    KeyValueRange<std::size_t, TSInputView> TSLInputView::modified_items() const
    {
        return KeyValueRange<std::size_t, TSInputView>{.context = this,
                                                       .memory = nullptr,
                                                       .limit = size(),
                                                       .predicate = &tsl_input_modified_child,
                                                       .projector = &tsl_input_project_item};
    }

    TSInputView TSLInputView::at(std::size_t index) &
    {
        if (index >= size()) { throw std::out_of_range("TSLInputView::at index out of range"); }
        if (view_.is_target_position())
        {
            auto list = view_.data_view_.as_list();
            return view_.child_from_target(list.at(index), index);
        }
        if (view_.node_ == nullptr || view_.node_->role != TSEndpointRole::NonPeered)
        {
            throw std::logic_error("TSLInputView::at requires a non-peered list or bound target");
        }
        auto *child = index < view_.node_->children.size() ? view_.node_->children[index].get() : nullptr;
        if (child == nullptr) { throw std::logic_error("TSLInputView::at selected an unplanned input slot"); }
        return view_.child_from_node(child);
    }

    TSInputView TSLInputView::at(std::size_t index) const &
    {
        return const_cast<TSLInputView *>(this)->at(index);
    }

    TSInputView TSLInputView::operator[](std::size_t index) &
    {
        return at(index);
    }

    TSInputView TSLInputView::operator[](std::size_t index) const &
    {
        return at(index);
    }

    TSSInputView::TSSInputView(TSInputView view)
        : TSInputTypedView<TSSInputView>(std::move(view))
    {
        validate_endpoint_kind(schema(), TSTypeKind::TSS, "TSSInputView");
    }

    TSSDataView TSSInputView::data_view() const
    {
        return view_.checked_target_data_view("TSSInputView::data_view").as_set();
    }

    std::size_t TSSInputView::size() const { return data_view().size(); }
    bool TSSInputView::empty() const { return data_view().empty(); }
    std::size_t TSSInputView::slot_capacity() const { return data_view().slot_capacity(); }
    bool TSSInputView::slot_occupied(std::size_t slot) const { return data_view().slot_occupied(slot); }
    bool TSSInputView::slot_live(std::size_t slot) const { return data_view().slot_live(slot); }
    bool TSSInputView::slot_added(std::size_t slot) const { return data_view().slot_added(slot); }
    bool TSSInputView::slot_removed(std::size_t slot) const { return data_view().slot_removed(slot); }
    ValueView TSSInputView::at_slot(std::size_t slot) const { return data_view().at_slot(slot); }
    bool TSSInputView::contains(const ValueView &key) const { return data_view().contains(key); }
    std::size_t TSSInputView::find_slot(const ValueView &key) const { return data_view().find_slot(key); }
    Range<ValueView> TSSInputView::values() const { return data_view().values(); }
    Range<ValueView> TSSInputView::added() const { return data_view().added(); }
    Range<ValueView> TSSInputView::removed() const { return data_view().removed(); }
    Range<ValueView> TSSInputView::added_values() const { return data_view().added_values(); }
    Range<ValueView> TSSInputView::removed_values() const { return data_view().removed_values(); }
    Range<ValueView>::iterator TSSInputView::begin() const { return data_view().begin(); }
    Range<ValueView>::iterator TSSInputView::end() const { return data_view().end(); }

    TSDInputView::TSDInputView(TSInputView view)
        : TSInputTypedView<TSDInputView>(std::move(view))
    {
        validate_endpoint_kind(schema(), TSTypeKind::TSD, "TSDInputView");
    }

    TSDDataView TSDInputView::data_view() const
    {
        return view_.checked_target_data_view("TSDInputView::data_view").as_dict();
    }

    std::size_t TSDInputView::size() const { return data_view().size(); }
    bool TSDInputView::empty() const { return data_view().empty(); }
    std::size_t TSDInputView::slot_capacity() const { return data_view().slot_capacity(); }
    bool TSDInputView::slot_occupied(std::size_t slot) const { return data_view().slot_occupied(slot); }
    bool TSDInputView::slot_live(std::size_t slot) const { return data_view().slot_live(slot); }
    bool TSDInputView::slot_added(std::size_t slot) const { return data_view().slot_added(slot); }
    bool TSDInputView::slot_removed(std::size_t slot) const { return data_view().slot_removed(slot); }
    bool TSDInputView::slot_modified(std::size_t slot) const { return data_view().slot_modified(slot); }
    ValueView TSDInputView::key_at_slot(std::size_t slot) const { return data_view().key_at_slot(slot); }

    TSInputView TSDInputView::at_slot(std::size_t slot) const
    {
        auto child = data_view().at_slot(slot);
        return view_.child_from_target(child, slot);
    }

    bool TSDInputView::contains(const ValueView &key) const { return data_view().contains(key); }
    std::size_t TSDInputView::find_slot(const ValueView &key) const { return data_view().find_slot(key); }

    TSInputView TSDInputView::at(const ValueView &key) const
    {
        const auto slot = find_slot(key);
        if (slot == TS_DATA_NO_CHILD_ID) { return TSInputView{}; }
        return at_slot(slot);
    }

    TSInputView TSDInputView::operator[](const ValueView &key) const
    {
        return at(key);
    }

    Range<ValueView> TSDInputView::keys() const { return data_view().keys(); }

    Range<TSInputView> TSDInputView::values() const
    {
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                  .predicate = &tsd_input_live_slot,
                                  .projector = &tsd_input_project_value};
    }

    KeyValueRange<ValueView, TSInputView> TSDInputView::items() const
    {
        return KeyValueRange<ValueView, TSInputView>{.context = this,
                                                     .memory = nullptr,
                                                     .limit = slot_capacity(),
                                                     .predicate = &tsd_input_live_slot,
                                                     .projector = &tsd_input_project_item};
    }

    Range<ValueView> TSDInputView::valid_keys() const { return data_view().valid_keys(); }

    Range<TSInputView> TSDInputView::valid_values() const
    {
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                  .predicate = &tsd_input_valid_slot,
                                  .projector = &tsd_input_project_value};
    }

    KeyValueRange<ValueView, TSInputView> TSDInputView::valid_items() const
    {
        return KeyValueRange<ValueView, TSInputView>{.context = this,
                                                     .memory = nullptr,
                                                     .limit = slot_capacity(),
                                                     .predicate = &tsd_input_valid_slot,
                                                     .projector = &tsd_input_project_item};
    }

    Range<ValueView> TSDInputView::modified_keys() const
    {
        return data_view().modified_keys(view_.evaluation_time());
    }

    Range<TSInputView> TSDInputView::modified_values() const
    {
        if (!modified()) { return empty_input_range<TSInputView>(); }
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                  .predicate = &tsd_input_modified_slot,
                                  .projector = &tsd_input_project_value};
    }

    KeyValueRange<ValueView, TSInputView> TSDInputView::modified_items() const
    {
        if (!modified()) { return empty_input_kv_range<ValueView, TSInputView>(); }
        return KeyValueRange<ValueView, TSInputView>{.context = this,
                                                     .memory = nullptr,
                                                     .limit = slot_capacity(),
                                                     .predicate = &tsd_input_modified_slot,
                                                     .projector = &tsd_input_project_item};
    }

    Range<ValueView> TSDInputView::added_keys() const { return data_view().added_keys(); }

    Range<TSInputView> TSDInputView::added_values() const
    {
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                  .predicate = &tsd_input_added_slot,
                                  .projector = &tsd_input_project_value};
    }

    KeyValueRange<ValueView, TSInputView> TSDInputView::added_items() const
    {
        return KeyValueRange<ValueView, TSInputView>{.context = this,
                                                     .memory = nullptr,
                                                     .limit = slot_capacity(),
                                                     .predicate = &tsd_input_added_slot,
                                                     .projector = &tsd_input_project_item};
    }

    Range<ValueView> TSDInputView::removed_keys() const { return data_view().removed_keys(); }

    Range<TSInputView> TSDInputView::removed_values() const
    {
        return Range<TSInputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                  .predicate = &tsd_input_removed_slot,
                                  .projector = &tsd_input_project_value};
    }

    KeyValueRange<ValueView, TSInputView> TSDInputView::removed_items() const
    {
        return KeyValueRange<ValueView, TSInputView>{.context = this,
                                                     .memory = nullptr,
                                                     .limit = slot_capacity(),
                                                     .predicate = &tsd_input_removed_slot,
                                                     .projector = &tsd_input_project_item};
    }

    TSWInputView::TSWInputView(TSInputView view)
        : TSInputTypedView<TSWInputView>(std::move(view))
    {
        validate_endpoint_kind(schema(), TSTypeKind::TSW, "TSWInputView");
    }

    TSWDataView TSWInputView::data_view() const
    {
        return view_.checked_target_data_view("TSWInputView::data_view").as_window();
    }

    bool TSWInputView::duration_based() const noexcept
    {
        return fallback_on_exception(false, [&] { return data_view().duration_based(); });
    }

    bool TSWInputView::size_based() const noexcept
    {
        return fallback_on_exception(false, [&] { return data_view().size_based(); });
    }

    bool TSWInputView::time_based() const noexcept
    {
        return fallback_on_exception(false, [&] { return data_view().time_based(); });
    }

    std::size_t TSWInputView::period() const { return data_view().period(); }
    std::size_t TSWInputView::min_period() const { return data_view().min_period(); }
    engine_time_delta_t TSWInputView::time_range() const { return data_view().time_range(); }
    engine_time_delta_t TSWInputView::min_time_range() const { return data_view().min_time_range(); }
    std::size_t TSWInputView::capacity() const { return data_view().capacity(); }
    std::size_t TSWInputView::size() const { return data_view().size(); }
    bool TSWInputView::empty() const { return data_view().empty(); }
    bool TSWInputView::full() const { return data_view().full(); }
    engine_time_t TSWInputView::first_modified_time() const { return data_view().first_modified_time(); }
    engine_time_t TSWInputView::time_at(std::size_t index) const { return data_view().time_at(index); }
    ValueView TSWInputView::time_value_at(std::size_t index) const { return data_view().time_value_at(index); }
    ValueView TSWInputView::at(std::size_t index) const { return data_view().at(index); }
    ValueView TSWInputView::operator[](std::size_t index) const { return data_view()[index]; }
    ValueView TSWInputView::front() const { return data_view().front(); }
    ValueView TSWInputView::back() const { return data_view().back(); }
    Range<ValueView> TSWInputView::values() const { return data_view().values(); }
    Range<ValueView> TSWInputView::time_values() const { return data_view().time_values(); }
    Range<engine_time_t> TSWInputView::value_times() const { return data_view().value_times(); }
    Range<ValueView>::iterator TSWInputView::begin() const { return data_view().begin(); }
    Range<ValueView>::iterator TSWInputView::end() const { return data_view().end(); }
}  // namespace hgraph

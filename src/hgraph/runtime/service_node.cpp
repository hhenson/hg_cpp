#include <hgraph/runtime/service_node.h>

#include <hgraph/runtime/graph.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_input/bundle_view.h>
#include <hgraph/types/time_series/ts_input/base_view.h>
#include <hgraph/types/time_series/ts_output/base_view.h>
#include <hgraph/types/time_series/ts_output/set_view.h>

#include <ankerl/unordered_dense.h>

#include <array>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        constexpr std::string_view subscription_key_source_storage_field{"subscription_key_source"};
        constexpr std::string_view subscription_key_capture_storage_field{"subscription_key_capture"};

        struct ValueKeyHash
        {
            using is_transparent = void;

            [[nodiscard]] std::size_t operator()(const Value &value) const
            {
                return value.has_value() ? value.hash() : 0;
            }
        };

        struct ValueKeyEqual
        {
            using is_transparent = void;

            [[nodiscard]] bool operator()(const Value &lhs, const Value &rhs) const
            {
                if (lhs.has_value() != rhs.has_value()) { return false; }
                return !lhs.has_value() || lhs.equals(rhs);
            }
        };

        struct SubscriptionKeyChange
        {
            Value key{};
            bool  add{true};
        };

        struct SubscriptionKeySourceStorage
        {
            ankerl::unordered_dense::map<Value, std::size_t, ValueKeyHash, ValueKeyEqual> counts{};
            std::vector<SubscriptionKeyChange>                                            pending{};
        };

        struct SubscriptionKeySourceContext
        {
            std::string path{};
            std::size_t storage_offset{0};
        };

        struct SubscriptionKeyCaptureStorage
        {
            Value previous_key{};
            bool  has_previous{false};
        };

        struct SubscriptionKeyCaptureContext
        {
            std::string path{};
            std::size_t storage_offset{0};
        };

        [[nodiscard]] std::vector<std::unique_ptr<SubscriptionKeySourceContext>> &
        subscription_key_source_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<SubscriptionKeySourceContext>>;
            return *contexts;
        }

        [[nodiscard]] const SubscriptionKeySourceContext &register_subscription_key_source_context(
            std::string path,
            std::size_t storage_offset)
        {
            auto context = std::make_unique<SubscriptionKeySourceContext>(SubscriptionKeySourceContext{
                .path           = std::move(path),
                .storage_offset = storage_offset,
            });
            const auto *result = context.get();
            subscription_key_source_contexts().push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] std::vector<std::unique_ptr<SubscriptionKeyCaptureContext>> &
        subscription_key_capture_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<SubscriptionKeyCaptureContext>>;
            return *contexts;
        }

        [[nodiscard]] const SubscriptionKeyCaptureContext &register_subscription_key_capture_context(
            std::string path,
            std::size_t storage_offset)
        {
            auto context = std::make_unique<SubscriptionKeyCaptureContext>(SubscriptionKeyCaptureContext{
                .path           = std::move(path),
                .storage_offset = storage_offset,
            });
            const auto *result = context.get();
            subscription_key_capture_contexts().push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] SubscriptionKeySourceStorage &source_storage_of(
            const NodeView &view,
            const SubscriptionKeySourceContext &context)
        {
            return *MemoryUtils::cast<SubscriptionKeySourceStorage>(
                MemoryUtils::advance(view.data(), context.storage_offset));
        }

        [[nodiscard]] SubscriptionKeyCaptureStorage &capture_storage_of(
            const NodeView &view,
            const SubscriptionKeyCaptureContext &context)
        {
            return *MemoryUtils::cast<SubscriptionKeyCaptureStorage>(
                MemoryUtils::advance(view.data(), context.storage_offset));
        }

        class SubscriptionKeySourceView
        {
          public:
            [[nodiscard]] static const void *node_view_type_id() noexcept
            {
                static const char token{};
                return &token;
            }

            [[nodiscard]] static SubscriptionKeySourceView from_node(NodeView view, const void *context)
            {
                if (context == nullptr)
                {
                    throw std::logic_error("SubscriptionKeySourceView requires a typed view context");
                }
                return SubscriptionKeySourceView{
                    std::move(view),
                    static_cast<const SubscriptionKeySourceContext *>(context),
                };
            }

            void enqueue(Value key, bool add, DateTime schedule_time) const
            {
                if (!key.has_value()) { return; }
                source_storage_of(view_, *context_).pending.push_back(SubscriptionKeyChange{
                    .key = std::move(key),
                    .add = add,
                });

                GraphValue *graph = view_.graph_value();
                if (graph == nullptr)
                {
                    throw std::logic_error("subscription key source node is not attached to a graph");
                }
                graph->schedule_node(view_.node_index(), schedule_time);
            }

          private:
            SubscriptionKeySourceView(NodeView view, const SubscriptionKeySourceContext *context) noexcept
                : view_(std::move(view)),
                  context_(context)
            {
            }

            NodeView                            view_{};
            const SubscriptionKeySourceContext *context_{nullptr};
        };

        [[nodiscard]] SubscriptionKeySourceView recover_source_view(
            const NodeView &capture,
            DateTime evaluation_time)
        {
            auto input         = capture.input(evaluation_time);
            auto bundle        = input.as_bundle();
            auto subscriptions = bundle.at("subscriptions");
            if (!subscriptions.bound())
            {
                throw std::logic_error("subscription key capture requires a bound subscriptions source");
            }

            NodeView source = subscriptions.bound_output().owner_node();
            if (!source.valid() || !source.is<SubscriptionKeySourceView>())
            {
                throw std::logic_error("subscription key capture is not bound to a subscription key source");
            }
            return source.as<SubscriptionKeySourceView>();
        }

        void apply_pending_subscription_key_changes(
            SubscriptionKeySourceStorage &storage,
            TSSDataMutationView &mutation)
        {
            for (SubscriptionKeyChange &change : storage.pending)
            {
                if (!change.key.has_value()) { continue; }

                if (change.add)
                {
                    auto [it, inserted] = storage.counts.emplace(change.key, 0U);
                    if (inserted) { static_cast<void>(mutation.add(it->first.view())); }
                    ++it->second;
                    continue;
                }

                auto it = storage.counts.find(change.key);
                if (it == storage.counts.end()) { continue; }
                if (it->second > 1U)
                {
                    --it->second;
                    continue;
                }

                Value removed_key{it->first};
                storage.counts.erase(it);
                static_cast<void>(mutation.remove(removed_key.view()));
            }
            storage.pending.clear();
        }

        bool subscription_key_source_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }

            const auto &typed_context = *static_cast<const SubscriptionKeySourceContext *>(
                view.binding()->ops_ref().extended_view_context);
            auto       &storage       = source_storage_of(view, typed_context);
            if (storage.pending.empty()) { return true; }

            auto output   = view.output(evaluation_time);
            auto set      = output.as_set();
            auto mutation = set.begin_mutation(evaluation_time);
            apply_pending_subscription_key_changes(storage, mutation);
            return true;
        }

        void subscription_key_source_stop(const NodeView &view, DateTime evaluation_time)
        {
            const auto *context = static_cast<const SubscriptionKeySourceContext *>(
                view.binding()->ops_ref().extended_view_context);
            auto &storage = source_storage_of(view, *context);

            storage.counts.clear();
            storage.pending.clear();

            auto output   = view.output(evaluation_time);
            auto set      = output.as_set();
            auto mutation = set.begin_mutation(evaluation_time);
            mutation.clear();
        }

        void record_subscription_key(
            const SubscriptionKeySourceView &source,
            SubscriptionKeyCaptureStorage &storage,
            const TSInputView &key_input,
            DateTime schedule_time)
        {
            if (!key_input.valid())
            {
                if (storage.has_previous) { source.enqueue(std::move(storage.previous_key), false, schedule_time); }
                storage.previous_key = Value{};
                storage.has_previous = false;
                return;
            }

            Value key{key_input.value()};
            if (storage.has_previous && storage.previous_key.equals(key)) { return; }

            if (storage.has_previous) { source.enqueue(std::move(storage.previous_key), false, schedule_time); }
            source.enqueue(key, true, schedule_time);
            storage.previous_key = std::move(key);
            storage.has_previous = true;
        }

        void capture_subscription_key(
            const SubscriptionKeyCaptureContext &context,
            const NodeView &view,
            DateTime evaluation_time,
            DateTime schedule_time)
        {
            auto input  = view.input(evaluation_time);
            auto bundle = input.as_bundle();
            auto key    = bundle.at("key");
            auto source = recover_source_view(view, evaluation_time);
            record_subscription_key(source, capture_storage_of(view, context), key, schedule_time);
        }

        void subscription_key_capture_stop(const NodeView &view, DateTime evaluation_time)
        {
            const auto *context = static_cast<const SubscriptionKeyCaptureContext *>(
                view.binding()->ops_ref().extended_view_context);
            auto &storage = capture_storage_of(view, *context);
            if (!storage.has_previous) { return; }

            auto source = recover_source_view(view, evaluation_time);
            source.enqueue(std::move(storage.previous_key), false, evaluation_time + MIN_TD);
            storage.previous_key = Value{};
            storage.has_previous = false;
        }

        [[nodiscard]] std::string subscription_key_path(std::string path)
        {
            if (path.empty()) { throw std::invalid_argument("subscription key path must not be empty"); }
            return path;
        }
    }  // namespace

    NodeBuilder make_subscription_key_source_node(std::string path, const ValueTypeMetaData &key_schema)
    {
        path = subscription_key_path(std::move(path));

        auto       &registry      = TypeRegistry::instance();
        const auto *output_schema = registry.tss(&key_schema);

        NodeTypeDescriptor descriptor;
        descriptor.schema.display_name  = "subscription_key_source";
        descriptor.schema.output_schema = output_schema;
        descriptor.schema.node_kind     = NodeKind::PullSource;

        const std::array fields{NodeStorageField{
            .name = subscription_key_source_storage_field,
            .plan = &MemoryUtils::plan_for<SubscriptionKeySourceStorage>(),
        }};
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        const auto *context = &register_subscription_key_source_context(
            path, descriptor.storage_plan->component(subscription_key_source_storage_field).offset);

        descriptor.callbacks.stop            = &subscription_key_source_stop;
        descriptor.ops.evaluate_impl         = &subscription_key_source_evaluate_impl;
        descriptor.ops.extended_view_type_id = SubscriptionKeySourceView::node_view_type_id();
        descriptor.ops.extended_view_context = context;

        NodeBuilder builder = NodeBuilder::from_descriptor(std::move(descriptor));
        builder.label(std::string{"subscription_key_source:"} + context->path);
        return builder;
    }

    NodeBuilder make_subscription_key_capture_node(std::string path, const ValueTypeMetaData &key_schema)
    {
        path = subscription_key_path(std::move(path));

        auto       &registry            = TypeRegistry::instance();
        const auto *key_ts_schema       = registry.ts(&key_schema);
        const auto *subscription_schema = registry.tss(&key_schema);

        NodeTypeDescriptor descriptor;
        descriptor.schema.display_name = "subscription_key_capture";
        descriptor.schema.input_schema = registry.un_named_tsb({
            {"key", key_ts_schema},
            {"subscriptions", subscription_schema},
        });
        descriptor.schema.node_kind    = NodeKind::Sink;
        descriptor.schema.active_inputs = std::vector<std::size_t>{0};
        descriptor.schema.valid_inputs  = std::vector<std::size_t>{};

        const std::array fields{NodeStorageField{
            .name = subscription_key_capture_storage_field,
            .plan = &MemoryUtils::plan_for<SubscriptionKeyCaptureStorage>(),
        }};
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        const auto *context = &register_subscription_key_capture_context(
            path, descriptor.storage_plan->component(subscription_key_capture_storage_field).offset);

        descriptor.callbacks.start = [context](const NodeView &view, DateTime evaluation_time) {
            capture_subscription_key(*context, view, evaluation_time, evaluation_time);
        };
        descriptor.callbacks.evaluate = [context](const NodeView &view, DateTime evaluation_time) {
            capture_subscription_key(*context, view, evaluation_time, evaluation_time + MIN_TD);
        };
        descriptor.callbacks.stop             = &subscription_key_capture_stop;
        descriptor.ops.extended_view_context  = context;

        NodeBuilder builder = NodeBuilder::from_descriptor(std::move(descriptor));
        builder.label(std::string{"subscription_key_capture:"} + context->path);
        return builder;
    }
}  // namespace hgraph

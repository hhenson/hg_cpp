#include <hgraph/runtime/service_node.h>

#include <hgraph/runtime/graph.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/time_series/ts_input/bundle_view.h>
#include <hgraph/types/time_series/ts_input/base_view.h>
#include <hgraph/types/time_series/ts_output/base_view.h>
#include <hgraph/types/time_series/ts_output/dict_view.h>
#include <hgraph/types/time_series/ts_output/set_view.h>
#include <hgraph/types/value/value_builder.h>

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
        constexpr std::string_view request_input_capture_storage_field{"request_input_capture"};

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

        struct RequestInputSourceContext
        {
            std::string path{};
        };

        struct RequestInputCaptureStorage
        {
            bool live{false};
        };

        struct RequestInputCaptureContext
        {
            std::string path{};
            Int         request_id{0};
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

        [[nodiscard]] std::vector<std::unique_ptr<RequestInputSourceContext>> &
        request_input_source_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<RequestInputSourceContext>>;
            return *contexts;
        }

        [[nodiscard]] const RequestInputSourceContext &register_request_input_source_context(std::string path)
        {
            auto context = std::make_unique<RequestInputSourceContext>(RequestInputSourceContext{
                .path = std::move(path),
            });
            const auto *result = context.get();
            request_input_source_contexts().push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] std::vector<std::unique_ptr<RequestInputCaptureContext>> &
        request_input_capture_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<RequestInputCaptureContext>>;
            return *contexts;
        }

        [[nodiscard]] const RequestInputCaptureContext &register_request_input_capture_context(
            std::string path,
            Int request_id,
            std::size_t storage_offset)
        {
            auto context = std::make_unique<RequestInputCaptureContext>(RequestInputCaptureContext{
                .path           = std::move(path),
                .request_id     = request_id,
                .storage_offset = storage_offset,
            });
            const auto *result = context.get();
            request_input_capture_contexts().push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] RequestInputCaptureStorage &capture_storage_of(
            const NodeView &view,
            const RequestInputCaptureContext &context)
        {
            return *MemoryUtils::cast<RequestInputCaptureStorage>(
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

            [[nodiscard]] std::size_t node_index() const noexcept { return view_.node_index(); }

          private:
            SubscriptionKeySourceView(NodeView view, const SubscriptionKeySourceContext *context) noexcept
                : view_(std::move(view)),
                  context_(context)
            {
            }

            NodeView                            view_{};
            const SubscriptionKeySourceContext *context_{nullptr};
        };

        class RequestInputSourceView
        {
          public:
            [[nodiscard]] static const void *node_view_type_id() noexcept
            {
                static const char token{};
                return &token;
            }

            [[nodiscard]] static RequestInputSourceView from_node(NodeView view, const void *context)
            {
                if (context == nullptr)
                {
                    throw std::logic_error("RequestInputSourceView requires a typed view context");
                }
                return RequestInputSourceView{
                    std::move(view),
                };
            }

            void set(Int request_id, Value delta, DateTime schedule_time) const
            {
                Value request_id_value{request_id};
                auto  state    = view_.state().as_bundle().begin_mutation();
                auto  removed  = state.field("removed").as_mutable_set();
                auto  modified = state.field("modified").as_mutable_map();

                static_cast<void>(removed.remove(request_id_value.view()));
                modified.set_item(request_id_value.view(), delta.view());
                schedule(schedule_time);
            }

            void remove(Int request_id, DateTime schedule_time) const
            {
                Value request_id_value{request_id};
                auto  state    = view_.state().as_bundle().begin_mutation();
                auto  removed  = state.field("removed").as_mutable_set();
                auto  modified = state.field("modified").as_mutable_map();

                static_cast<void>(modified.remove(request_id_value.view()));
                static_cast<void>(removed.add(request_id_value.view()));
                schedule(schedule_time);
            }

            [[nodiscard]] std::size_t node_index() const noexcept { return view_.node_index(); }

          private:
            explicit RequestInputSourceView(NodeView view) noexcept
                : view_(std::move(view))
            {
            }

            void schedule(DateTime schedule_time) const
            {
                GraphValue *graph = view_.graph_value();
                if (graph == nullptr)
                {
                    throw std::logic_error("request input source node is not attached to a graph");
                }
                graph->schedule_node(view_.node_index(), schedule_time);
            }

            NodeView view_{};
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

        [[nodiscard]] RequestInputSourceView recover_request_source_view(
            const NodeView &capture,
            DateTime evaluation_time)
        {
            auto input    = capture.input(evaluation_time);
            auto bundle   = input.as_bundle();
            auto requests = bundle.at("requests");
            if (!requests.bound())
            {
                throw std::logic_error("request input capture requires a bound requests source");
            }

            NodeView source = requests.bound_output().owner_node();
            if (!source.valid() || !source.is<RequestInputSourceView>())
            {
                throw std::logic_error("request input capture is not bound to a request input source");
            }
            return source.as<RequestInputSourceView>();
        }

        /**
         * Request stubs (subscription keys, request/reply requests) are the
         * sanctioned NEXT-cycle forwarders: their pairing with the service
         * source is deliberately rank-free (no rank dependency at wiring), so
         * the temporal break here — rather than a wiring edge — is what allows
         * a client's request to derive from the service's own response.
         * Capture during ``start`` schedules for the current engine time so
         * the first cycle publishes. (Shared-output relays are the opposite:
         * rank-correct and same-cycle — see shared_output_node.cpp.)
         */
        [[nodiscard]] DateTime request_stub_forward_time(DateTime evaluation_time, bool start_phase)
        {
            return start_phase ? evaluation_time : evaluation_time + MIN_TD;
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

        [[nodiscard]] ValueTypeRef value_binding_for(const ValueTypeMetaData *schema, const char *context)
        {
            if (schema == nullptr)
            {
                throw std::logic_error(std::string{context} + ": value schema is null");
            }
            const auto binding = ValuePlanFactory::instance().type_for(schema);
            if (!binding)
            {
                throw std::logic_error(std::string{context} + ": value schema has no binding");
            }
            return binding;
        }

        [[nodiscard]] const ValueTypeMetaData *request_input_state_schema(const TSValueTypeMetaData &request_schema)
        {
            if (request_schema.delta_value_schema == nullptr)
            {
                throw std::invalid_argument("request input source requires a request delta schema");
            }

            auto       &registry          = TypeRegistry::instance();
            const auto *request_id_schema = registry.register_scalar<Int>("int");
            const auto *removed_schema    = registry.mutable_set(request_id_schema);
            const auto *modified_schema   = registry.mutable_map(request_id_schema, request_schema.delta_value_schema);
            return registry.un_named_bundle({{"removed", removed_schema}, {"modified", modified_schema}});
        }

        [[nodiscard]] bool request_input_state_empty(const ValueView &state)
        {
            auto bundle = state.as_bundle();
            return bundle.field("removed").as_set().empty() &&
                   bundle.field("modified").as_map().empty();
        }

        void clear_request_input_state(const ValueView &state)
        {
            auto bundle = state.as_bundle().begin_mutation();
            bundle.field("removed").as_mutable_set().clear();
            bundle.field("modified").as_mutable_map().clear();
        }

        [[nodiscard]] Value request_input_delta_from_state(const TSValueTypeMetaData &output_schema,
                                                           const ValueView &state)
        {
            if (output_schema.delta_value_schema == nullptr ||
                output_schema.delta_value_schema->value_kind() != ValueTypeKind::Bundle ||
                output_schema.delta_value_schema->field_count != 2)
            {
                throw std::logic_error("request input source output has no TSD delta schema");
            }

            const auto state_bundle = state.as_bundle();
            const auto removed_state  = state_bundle.field("removed").as_set();
            const auto modified_state = state_bundle.field("modified").as_map();

            const ValueTypeMetaData *delta_schema    = output_schema.delta_value_schema;
            const ValueTypeMetaData *removed_schema  = delta_schema->fields[0].type;
            const ValueTypeMetaData *modified_schema = delta_schema->fields[1].type;
            if (removed_schema == nullptr || modified_schema == nullptr)
            {
                throw std::logic_error("request input source output delta schema is incomplete");
            }

            const auto &key_binding      = value_binding_for(removed_schema->element_type, "request input delta");
            const auto &request_binding  = value_binding_for(modified_schema->element_type, "request input delta");
            const auto &bundle_binding   = value_binding_for(delta_schema, "request input delta");

            SetBuilder removed{key_binding};
            for (const auto &key : removed_state.values()) { static_cast<void>(removed.insert_copy(key.data())); }

            MapBuilder modified{key_binding, request_binding};
            for (const auto &[request_id, request_delta] : modified_state.items())
            {
                modified.set_item_copy(request_id.data(), request_delta.data());
            }

            BundleBuilder bundle{bundle_binding};
            bundle.set("removed", removed.build());
            bundle.set("modified", modified.build());
            return bundle.build();
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

        bool request_input_source_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }

            if (request_input_state_empty(view.state())) { return true; }

            auto  output = view.output(evaluation_time);
            Value delta  = request_input_delta_from_state(*output.schema(), view.state());
            apply_delta(output, delta.view());
            clear_request_input_state(view.state());
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

        void request_input_source_stop(const NodeView &view, DateTime evaluation_time)
        {
            clear_request_input_state(view.state());

            auto output   = view.output(evaluation_time);
            auto dict     = output.as_dict();
            auto mutation = dict.begin_mutation(evaluation_time);
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
            bool start_phase)
        {
            auto input  = view.input(evaluation_time);
            auto bundle = input.as_bundle();
            auto key    = bundle.at("key");
            auto source = recover_source_view(view, evaluation_time);
            const DateTime schedule_time = request_stub_forward_time(evaluation_time, start_phase);
            record_subscription_key(source, capture_storage_of(view, context), key, schedule_time);
        }

        void capture_request_input(
            const RequestInputCaptureContext &context,
            const NodeView &view,
            DateTime evaluation_time,
            bool start_phase,
            bool force)
        {
            auto input   = view.input(evaluation_time);
            auto bundle  = input.as_bundle();
            auto request = bundle.at("request");
            auto source  = recover_request_source_view(view, evaluation_time);
            auto &storage = capture_storage_of(view, context);
            const DateTime schedule_time = request_stub_forward_time(evaluation_time, start_phase);

            if (!force && !request.modified()) { return; }

            if (request.valid())
            {
                source.set(context.request_id, capture_delta(request), schedule_time);
                storage.live = true;
                return;
            }

            if (storage.live)
            {
                source.remove(context.request_id, schedule_time);
                storage.live = false;
            }
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

        void request_input_capture_stop(const NodeView &view, DateTime evaluation_time)
        {
            const auto *context = static_cast<const RequestInputCaptureContext *>(
                view.binding()->ops_ref().extended_view_context);
            auto &storage = capture_storage_of(view, *context);
            if (!storage.live) { return; }

            auto source = recover_request_source_view(view, evaluation_time);
            source.remove(context->request_id, evaluation_time + MIN_TD);
            storage.live = false;
        }

        [[nodiscard]] std::string subscription_key_path(std::string path)
        {
            if (path.empty()) { throw std::invalid_argument("subscription key path must not be empty"); }
            return path;
        }

        [[nodiscard]] std::string request_input_path(std::string path)
        {
            if (path.empty()) { throw std::invalid_argument("request input path must not be empty"); }
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
            capture_subscription_key(*context, view, evaluation_time, true);
        };
        descriptor.callbacks.evaluate = [context](const NodeView &view, DateTime evaluation_time) {
            capture_subscription_key(*context, view, evaluation_time, false);
        };
        descriptor.callbacks.stop             = &subscription_key_capture_stop;
        descriptor.ops.extended_view_context  = context;

        NodeBuilder builder = NodeBuilder::from_descriptor(std::move(descriptor));
        builder.label(std::string{"subscription_key_capture:"} + context->path);
        return builder;
    }

    NodeBuilder make_request_input_source_node(std::string path, const TSValueTypeMetaData &request_schema)
    {
        path = request_input_path(std::move(path));

        auto       &registry      = TypeRegistry::instance();
        const auto *request_id    = registry.register_scalar<Int>("int");
        const auto *output_schema = registry.tsd(request_id, &request_schema);

        NodeTypeDescriptor descriptor;
        descriptor.schema.display_name  = "request_input_source";
        descriptor.schema.output_schema = output_schema;
        descriptor.schema.state_schema  = request_input_state_schema(request_schema);
        descriptor.schema.node_kind     = NodeKind::PullSource;

        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema);

        const auto *context = &register_request_input_source_context(path);

        descriptor.callbacks.stop            = &request_input_source_stop;
        descriptor.ops.evaluate_impl         = &request_input_source_evaluate_impl;
        descriptor.ops.extended_view_type_id = RequestInputSourceView::node_view_type_id();
        descriptor.ops.extended_view_context = context;

        NodeBuilder builder = NodeBuilder::from_descriptor(std::move(descriptor));
        builder.label(std::string{"request_input_source:"} + context->path);
        return builder;
    }

    NodeBuilder make_request_input_capture_node(
        std::string path,
        const TSValueTypeMetaData &request_schema,
        Int request_id)
    {
        path = request_input_path(std::move(path));

        auto       &registry        = TypeRegistry::instance();
        const auto *request_id_meta = registry.register_scalar<Int>("int");
        const auto *requests_schema = registry.tsd(request_id_meta, &request_schema);

        NodeTypeDescriptor descriptor;
        descriptor.schema.display_name = "request_input_capture";
        descriptor.schema.input_schema = registry.un_named_tsb({
            {"request", &request_schema},
            {"requests", requests_schema},
        });
        descriptor.schema.node_kind     = NodeKind::Sink;
        descriptor.schema.active_inputs = std::vector<std::size_t>{0};
        descriptor.schema.valid_inputs  = std::vector<std::size_t>{};

        const std::array fields{NodeStorageField{
            .name = request_input_capture_storage_field,
            .plan = &MemoryUtils::plan_for<RequestInputCaptureStorage>(),
        }};
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        const auto *context = &register_request_input_capture_context(
            path, request_id, descriptor.storage_plan->component(request_input_capture_storage_field).offset);

        descriptor.callbacks.start = [context](const NodeView &view, DateTime evaluation_time) {
            capture_request_input(*context, view, evaluation_time, true, true);
        };
        descriptor.callbacks.evaluate = [context](const NodeView &view, DateTime evaluation_time) {
            capture_request_input(*context, view, evaluation_time, false, false);
        };
        descriptor.callbacks.stop            = &request_input_capture_stop;
        descriptor.ops.extended_view_context = context;

        NodeBuilder builder = NodeBuilder::from_descriptor(std::move(descriptor));
        builder.label(std::string{"request_input_capture:"} + context->path);
        return builder;
    }
}  // namespace hgraph

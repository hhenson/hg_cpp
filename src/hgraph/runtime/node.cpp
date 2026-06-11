#include <hgraph/runtime/node.h>

#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <cassert>
#include <deque>
#include <memory>
#include <stdexcept>
#include <utility>

namespace hgraph
{
    namespace
    {
        void schedule_node_from_storage(GraphValue *graph, std::size_t node_index, DateTime modified_time);

        struct NodeRuntimeStorage final : Notifiable
        {
            NodeRuntimeStorage(const NodeTypeMetaData &schema, std::string runtime_label)
                : label(std::move(runtime_label))
            {
                if (label.empty() && schema.display_name != nullptr) { label = schema.display_name; }
            }

            void notify(DateTime modified_time) override
            {
                schedule_node_from_storage(graph, node_index, modified_time);
            }

            GraphValue   *graph{nullptr};
            std::size_t   node_index{0};
            std::string   label{};
            bool          started{false};
            bool          starting{false};
        };

        void schedule_node_from_storage(GraphValue *graph, std::size_t node_index, DateTime modified_time)
        {
            if (graph == nullptr) { return; }
            const DateTime when =
                modified_time != MIN_DT ? std::max(modified_time, graph->view().evaluation_time()) : graph->view().evaluation_time();
            graph->schedule_node(node_index, when);
        }

        struct NodeRuntimeLayout
        {
            static constexpr std::size_t npos = static_cast<std::size_t>(-1);

            std::size_t storage_offset{0};
            std::size_t input_offset{npos};
            std::size_t output_offset{npos};
            std::size_t state_offset{npos};
            std::size_t scalars_offset{npos};
            std::size_t scheduler_offset{npos};
            std::size_t global_state_offset{npos};
            std::size_t evaluation_clock_offset{npos};
            std::size_t error_output_offset{npos};
            std::size_t recordable_state_offset{npos};

            [[nodiscard]] bool has_input() const noexcept { return input_offset != npos; }
            [[nodiscard]] bool has_output() const noexcept { return output_offset != npos; }
            [[nodiscard]] bool has_state() const noexcept { return state_offset != npos; }
            [[nodiscard]] bool has_scalars() const noexcept { return scalars_offset != npos; }
            [[nodiscard]] bool has_scheduler() const noexcept { return scheduler_offset != npos; }
            [[nodiscard]] bool has_global_state() const noexcept { return global_state_offset != npos; }
            [[nodiscard]] bool has_evaluation_clock() const noexcept { return evaluation_clock_offset != npos; }
            [[nodiscard]] bool has_error_output() const noexcept { return error_output_offset != npos; }
            [[nodiscard]] bool has_recordable_state() const noexcept { return recordable_state_offset != npos; }
        };

        struct NodeRuntimeContext
        {
            NodeCallbacks                    callbacks{};
            NodeRuntimeLayout                layout{};
            const MemoryUtils::StoragePlan  *plan{nullptr};
        };

        [[nodiscard]] const NodeRuntimeContext &runtime_context(const void *context)
        {
            if (context == nullptr) { throw std::logic_error("Node runtime context is null"); }
            return *static_cast<const NodeRuntimeContext *>(context);
        }

        [[nodiscard]] void *node_component(void *memory, std::size_t offset)
        {
            if (memory == nullptr) { throw std::logic_error("Node storage is null"); }
            if (offset == NodeRuntimeLayout::npos) { throw std::logic_error("Node component is not present"); }
            return MemoryUtils::advance(memory, offset);
        }

        [[nodiscard]] const void *node_component(const void *memory, std::size_t offset)
        {
            if (memory == nullptr) { throw std::logic_error("Node storage is null"); }
            if (offset == NodeRuntimeLayout::npos) { throw std::logic_error("Node component is not present"); }
            return MemoryUtils::advance(memory, offset);
        }

        [[nodiscard]] NodeRuntimeStorage &node_storage(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<NodeRuntimeStorage>(node_component(memory, context.layout.storage_offset));
        }

        [[nodiscard]] const NodeRuntimeStorage &node_storage(const NodeRuntimeContext &context, const void *memory)
        {
            return *MemoryUtils::cast<NodeRuntimeStorage>(node_component(memory, context.layout.storage_offset));
        }

        [[nodiscard]] TSInput &node_input(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<TSInput>(node_component(memory, context.layout.input_offset));
        }

        [[nodiscard]] TSOutput &node_output(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<TSOutput>(node_component(memory, context.layout.output_offset));
        }

        [[nodiscard]] Value &node_state(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<Value>(node_component(memory, context.layout.state_offset));
        }

        [[nodiscard]] Value &node_scalars(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<Value>(node_component(memory, context.layout.scalars_offset));
        }

        [[nodiscard]] NodeSchedulerState &node_scheduler_state(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<NodeSchedulerState>(node_component(memory, context.layout.scheduler_offset));
        }

        [[nodiscard]] std::optional<GlobalStateView> &node_global_state_view(const NodeRuntimeContext &context,
                                                                             void *memory)
        {
            return *MemoryUtils::cast<std::optional<GlobalStateView>>(
                node_component(memory, context.layout.global_state_offset));
        }

        [[nodiscard]] EvaluationClockStorageRef &node_evaluation_clock_ref(const NodeRuntimeContext &context,
                                                                           void *memory)
        {
            return *MemoryUtils::cast<EvaluationClockStorageRef>(
                node_component(memory, context.layout.evaluation_clock_offset));
        }

        [[nodiscard]] TSOutput &node_error_output(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<TSOutput>(node_component(memory, context.layout.error_output_offset));
        }

        [[nodiscard]] TSOutput &node_recordable_state(const NodeRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<TSOutput>(node_component(memory, context.layout.recordable_state_offset));
        }

        [[nodiscard]] const NodeCallbacks &callbacks(const void *context)
        {
            return runtime_context(context).callbacks;
        }

        [[nodiscard]] TSEndpointSchema default_input_endpoint(const TSValueTypeMetaData &schema)
        {
            if (schema.kind != TSTypeKind::TSB)
            {
                return TSEndpointSchema::peered(&schema);
            }

            std::vector<TSEndpointSchema> children;
            children.reserve(schema.field_count());
            for (std::size_t index = 0; index < schema.field_count(); ++index)
            {
                children.push_back(TSEndpointSchema::peered(schema.fields()[index].type));
            }
            return TSEndpointSchema::non_peered(&schema, std::move(children));
        }

        [[nodiscard]] const TSInputBuilder &input_builder_for(const TSValueTypeMetaData &schema,
                                                              TSEndpointSchema endpoint)
        {
            if (endpoint.empty()) { endpoint = default_input_endpoint(schema); }
            return TSInputBuilderFactory::checked_builder_for(schema, endpoint);
        }

        [[nodiscard]] const ValueTypeBinding &state_binding_for(const ValueTypeMetaData *schema)
        {
            if (schema == nullptr) { throw std::logic_error("Node state schema is null"); }
            const auto *binding = ValuePlanFactory::instance().binding_for(schema);
            if (binding == nullptr)
            {
                throw std::logic_error("Node state schema has no value binding");
            }
            return *binding;
        }

        void destroy_constructed_components(
            const std::vector<const MemoryUtils::CompositeComponent *> &constructed,
            void *memory) noexcept
        {
            for (std::size_t index = constructed.size(); index > 0; --index)
            {
                const auto &component = *constructed[index - 1];
                component.plan->destroy(MemoryUtils::advance(memory, component.offset));
            }
        }

        void construct_node_storage_impl(const NodeRuntimeContext &context,
                                         const NodeTypeMetaData   &schema,
                                         TSEndpointSchema          input_endpoint,
                                         TSEndpointSchema          output_endpoint_override,
                                         std::string               runtime_label,
                                         const Value              &scalars,
                                         void                     *memory)
        {
            if (context.plan == nullptr) { throw std::logic_error("Node runtime context has no storage plan"); }
            const MemoryUtils::StoragePlan &plan = *context.plan;

            std::vector<const MemoryUtils::CompositeComponent *> constructed;
            constructed.reserve(9);
            auto rollback = make_scope_exit([&]() noexcept {
                destroy_constructed_components(constructed, memory);
            });

            const auto *runtime_storage_component = plan.find_component("runtime_storage");
            if (runtime_storage_component == nullptr)
            {
                throw std::logic_error("Node storage plan is missing runtime_storage");
            }
            std::construct_at(MemoryUtils::cast<NodeRuntimeStorage>(
                                  MemoryUtils::advance(memory, runtime_storage_component->offset)),
                              schema, std::move(runtime_label));
            constructed.push_back(runtime_storage_component);

            if (context.layout.has_input())
            {
                const auto *component = plan.find_component("input");
                if (component == nullptr) { throw std::logic_error("Node storage plan is missing input"); }
                const auto &input_builder = input_builder_for(*schema.input_schema, std::move(input_endpoint));
                std::construct_at(MemoryUtils::cast<TSInput>(
                                      MemoryUtils::advance(memory, component->offset)),
                                  input_builder);
                constructed.push_back(component);
            }

            if (context.layout.has_output())
            {
                const auto *component = plan.find_component("output");
                if (component == nullptr) { throw std::logic_error("Node storage plan is missing output"); }
                const TSEndpointSchema &output_endpoint =
                    !output_endpoint_override.empty() ? output_endpoint_override : schema.output_endpoint_schema;
                if (output_endpoint.empty())
                {
                    std::construct_at(MemoryUtils::cast<TSOutput>(
                                          MemoryUtils::advance(memory, component->offset)),
                                      *schema.output_schema);
                }
                else
                {
                    std::construct_at(MemoryUtils::cast<TSOutput>(
                                          MemoryUtils::advance(memory, component->offset)),
                                      output_endpoint);
                }
                constructed.push_back(component);
            }

            if (context.layout.has_state())
            {
                const auto *component = plan.find_component("state");
                if (component == nullptr) { throw std::logic_error("Node storage plan is missing state"); }
                std::construct_at(MemoryUtils::cast<Value>(
                                      MemoryUtils::advance(memory, component->offset)),
                                  state_binding_for(schema.state_schema));
                constructed.push_back(component);
            }

            if (context.layout.has_scalars())
            {
                const auto *component = plan.find_component("scalars");
                if (component == nullptr) { throw std::logic_error("Node storage plan is missing scalars"); }
                if (!scalars.has_value())
                {
                    throw std::logic_error("Node has a scalar schema but no scalar configuration value was provided");
                }
                std::construct_at(MemoryUtils::cast<Value>(
                                      MemoryUtils::advance(memory, component->offset)),
                                  scalars);   // copy the per-instance scalar configuration
                constructed.push_back(component);
            }

            if (context.layout.has_scheduler())
            {
                const auto *component = plan.find_component("scheduler");
                if (component == nullptr) { throw std::logic_error("Node storage plan is missing scheduler"); }
                std::construct_at(MemoryUtils::cast<NodeSchedulerState>(
                                      MemoryUtils::advance(memory, component->offset)));
                constructed.push_back(component);
            }

            if (context.layout.has_global_state())
            {
                const auto *component = plan.find_component("global_state");
                if (component == nullptr) { throw std::logic_error("Node storage plan is missing global_state"); }
                std::construct_at(MemoryUtils::cast<std::optional<GlobalStateView>>(
                                      MemoryUtils::advance(memory, component->offset)));
                constructed.push_back(component);
            }

            if (context.layout.has_evaluation_clock())
            {
                const auto *component = plan.find_component("evaluation_clock");
                if (component == nullptr) { throw std::logic_error("Node storage plan is missing evaluation_clock"); }
                std::construct_at(MemoryUtils::cast<EvaluationClockStorageRef>(
                                      MemoryUtils::advance(memory, component->offset)));
                constructed.push_back(component);
            }

            if (context.layout.has_error_output())
            {
                const auto *component = plan.find_component("error_output");
                if (component == nullptr) { throw std::logic_error("Node storage plan is missing error_output"); }
                std::construct_at(MemoryUtils::cast<TSOutput>(
                                      MemoryUtils::advance(memory, component->offset)),
                                  *schema.error_output_schema);
                constructed.push_back(component);
            }

            if (context.layout.has_recordable_state())
            {
                const auto *component = plan.find_component("recordable_state");
                if (component == nullptr) { throw std::logic_error("Node storage plan is missing recordable_state"); }
                std::construct_at(MemoryUtils::cast<TSOutput>(
                                      MemoryUtils::advance(memory, component->offset)),
                                  *schema.recordable_state_schema);
                constructed.push_back(component);
            }

            for (const MemoryUtils::CompositeComponent &component : plan.components())
            {
                const auto constructed_it = std::find(constructed.begin(), constructed.end(), &component);
                if (constructed_it != constructed.end()) { continue; }
                if (component.plan == nullptr)
                {
                    throw std::logic_error("Node storage plan component is missing a child plan");
                }
                component.plan->default_construct(MemoryUtils::advance(memory, component.offset));
                constructed.push_back(&component);
            }

            rollback.release();
        }

        [[nodiscard]] NodeRuntimeLayout layout_for(const MemoryUtils::StoragePlan &plan)
        {
            NodeRuntimeLayout layout;
            layout.storage_offset = plan.component("runtime_storage").offset;

            // Optional components: recorded only when the schema declares them.
            const std::pair<const char *, std::size_t NodeRuntimeLayout::*> optional_components[]{
                {"input", &NodeRuntimeLayout::input_offset},
                {"output", &NodeRuntimeLayout::output_offset},
                {"state", &NodeRuntimeLayout::state_offset},
                {"scalars", &NodeRuntimeLayout::scalars_offset},
                {"scheduler", &NodeRuntimeLayout::scheduler_offset},
                {"global_state", &NodeRuntimeLayout::global_state_offset},
                {"evaluation_clock", &NodeRuntimeLayout::evaluation_clock_offset},
                {"error_output", &NodeRuntimeLayout::error_output_offset},
                {"recordable_state", &NodeRuntimeLayout::recordable_state_offset},
            };
            for (const auto &[name, member] : optional_components)
            {
                if (const auto *component = plan.find_component(name); component != nullptr)
                {
                    layout.*member = component->offset;
                }
            }
            return layout;
        }

        void activate_input_slots(const NodeView &view, DateTime evaluation_time)
        {
            if (!view.has_input()) { return; }

            const auto *schema = view.schema()->input_schema;
            auto        input  = view.input(evaluation_time);
            if (schema == nullptr || schema->kind != TSTypeKind::TSB)
            {
                input.make_active();
                return;
            }

            auto bundle = input.as_bundle();
            const auto &slots = view.schema()->active_inputs;
            if (!slots.has_value())
            {
                for (std::size_t slot = 0; slot < schema->field_count(); ++slot) { bundle[slot].make_active(); }
                return;
            }

            for (const std::size_t slot : *slots)
            {
                if (slot >= schema->field_count()) { throw std::out_of_range("Node active input selector is out of range"); }
                bundle[slot].make_active();
            }
        }

        void deactivate_input_slots(const NodeView &view, DateTime evaluation_time)
        {
            if (!view.has_input()) { return; }

            const auto *schema = view.schema()->input_schema;
            auto        input  = view.input(evaluation_time);
            if (schema == nullptr || schema->kind != TSTypeKind::TSB)
            {
                input.make_passive();
                return;
            }

            auto bundle = input.as_bundle();
            const auto &slots = view.schema()->active_inputs;
            if (!slots.has_value())
            {
                for (std::size_t slot = 0; slot < schema->field_count(); ++slot) { bundle[slot].make_passive(); }
                return;
            }

            for (const std::size_t slot : *slots)
            {
                if (slot >= schema->field_count()) { throw std::out_of_range("Node active input selector is out of range"); }
                bundle[slot].make_passive();
            }
        }

        [[nodiscard]] TSInputView input_slot(const NodeView &view, std::size_t slot, DateTime evaluation_time)
        {
            const auto *schema = view.schema()->input_schema;
            if (schema == nullptr || schema->kind != TSTypeKind::TSB)
            {
                throw std::logic_error("Node input selectors require a TSB root input schema");
            }
            if (slot >= schema->field_count()) { throw std::out_of_range("Node input selector is out of range"); }
            auto root = view.input(evaluation_time);
            auto input = root.as_bundle();
            return input[slot];
        }

        [[nodiscard]] bool ready_to_evaluate(const NodeView &view, DateTime evaluation_time)
        {
            if (!view.has_input()) { return true; }

            const auto *schema = view.schema()->input_schema;
            const auto &valid_slots = view.schema()->valid_inputs;
            if (valid_slots.has_value())
            {
                for (const std::size_t slot : *valid_slots)
                {
                    if (!input_slot(view, slot, evaluation_time).valid()) { return false; }
                }
            }
            else if (schema != nullptr && schema->kind == TSTypeKind::TSB)
            {
                for (std::size_t slot = 0; slot < schema->field_count(); ++slot)
                {
                    if (!input_slot(view, slot, evaluation_time).valid()) { return false; }
                }
            }
            else if (!view.input(evaluation_time).valid())
            {
                return false;
            }

            for (const std::size_t slot : view.schema()->all_valid_inputs)
            {
                if (!input_slot(view, slot, evaluation_time).all_valid()) { return false; }
            }

            return true;
        }

        void attach_graph_impl(const void *context, void *memory, GraphValue *graph, std::size_t node_index)
        {
            auto &state = node_storage(runtime_context(context), memory);
            state.graph = graph;
            state.node_index = node_index;
        }

        GraphValue *graph_impl(const void *context, const void *memory) noexcept
        {
            return node_storage(runtime_context(context), memory).graph;
        }

        std::size_t node_index_impl(const void *context, const void *memory) noexcept
        {
            return node_storage(runtime_context(context), memory).node_index;
        }

        std::string_view label_impl(const void *context, const void *memory) noexcept
        {
            return node_storage(runtime_context(context), memory).label;
        }

        bool started_impl(const void *context, const void *memory) noexcept
        {
            return node_storage(runtime_context(context), memory).started;
        }

        bool has_input_impl(const void *context, const void *memory) noexcept
        {
            static_cast<void>(memory);
            return runtime_context(context).layout.has_input();
        }

        bool has_output_impl(const void *context, const void *memory) noexcept
        {
            static_cast<void>(memory);
            return runtime_context(context).layout.has_output();
        }

        bool has_state_impl(const void *context, const void *memory) noexcept
        {
            static_cast<void>(memory);
            return runtime_context(context).layout.has_state();
        }

        bool has_scalars_impl(const void *context, const void *memory) noexcept
        {
            static_cast<void>(memory);
            return runtime_context(context).layout.has_scalars();
        }

        bool has_scheduler_impl(const void *context, const void *memory) noexcept
        {
            static_cast<void>(memory);
            return runtime_context(context).layout.has_scheduler();
        }

        bool has_error_output_impl(const void *context, const void *memory) noexcept
        {
            static_cast<void>(memory);
            return runtime_context(context).layout.has_error_output();
        }

        bool has_recordable_state_impl(const void *context, const void *memory) noexcept
        {
            static_cast<void>(memory);
            return runtime_context(context).layout.has_recordable_state();
        }

        TSInputView input_view_impl(const void *context, void *memory, DateTime evaluation_time)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_input()) { throw std::logic_error("Node has no input"); }
            return node_input(runtime, memory).view(&node_storage(runtime, memory), evaluation_time);
        }

        TSOutputView output_view_impl(const void *context, void *memory, DateTime evaluation_time)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_output()) { throw std::logic_error("Node has no output"); }
            return node_output(runtime, memory).view(evaluation_time);
        }

        ValueView state_view_impl(const void *context, void *memory)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_state()) { throw std::logic_error("Node has no state"); }
            return node_state(runtime, memory).view();
        }

        ValueView scalars_view_impl(const void *context, void *memory)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_scalars()) { throw std::logic_error("Node has no scalar configuration"); }
            return node_scalars(runtime, memory).view();
        }

        NodeSchedulerState *scheduler_state_impl(const void *context, void *memory)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_scheduler()) { throw std::logic_error("Node has no scheduler"); }
            return &node_scheduler_state(runtime, memory);
        }

        GlobalStateView global_state_view_impl(const void *context, void *memory)
        {
            const auto &runtime = runtime_context(context);
            auto       &state   = node_storage(runtime, memory);
            if (!runtime.layout.has_global_state())
            {
                if (state.graph == nullptr)
                {
                    throw std::logic_error("Node global state requires an attached graph");
                }
                return state.graph->view().root().global_state();
            }

            auto &cached = node_global_state_view(runtime, memory);
            if (!cached.has_value())
            {
                if (state.graph == nullptr)
                {
                    throw std::logic_error("Node global state cache requires an attached graph");
                }
                cached.emplace(state.graph->view().root().global_state());
            }
            return *cached;
        }

        EvaluationClockStorageRef evaluation_clock_ref_impl(const void *context, void *memory)
        {
            const auto &runtime = runtime_context(context);
            auto       &state   = node_storage(runtime, memory);
            if (!runtime.layout.has_evaluation_clock())
            {
                if (state.graph == nullptr)
                {
                    throw std::logic_error("Node evaluation clock requires an attached graph");
                }
                return state.graph->view().executor().evaluation_clock_ref();
            }

            auto &cached = node_evaluation_clock_ref(runtime, memory);
            if (!cached.has_value())
            {
                if (state.graph == nullptr)
                {
                    throw std::logic_error("Node evaluation clock cache requires an attached graph");
                }
                cached = state.graph->view().executor().evaluation_clock_ref();
            }
            return cached;
        }

        TSOutputView error_output_view_impl(const void *context, void *memory, DateTime evaluation_time)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_error_output()) { throw std::logic_error("Node has no error output"); }
            return node_error_output(runtime, memory).view(evaluation_time);
        }

        TSOutputView recordable_state_view_impl(const void *context, void *memory, DateTime evaluation_time)
        {
            const auto &runtime = runtime_context(context);
            if (!runtime.layout.has_recordable_state()) { throw std::logic_error("Node has no recordable state output"); }
            return node_recordable_state(runtime, memory).view(evaluation_time);
        }

        void start_impl(const void *context, const NodeView &view, DateTime evaluation_time)
        {
            const auto &runtime = runtime_context(context);
            auto &state = node_storage(runtime, view.data());
            if (state.started) { return; }

            std::size_t activated = 0;
            auto rollback = UnwindCleanupGuard([&] {
                static_cast<void>(activated);
                deactivate_input_slots(view, evaluation_time);
            });

            state.starting = true;
            auto clear_starting = make_scope_exit([&] noexcept { state.starting = false; });
            activate_input_slots(view, evaluation_time);
            activated = 1;

            if (callbacks(context).start) { callbacks(context).start(view, evaluation_time); }
            state.started = true;
            rollback.release();

            // Declarative self-scheduling: a node with ``schedule_on_start`` is
            // marked to evaluate in the current cycle (the framework equivalent of
            // doing schedule_now() in a start hook). Done after the user start so
            // the node is fully started before it can be evaluated.
            if (view.schema() != nullptr && view.schema()->schedule_on_start && state.graph != nullptr)
            {
                state.graph->schedule_node(state.node_index, evaluation_time);
            }
        }

        void stop_impl(const void *context, const NodeView &view, DateTime evaluation_time)
        {
            const auto &runtime = runtime_context(context);
            auto &state = node_storage(runtime, view.data());
            if (!state.started) { return; }

            auto mark_stopped = make_scope_exit([&] noexcept { state.started = false; });
            auto deactivate = UnwindCleanupGuard([&] { deactivate_input_slots(view, evaluation_time); });
            if (callbacks(context).stop) { callbacks(context).stop(view, evaluation_time); }
            deactivate.complete();
        }

        void evaluate_impl(const void *context, const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return; }

            // Graph scheduling is the activation gate. Node eval only enforces
            // lifecycle/validity policy, then lets node-specific code decide any
            // additional guards. This mirrors Python's NodeImpl.eval: active inputs
            // schedule the node by notification; eval does not re-poll modified
            // flags.
            const auto         &runtime      = runtime_context(context);
            const bool          has_scheduler = runtime.layout.has_scheduler();
            NodeSchedulerState *scheduler     = has_scheduler ? &node_scheduler_state(runtime, view.data()) : nullptr;
            const bool          scheduled_now = scheduler != nullptr && !scheduler->events.empty() &&
                                       scheduler->events.begin()->first == evaluation_time;

            bool do_eval = ready_to_evaluate(view, evaluation_time);

            if (do_eval)
            {
                if (callbacks(context).evaluate) { callbacks(context).evaluate(view, evaluation_time); }
            }

            if (has_scheduler)
            {
                auto         &graph = *view.graph_value();
                NodeScheduler sched{*scheduler, &graph, view.node_index(), evaluation_time};
                if (scheduled_now)
                {
                    sched.advance();  // consume the fired event(s) and re-arm the next
                }
                else if (sched.is_scheduled())
                {
                    // Ran for another reason (an input ticked): just re-arm the timer.
                    graph.schedule_node(view.node_index(), sched.next_scheduled_time());
                }
            }
        }

        void cleanup_delta_impl(const void *context, const NodeView &view)
        {
            const auto &runtime = runtime_context(context);
            if (runtime.layout.has_output()) { node_output(runtime, view.data()).cleanup_delta(); }
            if (runtime.layout.has_error_output()) { node_error_output(runtime, view.data()).cleanup_delta(); }
            if (runtime.layout.has_recordable_state()) { node_recordable_state(runtime, view.data()).cleanup_delta(); }
        }

        void default_attach_graph_impl(const void *, void *, GraphValue *, std::size_t) {}
        GraphValue *default_graph_impl(const void *, const void *) noexcept { return nullptr; }
        std::size_t default_node_index_impl(const void *, const void *) noexcept { return 0; }
        std::string_view default_label_impl(const void *, const void *) noexcept { return {}; }
        bool default_started_impl(const void *, const void *) noexcept { return false; }

        void default_start_impl(const void *, const NodeView &, DateTime)
        {
            throw std::logic_error("NodeView::start requires a live node");
        }

        void default_stop_impl(const void *, const NodeView &, DateTime)
        {
            throw std::logic_error("NodeView::stop requires a live node");
        }

        void default_evaluate_impl(const void *, const NodeView &, DateTime)
        {
            throw std::logic_error("NodeView::evaluate requires a live node");
        }

        void default_cleanup_delta_impl(const void *, const NodeView &)
        {
            throw std::logic_error("NodeView::cleanup_delta requires a live node");
        }

        bool default_has_input_impl(const void *, const void *) noexcept { return false; }
        bool default_has_output_impl(const void *, const void *) noexcept { return false; }
        bool default_has_state_impl(const void *, const void *) noexcept { return false; }
        bool default_has_scalars_impl(const void *, const void *) noexcept { return false; }
        bool default_has_scheduler_impl(const void *, const void *) noexcept { return false; }
        bool default_has_error_output_impl(const void *, const void *) noexcept { return false; }
        bool default_has_recordable_state_impl(const void *, const void *) noexcept { return false; }

        TSInputView default_input_view_impl(const void *, void *, DateTime)
        {
            throw std::logic_error("NodeView::input requires a live node");
        }

        TSOutputView default_output_view_impl(const void *, void *, DateTime)
        {
            throw std::logic_error("NodeView::output requires a live node");
        }

        ValueView default_state_view_impl(const void *, void *)
        {
            throw std::logic_error("NodeView::state requires a live node");
        }

        ValueView default_scalars_view_impl(const void *, void *)
        {
            throw std::logic_error("NodeView::scalars requires a live node");
        }

        NodeSchedulerState *default_scheduler_state_impl(const void *, void *)
        {
            throw std::logic_error("NodeView::scheduler_state requires a live node");
        }

        GlobalStateView default_global_state_view_impl(const void *, void *)
        {
            throw std::logic_error("NodeView::global_state requires a live node");
        }

        EvaluationClockStorageRef default_evaluation_clock_ref_impl(const void *, void *) noexcept
        {
            return EvaluationClockStorageRef{};
        }

        TSOutputView default_error_output_view_impl(const void *, void *, DateTime)
        {
            throw std::logic_error("NodeView::error_output requires a live node");
        }

        TSOutputView default_recordable_state_view_impl(const void *, void *, DateTime)
        {
            throw std::logic_error("NodeView::recordable_state requires a live node");
        }

        struct NodeRuntimeRegistry
        {
            const NodeTypeBinding &make_binding(
                NodeTypeMetaData schema,
                NodeCallbacks callbacks,
                const MemoryUtils::StoragePlan &plan,
                NodeOps ops)
            {
                names.push_back(std::make_unique<std::string>(
                    schema.display_name != nullptr ? std::string{schema.display_name} : std::string{}));
                if (!names.back()->empty()) { schema.display_name = names.back()->c_str(); }

                contexts.push_back(NodeRuntimeContext{
                    .callbacks = std::move(callbacks),
                    .layout = layout_for(plan),
                    .plan = &plan,
                });
                schemas.push_back(std::move(schema));
                fill_default_ops(ops);
                ops.context = &contexts.back();
                ops_storage.push_back(ops);

                return NodeTypeBinding::intern(schemas.back(), plan, ops_storage.back());
            }

            static void fill_default_ops(NodeOps &ops)
            {
                if (ops.attach_graph_impl == nullptr) { ops.attach_graph_impl = &attach_graph_impl; }
                if (ops.graph_impl == nullptr) { ops.graph_impl = &graph_impl; }
                if (ops.node_index_impl == nullptr) { ops.node_index_impl = &node_index_impl; }
                if (ops.label_impl == nullptr) { ops.label_impl = &label_impl; }
                if (ops.started_impl == nullptr) { ops.started_impl = &started_impl; }
                if (ops.start_impl == nullptr) { ops.start_impl = &start_impl; }
                if (ops.stop_impl == nullptr) { ops.stop_impl = &stop_impl; }
                if (ops.evaluate_impl == nullptr) { ops.evaluate_impl = &evaluate_impl; }
                if (ops.cleanup_delta_impl == nullptr) { ops.cleanup_delta_impl = &cleanup_delta_impl; }
                if (ops.has_input_impl == nullptr) { ops.has_input_impl = &has_input_impl; }
                if (ops.has_output_impl == nullptr) { ops.has_output_impl = &has_output_impl; }
                if (ops.has_state_impl == nullptr) { ops.has_state_impl = &has_state_impl; }
                if (ops.has_scalars_impl == nullptr) { ops.has_scalars_impl = &has_scalars_impl; }
                if (ops.has_scheduler_impl == nullptr) { ops.has_scheduler_impl = &has_scheduler_impl; }
                if (ops.has_error_output_impl == nullptr) { ops.has_error_output_impl = &has_error_output_impl; }
                if (ops.has_recordable_state_impl == nullptr)
                {
                    ops.has_recordable_state_impl = &has_recordable_state_impl;
                }
                if (ops.input_view_impl == nullptr) { ops.input_view_impl = &input_view_impl; }
                if (ops.output_view_impl == nullptr) { ops.output_view_impl = &output_view_impl; }
                if (ops.state_view_impl == nullptr) { ops.state_view_impl = &state_view_impl; }
                if (ops.scalars_view_impl == nullptr) { ops.scalars_view_impl = &scalars_view_impl; }
                if (ops.scheduler_state_impl == nullptr) { ops.scheduler_state_impl = &scheduler_state_impl; }
                if (ops.global_state_view_impl == nullptr) { ops.global_state_view_impl = &global_state_view_impl; }
                if (ops.evaluation_clock_ref_impl == nullptr)
                {
                    ops.evaluation_clock_ref_impl = &evaluation_clock_ref_impl;
                }
                if (ops.error_output_view_impl == nullptr) { ops.error_output_view_impl = &error_output_view_impl; }
                if (ops.recordable_state_view_impl == nullptr)
                {
                    ops.recordable_state_view_impl = &recordable_state_view_impl;
                }
            }

            std::deque<NodeTypeMetaData>                 schemas{};
            std::deque<NodeRuntimeContext>               contexts{};
            std::deque<NodeOps>                          ops_storage{};
            std::vector<std::unique_ptr<std::string>>    names{};
        };

        NodeRuntimeRegistry &node_runtime_registry()
        {
            static NodeRuntimeRegistry registry;
            return registry;
        }

        const NodeOps &default_node_ops()
        {
            static const NodeOps table{
                .context = nullptr,
                .attach_graph_impl = &default_attach_graph_impl,
                .graph_impl = &default_graph_impl,
                .node_index_impl = &default_node_index_impl,
                .label_impl = &default_label_impl,
                .started_impl = &default_started_impl,
                .start_impl = &default_start_impl,
                .stop_impl = &default_stop_impl,
                .evaluate_impl = &default_evaluate_impl,
                .cleanup_delta_impl = &default_cleanup_delta_impl,
                .has_input_impl = &default_has_input_impl,
                .has_output_impl = &default_has_output_impl,
                .has_state_impl = &default_has_state_impl,
                .has_scalars_impl = &default_has_scalars_impl,
                .has_scheduler_impl = &default_has_scheduler_impl,
                .has_error_output_impl = &default_has_error_output_impl,
                .has_recordable_state_impl = &default_has_recordable_state_impl,
                .input_view_impl = &default_input_view_impl,
                .output_view_impl = &default_output_view_impl,
                .state_view_impl = &default_state_view_impl,
                .scalars_view_impl = &default_scalars_view_impl,
                .scheduler_state_impl = &default_scheduler_state_impl,
                .global_state_view_impl = &default_global_state_view_impl,
                .evaluation_clock_ref_impl = &default_evaluation_clock_ref_impl,
                .error_output_view_impl = &default_error_output_view_impl,
                .recordable_state_view_impl = &default_recordable_state_view_impl,
                .extended_view_type_id = nullptr,
                .extended_view_context = nullptr,
            };
            return table;
        }

        const NodeTypeBinding &default_node_binding()
        {
            static const NodeTypeMetaData meta{};
            static const NodeTypeBinding binding{
                .type_meta = &meta,
                .storage_plan = &MemoryUtils::plan_for<std::byte>(),
                .ops = &default_node_ops(),
            };
            return binding;
        }
    }  // namespace

    const MemoryUtils::StoragePlan &node_storage_plan_for(
        const NodeTypeMetaData &schema,
        std::span<const NodeStorageField> extra_fields,
        std::span<const NodeStorageField> extra_fields_after_output)
    {
        auto builder = MemoryUtils::named_tuple();
        builder.add_field("runtime_storage", MemoryUtils::plan_for<NodeRuntimeStorage>());
        if (schema.input_schema != nullptr) { builder.add_field("input", MemoryUtils::plan_for<TSInput>()); }
        for (const NodeStorageField &field : extra_fields)
        {
            if (field.plan == nullptr) { throw std::logic_error("Node storage field requires a storage plan"); }
            builder.add_field(field.name, *field.plan);
        }
        if (schema.output_schema != nullptr) { builder.add_field("output", MemoryUtils::plan_for<TSOutput>()); }
        // Destroyed BEFORE the output (reverse-order destruction): for fields
        // holding links INTO the node's own output (see the header note).
        for (const NodeStorageField &field : extra_fields_after_output)
        {
            if (field.plan == nullptr) { throw std::logic_error("Node storage field requires a storage plan"); }
            builder.add_field(field.name, *field.plan);
        }
        if (schema.state_schema != nullptr) { builder.add_field("state", MemoryUtils::plan_for<Value>()); }
        if (schema.scalar_schema != nullptr) { builder.add_field("scalars", MemoryUtils::plan_for<Value>()); }
        if (schema.uses_scheduler)
        {
            builder.add_field("scheduler", MemoryUtils::plan_for<NodeSchedulerState>());
        }
        if (schema.uses_global_state)
        {
            builder.add_field("global_state", MemoryUtils::plan_for<std::optional<GlobalStateView>>());
        }
        if (schema.uses_evaluation_clock)
        {
            builder.add_field("evaluation_clock", MemoryUtils::plan_for<EvaluationClockStorageRef>());
        }
        if (schema.error_output_schema != nullptr)
        {
            builder.add_field("error_output", MemoryUtils::plan_for<TSOutput>());
        }
        if (schema.recordable_state_schema != nullptr)
        {
            builder.add_field("recordable_state", MemoryUtils::plan_for<TSOutput>());
        }
        return builder.build();
    }

    std::string_view NodeTypeMetaData::name() const noexcept
    {
        return display_name != nullptr ? std::string_view{display_name} : std::string_view{};
    }

    bool NodeTypeMetaData::has_input() const noexcept { return input_schema != nullptr; }
    bool NodeTypeMetaData::has_output() const noexcept { return output_schema != nullptr; }
    bool NodeTypeMetaData::has_state() const noexcept { return state_schema != nullptr; }
    bool NodeTypeMetaData::has_scalars() const noexcept { return scalar_schema != nullptr; }
    bool NodeTypeMetaData::has_error_output() const noexcept { return error_output_schema != nullptr; }
    bool NodeTypeMetaData::has_recordable_state() const noexcept { return recordable_state_schema != nullptr; }

    NodeView::NodeView() noexcept
        : storage_(NodeStorageRef::empty(default_node_binding()))
    {
    }

    NodeView::NodeView(const NodeTypeBinding *binding, void *memory) noexcept
        : storage_(binding != nullptr && memory != nullptr ? binding : &default_node_binding(),
                   binding != nullptr && memory != nullptr ? memory : nullptr)
    {
    }

    bool NodeView::valid() const noexcept { return storage_.has_value(); }
    const NodeTypeBinding *NodeView::binding() const noexcept
    {
        return storage_.binding();
    }
    const NodeTypeMetaData *NodeView::schema() const noexcept
    {
        return binding()->type_meta;
    }
    void *NodeView::data() const noexcept { return storage_.data(); }

    std::string_view NodeView::label() const noexcept
    {
        return ops().label_impl(ops().context, data());
    }

    NodeKind NodeView::node_kind() const noexcept
    {
        return storage_.binding()->type_meta->node_kind;
    }

    bool NodeView::started() const noexcept
    {
        return ops().started_impl(ops().context, data());
    }

    std::size_t NodeView::node_index() const noexcept
    {
        return ops().node_index_impl(ops().context, data());
    }

    GraphValue *NodeView::graph_value() const noexcept
    {
        return ops().graph_impl(ops().context, data());
    }

    GraphView NodeView::graph() const
    {
        auto *graph = graph_value();
        return graph != nullptr ? graph->view() : GraphView{};
    }

    bool NodeView::has_input() const noexcept
    {
        return ops().has_input_impl(ops().context, data());
    }

    bool NodeView::has_output() const noexcept
    {
        return ops().has_output_impl(ops().context, data());
    }

    bool NodeView::has_state() const noexcept
    {
        return ops().has_state_impl(ops().context, data());
    }

    bool NodeView::has_scalars() const noexcept
    {
        return ops().has_scalars_impl(ops().context, data());
    }

    bool NodeView::has_scheduler() const noexcept
    {
        return ops().has_scheduler_impl(ops().context, data());
    }

    bool NodeView::has_error_output() const noexcept
    {
        return ops().has_error_output_impl(ops().context, data());
    }

    bool NodeView::has_recordable_state() const noexcept
    {
        return ops().has_recordable_state_impl(ops().context, data());
    }

    TSInputView NodeView::input(DateTime evaluation_time) const
    {
        return ops().input_view_impl(ops().context, data(), evaluation_time);
    }

    TSOutputView NodeView::output(DateTime evaluation_time) const
    {
        return ops().output_view_impl(ops().context, data(), evaluation_time);
    }

    ValueView NodeView::state() const
    {
        return ops().state_view_impl(ops().context, data());
    }

    ValueView NodeView::scalars() const
    {
        return ops().scalars_view_impl(ops().context, data());
    }

    NodeSchedulerState &NodeView::scheduler_state() const
    {
        return *ops().scheduler_state_impl(ops().context, data());
    }

    GlobalStateView NodeView::global_state() const
    {
        return ops().global_state_view_impl(ops().context, data());
    }

    EvaluationClockStorageRef NodeView::evaluation_clock_ref() const
    {
        return ops().evaluation_clock_ref_impl(ops().context, data());
    }

    EvaluationClockView NodeView::evaluation_clock() const
    {
        return EvaluationClockView{evaluation_clock_ref()};
    }

    TSOutputView NodeView::error_output(DateTime evaluation_time) const
    {
        return ops().error_output_view_impl(ops().context, data(), evaluation_time);
    }

    TSOutputView NodeView::recordable_state(DateTime evaluation_time) const
    {
        return ops().recordable_state_view_impl(ops().context, data(), evaluation_time);
    }

    void NodeView::start(DateTime evaluation_time) const { ops().start_impl(ops().context, *this, evaluation_time); }
    void NodeView::stop(DateTime evaluation_time) const { ops().stop_impl(ops().context, *this, evaluation_time); }
    void NodeView::evaluate(DateTime evaluation_time) const
    {
        ops().evaluate_impl(ops().context, *this, evaluation_time);
    }
    void NodeView::cleanup_delta() const { ops().cleanup_delta_impl(ops().context, *this); }
    const NodeOps &NodeView::ops() const
    {
        return storage_.binding()->ops_ref();
    }

    NodeValue::NodeValue() noexcept = default;

    NodeValue::NodeValue(const NodeBuilder &builder, std::size_t node_index)
    {
        const auto &binding = builder.binding();
        storage_ = storage_type::owning_constructed(binding, [&](void *dst) {
            builder.construct_node_storage(dst, node_index);
        });
    }

    NodeValue::~NodeValue() = default;

    NodeValue::NodeValue(NodeValue &&other) noexcept
        : storage_(std::move(other.storage_))
    {
    }

    NodeValue &NodeValue::operator=(NodeValue &&other) noexcept
    {
        if (this != &other) { storage_ = std::move(other.storage_); }
        return *this;
    }

    bool NodeValue::has_value() const noexcept { return storage_.has_value(); }
    const NodeTypeBinding *NodeValue::binding() const noexcept { return storage_.binding(); }
    const NodeTypeMetaData *NodeValue::schema() const noexcept
    {
        return binding() != nullptr ? binding()->type_meta : nullptr;
    }

    NodeView NodeValue::view()
    {
        return NodeView{binding(), storage_.data()};
    }

    NodeView NodeValue::view() const
    {
        return NodeView{binding(), const_cast<void *>(storage_.data())};
    }

    void NodeValue::attach_graph(GraphValue *graph, std::size_t node_index)
    {
        if (!has_value()) { return; }
        const auto &table = binding()->ops_ref();
        table.attach_graph_impl(table.context, storage_.data(), graph, node_index);
    }

    NodeBuilder::NodeBuilder() = default;

    NodeBuilder NodeBuilder::native(NodeTypeMetaData schema,
                                    NodeCallbacks callbacks,
                                    TSEndpointSchema input_endpoint)
    {
        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(schema);
        descriptor.callbacks = std::move(callbacks);
        return from_descriptor(std::move(descriptor), std::move(input_endpoint));
    }

    NodeBuilder NodeBuilder::from_descriptor(NodeTypeDescriptor descriptor,
                                             TSEndpointSchema input_endpoint)
    {
        if (descriptor.schema.input_schema != nullptr && !input_endpoint.empty() &&
            !time_series_schema_equivalent(descriptor.schema.input_schema, input_endpoint.schema()))
        {
            throw std::invalid_argument("NodeBuilder input endpoint schema does not match node input schema");
        }
        if (descriptor.schema.output_schema != nullptr && !descriptor.schema.output_endpoint_schema.empty() &&
            !time_series_schema_equivalent(descriptor.schema.output_schema,
                                           descriptor.schema.output_endpoint_schema.schema()))
        {
            throw std::invalid_argument("NodeBuilder output endpoint schema does not match node output schema");
        }
        if (descriptor.schema.output_schema == nullptr && !descriptor.schema.output_endpoint_schema.empty())
        {
            throw std::invalid_argument("NodeBuilder output endpoint requires a node output schema");
        }

        const auto &plan = descriptor.storage_plan != nullptr
                               ? *descriptor.storage_plan
                               : node_storage_plan_for(descriptor.schema);
        const auto &binding = node_runtime_registry().make_binding(
            std::move(descriptor.schema),
            std::move(descriptor.callbacks),
            plan,
            descriptor.ops);
        return NodeBuilder{binding, std::move(input_endpoint)};
    }

    NodeBuilder::NodeBuilder(const NodeTypeBinding &binding, TSEndpointSchema input_endpoint)
        : binding_(&binding),
          input_endpoint_(std::move(input_endpoint))
    {
    }

    NodeBuilder &NodeBuilder::label(std::string label)
    {
        label_ = std::move(label);
        return *this;
    }

    std::string_view NodeBuilder::label() const noexcept
    {
        return label_;
    }

    NodeBuilder &NodeBuilder::input_endpoint(TSEndpointSchema endpoint)
    {
        if (binding_ == nullptr)
        {
            if (!endpoint.empty()) { throw std::logic_error("NodeBuilder has no binding"); }
        }
        else
        {
            const auto *schema = binding_->type_meta != nullptr ? binding_->type_meta->input_schema : nullptr;
            if (schema != nullptr && !endpoint.empty() && !time_series_schema_equivalent(schema, endpoint.schema()))
            {
                throw std::invalid_argument("NodeBuilder input endpoint schema does not match node input schema");
            }
            if (schema == nullptr && !endpoint.empty())
            {
                throw std::invalid_argument("NodeBuilder input endpoint requires a node input schema");
            }
        }
        input_endpoint_ = std::move(endpoint);
        return *this;
    }

    NodeBuilder &NodeBuilder::scalars(Value scalars)
    {
        scalars_ = std::move(scalars);
        return *this;
    }

    const Value &NodeBuilder::scalars() const noexcept
    {
        return scalars_;
    }

    const NodeTypeBinding &NodeBuilder::binding() const
    {
        if (binding_ == nullptr) { throw std::logic_error("NodeBuilder has no binding"); }
        return *binding_;
    }

    const TSEndpointSchema &NodeBuilder::input_endpoint() const noexcept
    {
        return input_endpoint_;
    }

    NodeBuilder &NodeBuilder::output_endpoint(TSEndpointSchema endpoint)
    {
        const auto *type_meta = binding_ != nullptr ? binding_->type_meta : nullptr;
        if (type_meta == nullptr || type_meta->output_schema == nullptr)
        {
            throw std::invalid_argument("NodeBuilder output endpoint requires a node output schema");
        }
        if (!endpoint.empty() && !time_series_schema_equivalent(type_meta->output_schema, endpoint.schema()))
        {
            throw std::invalid_argument("NodeBuilder output endpoint schema does not match node output schema");
        }
        output_endpoint_ = std::move(endpoint);
        return *this;
    }

    const TSEndpointSchema &NodeBuilder::output_endpoint() const noexcept
    {
        return output_endpoint_;
    }

    void NodeBuilder::construct_node_storage(void *memory, std::size_t node_index) const
    {
        if (memory == nullptr) { throw std::logic_error("NodeBuilder::construct_node_storage requires memory"); }

        const auto &binding = this->binding();
        const auto &runtime = runtime_context(binding.ops_ref().context);
        construct_node_storage_impl(runtime,
                                    *binding.type_meta,
                                    input_endpoint(),
                                    output_endpoint(),
                                    std::string{label()},
                                    scalars(),
                                    memory);

        const auto &table = binding.ops_ref();
        table.attach_graph_impl(table.context, memory, nullptr, node_index);
    }

    NodeValue NodeBuilder::make_node(std::size_t node_index) const
    {
        return NodeValue{*this, node_index};
    }

}  // namespace hgraph

#include <hgraph/runtime/map_node.h>
#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/utils/value_slot_store.h>
#include <hgraph/util/scope.h>

#include "mapped_child_bindings.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        constexpr std::string_view map_storage_field_name{"map"};

        struct MapKeyEntry
        {
            explicit MapKeyEntry(Value key_)
                : key(std::move(key_))
            {
            }

            // Declaration order is load-bearing: members destroy in reverse, and
            // the child graph's inputs are subscribed to ``key_output`` — the
            // graph (the subscriber) must tear down BEFORE the output it
            // observes.
            Value                   key{};
            std::optional<TSOutput> key_output{};
            GraphValue              graph{};
        };

        struct MapNodeStorage final : SlotObserver
        {
            MapNodeStorage()
                : entries(MemoryUtils::plan_for<MapKeyEntry>())
            {
            }

            MapNodeStorage(const MapNodeStorage &)            = delete;
            MapNodeStorage &operator=(const MapNodeStorage &) = delete;
            MapNodeStorage(MapNodeStorage &&)                 = delete;
            MapNodeStorage &operator=(MapNodeStorage &&)      = delete;

            ~MapNodeStorage() override
            {
                unsubscribe_keys_noexcept();
                destroy_entries_without_output_noexcept();
            }

            // Slot ids mirror the current __keys__ source. ValueSlotStore keeps
            // entry addresses stable across capacity growth.
            ValueSlotStore entries{};
            // Cached bound-output handles of the outer inputs (tsd + broadcast
            // sources). Entry input bindings are established at creation and
            // refreshed only when an upstream source re-points.
            std::vector<TSOutputHandle> outer_sources{};
            TSOutputHandle              observed_keys_source{};
            bool                        observing_keys{false};
            bool primed{false};

            // Pause/resume cursor: the entry slot a child paused on, or ``npos`` when not
            // mid-pause. On resume the per-key reconciliation (setup) is skipped and the
            // child loop continues from this slot; reset to ``npos`` on completion.
            static constexpr std::size_t npos = static_cast<std::size_t>(-1);
            std::size_t                  resume_slot{npos};

            [[nodiscard]] std::size_t active_count() const noexcept
            {
                return entries.constructed.count();
            }

            [[nodiscard]] bool observe_keys_source(TSOutputHandle source)
            {
                if (source.same_as(observed_keys_source)) { return false; }

                unsubscribe_keys_noexcept();
                observed_keys_source = source;
                if (source.bound())
                {
                    auto data = source.data_view();
                    auto set  = data.as_set();
                    entries.reserve_to(set.slot_capacity());
                    try
                    {
                        set.subscribe_slot_observer(this);
                        observing_keys = true;
                    }
                    catch (const std::logic_error &)
                    {
                        // Target-link projections can expose TSS slot access
                        // without supporting direct slot observer hooks.
                        observing_keys = false;
                    }
                }
                return true;
            }

            void unsubscribe_keys_noexcept() noexcept
            {
                if (observing_keys)
                {
                    static_cast<void>(fallback_on_exception(false, [&] {
                        auto data = observed_keys_source.data_view();
                        if (data.valid()) { data.as_set().unsubscribe_slot_observer(this); }
                        return true;
                    }));
                }
                observed_keys_source.reset();
                observing_keys = false;
            }

            void destroy_entries_without_output_noexcept() noexcept
            {
                for (std::size_t slot = 0; slot < entries.slot_capacity(); ++slot)
                {
                    auto *entry = entries.try_value<MapKeyEntry>(slot);
                    if (entry == nullptr || !entry->graph.has_value()) { continue; }
                    static_cast<void>(fallback_on_exception(false, [&] {
                        entry->graph.view().stop();
                        return true;
                    }));
                }
                entries.destroy_all();
                primed = false;
            }

            void on_capacity(std::size_t, std::size_t new_capacity) override
            {
                entries.reserve_to(new_capacity);
            }

            void on_insert(std::size_t) override {}
            void on_remove(std::size_t) override {}
            void on_erase(std::size_t) override {}
            void on_clear() override {}
        };

        struct MapNodeContext
        {
            MapNodeSpec spec{};
            std::size_t storage_offset{0};
        };

        struct SourceRepointStatus
        {
            bool any_repointed{false};
            bool mux_repointed{false};    ///< any MULTIPLEXED source re-pointed
            bool keys_repointed{false};   ///< the __keys__ source re-pointed
        };

        // Program-lifetime, intentionally-leaked context storage — same rationale
        // as single_nested_graph_contexts (see nested_graph_node.cpp).
        [[nodiscard]] std::vector<std::unique_ptr<MapNodeContext>> &map_node_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<MapNodeContext>>;
            return *contexts;
        }

        [[nodiscard]] const MapNodeContext &register_map_node_context(MapNodeSpec spec, std::size_t storage_offset)
        {
            auto context = std::make_unique<MapNodeContext>(MapNodeContext{
                .spec           = std::move(spec),
                .storage_offset = storage_offset,
            });
            const auto *result = context.get();
            map_node_contexts().push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] TSOutputHandle effective_output_handle(TSOutputView source)
        {
            if (!source.bound()) { return {}; }

            TSOutputHandle current = source.handle();
            while (source.forwarding())
            {
                TSOutputHandle target = source.forwarding_target();
                if (!target.bound() || target.same_as(current)) { break; }
                current = target;
                source = target.view(source.evaluation_time());
            }
            return current;
        }

        [[nodiscard]] SourceRepointStatus update_source_handles(const TSInputView &root_input,
                                                                MapNodeStorage &storage,
                                                                const std::vector<std::size_t> &multiplexed_inputs,
                                                                std::size_t keys_input_index)
        {
            const std::size_t outer_count = root_input.as_bundle().size();
            const bool        initialized = storage.outer_sources.size() == outer_count;
            storage.outer_sources.resize(outer_count);

            SourceRepointStatus status;
            for (std::size_t i = 0; i < outer_count; ++i)
            {
                TSOutputHandle current = effective_output_handle(
                    walk_ts_path(root_input.borrowed_ref(), std::vector<std::size_t>{i}).bound_output());
                if (!current.same_as(storage.outer_sources[i]))
                {
                    storage.outer_sources[i] = current;
                    if (initialized)
                    {
                        status.any_repointed = true;
                        if (std::find(multiplexed_inputs.begin(), multiplexed_inputs.end(), i) !=
                            multiplexed_inputs.end())
                        {
                            status.mux_repointed = true;
                        }
                        if (i == keys_input_index) { status.keys_repointed = true; }
                    }
                }
            }
            return status;
        }

        void remove_entry_at_slot(MapNodeStorage &storage, TSDDataMutationView &output_mutation, std::size_t slot)
        {
            auto *entry = storage.entries.try_value<MapKeyEntry>(slot);
            if (entry == nullptr) { return; }

            if (entry->graph.has_value()) { entry->graph.view().stop(); }
            (void)output_mutation.erase(entry->key.view());
            storage.entries.destroy_at(slot);
        }

        void remove_all_entries(MapNodeStorage &storage, TSDDataMutationView &output_mutation)
        {
            for (std::size_t slot = 0; slot < storage.entries.slot_capacity(); ++slot)
            {
                remove_entry_at_slot(storage, output_mutation, slot);
            }
        }

        void create_entry_at_slot(const NodeView &view, const MapNodeContext &context, MapNodeStorage &storage,
                                  TSDDataMutationView &output_mutation, const TSSInputView &keys_set,
                                  std::size_t slot, DateTime evaluation_time)
        {
            if (storage.entries.has_slot(slot)) { return; }

            const MapNodeSpec &spec     = context.spec;
            const ValueView    key_view = keys_set.at_slot(slot);
            storage.entries.reserve_to(std::max(storage.entries.slot_capacity(), slot + 1));
            auto &entry = storage.entries.construct_at<MapKeyEntry>(slot, Value{key_view});
            auto rollback = UnwindCleanupGuard([&] {
                if (entry.graph.has_value() && entry.graph.view().started()) { entry.graph.view().stop(); }
                (void)output_mutation.erase(entry.key.view());
                storage.entries.destroy_at(slot);
            });

            entry.graph = spec.child.graph_builder.make_nested_graph(NodeStorageRef{view.binding(), view.data()});
            if (spec.key_output_schema != nullptr)
            {
                entry.key_output.emplace(*spec.key_output_schema);
                auto mutation = entry.key_output->view(evaluation_time).begin_mutation(evaluation_time);
                if (!mutation.copy_value_from(key_view))
                {
                    throw std::logic_error("map_: failed to write the key value into the per-key output");
                }
            }

            // Instantiate the key's element in the owned TSD output and attach
            // the child's terminal forwarding output to it (stable for the
            // entry's lifetime).
            (void)output_mutation[key_view];

            const TSOutputView key_source = entry.key_output.has_value()
                                                ? entry.key_output->view(evaluation_time)
                                                : TSOutputView{};
            runtime_detail::bind_mapped_child_inputs(view, entry.graph.view(), evaluation_time,
                                                     spec.child, spec.args, entry.key.view(), key_source);
            runtime_detail::bind_mapped_child_output(view, entry.graph.view(), evaluation_time,
                                                     spec.child.output_binding, entry.key.view());
            entry.graph.view().start(evaluation_time);
            rollback.release();
        }

        void create_live_key_entries(const NodeView &view, const MapNodeContext &context, MapNodeStorage &storage,
                                     TSDDataMutationView &output_mutation, const TSSInputView &keys_set,
                                     DateTime evaluation_time)
        {
            storage.entries.reserve_to(keys_set.slot_capacity());
            for (std::size_t slot = 0; slot < keys_set.slot_capacity(); ++slot)
            {
                if (keys_set.slot_live(slot))
                {
                    create_entry_at_slot(view, context, storage, output_mutation, keys_set, slot, evaluation_time);
                }
            }
        }

        // Per-key lifecycle reconciliation (the cycle "setup"): re-point source handles,
        // create/remove children from the __keys__ set. Returns whether surviving children
        // need their inputs re-bound. Skipped on a pause/resume re-entry (the key set does
        // not change mid-cycle).
        [[nodiscard]] bool map_reconcile_keys(const NodeView &view, const MapNodeContext &context,
                                              MapNodeStorage &storage, DateTime evaluation_time)
        {
            const auto &spec       = context.spec;
            const auto  keys_index = *spec.keys_input_index;

            auto root_input = view.input(evaluation_time);
            SourceRepointStatus source_status =
                update_source_handles(root_input.borrowed_ref(), storage, spec.multiplexed_inputs, keys_index);
            bool bindings_need_refresh = source_status.any_repointed;

            // Multiplexed-dict membership changes re-bind surviving entries
            // (an element may appear or vanish in one dict while the key stays
            // live); the LIFECYCLE itself is driven solely by the __keys__
            // input below.
            bool any_modified = false;
            for (const std::size_t mux_index : spec.multiplexed_inputs)
            {
                auto mux_input = walk_ts_path(root_input.borrowed_ref(), std::vector<std::size_t>{mux_index});
                if (mux_input.bound() && mux_input.modified()) { any_modified = true; }
            }
            if (any_modified) { bindings_need_refresh = true; }

            // Explicit ``__keys__``: the TSS alone drives the lifecycle —
            // children exist exactly for its members. The multiplexed dicts
            // only feed elements; their membership changes re-bind surviving
            // entries through ``bindings_need_refresh`` above.
            auto keys_input = walk_ts_path(root_input.borrowed_ref(), std::vector<std::size_t>{keys_index});
            auto keys_source = keys_input.bound_output();
            const bool keys_observer_changed = storage.observe_keys_source(keys_source.handle());
            if (!keys_input.valid())
            {
                auto output          = view.output(evaluation_time);
                auto output_dict     = output.as_dict();
                auto output_mutation = output_dict.begin_mutation(evaluation_time);
                remove_all_entries(storage, output_mutation);
                output_mutation.clear();
                storage.primed = false;
            }
            else
            {
                auto key_set = keys_input.as_set();
                storage.entries.reserve_to(key_set.slot_capacity());

                const bool rebuild = !storage.primed || source_status.keys_repointed || keys_observer_changed;
                if (rebuild)
                {
                    auto output          = view.output(evaluation_time);
                    auto output_dict     = output.as_dict();
                    auto output_mutation = output_dict.begin_mutation(evaluation_time);
                    remove_all_entries(storage, output_mutation);
                    output_mutation.clear();
                    create_live_key_entries(view, context, storage, output_mutation, key_set, evaluation_time);
                    storage.primed = true;
                }
                else if (keys_input.modified())
                {
                    auto output          = view.output(evaluation_time);
                    auto output_dict     = output.as_dict();
                    auto output_mutation = output_dict.begin_mutation(evaluation_time);

                    for (std::size_t slot = 0; slot < key_set.slot_capacity(); ++slot)
                    {
                        if (key_set.slot_removed(slot)) { remove_entry_at_slot(storage, output_mutation, slot); }
                    }

                    for (std::size_t slot = 0; slot < key_set.slot_capacity(); ++slot)
                    {
                        if (key_set.slot_live(slot) && key_set.slot_added(slot))
                        {
                            create_entry_at_slot(view, context, storage, output_mutation, key_set, slot,
                                                 evaluation_time);
                        }
                    }
                }
            }
            return bindings_need_refresh;
        }

        // Evaluates the keyed children, supporting pause/resume: a child that pauses (a
        // mesh nested in the child needs a sibling) propagates the pause — we save the slot
        // cursor and return false so the enclosing mesh resolves the dependency and
        // re-evaluates us. On resume the key reconciliation is skipped and the loop
        // continues from the saved slot; completion resets the cursor.
        bool map_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }

            auto        map_view = view.as<MapNodeView>();
            const auto &context  = *static_cast<const MapNodeContext *>(map_view.internal_context());
            auto       &storage  = *MemoryUtils::cast<MapNodeStorage>(map_view.internal_storage());
            const auto &spec     = context.spec;

            const bool resuming = storage.resume_slot != MapNodeStorage::npos;
            const bool bindings_need_refresh =
                resuming ? false : map_reconcile_keys(view, context, storage, evaluation_time);

            // Due children write their TSD elements directly via their terminal forwarding
            // outputs (no post-evaluation collection). A child evaluation propagates its own
            // next scheduled time back to this node; unevaluated children pull theirs up.
            const std::size_t start_slot = resuming ? storage.resume_slot : 0;
            for (std::size_t slot = start_slot; slot < storage.entries.slot_capacity(); ++slot)
            {
                auto *entry = storage.entries.try_value<MapKeyEntry>(slot);
                if (entry == nullptr || !entry->graph.has_value()) { continue; }

                auto child = entry->graph.view();
                if (bindings_need_refresh)
                {
                    const TSOutputView key_source = entry->key_output.has_value()
                                                        ? entry->key_output->view(evaluation_time)
                                                        : TSOutputView{};
                    runtime_detail::bind_mapped_child_inputs(view, child, evaluation_time, spec.child,
                                                             spec.args, entry->key.view(), key_source);
                }

                const bool resume_this = resuming && slot == storage.resume_slot;
                if (child.next_scheduled_time() <= evaluation_time || resume_this)
                {
                    if (!child.evaluate(evaluation_time))
                    {
                        storage.resume_slot = slot;  // pause here; resume from this slot
                        return false;
                    }
                }
                if (const DateTime next = child.next_scheduled_time(); next != MAX_DT && next > evaluation_time)
                {
                    view.graph().schedule_node(view.node_index(), next);
                }
            }
            storage.resume_slot = MapNodeStorage::npos;  // completed the cycle
            return true;
        }

        void map_node_stop(const NodeView &view, DateTime evaluation_time)
        {
            auto  map_view = view.as<MapNodeView>();
            auto &storage  = *MemoryUtils::cast<MapNodeStorage>(map_view.internal_storage());

            auto output          = view.output(evaluation_time);
            auto output_dict     = output.as_dict();
            auto output_mutation = output_dict.begin_mutation(evaluation_time);
            remove_all_entries(storage, output_mutation);
            output_mutation.clear();
            storage.unsubscribe_keys_noexcept();
            storage.primed = false;
        }

        void validate_map_node_spec(const NodeTypeMetaData &meta, const MapNodeSpec &spec)
        {
            if (!spec.child.output_binding.has_value())
            {
                throw std::invalid_argument("map_node requires a child output binding (sink maps are not supported yet)");
            }
            if (meta.input_schema == nullptr || meta.input_schema->kind != TSTypeKind::TSB)
            {
                throw std::invalid_argument("map_node requires a TSB input schema");
            }
            if (meta.output_schema == nullptr || meta.output_schema->kind != TSTypeKind::TSD)
            {
                throw std::invalid_argument("map_node requires a TSD output schema");
            }
            if (spec.multiplexed_inputs.empty())
            {
                throw std::invalid_argument("map_node requires at least one multiplexed TSD input");
            }
            const auto *input_fields = meta.input_schema->fields();
            const TSValueTypeMetaData *anchor_tsd = nullptr;
            std::vector<std::size_t> seen_mux_inputs;
            seen_mux_inputs.reserve(spec.multiplexed_inputs.size());
            for (const std::size_t mux_index : spec.multiplexed_inputs)
            {
                if (mux_index >= meta.input_schema->field_count())
                {
                    throw std::invalid_argument("map_node multiplexed input index is out of range");
                }
                if (std::find(seen_mux_inputs.begin(), seen_mux_inputs.end(), mux_index) !=
                    seen_mux_inputs.end())
                {
                    throw std::invalid_argument("map_node multiplexed input indices must be unique");
                }
                seen_mux_inputs.push_back(mux_index);
                const auto *tsd_schema = TypeRegistry::instance().dereference(input_fields[mux_index].type);
                if (tsd_schema == nullptr || tsd_schema->kind != TSTypeKind::TSD)
                {
                    throw std::invalid_argument("map_node multiplexed input index must select a TSD input field");
                }
                if (tsd_schema->key_type() != meta.output_schema->key_type())
                {
                    throw std::invalid_argument(
                        "map_node output key type must match every multiplexed input key type");
                }
                if (anchor_tsd == nullptr) { anchor_tsd = tsd_schema; }
            }
            const auto *tsd_schema = anchor_tsd;

            if (!spec.keys_input_index.has_value())
            {
                throw std::invalid_argument(
                    "map_node requires a __keys__ input (the lifecycle is keys-driven; map_ wiring derives it "
                    "from the multiplexed inputs when not supplied)");
            }
            {
                if (*spec.keys_input_index >= meta.input_schema->field_count())
                {
                    throw std::invalid_argument("map_node __keys__ input index is out of range");
                }
                const auto *keys_schema =
                    TypeRegistry::instance().dereference(input_fields[*spec.keys_input_index].type);
                if (keys_schema == nullptr || keys_schema->kind != TSTypeKind::TSS)
                {
                    throw std::invalid_argument("map_node __keys__ input must be a TSS");
                }
                if (keys_schema != TypeRegistry::instance().tss(meta.output_schema->key_type()))
                {
                    throw std::invalid_argument(
                        "map_node __keys__ element type must match the mapped key type");
                }
            }

            const std::size_t child_node_count = spec.child.graph_builder.node_count();
            const auto       &output_binding   = *spec.child.output_binding;
            if (output_binding.kind != NestedGraphOutputBinding::Kind::ChildOutput)
            {
                throw std::invalid_argument(
                    "map_node requires the child graph output to be a real child output, not a parent-input alias");
            }
            if (!output_binding.target_path.empty())
            {
                throw std::invalid_argument("map_node child output binding must target the map element root");
            }
            if (output_binding.source.node >= child_node_count)
            {
                throw std::invalid_argument("map_node child output source node is out of range");
            }

            bool key_source_seen = false;
            bool element_source_seen = false;
            for (const NestedGraphInputBinding &binding : spec.child.input_bindings)
            {
                if (binding.source_path.empty())
                {
                    throw std::invalid_argument("map_node child input binding requires a boundary argument ordinal");
                }
                if (binding.source_path[0] >= spec.args.size())
                {
                    throw std::invalid_argument("map_node child input binding source ordinal is out of range");
                }
                if (binding.target.node >= child_node_count)
                {
                    throw std::invalid_argument("map_node child input target node is out of range");
                }

                const MapArgSource &arg = spec.args[binding.source_path[0]];
                switch (arg.kind)
                {
                    case MapArgSourceKind::Key:
                        key_source_seen = true;
                        break;
                    case MapArgSourceKind::Element:
                        element_source_seen = true;
                        if (arg.outer_index >= meta.input_schema->field_count())
                        {
                            throw std::invalid_argument("map_node element source index is out of range");
                        }
                        if (std::find(spec.multiplexed_inputs.begin(), spec.multiplexed_inputs.end(),
                                      arg.outer_index) == spec.multiplexed_inputs.end())
                        {
                            throw std::invalid_argument(
                                "map_node element source index must select a multiplexed TSD input");
                        }
                        break;
                    case MapArgSourceKind::OuterInput:
                        if (arg.outer_index >= meta.input_schema->field_count())
                        {
                            throw std::invalid_argument("map_node outer input source index is out of range");
                        }
                        break;
                }
            }
            if (!element_source_seen)
            {
                throw std::invalid_argument("map_node requires one child input sourced from the mapped TSD element");
            }

            if (key_source_seen)
            {
                if (spec.key_output_schema == nullptr)
                {
                    throw std::invalid_argument("map_node key argument requires a key output schema");
                }
                if (spec.key_output_schema->kind != TSTypeKind::TS ||
                    spec.key_output_schema->value_schema != tsd_schema->key_type())
                {
                    throw std::invalid_argument("map_node key output schema must be TS<K> for the mapped key type");
                }
            }
            else if (spec.key_output_schema != nullptr)
            {
                throw std::invalid_argument("map_node key output schema was supplied but no key argument is bound");
            }
        }
    }  // namespace

    const void *MapNodeView::node_view_type_id() noexcept
    {
        static const char token{};
        return &token;
    }

    MapNodeView MapNodeView::from_node(NodeView view, const void *context)
    {
        if (context == nullptr) { throw std::logic_error("MapNodeView requires a typed view context"); }
        const auto &typed_context = *static_cast<const MapNodeContext *>(context);
        void       *storage = MemoryUtils::advance(view.data(), typed_context.storage_offset);
        return MapNodeView{std::move(view), context, storage};
    }

    const NodeView &MapNodeView::node() const noexcept { return view_; }

    std::size_t MapNodeView::active_count() const noexcept
    {
        return MemoryUtils::cast<MapNodeStorage>(storage_)->active_count();
    }

    MapNodeView::MapNodeView(NodeView view, const void *context, void *storage) noexcept
        : view_(std::move(view)),
          context_(context),
          storage_(storage)
    {
    }

    NodeBuilder map_node(NodeTypeMetaData meta, MapNodeSpec spec)
    {
        validate_map_node_spec(meta, spec);

        meta.node_kind = NodeKind::Nested;
        meta.valid_inputs = std::vector<std::size_t>{};

        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(meta);

        const std::array fields{NodeStorageField{
            .name = map_storage_field_name,
            .plan = &MemoryUtils::plan_for<MapNodeStorage>(),
        }};
        // The map field destroys BEFORE the owned TSD output: the children's
        // terminal forwarding outputs hold links INTO it.
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, {}, fields);

        const auto &context = register_map_node_context(
            std::move(spec), descriptor.storage_plan->component(map_storage_field_name).offset);

        descriptor.callbacks.stop            = &map_node_stop;
        descriptor.ops.evaluate_impl         = &map_evaluate_impl;
        descriptor.ops.extended_view_type_id = MapNodeView::node_view_type_id();
        descriptor.ops.extended_view_context = &context;

        return NodeBuilder::from_descriptor(std::move(descriptor));
    }
}  // namespace hgraph

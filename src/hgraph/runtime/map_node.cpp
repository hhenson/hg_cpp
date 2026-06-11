#include <hgraph/runtime/map_node.h>
#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/util/scope.h>

#include <ankerl/unordered_dense.h>

#include <array>
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
            // Declaration order is load-bearing: members destroy in reverse, and
            // the child graph's inputs are subscribed to ``key_output`` — the
            // graph (the subscriber) must tear down BEFORE the output it
            // observes.
            std::optional<TSOutput> key_output{};
            GraphValue              graph{};
        };

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

        struct MapNodeStorage
        {
            MapNodeStorage()                                  = default;
            MapNodeStorage(const MapNodeStorage &)            = delete;
            MapNodeStorage &operator=(const MapNodeStorage &) = delete;
            MapNodeStorage(MapNodeStorage &&) noexcept        = default;
            MapNodeStorage &operator=(MapNodeStorage &&)      = default;
            ~MapNodeStorage()                                 = default;

            // unique_ptr entries: pointer-stable across rehash — a relocated live
            // GraphValue would break its nodes' notifier subscriptions.
            ankerl::unordered_dense::map<Value, std::unique_ptr<MapKeyEntry>, ValueKeyHash, ValueKeyEqual> active{};
            // Cached bound-output handles of the outer inputs (tsd + broadcast
            // sources). Entry input bindings are established at creation and
            // refreshed only when an upstream source re-points.
            std::vector<TSOutputHandle> outer_sources{};
            bool primed{false};
        };

        struct MapNodeContext
        {
            MapNodeSpec spec{};
            std::size_t storage_offset{0};
        };

        struct SourceRepointStatus
        {
            bool any_repointed{false};
            bool tsd_repointed{false};
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

        [[nodiscard]] std::vector<std::size_t> path_suffix(const std::vector<std::size_t> &path)
        {
            return std::vector<std::size_t>{path.begin() + 1, path.end()};
        }

        [[nodiscard]] SourceRepointStatus update_source_handles(const TSInputView &root_input,
                                                                MapNodeStorage &storage,
                                                                std::size_t tsd_input_index)
        {
            const std::size_t outer_count = root_input.as_bundle().size();
            const bool        initialized = storage.outer_sources.size() == outer_count;
            storage.outer_sources.resize(outer_count);

            SourceRepointStatus status;
            for (std::size_t i = 0; i < outer_count; ++i)
            {
                TSOutputHandle current{
                    walk_ts_path(root_input.borrowed_ref(), std::vector<std::size_t>{i}).bound_output()};
                if (!current.same_as(storage.outer_sources[i]))
                {
                    storage.outer_sources[i] = current;
                    if (initialized)
                    {
                        status.any_repointed = true;
                        if (i == tsd_input_index) { status.tsd_repointed = true; }
                    }
                }
            }
            return status;
        }

        void remove_entry(MapNodeStorage &storage, TSDDataMutationView &output_mutation, const Value &key)
        {
            auto it = storage.active.find(key);
            if (it == storage.active.end()) { return; }

            if (it->second->graph.has_value()) { it->second->graph.view().stop(); }
            (void)output_mutation.erase(key.view());
            storage.active.erase(it);
        }

        void remove_entries_missing_from_input(MapNodeStorage &storage,
                                               TSDDataMutationView &output_mutation,
                                               const TSDInputView &dict_input)
        {
            std::vector<Value> stale_keys;
            for (const auto &[key, entry] : storage.active)
            {
                static_cast<void>(entry);
                if (!dict_input.contains(key.view())) { stale_keys.push_back(key); }
            }
            for (const Value &key : stale_keys) { remove_entry(storage, output_mutation, key); }
        }

        void remove_all_entries(MapNodeStorage &storage, TSDDataMutationView &output_mutation)
        {
            std::vector<Value> keys;
            keys.reserve(storage.active.size());
            for (const auto &[key, entry] : storage.active)
            {
                static_cast<void>(entry);
                keys.push_back(key);
            }
            for (const Value &key : keys) { remove_entry(storage, output_mutation, key); }
        }

        /** Bind (or re-bind) one entry's child boundary inputs for its key. */
        void bind_entry(const NodeView &view, const MapNodeContext &context, MapKeyEntry &entry, const Value &key,
                        DateTime evaluation_time)
        {
            const MapNodeSpec &spec = context.spec;
            if (spec.child.input_bindings.empty()) { return; }

            auto root_input = view.input(evaluation_time);
            auto tsd_source = walk_ts_path(root_input.borrowed_ref(),
                                           std::vector<std::size_t>{spec.tsd_input_index})
                                  .bound_output();
            auto child = entry.graph.view();

            for (const NestedGraphInputBinding &binding : spec.child.input_bindings)
            {
                const MapArgSource &arg = spec.args.at(binding.source_path[0]);

                TSOutputView source{};
                switch (arg.kind)
                {
                    case MapArgSourceKind::Key:
                        source = entry.key_output->view(evaluation_time);
                        break;
                    case MapArgSourceKind::Element:
                    {
                        if (tsd_source.bound())
                        {
                            auto dict = tsd_source.as_dict();
                            if (dict.contains(key.view()))
                            {
                                source = walk_ts_path(dict.at(key.view()), path_suffix(binding.source_path));
                            }
                        }
                        break;
                    }
                    case MapArgSourceKind::OuterInput:
                    {
                        std::vector<std::size_t> outer_path = path_suffix(binding.source_path);
                        outer_path.insert(outer_path.begin(), arg.outer_index);
                        source = walk_ts_path(root_input.borrowed_ref(), outer_path).bound_output();
                        break;
                    }
                }

                auto target = walk_ts_path(child.node_at(binding.target.node).input(evaluation_time),
                                           binding.target.path);
                bind_input_to_source(std::move(target), source);
            }
        }

        /**
         * Bind the child's terminal **forwarding** output onto the parent's
         * TSD element for this key — the child node then writes the parent's
         * storage directly (no copy). Bound ONCE at entry creation: elements
         * live in stable slot storage and exist exactly as long as the entry.
         */
        void bind_entry_output(const NodeView &view, const MapNodeContext &context, MapKeyEntry &entry,
                               const Value &key, DateTime evaluation_time)
        {
            const auto &output_binding = context.spec.child.output_binding;
            if (!output_binding.has_value()) { return; }

            auto output = view.output(evaluation_time);
            auto dict   = output.as_dict();
            if (!dict.contains(key.view())) { return; }
            auto element = dict.at(key.view());

            auto child_terminal = walk_ts_path(
                entry.graph.view().node_at(output_binding->source.node).output(evaluation_time),
                output_binding->source.path);
            bind_forwarding_output_to_source(child_terminal, element);
        }

        void create_entry(const NodeView &view, const MapNodeContext &context, MapNodeStorage &storage,
                          TSDDataMutationView &output_mutation, const ValueView &key_view,
                          DateTime evaluation_time)
        {
            const MapNodeSpec &spec  = context.spec;
            auto               entry = std::make_unique<MapKeyEntry>();
            Value              key{key_view};

            entry->graph = spec.child.graph_builder.make_nested_graph(NodeStorageRef{view.binding(), view.data()});
            if (spec.key_output_schema != nullptr)
            {
                entry->key_output.emplace(*spec.key_output_schema);
                auto mutation = entry->key_output->view(evaluation_time).begin_mutation(evaluation_time);
                if (!mutation.copy_value_from(key_view))
                {
                    throw std::logic_error("map_: failed to write the key value into the per-key output");
                }
            }

            // Instantiate the key's element in the owned TSD output and attach
            // the child's terminal forwarding output to it (stable for the
            // entry's lifetime).
            (void)output_mutation[key_view];
            auto rollback = UnwindCleanupGuard([&] {
                if (entry->graph.has_value() && entry->graph.view().started()) { entry->graph.view().stop(); }
                (void)output_mutation.erase(key.view());
            });

            bind_entry(view, context, *entry, key, evaluation_time);
            bind_entry_output(view, context, *entry, key, evaluation_time);
            entry->graph.view().start(evaluation_time);

            storage.active.emplace(std::move(key), std::move(entry));
            rollback.release();
        }

        void map_evaluate(const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return; }

            auto        map_view = view.as<MapNodeView>();
            const auto &context  = *static_cast<const MapNodeContext *>(map_view.internal_context());
            auto       &storage  = *MemoryUtils::cast<MapNodeStorage>(map_view.internal_storage());
            const auto &spec     = context.spec;

            auto root_input = view.input(evaluation_time);
            auto tsd_input  = walk_ts_path(root_input.borrowed_ref(),
                                           std::vector<std::size_t>{spec.tsd_input_index});
            SourceRepointStatus source_status =
                update_source_handles(root_input.borrowed_ref(), storage, spec.tsd_input_index);
            bool bindings_need_refresh = source_status.any_repointed;

            if (tsd_input.valid())
            {
                // Key lifecycle in one output-mutation scope: reconcile stale
                // children against the current key set, then create one fresh
                // child per new key. Child evaluations below write through their
                // forwarding outputs and need no mutation scope here.
                auto output          = view.output(evaluation_time);
                auto output_dict     = output.as_dict();
                auto output_mutation = output_dict.begin_mutation(evaluation_time);

                auto dict_input = tsd_input.as_dict();

                if (source_status.tsd_repointed || tsd_input.modified() || !storage.primed)
                {
                    remove_entries_missing_from_input(storage, output_mutation, dict_input);
                    for (const ValueView &key : dict_input.keys())
                    {
                        if (storage.active.find(Value{key}) == storage.active.end())
                        {
                            create_entry(view, context, storage, output_mutation, key, evaluation_time);
                        }
                    }
                    storage.primed = true;
                    if (tsd_input.modified()) { bindings_need_refresh = true; }
                }
            }
            else if (source_status.tsd_repointed || tsd_input.modified())
            {
                if (!storage.active.empty())
                {
                    auto output          = view.output(evaluation_time);
                    auto output_dict     = output.as_dict();
                    auto output_mutation = output_dict.begin_mutation(evaluation_time);
                    remove_all_entries(storage, output_mutation);
                }
                storage.primed = false;
            }

            // Evaluate due children: their terminal forwarding outputs write the
            // parent's TSD elements directly, so there is no post-evaluation
            // collection step. A child evaluation propagates its own next
            // scheduled time back to this node; children left unevaluated still
            // pull their pending schedule up.
            for (auto &[key, entry] : storage.active)
            {
                auto child = entry->graph.view();
                if (bindings_need_refresh) { bind_entry(view, context, *entry, key, evaluation_time); }

                if (child.next_scheduled_time() <= evaluation_time)
                {
                    child.evaluate(evaluation_time);
                }
                else if (const DateTime next = child.next_scheduled_time(); next != MAX_DT)
                {
                    view.graph().schedule_node(view.node_index(), next);
                }
            }
        }

        void map_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            map_evaluate(view, evaluation_time);
        }

        void map_node_stop(const NodeView &view, DateTime)
        {
            auto  map_view = view.as<MapNodeView>();
            auto &storage  = *MemoryUtils::cast<MapNodeStorage>(map_view.internal_storage());
            for (auto &[key, entry] : storage.active)
            {
                if (entry->graph.has_value()) { entry->graph.view().stop(); }
            }
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
            if (spec.tsd_input_index >= meta.input_schema->field_count())
            {
                throw std::invalid_argument("map_node TSD input index is out of range");
            }

            const auto *input_fields = meta.input_schema->fields();
            const auto *tsd_schema =
                TypeRegistry::instance().dereference(input_fields[spec.tsd_input_index].type);
            if (tsd_schema == nullptr || tsd_schema->kind != TSTypeKind::TSD)
            {
                throw std::invalid_argument("map_node TSD input index must select a TSD input field");
            }
            if (tsd_schema->key_type() != meta.output_schema->key_type())
            {
                throw std::invalid_argument("map_node output key type must match the multiplexed input key type");
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
        return MemoryUtils::cast<MapNodeStorage>(storage_)->active.size();
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

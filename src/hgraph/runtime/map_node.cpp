#include <hgraph/runtime/map_node.h>
#include <hgraph/runtime/nested_bindings.h>

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
            // sources). Bindings are made ONCE at entry creation; a change here
            // (an upstream re-point) is the only thing that re-binds entries.
            std::vector<TSOutputHandle> outer_sources{};
            bool primed{false};
        };

        struct MapNodeContext
        {
            MapNodeSpec spec{};
            std::size_t storage_offset{0};
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

            bind_entry(view, context, *entry, key, evaluation_time);
            bind_entry_output(view, context, *entry, key, evaluation_time);
            entry->graph.view().start(evaluation_time);

            storage.active.emplace(std::move(key), std::move(entry));
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

            if (tsd_input.valid())
            {
                // Key lifecycle in one output-mutation scope: removed keys first
                // (the input's delta surface), then a fresh child per new key
                // (the full key scan also covers the first evaluation over an
                // already-valid input). Child evaluations below write through
                // their forwarding outputs and need no mutation scope here.
                auto output          = view.output(evaluation_time);
                auto output_dict     = output.as_dict();
                auto output_mutation = output_dict.begin_mutation(evaluation_time);

                auto dict_input = tsd_input.as_dict();

                if (tsd_input.modified())
                {
                    for (const ValueView &key : dict_input.removed_keys())
                    {
                        if (auto it = storage.active.find(Value{key}); it != storage.active.end())
                        {
                            // Destroy the child (its terminal unbinds from the
                            // element) BEFORE removing the element itself.
                            it->second->graph.view().stop();
                            storage.active.erase(it);
                            (void)output_mutation.erase(key);
                        }
                    }
                }
                if (tsd_input.modified() || !storage.primed)
                {
                    for (const ValueView &key : dict_input.keys())
                    {
                        if (storage.active.find(Value{key}) == storage.active.end())
                        {
                            create_entry(view, context, storage, output_mutation, key, evaluation_time);
                        }
                    }
                    storage.primed = true;
                }
            }

            // Bindings are made once at entry creation; the only invalidation
            // is an outer input's bound output re-pointing (e.g. an upstream
            // REF retarget) — detected with one handle compare per outer input
            // per cycle, re-binding every entry's inputs only then. The output
            // forwarding never re-binds: the owned element is stable for the
            // entry's lifetime.
            bool outer_repointed = false;
            {
                const std::size_t outer_count = root_input.as_bundle().size();
                storage.outer_sources.resize(outer_count);
                for (std::size_t i = 0; i < outer_count; ++i)
                {
                    TSOutputHandle current{
                        walk_ts_path(root_input.borrowed_ref(), std::vector<std::size_t>{i}).bound_output()};
                    if (!current.same_as(storage.outer_sources[i]))
                    {
                        storage.outer_sources[i] = current;
                        outer_repointed          = true;
                    }
                }
            }

            // Evaluate due children: their terminal forwarding outputs write the
            // parent's TSD elements directly, so there is no post-evaluation
            // collection step. A child evaluation propagates its own next
            // scheduled time back to this node; children left unevaluated still
            // pull their pending schedule up.
            for (auto &[key, entry] : storage.active)
            {
                auto child = entry->graph.view();
                if (outer_repointed) { bind_entry(view, context, *entry, key, evaluation_time); }

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
        if (!spec.child.output_binding.has_value())
        {
            throw std::invalid_argument("map_node requires a child output binding (sink maps are not supported yet)");
        }
        if (meta.output_schema == nullptr || meta.output_schema->kind != TSTypeKind::TSD)
        {
            throw std::invalid_argument("map_node requires a TSD output schema");
        }

        meta.node_kind = NodeKind::Nested;

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

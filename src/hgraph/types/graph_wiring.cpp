#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/operator_dispatch.h>   // context scope stack (OperatorRegistry)
#include <hgraph/types/subgraph_wiring.h>

#include <array>
#include <algorithm>
#include <cstdint>
#include <deque>
#include <stdexcept>
#include <typeindex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        // Interning key: node definition identity (typeid of the static node type)
        // + input edges by (producing instance, source path, target path) + the scalar configuration
        // values. The output schema is implied by the node + path, so it is not
        // part of the key. ``scalars`` is empty for a node with no scalar inputs.
        // The node's resolved schema identity (``WiringNodeSchema``) enters the key
        // because for a GENERIC node two wirings of one definition resolve to
        // different schemas (e.g. const_ over int vs double) and must not dedup.
        // (The NodeTypeMetaData object itself is freshly built per builder, so it
        // cannot be used as identity.)
        [[nodiscard]] WiringNodeSchema resolved_schema_of(const NodeBuilder &builder)
        {
            const auto *tm = builder.binding().type_meta;
            if (tm == nullptr) { return WiringNodeSchema{}; }
            return WiringNodeSchema{tm->input_schema,
                                    tm->output_schema,
                                    tm->error_output_schema,
                                    tm->recordable_state_schema,
                                    tm->scalar_schema,
                                    tm->state_schema};
        }

        struct SourceKey
        {
            WiringPortRef::SourceKind kind{WiringPortRef::SourceKind::Unbound};
            GraphEdgeSourceKind       peered_output_kind{GraphEdgeSourceKind::Output};
            const WiringInstance      *peered_node{nullptr};
            std::vector<std::size_t>   peered_path{};
            const TSValueTypeMetaData *schema{nullptr};
            std::vector<SourceKey>     structural_children{};
            std::size_t                boundary_arg{static_cast<std::size_t>(-1)};
            std::vector<std::size_t>   boundary_path{};

            bool operator==(const SourceKey &) const noexcept = default;
        };

        struct InputKey
        {
            SourceKey               source{};
            std::vector<std::size_t> target_path{};
            bool                    rank_dependency{true};

            bool operator==(const InputKey &) const noexcept = default;
        };

        struct InstanceKey
        {
            std::type_index        def;
            WiringNodeSchema       schema;
            std::vector<InputKey>  inputs;
            Value                  scalars;

            bool operator==(const InstanceKey &other) const noexcept
            {
                if (def != other.def || !(schema == other.schema) || inputs != other.inputs) { return false; }
                if (scalars.has_value() != other.scalars.has_value()) { return false; }
                if (!scalars.has_value()) { return true; }
                return scalars.equals(other.scalars);
            }
        };

        struct InstanceKeyHash
        {
            std::size_t operator()(const InstanceKey &key) const noexcept
            {
                std::size_t h = std::hash<std::type_index>{}(key.def);
                combine(h, std::hash<const void *>{}(key.schema.input));
                combine(h, std::hash<const void *>{}(key.schema.output));
                combine(h, std::hash<const void *>{}(key.schema.error_output));
                combine(h, std::hash<const void *>{}(key.schema.recordable_state));
                combine(h, std::hash<const void *>{}(key.schema.scalar));
                combine(h, std::hash<const void *>{}(key.schema.state));
                for (const auto &input : key.inputs)
                {
                    hash_source(input.source, h);
                    for (std::size_t p : input.target_path) { combine(h, std::hash<std::size_t>{}(p)); }
                    combine(h, std::hash<bool>{}(input.rank_dependency));
                    combine(h, 0xA7A7A7A7ULL);  // target-path separator
                }
                combine(h, key.scalars.has_value() ? key.scalars.hash() : std::size_t{0});
                return h;
            }

          private:
            static void combine(std::size_t &h, std::size_t v) noexcept
            {
                h ^= v + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
            }

            static void hash_source(const SourceKey &source, std::size_t &h) noexcept
            {
                combine(h, std::hash<int>{}(static_cast<int>(source.kind)));
                combine(h, std::hash<int>{}(static_cast<int>(source.peered_output_kind)));
                combine(h, std::hash<const void *>{}(source.peered_node));
                combine(h, std::hash<const void *>{}(source.schema));
                for (std::size_t p : source.peered_path) { combine(h, std::hash<std::size_t>{}(p)); }
                combine(h, 0xF1F1F1F1ULL);  // path separator
                for (const SourceKey &child : source.structural_children) { hash_source(child, h); }
                combine(h, 0xC8C8C8C8ULL);  // children separator
                combine(h, std::hash<std::size_t>{}(source.boundary_arg));
                for (std::size_t p : source.boundary_path) { combine(h, std::hash<std::size_t>{}(p)); }
                combine(h, 0xB0B0B0B0ULL);  // boundary separator
            }
        };

        [[nodiscard]] SourceKey source_key_for(const WiringPortRef &source)
        {
            SourceKey key{.kind = source.source_kind(), .schema = source.schema};
            if (source.is_peered_source())
            {
                key.peered_node        = source.peered_node();
                key.peered_path        = source.peered_path();
                key.peered_output_kind = source.peered_output_kind();
            }
            else if (source.is_structural_source())
            {
                const auto &children = source.structural_children();
                key.structural_children.reserve(children.size());
                for (const WiringPortRef &child : children) { key.structural_children.push_back(source_key_for(child)); }
            }
            else if (source.is_boundary_source())
            {
                key.boundary_arg  = source.boundary_arg_index();
                key.boundary_path = source.boundary_path();
            }
            return key;
        }

        [[nodiscard]] InstanceKey make_key(std::type_index def, WiringNodeSchema schema,
                                           std::span<const WiringInputRef> inputs, const Value &scalars)
        {
            InstanceKey key{def, schema, {}, scalars};
            key.inputs.reserve(inputs.size());
            for (std::size_t index = 0; index < inputs.size(); ++index)
            {
                const WiringInputRef &input = inputs[index];
                key.inputs.push_back(InputKey{
                    .source      = source_key_for(input.source),
                    .target_path = input.target_path.empty() ? std::vector<std::size_t>{index} : input.target_path,
                    .rank_dependency = input.rank_dependency,
                });
            }
            return key;
        }

        [[nodiscard]] const TSValueTypeMetaData *output_schema_of(const WiringInstance &instance)
        {
            const auto *meta = instance.builder.binding().type_meta;
            return meta != nullptr ? meta->output_schema : nullptr;
        }

        struct StructuralRefNodeTag
        {
        };

        void evaluate_structural_ref_node(const NodeView &view, DateTime evaluation_time)
        {
            auto root   = view.input(evaluation_time);
            auto bundle = root.as_bundle();
            auto ts     = bundle.field("ts");

            Value reference{ts.reference()};
            auto  output   = view.output(evaluation_time);
            auto  mutation = output.begin_mutation(evaluation_time);
            if (!mutation.move_value_from(std::move(reference)))
            {
                throw std::logic_error("structural REF node failed to move the reference value");
            }
        }

        [[nodiscard]] NodeBuilder structural_ref_node_builder(const TSValueTypeMetaData *target_schema,
                                                              const WiringPortRef       &source)
        {
            if (target_schema == nullptr)
            {
                throw std::logic_error("structural REF node requires a target schema");
            }

            auto       &registry      = TypeRegistry::instance();
            const auto *input_schema  = registry.un_named_tsb({{"ts", target_schema}});
            const auto *output_schema = registry.ref(target_schema);

            NodeTypeMetaData schema;
            schema.display_name      = "structural_ref";
            schema.input_schema      = input_schema;
            schema.output_schema     = output_schema;
            schema.node_kind         = NodeKind::Compute;
            // NO validity gating (an explicitly EMPTY required set - nullopt
            // would mean "all fields"): the node must re-evaluate when the
            // source goes INVALID too, so consumers observe the emptied
            // reference (race re-races on winner invalidation).
            schema.valid_inputs.emplace();

            NodeCallbacks callbacks;
            callbacks.evaluate = &evaluate_structural_ref_node;

            std::array<WiringPortRef, 1> inputs{source};
            NodeBuilder builder = NodeBuilder::native(std::move(schema), std::move(callbacks));
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
            builder.label("structural_ref");
            return builder;
        }

        void collect_producers(const WiringPortRef &source, std::vector<const WiringInstance *> &producers)
        {
            if (source.is_peered_source())
            {
                producers.push_back(source.peered_node());
                return;
            }
            if (source.is_null_source() || source.is_boundary_source()) { return; }
            if (source.is_unbound_source())
            {
                throw std::logic_error("Wiring::finish encountered an unbound wiring source");
            }
            for (const WiringPortRef &child : source.structural_children()) { collect_producers(child, producers); }
        }

        // One edge emitter for both finish flavours. A boundary source is only
        // legal when compiling a sub-graph (``boundary_bindings`` supplied): it
        // becomes a nested-graph input binding (outer input root path =
        // {arg} + boundary path) instead of an edge.
        void emit_edges(const WiringPortRef                                             &source,
                        const std::vector<std::size_t>                                 &target_path,
                        const std::unordered_map<const WiringInstance *, std::size_t>   &index_of,
                        GraphBuilder                                                   &graph_builder,
                        std::size_t                                                     target_node,
                        std::vector<NestedGraphInputBinding>                           *boundary_bindings)
        {
            if (source.is_peered_source())
            {
                graph_builder.add_edge(GraphEdge{
                    .source_node = make_graph_edge_source(index_of.at(source.peered_node()),
                                                          source.peered_output_kind()),
                    .source_path = source.peered_path(),
                    .target_node = target_node,
                    .target_path = target_path,
                });
                return;
            }
            if (source.is_null_source()) { return; }
            if (source.is_boundary_source())
            {
                if (boundary_bindings == nullptr)
                {
                    throw std::logic_error(
                        "Wiring::finish encountered a sub-graph boundary source; compile with finish_subgraph "
                        "instead");
                }
                std::vector<std::size_t> source_path;
                source_path.reserve(1 + source.boundary_path().size());
                source_path.push_back(source.boundary_arg_index());
                source_path.insert(source_path.end(), source.boundary_path().begin(), source.boundary_path().end());
                boundary_bindings->push_back(NestedGraphInputBinding{
                    .source_path = std::move(source_path),
                    .target      = NestedGraphEndpoint{.node = target_node, .path = target_path},
                });
                return;
            }
            if (source.is_unbound_source())
            {
                throw std::logic_error("Wiring::finish encountered an unbound wiring source");
            }
            const auto &children = source.structural_children();
            for (std::size_t index = 0; index < children.size(); ++index)
            {
                std::vector<std::size_t> child_target_path = target_path;
                child_target_path.push_back(index);
                emit_edges(children[index], child_target_path, index_of, graph_builder, target_node,
                           boundary_bindings);
            }
        }

        struct RankedGraphBuild
        {
            GraphBuilder                                            graph_builder{};
            std::unordered_map<const WiringInstance *, std::size_t> index_of{};
        };

        // The one rank-and-build pass behind both ``finish`` flavours: Kahn
        // topological sort (an input edge is producer -> consumer; insertion
        // order breaks ties), then nodes + edges into a GraphBuilder.
        [[nodiscard]] RankedGraphBuild build_ranked_graph(
            const std::deque<WiringInstance>     &instances,
            std::vector<NestedGraphInputBinding> *boundary_bindings)
        {
            std::vector<const WiringInstance *> all;
            all.reserve(instances.size());
            for (const auto &instance : instances) { all.push_back(&instance); }

            std::unordered_map<const WiringInstance *, std::size_t>                         indegree;
            std::unordered_map<const WiringInstance *, std::vector<const WiringInstance *>>  consumers;
            for (const WiringInstance *instance : all) { indegree.try_emplace(instance, 0); }
            for (const WiringInstance *instance : all)
            {
                for (const auto &input : instance->inputs)
                {
                    if (!input.rank_dependency) { continue; }
                    std::vector<const WiringInstance *> producers;
                    collect_producers(input.source, producers);
                    for (const WiringInstance *producer : producers)
                    {
                        ++indegree[instance];
                        consumers[producer].push_back(instance);
                    }
                }
                for (const WiringInstance *producer : instance->rank_dependencies)
                {
                    if (producer == nullptr) { continue; }
                    ++indegree[instance];
                    consumers[producer].push_back(instance);
                }
            }

            std::deque<const WiringInstance *> ready;
            for (const WiringInstance *instance : all)  // insertion order → stable tie-break
            {
                if (indegree[instance] == 0) { ready.push_back(instance); }
            }

            std::vector<const WiringInstance *> ranked;
            ranked.reserve(all.size());
            while (!ready.empty())
            {
                const WiringInstance *instance = ready.front();
                ready.pop_front();
                ranked.push_back(instance);
                for (const WiringInstance *consumer : consumers[instance])
                {
                    if (--indegree[consumer] == 0) { ready.push_back(consumer); }
                }
            }

            if (ranked.size() != all.size())
            {
                auto name_of = [](const WiringInstance *instance) {
                    if (instance == nullptr) { return std::string{"<null>"}; }
                    const auto &builder = instance->builder;
                    std::string label = std::string{builder.label()};
                    if (!label.empty()) { return label; }
                    const auto *meta = builder.binding().type_meta;
                    return meta != nullptr && meta->display_name != nullptr
                        ? std::string{meta->display_name}
                        : std::string{"<unnamed>"};
                };

                std::string message = "Wiring::finish detected a cycle in the wiring graph";
                std::size_t shown = 0;
                for (const auto &[instance, degree] : indegree)
                {
                    if (degree == 0) { continue; }
                    message += shown == 0 ? ": " : ", ";
                    message += name_of(instance);
                    if (++shown == 8) { break; }
                }
                shown = 0;
                for (const auto &[instance, degree] : indegree)
                {
                    if (degree == 0 || shown == 4) { continue; }
                    message += "; ";
                    message += name_of(instance);
                    message += " waits on ";
                    bool first = true;
                    for (const auto &input : instance->inputs)
                    {
                        if (!input.rank_dependency) { continue; }
                        std::vector<const WiringInstance *> producers;
                        collect_producers(input.source, producers);
                        for (const WiringInstance *producer : producers)
                        {
                            if (indegree[producer] == 0) { continue; }
                            if (!first) { message += ", "; }
                            message += name_of(producer);
                            first = false;
                        }
                    }
                    for (const WiringInstance *producer : instance->rank_dependencies)
                    {
                        if (producer == nullptr || indegree[producer] == 0) { continue; }
                        if (!first) { message += ", "; }
                        message += name_of(producer);
                        first = false;
                    }
                    if (first) { message += "<none in cycle>"; }
                    ++shown;
                }
                throw std::runtime_error(message);
            }

            RankedGraphBuild build;
            build.index_of.reserve(ranked.size());
            for (std::size_t i = 0; i < ranked.size(); ++i) { build.index_of.emplace(ranked[i], i); }

            for (const WiringInstance *instance : ranked) { build.graph_builder.add_node(instance->builder); }
            for (std::size_t i = 0; i < ranked.size(); ++i)
            {
                const WiringInstance *instance = ranked[i];
                for (std::size_t input_index = 0; input_index < instance->inputs.size(); ++input_index)
                {
                    const WiringInputRef &input = instance->inputs[input_index];
                    std::vector<std::size_t> target_path =
                        input.target_path.empty() ? std::vector<std::size_t>{input_index} : input.target_path;
                    emit_edges(input.source, target_path, build.index_of, build.graph_builder, i,
                               boundary_bindings);
                }
            }
            return build;
        }
    }  // namespace

    WiringPortRef graph_wiring_detail::adapt_source_for_input(Wiring &w,
                                                              const TSValueTypeMetaData *input_schema,
                                                              WiringPortRef source)
    {
        if (input_schema != nullptr && input_schema->kind == TSTypeKind::REF && source.is_structural_source())
        {
            const TSValueTypeMetaData *target_schema = input_schema->referenced_ts();
            if (!input_accepts_output_schema(input_schema, source.schema))
            {
                throw std::logic_error("wire<T>: structural source schema does not match REF input target");
            }

            std::array<WiringPortRef, 1> inputs{std::move(source)};
            NodeBuilder builder = structural_ref_node_builder(target_schema, inputs[0]);
            return w.add_node(std::type_index(typeid(StructuralRefNodeTag)), std::move(builder),
                              std::span<const WiringPortRef>{inputs.data(), inputs.size()}, Value{});
        }
        return source;
    }

    WiringPortRef graph_wiring_detail::resolve_context_source(const Wiring &w, std::string_view name)
    {
        const auto *entry = OperatorRegistry::instance().resolve_context_scope(name);
        if (entry == nullptr)
        {
            throw std::logic_error("no enclosing context::scope publishes context '" + std::string{name} +
                                   "' (required by a Context<\"" + std::string{name} + "\", ...> input)");
        }
        if (entry->wiring != static_cast<const void *>(&w))
        {
            throw std::logic_error(
                "context '" + std::string{name} +
                "' was published in a different wiring: importing a context into a compiled "
                "sub-graph (map_/switch_/nested_ child) is not supported yet (see services.rst, Contexts)");
        }
        return entry->port;
    }

    bool graph_wiring_detail::has_context_source(const Wiring &w, std::string_view name) noexcept
    {
        const auto *entry = OperatorRegistry::instance().resolve_context_scope(name);
        return entry != nullptr && entry->wiring == static_cast<const void *>(&w);
    }

    void graph_wiring_detail::push_context_source(const Wiring &w, std::string_view name, WiringPortRef port)
    {
        OperatorRegistry::instance().push_context_scope(name, std::move(port), static_cast<const void *>(&w));
    }

    void graph_wiring_detail::pop_context_source() noexcept { OperatorRegistry::instance().pop_context_scope(); }

    struct Wiring::Impl
    {
        struct ServiceImplementationScope
        {
            std::string                          description{};
            std::unordered_set<std::string>      required_endpoints{};
            std::unordered_map<std::string, ResolutionMap> endpoint_resolutions{};
            std::unordered_set<std::string>      used_endpoints{};
        };

        struct ServiceClientRank
        {
            std::string                 path{};
            std::string                 kind{};
            const WiringInstance       *node{nullptr};
            bool                        receive{true};
        };

        /** A same-cycle boundary pairing: the capture must rank before the source. */
        struct SameCyclePair
        {
            const WiringInstance *capture{nullptr};
            const WiringInstance *source{nullptr};
        };

        std::deque<WiringInstance>                                        instances{};
        std::unordered_map<InstanceKey, WiringInstance *, InstanceKeyHash> interned{};
        std::unordered_map<std::string, std::string>                       built_service_paths{};
        std::unordered_map<std::string, std::string>                       client_service_paths{};
        std::unordered_map<std::string, const WiringInstance *>            service_rank_anchors{};
        std::vector<ServiceClientRank>                                      service_client_ranks{};
        std::vector<SameCyclePair>                                          same_cycle_pairs{};
        GlobalState                                                         traits{};   // value-layer Map<string, Any>
        std::vector<ServiceImplementationScope>                            implementation_scopes{};
        GlobalState                                                        global_state{};
    };

    Wiring::Wiring() : impl_(std::make_unique<Impl>()) {}
    Wiring::~Wiring()                              = default;
    Wiring::Wiring(Wiring &&) noexcept             = default;
    Wiring &Wiring::operator=(Wiring &&) noexcept  = default;

    WiringPortRef Wiring::add_node(std::type_index def, NodeBuilder builder, std::span<const WiringInputRef> inputs,
                                   Value scalars)
    {
        // The passive marker (Python's ``passive(ts)``): a Passive-tagged
        // source removes the receiving slot from THIS node's active list.
        // Adjusted before schema resolution so node identity reflects it.
        std::vector<std::size_t> passive_slots;
        for (std::size_t slot = 0; slot < inputs.size(); ++slot)
        {
            if (inputs[slot].source.arg_tag == WiringPortRef::ArgTag::Passive) { passive_slots.push_back(slot); }
        }
        if (!passive_slots.empty())
        {
            builder = builder.with_passive_inputs({passive_slots.data(), passive_slots.size()});
        }

        const WiringNodeSchema schema = resolved_schema_of(builder);

        // Output-less (sink / side-effecting) nodes are never deduped: two identical
        // sinks must stay distinct runtime nodes so each performs its side effect,
        // matching Python where node calls are not common-subexpression-eliminated.
        // Only value-producing nodes are interned, where an identical
        // (def, schema, inputs, scalars) genuinely is one shared subexpression.
        const bool interns = schema.output != nullptr;

        InstanceKey key = make_key(def, schema, inputs, scalars);
        if (interns)
        {
            if (auto it = impl_->interned.find(key); it != impl_->interned.end())
            {
                const WiringInstance *existing = it->second;
                return WiringPortRef::peered_source(existing, {}, output_schema_of(*existing));
            }
        }

        builder.scalars(std::move(scalars));   // record the scalar configuration on the build artifact

        WiringInstance &instance = impl_->instances.emplace_back();
        instance.builder         = std::move(builder);
        instance.inputs.assign(inputs.begin(), inputs.end());
        if (interns) { impl_->interned.emplace(std::move(key), &instance); }

        return WiringPortRef::peered_source(&instance, {}, output_schema_of(instance));
    }

    WiringPortRef Wiring::add_node(std::type_index def, NodeBuilder builder, std::span<const WiringPortRef> inputs,
                                   Value scalars)
    {
        std::vector<WiringInputRef> input_refs;
        input_refs.reserve(inputs.size());
        for (const WiringPortRef &input : inputs) { input_refs.push_back(WiringInputRef{.source = input}); }
        return add_node(def, std::move(builder),
                        std::span<const WiringInputRef>{input_refs.data(), input_refs.size()},
                        std::move(scalars));
    }

    WiringPortRef Wiring::add_unique_node(std::type_index def, NodeBuilder builder,
                                          std::span<const WiringInputRef> inputs,
                                          Value scalars)
    {
        static_cast<void>(def);

        builder.scalars(std::move(scalars));

        WiringInstance &instance = impl_->instances.emplace_back();
        instance.builder         = std::move(builder);
        instance.inputs.assign(inputs.begin(), inputs.end());

        return WiringPortRef::peered_source(&instance, {}, output_schema_of(instance));
    }

    WiringPortRef Wiring::add_unique_node(std::type_index def, NodeBuilder builder,
                                          std::span<const WiringPortRef> inputs,
                                          Value scalars)
    {
        std::vector<WiringInputRef> input_refs;
        input_refs.reserve(inputs.size());
        for (const WiringPortRef &input : inputs) { input_refs.push_back(WiringInputRef{.source = input}); }
        return add_unique_node(def, std::move(builder),
                               std::span<const WiringInputRef>{input_refs.data(), input_refs.size()},
                               std::move(scalars));
    }

    void Wiring::add_rank_dependency(const WiringInstance *node, const WiringInstance *depends_on)
    {
        if (node == nullptr || depends_on == nullptr) { throw std::invalid_argument("rank dependency requires nodes"); }
        if (node == depends_on) { throw std::invalid_argument("rank dependency cannot target the same node"); }

        auto &dependencies = const_cast<WiringInstance *>(node)->rank_dependencies;
        if (std::find(dependencies.begin(), dependencies.end(), depends_on) == dependencies.end())
        {
            dependencies.push_back(depends_on);
        }
    }

    void Wiring::set_trait(std::string_view name, const ValueView &value)
    {
        if (name.empty()) { throw std::invalid_argument("graph trait name must not be empty"); }
        impl_->traits.view().set(name, value);
    }

    void Wiring::set_trait(std::string_view name, Value &&value)
    {
        if (name.empty()) { throw std::invalid_argument("graph trait name must not be empty"); }
        impl_->traits.view().set(name, std::move(value));
    }

    void Wiring::add_same_cycle_pair(const WiringInstance *capture, const WiringInstance *source)
    {
        // A same-cycle boundary pairing (shared-output relays): the source is
        // rank-constrained after the capture, and finish() validates the final
        // order so the RUNTIME can trust it unconditionally — the capture
        // schedules the source for the current evaluation time with no checks
        // on the hot path (wiring-time validation over run-time cost).
        add_rank_dependency(source, capture);
        impl_->same_cycle_pairs.push_back(Impl::SameCyclePair{capture, source});
    }

    void Wiring::validate_same_cycle_pairs(
        const std::unordered_map<const WiringInstance *, std::size_t> &index_of) const
    {
        auto name_of = [](const WiringInstance *instance) {
            if (instance == nullptr) { return std::string{"<null>"}; }
            std::string label = std::string{instance->builder.label()};
            if (!label.empty()) { return label; }
            const auto *meta = instance->builder.binding().type_meta;
            return meta != nullptr && meta->display_name != nullptr ? std::string{meta->display_name}
                                                                    : std::string{"<unnamed>"};
        };

        for (const auto &pair : impl_->same_cycle_pairs)
        {
            const auto capture = index_of.find(pair.capture);
            const auto source  = index_of.find(pair.source);
            if (capture == index_of.end() || source == index_of.end())
            {
                throw std::logic_error("Wiring::finish lost a same-cycle boundary pair node ('" +
                                       name_of(pair.capture) + "' / '" + name_of(pair.source) + "')");
            }
            if (capture->second >= source->second)
            {
                throw std::logic_error(
                    "Wiring::finish rank violation: same-cycle boundary capture '" + name_of(pair.capture) +
                    "' (rank " + std::to_string(capture->second) + ") must rank before its paired source '" +
                    name_of(pair.source) + "' (rank " + std::to_string(source->second) + ")");
            }
        }
    }

    void Wiring::register_built_service_path(std::string path, std::string_view kind)
    {
        if (path.empty()) { throw std::invalid_argument("service/adaptor implementation path must not be empty"); }

        auto [it, inserted] = impl_->built_service_paths.try_emplace(path, std::string{kind});
        if (!inserted)
        {
            throw std::invalid_argument(
                "duplicate " + std::string{kind} + " implementation registration for '" + path +
                "'; already registered as " + it->second);
        }
    }

    void Wiring::register_service_client_path(std::string path, std::string_view kind)
    {
        if (path.empty()) { throw std::invalid_argument("service/adaptor client path must not be empty"); }
        impl_->client_service_paths.try_emplace(std::move(path), std::string{kind});
    }

    void Wiring::register_service_rank_anchor(std::string path, const WiringInstance *node)
    {
        if (path.empty()) { throw std::invalid_argument("service/adaptor rank anchor path must not be empty"); }
        if (node == nullptr) { return; }

        auto [it, inserted] = impl_->service_rank_anchors.try_emplace(std::move(path), node);
        if (!inserted && it->second != node)
        {
            throw std::invalid_argument("conflicting service/adaptor rank anchor for '" + it->first + "'");
        }
    }

    void Wiring::register_service_client_rank(std::string path, std::string_view kind,
                                              const WiringInstance *node, bool receive)
    {
        if (path.empty()) { throw std::invalid_argument("service/adaptor client rank path must not be empty"); }
        if (node == nullptr) { return; }

        impl_->service_client_ranks.push_back(Impl::ServiceClientRank{
            .path = std::move(path),
            .kind = std::string{kind},
            .node = node,
            .receive = receive,
        });
    }

    void Wiring::begin_service_implementation(std::string description, std::vector<std::string> required_endpoints)
    {
        std::vector<WiringServiceImplementationEndpoint> endpoints;
        endpoints.reserve(required_endpoints.size());
        for (auto &endpoint : required_endpoints)
        {
            endpoints.push_back(WiringServiceImplementationEndpoint{std::move(endpoint), ResolutionMap{}});
        }
        begin_service_implementation(std::move(description), std::move(endpoints));
    }

    void Wiring::begin_service_implementation(std::string description,
                                              std::vector<WiringServiceImplementationEndpoint> required_endpoints)
    {
        Impl::ServiceImplementationScope scope;
        scope.description = std::move(description);
        scope.required_endpoints.reserve(required_endpoints.size());
        for (auto &endpoint : required_endpoints)
        {
            if (endpoint.endpoint.empty())
            {
                throw std::invalid_argument("service/adaptor implementation endpoint must not be empty");
            }
            scope.required_endpoints.insert(endpoint.endpoint);
            scope.endpoint_resolutions.emplace(std::move(endpoint.endpoint), std::move(endpoint.resolution));
        }
        impl_->implementation_scopes.push_back(std::move(scope));
    }

    void Wiring::register_service_implementation_stub(std::string endpoint, std::string_view kind)
    {
        if (impl_->implementation_scopes.empty())
        {
            throw std::invalid_argument(
                std::string{kind} + " implementation stub may only be used inside a registered implementation graph");
        }

        auto &scope = impl_->implementation_scopes.back();
        if (!scope.required_endpoints.empty() && !scope.required_endpoints.contains(endpoint))
        {
            throw std::invalid_argument(
                std::string{kind} + " implementation stub for '" + endpoint +
                "' is not part of active implementation '" + scope.description + "'");
        }
        scope.used_endpoints.insert(std::move(endpoint));
    }

    ResolutionMap Wiring::service_implementation_stub_resolution(const std::string &endpoint) const
    {
        if (impl_->implementation_scopes.empty()) { return ResolutionMap{}; }
        const auto &scope = impl_->implementation_scopes.back();
        auto it = scope.endpoint_resolutions.find(endpoint);
        return it != scope.endpoint_resolutions.end() ? it->second : ResolutionMap{};
    }

    void Wiring::end_service_implementation()
    {
        if (impl_->implementation_scopes.empty())
        {
            throw std::logic_error("end_service_implementation called without an active implementation");
        }

        auto scope = std::move(impl_->implementation_scopes.back());
        impl_->implementation_scopes.pop_back();

        std::vector<std::string> missing;
        for (const auto &endpoint : scope.required_endpoints)
        {
            if (!scope.used_endpoints.contains(endpoint)) { missing.push_back(endpoint); }
        }
        if (!missing.empty())
        {
            std::sort(missing.begin(), missing.end());
            std::string message = "implementation '" + scope.description + "' did not wire required stub";
            if (missing.size() != 1) { message.push_back('s'); }
            message.append(": ");
            for (std::size_t i = 0; i < missing.size(); ++i)
            {
                if (i != 0) { message.append(", "); }
                message.append(missing[i]);
            }
            throw std::invalid_argument(message);
        }
    }

    void Wiring::cancel_service_implementation() noexcept
    {
        if (!impl_->implementation_scopes.empty()) { impl_->implementation_scopes.pop_back(); }
    }

    WiringPortRef Wiring::add_node(std::type_index def, const WiringNodeSchema &schema,
                                   std::span<const WiringPortRef> inputs, Value scalars,
                                   std::function<NodeBuilder()> make_builder)
    {
        std::vector<WiringInputRef> input_refs;
        input_refs.reserve(inputs.size());
        for (const WiringPortRef &input : inputs) { input_refs.push_back(WiringInputRef{.source = input}); }

        const bool interns = schema.output != nullptr;
        InstanceKey key = make_key(def, schema,
                                   std::span<const WiringInputRef>{input_refs.data(), input_refs.size()}, scalars);
        if (interns)
        {
            if (auto it = impl_->interned.find(key); it != impl_->interned.end())
            {
                const WiringInstance *existing = it->second;
                return WiringPortRef::peered_source(existing, {}, output_schema_of(*existing));
            }
        }

        NodeBuilder builder = make_builder();   // intern miss: only now pay for (and register) the builder
        builder.scalars(std::move(scalars));

        WiringInstance &instance = impl_->instances.emplace_back();
        instance.builder         = std::move(builder);
        instance.inputs          = std::move(input_refs);
        if (interns) { impl_->interned.emplace(std::move(key), &instance); }

        return WiringPortRef::peered_source(&instance, {}, output_schema_of(instance));
    }

    const TSValueTypeMetaData *Wiring::activate_error_capture(const WiringInstance *node,
                                                             const TSValueTypeMetaData *error_schema)
    {
        if (node == nullptr) { throw std::invalid_argument("activate_error_capture: null node"); }
        // The deque owns the instances (stable addresses); the const port ref
        // names one we own and may amend before finish.
        WiringInstance         &instance = const_cast<WiringInstance &>(*node);
        const NodeTypeMetaData *meta     = instance.builder.binding().type_meta;
        if (meta == nullptr || meta->error_output_schema == nullptr)
        {
            instance.builder = instance.builder.with_error_capture(error_schema);
        }
        return instance.builder.binding().type_meta->error_output_schema;
    }

    GlobalStateView Wiring::global_state() noexcept { return impl_->global_state.view(); }

    void Wiring::apply_service_rank_dependencies()
    {
        for (const auto &client : impl_->service_client_ranks)
        {
            auto anchor_it = impl_->service_rank_anchors.find(client.path);
            if (anchor_it == impl_->service_rank_anchors.end() || anchor_it->second == nullptr)
            {
                continue;
            }

            const WiringInstance *anchor = anchor_it->second;
            if (anchor == client.node) { continue; }

            if (client.receive)
            {
                add_rank_dependency(client.node, anchor);
            }
            else
            {
                add_rank_dependency(anchor, client.node);
            }
        }
    }

    GraphBuilder Wiring::finish() &&
    {
        if (!impl_->implementation_scopes.empty())
        {
            throw std::logic_error("Wiring::finish encountered an unterminated service/adaptor implementation scope");
        }
        for (const auto &[path, kind] : impl_->client_service_paths)
        {
            if (!impl_->built_service_paths.contains(path))
            {
                throw std::invalid_argument("missing implementation for " + kind + " '" + path + "'");
            }
        }

        apply_service_rank_dependencies();
        RankedGraphBuild build = build_ranked_graph(impl_->instances, nullptr);
        validate_same_cycle_pairs(build.index_of);
        build.graph_builder.global_state(std::move(impl_->global_state));  // carry wiring-time entries onto the graph
        for (const auto [key, boxed] : impl_->traits.as_value().view().as_map())
        {
            build.graph_builder.trait(key.checked_as<Str>(), boxed.as_any().get());
        }
        return std::move(build.graph_builder);
    }

    CompiledSubGraph Wiring::finish_subgraph(std::optional<WiringPortRef> output,
                                             std::vector<const TSValueTypeMetaData *> input_schemas) &&
    {
        if (!impl_->implementation_scopes.empty())
        {
            throw std::logic_error(
                "Wiring::finish_subgraph encountered an unterminated service/adaptor implementation scope");
        }
        for (const auto &[path, kind] : impl_->client_service_paths)
        {
            if (!impl_->built_service_paths.contains(path))
            {
                throw std::invalid_argument("missing implementation for " + kind + " '" + path + "'");
            }
        }

        apply_service_rank_dependencies();
        CompiledSubGraph compiled;
        compiled.input_schemas = std::move(input_schemas);

        RankedGraphBuild build = build_ranked_graph(impl_->instances, &compiled.input_bindings);
        validate_same_cycle_pairs(build.index_of);
        for (const auto [key, boxed] : impl_->traits.as_value().view().as_map())
        {
            build.graph_builder.trait(key.checked_as<Str>(), boxed.as_any().get());
        }
        compiled.graph_builder = std::move(build.graph_builder);
        const auto &index_of   = build.index_of;

        if (output.has_value())
        {
            if (output->is_boundary_source())
            {
                // Pass-through: the sub-graph returns a boundary input directly
                // (alias_parent_input) — the outer output aliases the upstream
                // output the outer input is bound to.
                std::vector<std::size_t> parent_path;
                parent_path.reserve(1 + output->boundary_path().size());
                parent_path.push_back(output->boundary_arg_index());
                parent_path.insert(parent_path.end(), output->boundary_path().begin(),
                                   output->boundary_path().end());
                compiled.output_binding = NestedGraphOutputBinding{
                    .kind               = NestedGraphOutputBinding::Kind::ParentInput,
                    .parent_source_path = std::move(parent_path),
                };
                compiled.output_schema = output->schema;
            }
            else if (output->is_peered_source())
            {
                if (output->peered_output_kind() != GraphEdgeSourceKind::Output)
                {
                    throw std::invalid_argument(
                        "Wiring::finish_subgraph: error/recordable-state outputs cannot be a sub-graph output");
                }
                const auto it = index_of.find(output->peered_node());
                if (it == index_of.end())
                {
                    throw std::invalid_argument(
                        "Wiring::finish_subgraph: the sub-graph output port does not belong to this sub-graph");
                }
                compiled.output_binding = NestedGraphOutputBinding{
                    .source = NestedGraphEndpoint{.node = it->second, .path = output->peered_path()},
                };
                compiled.output_schema = output->schema;
            }
            else
            {
                throw std::invalid_argument(
                    "Wiring::finish_subgraph: the sub-graph output must be a node output or a boundary input "
                    "(structural outputs are not supported)");
            }
        }

        // Wiring-time GlobalState entries cannot be carried by a sub-graph:
        // nested graphs delegate global state to the root graph at runtime, so
        // the seeded store would be silently discarded. Seed it on the OUTER
        // wiring instead.
        if (impl_->global_state.as_value().view().as_map().size() != 0)
        {
            throw std::invalid_argument(
                "Wiring::finish_subgraph: a sub-graph compose must not seed GlobalState (nested graphs share the "
                "root graph's state); seed it on the outer wiring");
        }

        return compiled;
    }
}  // namespace hgraph

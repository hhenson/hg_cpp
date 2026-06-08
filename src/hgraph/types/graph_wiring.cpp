#include <hgraph/types/graph_wiring.h>

#include <array>
#include <cstdint>
#include <deque>
#include <stdexcept>
#include <typeindex>
#include <unordered_map>
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
        // The node's *resolved* schema identity: the (registry-interned, hence stable)
        // input / output / scalar / state schema pointers. For a concrete node these
        // are implied by ``def``; for a GENERIC node two wirings of the same definition
        // resolve to different schemas (e.g. const_ over int vs double), so they must
        // enter the key or distinct resolutions would wrongly dedup. (The NodeTypeMetaData
        // object itself is freshly built per builder, so it cannot be used as identity.)
        struct ResolvedSchema
        {
            const void *input;
            const void *output;
            const void *scalar;
            const void *state;

            bool operator==(const ResolvedSchema &) const noexcept = default;
        };

        [[nodiscard]] ResolvedSchema resolved_schema_of(const NodeBuilder &builder)
        {
            const auto *tm = builder.binding().type_meta;
            if (tm == nullptr) { return ResolvedSchema{nullptr, nullptr, nullptr, nullptr}; }
            return ResolvedSchema{tm->input_schema, tm->output_schema, tm->scalar_schema, tm->state_schema};
        }

        struct SourceKey
        {
            WiringPortRef::SourceKind kind{WiringPortRef::SourceKind::Unbound};
            const WiringInstance      *peered_node{nullptr};
            std::vector<std::size_t>   peered_path{};
            const TSValueTypeMetaData *schema{nullptr};
            std::vector<SourceKey>     structural_children{};

            bool operator==(const SourceKey &) const noexcept = default;
        };

        struct InputKey
        {
            SourceKey               source{};
            std::vector<std::size_t> target_path{};

            bool operator==(const InputKey &) const noexcept = default;
        };

        struct InstanceKey
        {
            std::type_index        def;
            ResolvedSchema         schema;
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
                combine(h, std::hash<const void *>{}(key.schema.scalar));
                combine(h, std::hash<const void *>{}(key.schema.state));
                for (const auto &input : key.inputs)
                {
                    hash_source(input.source, h);
                    for (std::size_t p : input.target_path) { combine(h, std::hash<std::size_t>{}(p)); }
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
                combine(h, std::hash<const void *>{}(source.peered_node));
                combine(h, std::hash<const void *>{}(source.schema));
                for (std::size_t p : source.peered_path) { combine(h, std::hash<std::size_t>{}(p)); }
                combine(h, 0xF1F1F1F1ULL);  // path separator
                for (const SourceKey &child : source.structural_children) { hash_source(child, h); }
                combine(h, 0xC8C8C8C8ULL);  // children separator
            }
        };

        [[nodiscard]] SourceKey source_key_for(const WiringPortRef &source)
        {
            SourceKey key{.kind = source.source_kind(), .schema = source.schema};
            if (source.is_peered_source())
            {
                key.peered_node = source.peered_node();
                key.peered_path = source.peered_path();
            }
            else if (source.is_structural_source())
            {
                const auto &children = source.structural_children();
                key.structural_children.reserve(children.size());
                for (const WiringPortRef &child : children) { key.structural_children.push_back(source_key_for(child)); }
            }
            return key;
        }

        [[nodiscard]] InstanceKey make_key(std::type_index def, ResolvedSchema schema,
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

        void evaluate_structural_ref_node(const NodeView &view, engine_time_t evaluation_time)
        {
            auto root   = view.input(evaluation_time);
            auto bundle = root.as_bundle();
            auto ts     = bundle.field("ts");

            Value reference{ts.reference()};
            auto  output   = view.output(evaluation_time);
            auto  mutation = output.begin_mutation(evaluation_time);
            if (!mutation.copy_value_from(reference.view()))
            {
                throw std::logic_error("structural REF node failed to copy the reference value");
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
            schema.valid_inputs      = {0};

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
            if (source.is_null_source()) { return; }
            if (source.is_unbound_source())
            {
                throw std::logic_error("Wiring::finish encountered an unbound wiring source");
            }
            for (const WiringPortRef &child : source.structural_children()) { collect_producers(child, producers); }
        }

        void emit_edges(const WiringPortRef                                             &source,
                        const std::vector<std::size_t>                                 &target_path,
                        const std::unordered_map<const WiringInstance *, std::size_t>   &index_of,
                        GraphBuilder                                                   &graph_builder,
                        std::size_t                                                     target_node)
        {
            if (source.is_peered_source())
            {
                graph_builder.add_edge(GraphEdge{
                    .source_node = index_of.at(source.peered_node()),
                    .source_path = source.peered_path(),
                    .target_node = target_node,
                    .target_path = target_path,
                });
                return;
            }
            if (source.is_null_source()) { return; }
            if (source.is_unbound_source())
            {
                throw std::logic_error("Wiring::finish encountered an unbound wiring source");
            }
            const auto &children = source.structural_children();
            for (std::size_t index = 0; index < children.size(); ++index)
            {
                std::vector<std::size_t> child_target_path = target_path;
                child_target_path.push_back(index);
                emit_edges(children[index], child_target_path, index_of, graph_builder, target_node);
            }
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

    struct Wiring::Impl
    {
        std::deque<WiringInstance>                                        instances{};
        std::unordered_map<InstanceKey, WiringInstance *, InstanceKeyHash> interned{};
        GlobalState                                                        global_state{};
    };

    Wiring::Wiring() : impl_(std::make_unique<Impl>()) {}
    Wiring::~Wiring()                              = default;
    Wiring::Wiring(Wiring &&) noexcept             = default;
    Wiring &Wiring::operator=(Wiring &&) noexcept  = default;

    WiringPortRef Wiring::add_node(std::type_index def, NodeBuilder builder, std::span<const WiringInputRef> inputs,
                                   Value scalars)
    {
        const ResolvedSchema schema = resolved_schema_of(builder);

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

    GlobalStateView Wiring::global_state() noexcept { return impl_->global_state.view(); }

    GraphBuilder Wiring::finish() &&
    {
        std::vector<const WiringInstance *> all;
        all.reserve(impl_->instances.size());
        for (const auto &instance : impl_->instances) { all.push_back(&instance); }

        // Kahn topological sort: an input edge is producer -> consumer.
        std::unordered_map<const WiringInstance *, std::size_t>                         indegree;
        std::unordered_map<const WiringInstance *, std::vector<const WiringInstance *>>  consumers;
        for (const WiringInstance *instance : all) { indegree.try_emplace(instance, 0); }
        for (const WiringInstance *instance : all)
        {
            for (const auto &input : instance->inputs)
            {
                std::vector<const WiringInstance *> producers;
                collect_producers(input.source, producers);
                for (const WiringInstance *producer : producers)
                {
                    ++indegree[instance];
                    consumers[producer].push_back(instance);
                }
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
            throw std::runtime_error("Wiring::finish detected a cycle in the wiring graph");
        }

        std::unordered_map<const WiringInstance *, std::size_t> index_of;
        index_of.reserve(ranked.size());
        for (std::size_t i = 0; i < ranked.size(); ++i) { index_of.emplace(ranked[i], i); }

        GraphBuilder graph_builder;
        for (const WiringInstance *instance : ranked) { graph_builder.add_node(instance->builder); }
        for (std::size_t i = 0; i < ranked.size(); ++i)
        {
            const WiringInstance *instance = ranked[i];
            for (std::size_t input_index = 0; input_index < instance->inputs.size(); ++input_index)
            {
                const WiringInputRef &input = instance->inputs[input_index];
                std::vector<std::size_t> target_path =
                    input.target_path.empty() ? std::vector<std::size_t>{input_index} : input.target_path;
                emit_edges(input.source, target_path, index_of, graph_builder, i);
            }
        }
        graph_builder.global_state(std::move(impl_->global_state));  // carry wiring-time entries onto the graph
        return graph_builder;
    }
}  // namespace hgraph

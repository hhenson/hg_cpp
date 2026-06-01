#include <hgraph/types/graph_wiring.h>

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
        // + input ports by (producing instance, path) + the scalar configuration
        // values. The output schema is implied by the node + path, so it is not
        // part of the key. ``scalars`` is empty for a node with no scalar inputs.
        struct InstanceKey
        {
            std::type_index                                                         def;
            std::vector<std::pair<const WiringInstance *, std::vector<std::size_t>>> inputs;
            Value                                                                   scalars;

            bool operator==(const InstanceKey &other) const noexcept
            {
                if (def != other.def || inputs != other.inputs) { return false; }
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
                auto combine = [&h](std::size_t v) noexcept {
                    h ^= v + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
                };
                for (const auto &[node, path] : key.inputs)
                {
                    combine(std::hash<const void *>{}(node));
                    for (std::size_t p : path) { combine(std::hash<std::size_t>{}(p)); }
                    combine(0xF1F1F1F1ULL);  // path separator
                }
                combine(key.scalars.has_value() ? key.scalars.hash() : std::size_t{0});
                return h;
            }
        };

        [[nodiscard]] InstanceKey make_key(std::type_index def, std::span<const WiringPortRef> inputs,
                                           const Value &scalars)
        {
            InstanceKey key{def, {}, scalars};
            key.inputs.reserve(inputs.size());
            for (const auto &port : inputs) { key.inputs.emplace_back(port.node, port.path); }
            return key;
        }

        [[nodiscard]] const TSValueTypeMetaData *output_schema_of(const WiringInstance &instance)
        {
            const auto *meta = instance.builder.binding().type_meta;
            return meta != nullptr ? meta->output_schema : nullptr;
        }
    }  // namespace

    struct Wiring::Impl
    {
        std::deque<WiringInstance>                                        instances{};
        std::unordered_map<InstanceKey, WiringInstance *, InstanceKeyHash> interned{};
    };

    Wiring::Wiring() : impl_(std::make_unique<Impl>()) {}
    Wiring::~Wiring()                              = default;
    Wiring::Wiring(Wiring &&) noexcept             = default;
    Wiring &Wiring::operator=(Wiring &&) noexcept  = default;

    WiringPortRef Wiring::add_node(std::type_index def, NodeBuilder builder, std::span<const WiringPortRef> inputs,
                                   Value scalars)
    {
        InstanceKey key = make_key(def, inputs, scalars);

        if (auto it = impl_->interned.find(key); it != impl_->interned.end())
        {
            const WiringInstance *existing = it->second;
            return WiringPortRef{existing, {}, output_schema_of(*existing)};
        }

        builder.scalars(std::move(scalars));   // record the scalar configuration on the build artifact

        WiringInstance &instance = impl_->instances.emplace_back();
        instance.builder         = std::move(builder);
        instance.inputs.assign(inputs.begin(), inputs.end());
        impl_->interned.emplace(std::move(key), &instance);

        return WiringPortRef{&instance, {}, output_schema_of(instance)};
    }

    GraphBuilder Wiring::finish() &&
    {
        std::vector<const WiringInstance *> all;
        all.reserve(impl_->instances.size());
        for (const auto &instance : impl_->instances) { all.push_back(&instance); }

        // Kahn topological sort: an input port is an edge producer -> consumer.
        std::unordered_map<const WiringInstance *, std::size_t>                         indegree;
        std::unordered_map<const WiringInstance *, std::vector<const WiringInstance *>>  consumers;
        for (const WiringInstance *instance : all) { indegree.try_emplace(instance, 0); }
        for (const WiringInstance *instance : all)
        {
            for (const auto &port : instance->inputs)
            {
                ++indegree[instance];
                consumers[port.node].push_back(instance);
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
                const WiringPortRef &port = instance->inputs[input_index];
                graph_builder.add_edge(GraphEdge{
                    .source_node = index_of.at(port.node),
                    .source_path = port.path,
                    .target_node = i,
                    .target_path = {input_index},
                });
            }
        }
        return graph_builder;
    }
}  // namespace hgraph

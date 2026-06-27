#include <hgraph/runtime/context_node.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series_reference.h>

#include <deque>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        struct ContextNodeConfig
        {
            std::string                 key{};
            const TSValueTypeMetaData  *target_schema{nullptr};
        };

        [[nodiscard]] const ContextNodeConfig &register_context_node_config(
            std::string key,
            const TSValueTypeMetaData &target_schema)
        {
            if (key.empty()) { throw std::invalid_argument("context node key must not be empty"); }
            // Node callback tables are interned, so builder config storage must
            // outlive every graph instance created from the builder.
            static auto *configs = new std::deque<ContextNodeConfig>;
            configs->push_back(ContextNodeConfig{
                .key = std::move(key),
                .target_schema = &target_schema,
            });
            return configs->back();
        }

        [[nodiscard]] TimeSeriesReference normalize_context_reference(
            const ContextNodeConfig &config,
            TimeSeriesReference reference)
        {
            const auto *actual = reference.target_schema();
            if (actual == nullptr)
            {
                if (reference.is_empty()) { return TimeSeriesReference::empty(config.target_schema); }
                throw std::invalid_argument("context reference has no target schema");
            }

            auto &registry = TypeRegistry::instance();
            if (!time_series_schema_equivalent(registry.dereference(actual),
                                               registry.dereference(config.target_schema)))
            {
                throw std::invalid_argument("context reference target schema does not match context schema");
            }
            return reference;
        }
    }  // namespace

    std::string context_output_key(std::string_view scope, std::string_view path)
    {
        if (scope.empty()) { throw std::invalid_argument("context output scope must not be empty"); }
        if (path.empty()) { throw std::invalid_argument("context output path must not be empty"); }

        std::string key;
        key.reserve(std::string_view{"context-"}.size() + scope.size() + 1U + path.size());
        key.append("context-");
        key.append(scope);
        key.push_back('-');
        key.append(path);
        return key;
    }

    NodeBuilder make_context_capture_node(std::string key, const TSValueTypeMetaData &target_schema)
    {
        const auto *config = &register_context_node_config(std::move(key), target_schema);
        auto       &registry = TypeRegistry::instance();
        (void)registry.ref(&target_schema);

        NodeTypeMetaData schema;
        schema.display_name      = "context_capture";
        schema.input_schema      = registry.un_named_tsb({{"ts", &target_schema}});
        schema.node_kind         = NodeKind::Sink;
        schema.uses_global_state = true;
        schema.active_inputs     = std::vector<std::size_t>{};
        schema.valid_inputs      = std::vector<std::size_t>{};

        NodeCallbacks callbacks;
        callbacks.start = [config](const NodeView &view, DateTime evaluation_time) {
            auto input = view.input(evaluation_time);
            auto bundle = input.as_bundle();
            Value value{normalize_context_reference(*config, TimeSeriesReference{bundle[0]})};
            view.global_state().set(config->key, value);
        };
        callbacks.stop = [config](const NodeView &view, DateTime) {
            (void)view.global_state().erase(config->key);
        };

        NodeBuilder builder = NodeBuilder::native(std::move(schema), std::move(callbacks));
        builder.label("context_capture");
        return builder;
    }

    NodeBuilder make_context_stub_source_node(std::string key, const TSValueTypeMetaData &target_schema)
    {
        const auto *config = &register_context_node_config(std::move(key), target_schema);
        auto       &registry = TypeRegistry::instance();

        NodeTypeMetaData schema;
        schema.display_name      = "context_stub";
        schema.output_schema     = registry.ref(&target_schema);
        schema.node_kind         = NodeKind::PullSource;
        schema.uses_global_state = true;
        schema.schedule_on_start = true;

        NodeCallbacks callbacks;
        callbacks.evaluate = [config](const NodeView &view, DateTime evaluation_time) {
            ValueView stored = view.global_state().get(config->key);
            if (!stored.valid())
            {
                throw std::runtime_error("missing captured context output: " + config->key);
            }

            Value reference{normalize_context_reference(*config, stored.checked_as<TimeSeriesReference>())};
            auto  mutation = view.output(evaluation_time).begin_mutation(evaluation_time);
            if (!mutation.move_value_from(std::move(reference)))
            {
                throw std::logic_error("context stub failed to publish the captured reference");
            }
        };

        NodeBuilder builder = NodeBuilder::native(std::move(schema), std::move(callbacks));
        builder.label("context_stub");
        return builder;
    }
}  // namespace hgraph

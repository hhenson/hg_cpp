#include <hgraph/runtime/shared_output_node.h>

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
        struct SharedOutputConfig
        {
            std::string                 key{};
            const TSValueTypeMetaData  *target_schema{nullptr};
            bool                        strict{true};
        };

        [[nodiscard]] const SharedOutputConfig &register_shared_output_config(
            std::string key,
            const TSValueTypeMetaData &target_schema,
            bool strict)
        {
            if (key.empty()) { throw std::invalid_argument("shared output key must not be empty"); }
            // Node callback tables are interned, so builder config storage must
            // outlive every graph instance created from the builder.
            static auto *configs = new std::deque<SharedOutputConfig>;
            configs->push_back(SharedOutputConfig{
                .key = std::move(key),
                .target_schema = &target_schema,
                .strict = strict,
            });
            return configs->back();
        }

        [[nodiscard]] TimeSeriesReference normalize_shared_output_reference(
            const SharedOutputConfig &config,
            TimeSeriesReference reference)
        {
            const auto *actual = reference.target_schema();
            if (actual == nullptr)
            {
                if (reference.is_empty()) { return TimeSeriesReference::empty(config.target_schema); }
                throw std::invalid_argument("shared output reference has no target schema");
            }

            auto &registry = TypeRegistry::instance();
            if (!time_series_schema_equivalent(registry.dereference(actual),
                                               registry.dereference(config.target_schema)))
            {
                throw std::invalid_argument("shared output reference target schema does not match output schema");
            }
            return reference;
        }
    }  // namespace

    std::string output_key(std::string_view path)
    {
        if (path.empty()) { throw std::invalid_argument("shared output path must not be empty"); }
        return std::string{path};
    }

    std::string output_subscriber_key(std::string_view path)
    {
        std::string key = output_key(path);
        key.append("_subscriber");
        return key;
    }

    NodeBuilder make_shared_output_capture_node(std::string path, const TSValueTypeMetaData &target_schema)
    {
        const auto *config = &register_shared_output_config(output_key(path), target_schema, true);
        auto       &registry = TypeRegistry::instance();
        (void)registry.ref(&target_schema);

        NodeTypeMetaData schema;
        schema.display_name      = "shared_output_capture";
        schema.input_schema      = registry.un_named_tsb({{"ts", &target_schema}});
        schema.node_kind         = NodeKind::Sink;
        schema.uses_global_state = true;
        schema.active_inputs     = std::vector<std::size_t>{};
        schema.valid_inputs      = std::vector<std::size_t>{};

        NodeCallbacks callbacks;
        callbacks.start = [config](const NodeView &view, DateTime evaluation_time) {
            auto input = view.input(evaluation_time);
            auto bundle = input.as_bundle();
            Value value{normalize_shared_output_reference(*config, TimeSeriesReference{bundle[0]})};
            view.global_state().set(config->key, value);
        };
        callbacks.stop = [config](const NodeView &view, DateTime) {
            (void)view.global_state().erase(config->key);
        };

        NodeBuilder builder = NodeBuilder::native(std::move(schema), std::move(callbacks));
        builder.label("shared_output_capture");
        return builder;
    }

    NodeBuilder make_shared_output_stub_source_node(
        std::string path,
        const TSValueTypeMetaData &target_schema,
        bool strict)
    {
        const auto *config = &register_shared_output_config(output_key(path), target_schema, strict);
        auto       &registry = TypeRegistry::instance();

        NodeTypeMetaData schema;
        schema.display_name      = "shared_output_stub";
        schema.output_schema     = registry.ref(&target_schema);
        schema.node_kind         = NodeKind::PullSource;
        schema.uses_global_state = true;
        schema.schedule_on_start = true;

        NodeCallbacks callbacks;
        callbacks.evaluate = [config](const NodeView &view, DateTime evaluation_time) {
            ValueView stored = view.global_state().get(config->key);
            if (!stored.valid())
            {
                if (config->strict)
                {
                    throw std::runtime_error("missing shared output: " + config->key);
                }
                return;
            }

            Value reference{normalize_shared_output_reference(*config, stored.checked_as<TimeSeriesReference>())};
            auto  mutation = view.output(evaluation_time).begin_mutation(evaluation_time);
            if (!mutation.move_value_from(std::move(reference)))
            {
                throw std::logic_error("shared output stub failed to publish the captured reference");
            }
        };

        NodeBuilder builder = NodeBuilder::native(std::move(schema), std::move(callbacks));
        builder.label("shared_output_stub");
        return builder;
    }
}  // namespace hgraph

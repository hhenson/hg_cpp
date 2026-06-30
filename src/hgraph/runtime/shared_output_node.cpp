#include <hgraph/runtime/shared_output_node.h>

#include <hgraph/runtime/graph.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_input/bundle_view.h>
#include <hgraph/types/time_series/ts_input/base_view.h>
#include <hgraph/types/time_series/ts_output/base_view.h>
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

        void capture_shared_output_reference(
            const SharedOutputConfig &config,
            const NodeView &view,
            DateTime evaluation_time,
            DateTime schedule_time)
        {
            auto input = view.input(evaluation_time);
            auto bundle = input.as_bundle();
            auto reference = normalize_shared_output_reference(config, TimeSeriesReference{bundle[0]});

            TSOutputView source_output = bundle[1].bound_output();
            NodeView     source_node   = source_output.owner_node();
            if (!source_node.valid())
            {
                throw std::logic_error("shared output capture could not recover the shared output source node");
            }
            if (!source_node.has_state())
            {
                throw std::logic_error("shared output capture target node has no reference state");
            }

            const auto &previous = source_node.state().checked_as<TimeSeriesReference>();
            if (previous == reference) { return; }

            source_node.replace_state(Value{std::move(reference)});

            GraphValue *graph = source_node.graph_value();
            if (graph == nullptr)
            {
                throw std::logic_error("shared output source node is not attached to a graph");
            }
            graph->schedule_node(source_node.node_index(), schedule_time);
        }
    }  // namespace

    std::string output_key(std::string_view path)
    {
        if (path.empty()) { throw std::invalid_argument("shared output path must not be empty"); }
        return std::string{path};
    }

    NodeBuilder make_shared_output_source_node(
        std::string path,
        const TSValueTypeMetaData &target_schema,
        bool strict)
    {
        const auto *config = &register_shared_output_config(output_key(path), target_schema, strict);
        auto       &registry = TypeRegistry::instance();
        const auto *output_schema = registry.ref(&target_schema);

        NodeTypeMetaData schema;
        schema.display_name      = "shared_output_source";
        schema.output_schema     = output_schema;
        schema.state_schema      = output_schema->value_schema;
        schema.node_kind         = NodeKind::PullSource;
        schema.schedule_on_start = true;

        NodeCallbacks callbacks;
        callbacks.evaluate = [config](const NodeView &view, DateTime evaluation_time) {
            const auto &reference = view.state().checked_as<TimeSeriesReference>();
            if (reference.is_empty() && reference.target_schema() == nullptr)
            {
                if (config->strict) { throw std::runtime_error("missing shared output: " + config->key); }
                return;
            }

            Value value{normalize_shared_output_reference(*config, reference)};
            auto  mutation = view.output(evaluation_time).begin_mutation(evaluation_time);
            if (!mutation.move_value_from(std::move(value)))
            {
                throw std::logic_error("shared output source failed to publish the captured reference");
            }
        };
        callbacks.stop = [](const NodeView &view, DateTime) {
            view.replace_state(Value{TimeSeriesReference{}});
        };

        NodeBuilder builder = NodeBuilder::native(std::move(schema), std::move(callbacks));
        builder.label(std::string{"shared_output_source:"} + config->key);
        return builder;
    }

    NodeBuilder make_shared_output_capture_node(std::string path, const TSValueTypeMetaData &target_schema)
    {
        const auto *config = &register_shared_output_config(output_key(path), target_schema, true);
        auto       &registry  = TypeRegistry::instance();
        const auto *ref_schema = registry.ref(&target_schema);

        NodeTypeMetaData schema;
        schema.display_name      = "shared_output_capture";
        schema.input_schema      = registry.un_named_tsb({
            {"ts", &target_schema},
            {"shared_output", ref_schema},
        });
        schema.node_kind         = NodeKind::Sink;
        schema.active_inputs     = std::vector<std::size_t>{0};
        schema.valid_inputs      = std::vector<std::size_t>{};

        NodeCallbacks callbacks;
        callbacks.start = [config](const NodeView &view, DateTime evaluation_time) {
            capture_shared_output_reference(*config, view, evaluation_time, evaluation_time);
        };
        callbacks.evaluate = [config](const NodeView &view, DateTime evaluation_time) {
            capture_shared_output_reference(*config, view, evaluation_time, evaluation_time + MIN_TD);
        };

        NodeBuilder builder = NodeBuilder::native(std::move(schema), std::move(callbacks));
        builder.label(std::string{"shared_output_capture:"} + config->key);
        return builder;
    }
}  // namespace hgraph

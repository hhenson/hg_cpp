#include <hgraph/runtime/node_error.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value_builder.h>

#include <string>
#include <utility>
#include <vector>

namespace hgraph
{
    const ValueTypeMetaData *node_error_value_meta()
    {
        auto       &registry = TypeRegistry::instance();
        const auto *str_meta = scalar_descriptor<std::string>::value_meta();

        // Rebuild per call: the registry is reset between tests, so a cached
        // ``str_meta`` would dangle. ``bundle`` interns, so this is cheap.
        const std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields{
            {"signature_name", str_meta},
            {"label", str_meta},
            {"wiring_path", str_meta},
            {"error_msg", str_meta},
            {"stack_trace", str_meta},
            {"activation_back_trace", str_meta},
        };
        return registry.bundle("NodeError", fields);
    }

    const TSValueTypeMetaData *node_error_ts_meta()
    {
        return TypeRegistry::instance().ts(node_error_value_meta());
    }

    Value make_node_error_value(const NodeErrorFields &fields)
    {
        const auto *meta    = node_error_value_meta();
        const auto binding = ValuePlanFactory::instance().type_for(meta);
        if (!binding) { throw std::logic_error("NodeError: bundle schema has no canonical binding"); }

        BundleBuilder builder{binding};
        const auto    set_field = [&](std::string_view name, const std::string &text) {
            builder.set(name, Value{text});
        };
        set_field("signature_name", fields.signature_name);
        set_field("label", fields.label);
        set_field("wiring_path", fields.wiring_path);
        set_field("error_msg", fields.error_msg);
        set_field("stack_trace", fields.stack_trace);
        set_field("activation_back_trace", fields.activation_back_trace);
        return builder.build();
    }
}  // namespace hgraph

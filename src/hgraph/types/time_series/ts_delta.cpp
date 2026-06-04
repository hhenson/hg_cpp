#include <hgraph/types/time_series/ts_delta.h>

#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_view.h>

#include <fmt/format.h>

#include <cstdint>
#include <stdexcept>

namespace hgraph
{
    namespace
    {
        [[noreturn]] void unsupported(const char *fn, const TSValueTypeMetaData *schema)
        {
            throw std::logic_error(
                fmt::format("{}: time-series kind {} is not yet supported (only TS / TSS / TSL)", fn,
                            schema != nullptr ? static_cast<int>(schema->kind) : -1));
        }

        [[nodiscard]] const ValueTypeBinding &binding_for(const ValueTypeMetaData *meta, const char *fn)
        {
            const auto *binding = ValuePlanFactory::instance().binding_for(meta);
            if (binding == nullptr) { throw std::logic_error(fmt::format("{}: unresolved value binding", fn)); }
            return *binding;
        }
    }  // namespace

    Value capture_delta(const TSInputView &in)
    {
        const TSValueTypeMetaData *schema = in.schema();
        if (schema == nullptr) { throw std::logic_error("capture_delta: input view has no schema"); }

        switch (schema->kind)
        {
        case TSTypeKind::TS:
            // delta_value_schema == value_type; the scalar value IS the delta.
            return Value{in.value()};

        case TSTypeKind::TSS:
        {
            // delta_value_schema == Bundle{added: Set<E>, removed: Set<E>}.
            const ValueTypeMetaData *bundle_meta = schema->delta_value_schema;
            const ValueTypeMetaData *elem_meta   = schema->value_schema->element_type;  // the Set's element E
            const ValueTypeBinding  &elem_binding = binding_for(elem_meta, "capture_delta");

            const auto set = in.as_set();
            SetBuilder added{elem_binding};
            for (const auto &e : set.added()) { (void)added.insert_copy(e.data()); }
            SetBuilder removed{elem_binding};
            for (const auto &e : set.removed()) { (void)removed.insert_copy(e.data()); }

            BundleBuilder bundle{binding_for(bundle_meta, "capture_delta")};
            bundle.set("added", added.build().view());
            bundle.set("removed", removed.build().view());
            return bundle.build();
        }

        case TSTypeKind::TSL:
        {
            // delta_value_schema == Map<int64, delta(child)>; only modified children appear.
            const ValueTypeMetaData *map_meta    = schema->delta_value_schema;
            const ValueTypeBinding  &key_binding = binding_for(map_meta->key_type, "capture_delta");
            const ValueTypeBinding  &val_binding = binding_for(map_meta->element_type, "capture_delta");

            MapBuilder builder{key_binding, val_binding};
            const auto list = in.as_list();
            for (const auto &[index, child] : list.modified_items())
            {
                const std::int64_t key = static_cast<std::int64_t>(index);
                // The child's canonical delta is exactly the map's value schema, so a
                // whole-value copy of the (owned, rebuilt) child delta is correct for
                // both scalar children (delta == value) and container children.
                const Value child_delta = capture_delta(child);
                builder.set_item_copy(std::addressof(key), child_delta.view().data());
            }
            return builder.build();
        }

        default: unsupported("capture_delta", schema);
        }
    }

    void apply_delta(const TSOutputView &out, const ValueView &delta)
    {
        const TSValueTypeMetaData *schema = out.schema();
        if (schema == nullptr) { throw std::logic_error("apply_delta: output view has no schema"); }

        switch (schema->kind)
        {
        case TSTypeKind::TS:
        {
            auto mutation = out.begin_mutation(out.evaluation_time());
            if (!mutation.copy_value_from(delta)) { throw std::logic_error("apply_delta: failed to copy TS value"); }
            break;
        }

        case TSTypeKind::TSS:
        {
            // Remove before add, mirroring the canonical set-delta application order.
            const auto bundle  = delta.as_bundle();
            auto       set_out = out.as_set();
            auto       mutation = set_out.begin_mutation(out.evaluation_time());
            const auto removed = bundle.field("removed").as_indexed_view();
            for (std::size_t i = 0; i < removed.size(); ++i) { (void)mutation.remove(removed.at(i)); }
            const auto added = bundle.field("added").as_indexed_view();
            for (std::size_t i = 0; i < added.size(); ++i) { (void)mutation.add(added.at(i)); }
            break;
        }

        case TSTypeKind::TSL:
        {
            auto       list_out = out.as_list();
            const auto map      = delta.as_map();
            for (const auto &[key, child_delta] : map)
            {
                auto child = list_out.at(static_cast<std::size_t>(key.template checked_as<std::int64_t>()));
                apply_delta(child, child_delta);
            }
            break;
        }

        default: unsupported("apply_delta", schema);
        }
    }
}  // namespace hgraph

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
                fmt::format("{}: time-series kind {} is not yet supported (supported: TS / SIGNAL / TSS / TSD / TSL / TSB / TSW)", fn,
                            schema != nullptr ? static_cast<int>(schema->kind) : -1));
        }

        [[nodiscard]] const ValueTypeBinding &binding_for(const ValueTypeMetaData *meta, const char *fn)
        {
            const auto *binding = ValuePlanFactory::instance().binding_for(meta);
            if (binding == nullptr) { throw std::logic_error(fmt::format("{}: unresolved value binding", fn)); }
            return *binding;
        }

        [[nodiscard]] bool delta_has_effect(const TSValueTypeMetaData *schema, const ValueView &delta)
        {
            if (schema == nullptr || !delta.has_value()) { return false; }

            switch (schema->kind)
            {
            case TSTypeKind::TS:
            case TSTypeKind::SIGNAL:
            case TSTypeKind::TSW:
                return true;

            case TSTypeKind::TSS:
            {
                const auto bundle = delta.as_bundle();
                return bundle.field("added").as_indexed_view().size() != 0 ||
                       bundle.field("removed").as_indexed_view().size() != 0;
            }

            case TSTypeKind::TSD:
            {
                const auto bundle = delta.as_bundle();
                return bundle.field("removed").as_indexed_view().size() != 0 ||
                       bundle.field("modified").as_map().size() != 0;
            }

            case TSTypeKind::TSL:
                return delta.as_map().size() != 0;

            case TSTypeKind::TSB:
            {
                const auto bundle = delta.as_bundle();
                for (std::size_t index = 0; index < bundle.size(); ++index)
                {
                    if (delta_has_effect(schema->fields()[index].type, bundle.at(index))) { return true; }
                }
                return false;
            }

            default:
                return true;
            }
        }

        [[nodiscard]] Value empty_delta_value(const TSValueTypeMetaData *schema);

        void initialize_tsb_delta_defaults(const TSValueTypeMetaData *schema, BundleBuilder &builder)
        {
            for (std::size_t index = 0; index < schema->field_count(); ++index)
            {
                const TSValueTypeMetaData *child_schema = schema->fields()[index].type;
                if (child_schema == nullptr) { throw std::logic_error("capture_delta: TSB field schema is null"); }
                switch (child_schema->kind)
                {
                case TSTypeKind::TS:
                case TSTypeKind::SIGNAL:
                case TSTypeKind::TSW:
                    break;

                default:
                {
                    Value empty = empty_delta_value(child_schema);
                    builder.set(index, empty.view());
                    break;
                }
                }
            }
        }

        [[nodiscard]] Value empty_delta_value(const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr) { throw std::logic_error("empty_delta_value: schema is null"); }

            switch (schema->kind)
            {
            case TSTypeKind::TSS:
            {
                const ValueTypeBinding &elem_binding =
                    binding_for(schema->value_schema->element_type, "empty_delta_value");
                SetBuilder    added{elem_binding};
                SetBuilder    removed{elem_binding};
                BundleBuilder bundle{binding_for(schema->delta_value_schema, "empty_delta_value")};
                bundle.set("added", added.build().view());
                bundle.set("removed", removed.build().view());
                return bundle.build();
            }

            case TSTypeKind::TSD:
            {
                const ValueTypeMetaData *bundle_meta = schema->delta_value_schema;
                const ValueTypeBinding  &key_binding = binding_for(schema->key_type(), "empty_delta_value");
                const ValueTypeBinding  &delta_binding =
                    binding_for(schema->element_ts()->delta_value_schema, "empty_delta_value");
                SetBuilder    removed{key_binding};
                MapBuilder    modified{key_binding, delta_binding};
                BundleBuilder bundle{binding_for(bundle_meta, "empty_delta_value")};
                bundle.set("removed", removed.build().view());
                bundle.set("modified", modified.build().view());
                return bundle.build();
            }

            case TSTypeKind::TSL:
            {
                const ValueTypeMetaData *map_meta    = schema->delta_value_schema;
                const ValueTypeBinding  &key_binding = binding_for(map_meta->key_type, "empty_delta_value");
                const ValueTypeBinding  &val_binding = binding_for(map_meta->element_type, "empty_delta_value");
                MapBuilder               builder{key_binding, val_binding};
                return builder.build();
            }

            case TSTypeKind::TSB:
            {
                BundleBuilder builder{binding_for(schema->delta_value_schema, "empty_delta_value")};
                initialize_tsb_delta_defaults(schema, builder);
                return builder.build();
            }

            default:
                return Value{binding_for(schema->delta_value_schema, "empty_delta_value")};
            }
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

        case TSTypeKind::SIGNAL:
            // SIGNAL is an input-side tick projection. Its canonical delta is the
            // boolean tick marker, independent of the upstream value schema.
            return Value{true};

        case TSTypeKind::TSW:
            // TSW delta_value_schema == element type; the pushed element is the delta.
            return Value{in.delta_value()};

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

        case TSTypeKind::TSD:
        {
            // delta_value_schema == Bundle{removed: Set<K>, modified: Map<K, delta(V)>}.
            const ValueTypeMetaData *bundle_meta = schema->delta_value_schema;
            const ValueTypeBinding  &bundle_binding = binding_for(bundle_meta, "capture_delta");
            const ValueTypeBinding  &key_binding = binding_for(schema->key_type(), "capture_delta");
            const ValueTypeBinding  &delta_binding =
                binding_for(schema->element_ts()->delta_value_schema, "capture_delta");

            const auto dict = in.as_dict();
            SetBuilder removed{key_binding};
            for (const auto &key : dict.removed_keys()) { (void)removed.insert_copy(key.data()); }

            MapBuilder modified{key_binding, delta_binding};
            for (const auto &[key, child] : dict.modified_items())
            {
                const Value child_delta = capture_delta(child);
                modified.set_item_copy(key.data(), child_delta.view().data());
            }

            BundleBuilder bundle{bundle_binding};
            bundle.set("removed", removed.build().view());
            bundle.set("modified", modified.build().view());
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

        case TSTypeKind::TSB:
        {
            // delta_value_schema == Bundle{field: delta(field_ts)...}. The bundle
            // shape is fixed; only children modified at this tick overwrite their
            // default typed-null / empty delta field.
            BundleBuilder builder{binding_for(schema->delta_value_schema, "capture_delta")};
            initialize_tsb_delta_defaults(schema, builder);
            auto          bundle = in.as_bundle();
            for (std::size_t index = 0; index < bundle.size(); ++index)
            {
                auto child = bundle.at(index);
                if (!child.modified()) { continue; }
                const Value child_delta = capture_delta(child);
                builder.set(index, child_delta.view());
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
        if (!delta_has_effect(schema, delta)) { return; }

        switch (schema->kind)
        {
        case TSTypeKind::TS:
        case TSTypeKind::SIGNAL:
        {
            auto mutation = out.begin_mutation(out.evaluation_time());
            if (!mutation.copy_value_from(delta)) { throw std::logic_error("apply_delta: failed to copy scalar value"); }
            break;
        }

        case TSTypeKind::TSW:
        {
            auto window_out = out.as_window();
            window_out.begin_mutation(out.evaluation_time()).push(delta);
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

        case TSTypeKind::TSD:
        {
            const auto bundle = delta.as_bundle();
            auto       dict_out = out.as_dict();
            auto       mutation = dict_out.begin_mutation(out.evaluation_time());

            const auto removed = bundle.field("removed").as_indexed_view();
            for (std::size_t i = 0; i < removed.size(); ++i) { (void)mutation.erase(removed.at(i)); }

            const auto modified = bundle.field("modified").as_map();
            for (const auto &[key, child_delta] : modified)
            {
                auto child = mutation.at(key);
                apply_delta(TSOutputView{out.output(), child, out.evaluation_time()}, child_delta);
            }
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

        case TSTypeKind::TSB:
        {
            auto       bundle_out = out.as_bundle();
            const auto bundle     = delta.as_bundle();
            for (std::size_t index = 0; index < bundle.size(); ++index)
            {
                const ValueView child_delta = bundle.at(index);
                if (!delta_has_effect(schema->fields()[index].type, child_delta)) { continue; }
                apply_delta(bundle_out.at(index), child_delta);
            }
            break;
        }

        default: unsupported("apply_delta", schema);
        }
    }
}  // namespace hgraph

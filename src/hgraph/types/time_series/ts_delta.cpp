#include <hgraph/types/time_series/ts_delta.h>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
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
        [[nodiscard]] const ValueTypeBinding &binding_for(const ValueTypeMetaData *meta, const char *fn)
        {
            const auto *binding = ValuePlanFactory::instance().binding_for(meta);
            if (binding == nullptr) { throw std::logic_error(fmt::format("{}: unresolved value binding", fn)); }
            return *binding;
        }

        [[nodiscard]] const TSDataBinding &ts_binding_for(const TSValueTypeMetaData *schema, const char *fn)
        {
            const auto *binding = TSDataPlanFactory::instance().binding_for(schema);
            if (binding == nullptr) { throw std::logic_error(fmt::format("{}: unresolved TSData binding", fn)); }
            return *binding;
        }

        [[nodiscard]] const TSDataBinding &require_binding(const TSDataBinding *binding, const char *fn)
        {
            if (binding == nullptr) { throw std::logic_error(fmt::format("{}: view has no TSData binding", fn)); }
            return *binding;
        }

        [[nodiscard]] const TSValueTypeMetaData &require_schema(const TSValueTypeMetaData *schema, const char *fn)
        {
            if (schema == nullptr) { throw std::logic_error(fmt::format("{}: view has no TSData schema", fn)); }
            return *schema;
        }

        void initialize_tsb_delta_defaults(const TSDataBinding &binding, BundleBuilder &builder)
        {
            const auto *schema = binding.type_meta;
            if (schema == nullptr || schema->kind != TSTypeKind::TSB)
            {
                throw std::logic_error("empty_delta_tsb: binding is not a TSB schema");
            }
            for (std::size_t index = 0; index < schema->field_count(); ++index)
            {
                const TSValueTypeMetaData *child_schema = schema->fields()[index].type;
                if (child_schema == nullptr) { throw std::logic_error("empty_delta_tsb: TSB field schema is null"); }
                if (!child_schema->is_collection()) { continue; }

                const TSDataBinding &child_binding = ts_binding_for(child_schema, "empty_delta_tsb");
                const auto          &child_ops     = child_binding.checked_ops();
                Value                empty         = child_ops.empty_delta_impl(child_binding);
                builder.set(index, empty.view());
            }
        }
    }  // namespace

    namespace ts_data_detail
    {
        [[nodiscard]] Value empty_delta_atomic(const TSDataBinding &binding)
        {
            const auto *schema = binding.type_meta;
            if (schema == nullptr || schema->delta_value_schema == nullptr)
            {
                throw std::logic_error("empty_delta_atomic: schema is not resolved");
            }
            return Value{*schema->delta_value_schema};
        }

        [[nodiscard]] Value empty_delta_tss(const TSDataBinding &binding)
        {
            const auto *schema = binding.type_meta;
            if (schema == nullptr || schema->value_schema == nullptr || schema->delta_value_schema == nullptr)
            {
                throw std::logic_error("empty_delta_tss: schema is not resolved");
            }

            const ValueTypeBinding &elem_binding =
                binding_for(schema->value_schema->element_type, "empty_delta_tss");
            SetBuilder    added{elem_binding};
            SetBuilder    removed{elem_binding};
            BundleBuilder bundle{binding_for(schema->delta_value_schema, "empty_delta_tss")};
            bundle.set("added", added.build().view());
            bundle.set("removed", removed.build().view());
            return bundle.build();
        }

        [[nodiscard]] Value empty_delta_tsd(const TSDataBinding &binding)
        {
            const auto *schema = binding.type_meta;
            if (schema == nullptr || schema->delta_value_schema == nullptr || schema->element_ts() == nullptr)
            {
                throw std::logic_error("empty_delta_tsd: schema is not resolved");
            }

            const ValueTypeBinding &key_binding = binding_for(schema->key_type(), "empty_delta_tsd");
            const ValueTypeBinding &delta_binding =
                binding_for(schema->element_ts()->delta_value_schema, "empty_delta_tsd");
            SetBuilder    removed{key_binding};
            MapBuilder    modified{key_binding, delta_binding};
            BundleBuilder bundle{binding_for(schema->delta_value_schema, "empty_delta_tsd")};
            bundle.set("removed", removed.build().view());
            bundle.set("modified", modified.build().view());
            return bundle.build();
        }

        [[nodiscard]] Value empty_delta_tsl(const TSDataBinding &binding)
        {
            const auto *schema = binding.type_meta;
            if (schema == nullptr || schema->delta_value_schema == nullptr)
            {
                throw std::logic_error("empty_delta_tsl: schema is not resolved");
            }

            const ValueTypeMetaData *map_meta    = schema->delta_value_schema;
            const ValueTypeBinding  &key_binding = binding_for(map_meta->key_type, "empty_delta_tsl");
            const ValueTypeBinding  &val_binding = binding_for(map_meta->element_type, "empty_delta_tsl");
            MapBuilder               builder{key_binding, val_binding};
            return builder.build();
        }

        [[nodiscard]] Value empty_delta_tsb(const TSDataBinding &binding)
        {
            const auto *schema = binding.type_meta;
            if (schema == nullptr || schema->delta_value_schema == nullptr)
            {
                throw std::logic_error("empty_delta_tsb: schema is not resolved");
            }

            BundleBuilder builder{binding_for(schema->delta_value_schema, "empty_delta_tsb")};
            initialize_tsb_delta_defaults(binding, builder);
            return builder.build();
        }

        Value capture_delta_ts(const TSInputView &in)
        {
            return Value{in.value()};
        }

        Value capture_delta_signal(const TSInputView &)
        {
            return Value{true};
        }

        Value capture_delta_tsw(const TSInputView &in)
        {
            return Value{in.delta_value()};
        }

        Value capture_delta_tss(const TSInputView &in)
        {
            const TSValueTypeMetaData *schema = in.schema();
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

        Value capture_delta_tsd(const TSInputView &in)
        {
            const TSValueTypeMetaData *schema = in.schema();
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

        Value capture_delta_tsl(const TSInputView &in)
        {
            const TSValueTypeMetaData *schema = in.schema();
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

        Value capture_delta_tsb(const TSInputView &in)
        {
            const auto &binding = ts_binding_for(&require_schema(in.schema(), "capture_delta"), "capture_delta");
            const auto *schema  = binding.type_meta;
            BundleBuilder builder{binding_for(schema->delta_value_schema, "capture_delta")};
            initialize_tsb_delta_defaults(binding, builder);
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

        bool delta_has_effect_atomic(const TSOutputView &, const ValueView &delta)
        {
            return delta.has_value();
        }

        bool delta_has_effect_tss(const TSOutputView &, const ValueView &delta)
        {
            if (!delta.has_value()) { return false; }
            const auto bundle = delta.as_bundle();
            return bundle.field("added").as_indexed_view().size() != 0 ||
                   bundle.field("removed").as_indexed_view().size() != 0;
        }

        bool delta_has_effect_tsd(const TSOutputView &, const ValueView &delta)
        {
            if (!delta.has_value()) { return false; }
            const auto bundle = delta.as_bundle();
            return bundle.field("removed").as_indexed_view().size() != 0 ||
                   bundle.field("modified").as_map().size() != 0;
        }

        bool delta_has_effect_tsl(const TSOutputView &, const ValueView &delta)
        {
            return delta.has_value() && delta.as_map().size() != 0;
        }

        bool delta_has_effect_tsb(const TSOutputView &out, const ValueView &delta)
        {
            if (!delta.has_value()) { return false; }

            auto       bundle_out = out.as_bundle();
            const auto bundle     = delta.as_bundle();
            for (std::size_t index = 0; index < bundle.size(); ++index)
            {
                auto        child     = bundle_out.at(index);
                const auto &child_ops = child.data_view().ops();
                if (child_ops.delta_has_effect_impl(child, bundle.at(index))) { return true; }
            }
            return false;
        }

        void apply_delta_atomic(const TSOutputView &out, const ValueView &delta)
        {
            auto mutation = out.begin_mutation(out.evaluation_time());
            if (!mutation.copy_value_from(delta)) { throw std::logic_error("apply_delta: failed to copy scalar value"); }
        }

        void apply_delta_tsw(const TSOutputView &out, const ValueView &delta)
        {
            auto window_out = out.as_window();
            window_out.begin_mutation(out.evaluation_time()).push(delta);
        }

        void apply_delta_tss(const TSOutputView &out, const ValueView &delta)
        {
            // Remove before add, mirroring the canonical set-delta application order.
            const auto bundle  = delta.as_bundle();
            auto       set_out = out.as_set();
            auto       mutation = set_out.begin_mutation(out.evaluation_time());
            const auto removed = bundle.field("removed").as_indexed_view();
            for (std::size_t i = 0; i < removed.size(); ++i) { (void)mutation.remove(removed.at(i)); }
            const auto added = bundle.field("added").as_indexed_view();
            for (std::size_t i = 0; i < added.size(); ++i) { (void)mutation.add(added.at(i)); }
        }

        void apply_delta_tsd(const TSOutputView &out, const ValueView &delta)
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
        }

        void apply_delta_tsl(const TSOutputView &out, const ValueView &delta)
        {
            auto       list_out = out.as_list();
            const auto map      = delta.as_map();
            for (const auto &[key, child_delta] : map)
            {
                auto child = list_out.at(static_cast<std::size_t>(key.template checked_as<std::int64_t>()));
                apply_delta(child, child_delta);
            }
        }

        void apply_delta_tsb(const TSOutputView &out, const ValueView &delta)
        {
            auto       bundle_out = out.as_bundle();
            const auto bundle     = delta.as_bundle();
            for (std::size_t index = 0; index < bundle.size(); ++index)
            {
                apply_delta(bundle_out.at(index), bundle.at(index));
            }
        }
    }  // namespace ts_data_detail

    Value capture_delta(const TSInputView &in)
    {
        const auto &binding = ts_binding_for(&require_schema(in.schema(), "capture_delta"), "capture_delta");
        return binding.checked_ops().capture_delta_impl(in);
    }

    void apply_delta(const TSOutputView &out, const ValueView &delta)
    {
        const auto &binding = require_binding(out.binding(), "apply_delta");
        const auto &ops     = binding.checked_ops();
        if (!ops.delta_has_effect_impl(out, delta)) { return; }
        ops.apply_delta_impl(out, delta);
    }
}  // namespace hgraph

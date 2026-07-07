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

#include <array>
#include <cstdint>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

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
                const auto          &child_ops     = child_binding.ops_ref();
                Value                empty         = child_ops.empty_delta_impl(child_binding);
                builder.set(index, std::move(empty));
            }
        }

        [[nodiscard]] bool field_name_equal(const TSFieldMetaData &ts_field,
                                            const ValueFieldMetaData &value_field) noexcept
        {
            const std::string_view ts_name =
                ts_field.name != nullptr ? std::string_view{ts_field.name} : std::string_view{};
            const std::string_view value_name =
                value_field.name != nullptr ? std::string_view{value_field.name} : std::string_view{};
            return ts_name == value_name;
        }

        [[nodiscard]] bool current_value_schema_compatible_ts(const TSValueTypeMetaData &schema,
                                                              const ValueTypeMetaData   &value_schema) noexcept
        {
            if (schema.value_schema == &value_schema) { return true; }
            // Variadic tuple vs list: distinct schema identity, ONE layout
            // (same element type; flags differ only by VariadicTuple).
            const auto *target = schema.value_schema;
            return target != nullptr && target->kind == ValueTypeKind::List &&
                   value_schema.kind == ValueTypeKind::List && target->fixed_size == 0 &&
                   value_schema.fixed_size == 0 && !target->is_mutable() && !value_schema.is_mutable() &&
                   target->element_type == value_schema.element_type;
        }

        [[nodiscard]] bool current_value_schema_compatible_tss(const TSValueTypeMetaData &schema,
                                                               const ValueTypeMetaData   &value_schema) noexcept
        {
            return schema.value_schema == &value_schema;
        }

        [[nodiscard]] bool current_value_schema_compatible_tsd(const TSValueTypeMetaData &schema,
                                                               const ValueTypeMetaData   &value_schema)
        {
            return value_schema.kind == ValueTypeKind::Map &&
                   schema.key_type() == value_schema.key_type &&
                   schema.element_ts() != nullptr &&
                   value_schema.element_type != nullptr &&
                   current_value_schema_compatible(*schema.element_ts(), *value_schema.element_type);
        }

        [[nodiscard]] bool current_value_schema_compatible_tsl(const TSValueTypeMetaData &schema,
                                                               const ValueTypeMetaData   &value_schema)
        {
            return value_schema.kind == ValueTypeKind::List &&
                   schema.element_ts() != nullptr &&
                   value_schema.element_type != nullptr &&
                   (schema.fixed_size() == 0 || value_schema.fixed_size == 0 ||
                    schema.fixed_size() == value_schema.fixed_size) &&
                   current_value_schema_compatible(*schema.element_ts(), *value_schema.element_type);
        }

        [[nodiscard]] bool current_value_schema_compatible_tsb(const TSValueTypeMetaData &schema,
                                                               const ValueTypeMetaData   &value_schema)
        {
            if (value_schema.kind != ValueTypeKind::Bundle ||
                schema.field_count() != value_schema.field_count)
            {
                return false;
            }
            for (std::size_t index = 0; index < schema.field_count(); ++index)
            {
                const auto &ts_field    = schema.fields()[index];
                const auto &value_field = value_schema.fields[index];
                if (!field_name_equal(ts_field, value_field) ||
                    ts_field.type == nullptr ||
                    value_field.type == nullptr ||
                    !current_value_schema_compatible(*ts_field.type, *value_field.type))
                {
                    return false;
                }
            }
            return true;
        }

        using CurrentValueSchemaCompatibleFn = bool (*)(const TSValueTypeMetaData &, const ValueTypeMetaData &);

        [[nodiscard]] constexpr std::size_t ts_kind_index(TSTypeKind kind) noexcept
        {
            return static_cast<std::size_t>(kind);
        }

        [[nodiscard]] CurrentValueSchemaCompatibleFn current_value_schema_compatible_for(
            TSTypeKind kind) noexcept
        {
            static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
            static const std::array<CurrentValueSchemaCompatibleFn, kind_count> table{
                &current_value_schema_compatible_ts,
                &current_value_schema_compatible_tss,
                &current_value_schema_compatible_tsd,
                &current_value_schema_compatible_tsl,
                &current_value_schema_compatible_ts,
                &current_value_schema_compatible_tsb,
                &current_value_schema_compatible_ts,
                &current_value_schema_compatible_ts,
            };

            const auto index = ts_kind_index(kind);
            return index < table.size() ? table[index] : &current_value_schema_compatible_ts;
        }

        [[nodiscard]] bool schema_contains_tsd(const TSValueTypeMetaData *schema) noexcept
        {
            if (schema == nullptr) { return false; }
            if (schema->kind == TSTypeKind::TSD) { return true; }
            if (schema->kind == TSTypeKind::TSL) { return schema_contains_tsd(schema->element_ts()); }
            if (schema->kind == TSTypeKind::TSB)
            {
                for (std::size_t index = 0; index < schema->field_count(); ++index)
                {
                    if (schema_contains_tsd(schema->fields()[index].type)) { return true; }
                }
            }
            return false;
        }

        void apply_current_value_direct(const TSOutputView &out, const ValueView &value)
        {
            auto mutation = out.begin_mutation(out.evaluation_time());
            static_cast<void>(mutation.copy_value_from(value));
        }

        void apply_current_value_tsd(const TSOutputView &out, const ValueView &value)
        {
            auto       dict_out   = out.as_dict();
            auto       mutation   = dict_out.begin_mutation(out.evaluation_time());
            const auto source_map = value.as_map();

            mutation.touch();
            for (const auto &[key, child_value] : source_map)
            {
                auto child = mutation.at(key);
                apply_current_value(TSOutputView{out.output(), child, out.evaluation_time()}, child_value);
            }

            std::vector<Value> removals;
            for (const auto key : mutation.keys())
            {
                if (!source_map.contains(key)) { removals.emplace_back(key); }
            }
            for (const auto &key : removals) { static_cast<void>(mutation.erase(key.view())); }
        }

        void apply_current_value_tsl(const TSOutputView &out, const ValueView &value)
        {
            const auto *schema = out.schema();
            if (schema == nullptr) { throw std::logic_error("apply_current_value: TSL output schema is missing"); }
            if (value.schema() == schema->value_schema && !schema_contains_tsd(schema))
            {
                apply_current_value_direct(out, value);
                return;
            }

            const auto source_values = value.as_indexed_view();
            if (schema->fixed_size() != 0 && source_values.size() != schema->fixed_size())
            {
                throw std::invalid_argument("apply_current_value: fixed TSL value has the wrong child count");
            }

            auto list_out = out.as_list();
            if (schema->fixed_size() == 0 && source_values.size() < list_out.size())
            {
                throw std::invalid_argument("apply_current_value: dynamic TSL current value cannot shrink");
            }

            for (std::size_t index = 0; index < source_values.size(); ++index)
            {
                auto source_child = source_values.at(index);
                // UNSET source children are skipped (the TSL carries the
                // same field-validity contract as TSB).
                if (!source_child.valid()) { continue; }
                apply_current_value(list_out.at(index), source_child);
            }
        }

        void apply_current_value_tsb(const TSOutputView &out, const ValueView &value)
        {
            const auto *schema = out.schema();
            if (schema == nullptr) { throw std::logic_error("apply_current_value: TSB output schema is missing"); }
            const auto source_values = value.as_indexed_view();
            if (source_values.size() != schema->field_count())
            {
                throw std::invalid_argument("apply_current_value: TSB value has the wrong field count");
            }

            auto bundle_out = out.as_bundle();
            for (std::size_t index = 0; index < source_values.size(); ++index)
            {
                auto source_child = source_values.at(index);
                // UNSET source fields are skipped (Bundle field validity):
                // applying a partially-valid value writes only its live
                // fields - never defaults.
                if (!source_child.valid()) { continue; }
                apply_current_value(bundle_out.at(index), source_child);
            }
        }

        using CurrentValueApplyFn = void (*)(const TSOutputView &, const ValueView &);

        [[nodiscard]] CurrentValueApplyFn current_value_apply_for(TSTypeKind kind) noexcept
        {
            static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
            static const std::array<CurrentValueApplyFn, kind_count> table{
                &apply_current_value_direct,
                &apply_current_value_direct,
                &apply_current_value_tsd,
                &apply_current_value_tsl,
                &apply_current_value_direct,
                &apply_current_value_tsb,
                &apply_current_value_direct,
                &apply_current_value_direct,
            };

            const auto index = ts_kind_index(kind);
            return index < table.size() ? table[index] : &apply_current_value_direct;
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
            bundle.set("added", added.build());
            bundle.set("removed", removed.build());
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
            bundle.set("removed", removed.build());
            bundle.set("modified", modified.build());
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
            bundle.set("added", added.build());
            bundle.set("removed", removed.build());
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
                if (!child.valid()) { continue; }   // empty-reference elements have no value
                const Value child_delta = capture_delta(child);
                modified.set_item_copy(key.data(), child_delta.view().data());
            }

            BundleBuilder bundle{bundle_binding};
            bundle.set("removed", removed.build());
            bundle.set("modified", modified.build());
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
                if (!child.valid()) { continue; }   // empty-reference elements have no value
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
                // modified-but-INVALID children are skipped: a from-REF
                // rebind marks the position modified, but an EMPTY reference
                // leaves it unbound - there is no value to capture.
                if (!child.modified() || !child.valid()) { continue; }
                Value child_delta = capture_delta(child);
                builder.set(index, std::move(child_delta));
            }
            return builder.build();
        }

        bool delta_has_effect_atomic(const TSOutputView &, const ValueView &delta)
        {
            return delta.has_value();
        }

        bool delta_has_effect_tss(const TSOutputView &out, const ValueView &delta)
        {
            if (!delta.has_value()) { return false; }
            const auto bundle = delta.as_bundle();
            if (bundle.field("added").as_indexed_view().size() != 0 ||
                bundle.field("removed").as_indexed_view().size() != 0)
            {
                return true;
            }
            // An explicitly EMPTY tick VALIDATES a fresh set (hgraph: the
            // TSS becomes valid with the empty value); on an already-valid
            // set it stays a no-op (dedup).
            return !out.valid();
        }

        bool delta_has_effect_tsd(const TSOutputView &out, const ValueView &delta)
        {
            if (!delta.has_value()) { return false; }
            const auto bundle = delta.as_bundle();
            if (bundle.field("removed").as_indexed_view().size() != 0 ||
                bundle.field("modified").as_map().size() != 0)
            {
                return true;
            }
            // The empty-tick validation rule, as for TSS.
            return !out.valid();
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
            // Touch LAST: the once-per-time notification rides the FIRST
            // record and must carry the real changes; the trailing touch
            // VALIDATES an explicitly-empty tick (hgraph: an empty first
            // tick makes the set valid with the empty value).
            mutation.touch();
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
            mutation.touch();   // LAST - the empty-tick validation rule (see apply_delta_tss)
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
        return binding.ops_ref().capture_delta_impl(in);
    }

    void apply_delta(const TSOutputView &out, const ValueView &delta)
    {
        const auto &binding = require_binding(out.binding(), "apply_delta");
        const auto &ops     = binding.ops_ref();
        if (!ops.delta_has_effect_impl(out, delta)) { return; }
        ops.apply_delta_impl(out, delta);
    }

    bool current_value_schema_compatible(const TSValueTypeMetaData &schema,
                                         const ValueTypeMetaData   &value_schema)
    {
        return current_value_schema_compatible_for(schema.kind)(schema, value_schema);
    }

    void apply_current_value(const TSOutputView &out, const ValueView &value)
    {
        const auto &schema = require_schema(out.schema(), "apply_current_value");
        if (!value.has_value())
        {
            throw std::invalid_argument("apply_current_value requires a live source value");
        }
        const auto *value_schema = value.schema();
        if (value_schema == nullptr || !current_value_schema_compatible(schema, *value_schema))
        {
            throw std::invalid_argument("apply_current_value source value schema does not match the output schema");
        }

        current_value_apply_for(schema.kind)(out, value);
    }
}  // namespace hgraph

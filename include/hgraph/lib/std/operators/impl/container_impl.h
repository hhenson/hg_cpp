#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_CONTAINER_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_CONTAINER_IMPL_H

#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/subgraph_wiring.h>

#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace hgraph::stdlib
{
    namespace container_impl_detail
    {
        [[nodiscard]] inline std::size_t normalize_item_index(Int index, std::size_t size,
                                                              std::string_view subject)
        {
            const Int signed_size = static_cast<Int>(size);
            const Int normalized  = index < 0 ? signed_size + index : index;
            if (normalized < 0 || normalized >= signed_size)
            {
                std::string message{"getitem_: "};
                message += subject;
                message += " index out of range";
                throw std::out_of_range(message);
            }
            return static_cast<std::size_t>(normalized);
        }

        template <typename T>
        void set_if_changed(const Out<TS<T>> &out, const T &value)
        {
            if (!out.valid() || out.value().template checked_as<T>() != value) { out.set(value); }
        }

        [[nodiscard]] inline const TSValueTypeMetaData *direct_tsb_schema(const WiringArg &arg) noexcept
        {
            if (arg.kind != WiringArg::Kind::TimeSeries) { return nullptr; }
            const TSValueTypeMetaData *schema = arg.port.schema;
            return schema != nullptr && schema->kind == TSTypeKind::TSB ? schema : nullptr;
        }

        [[nodiscard]] inline const TSValueTypeMetaData *direct_tsl_schema(const WiringArg &arg) noexcept
        {
            if (arg.kind != WiringArg::Kind::TimeSeries) { return nullptr; }
            const TSValueTypeMetaData *schema = TypeRegistry::instance().dereference(arg.port.schema);
            return schema != nullptr && schema->kind == TSTypeKind::TSL ? schema : nullptr;
        }

        [[nodiscard]] inline const TSValueTypeMetaData *direct_tsd_schema(const WiringArg &arg) noexcept
        {
            if (arg.kind != WiringArg::Kind::TimeSeries) { return nullptr; }
            const TSValueTypeMetaData *schema = TypeRegistry::instance().dereference(arg.port.schema);
            return schema != nullptr && schema->kind == TSTypeKind::TSD ? schema : nullptr;
        }

        [[nodiscard]] inline std::optional<std::size_t> find_tsb_field_index(const TSValueTypeMetaData &schema,
                                                                             std::string_view name) noexcept
        {
            for (std::size_t index = 0; index < schema.field_count(); ++index)
            {
                const TSFieldMetaData &field = schema.fields()[index];
                if (field.name != nullptr && name == std::string_view{field.name}) { return index; }
            }
            return std::nullopt;
        }

        [[nodiscard]] inline std::optional<std::size_t> find_tsb_field_index(const TSValueTypeMetaData &schema,
                                                                             Int index)
        {
            if (schema.field_count() == 0) { return std::nullopt; }
            const Int signed_size = static_cast<Int>(schema.field_count());
            const Int normalized  = index < 0 ? signed_size + index : index;
            if (normalized < 0 || normalized >= signed_size) { return std::nullopt; }
            return static_cast<std::size_t>(normalized);
        }

        template <typename Key>
        [[nodiscard]] inline const TSValueTypeMetaData *tsb_field_schema(const TSValueTypeMetaData &schema,
                                                                         const Key &key)
        {
            std::optional<std::size_t> index = find_tsb_field_index(schema, key);
            return index.has_value() ? schema.fields()[*index].type : nullptr;
        }
    }  // namespace container_impl_detail

    struct getitem_string
    {
        static void eval(In<"ts", TS<Str>> ts, In<"key", TS<Int>> key, Out<TS<Str>> out)
        {
            const Str         value = ts.value();
            const std::size_t index = container_impl_detail::normalize_item_index(key.value(), value.size(), "string");
            out.set(value.substr(index, 1));
        }
    };

    struct contains_string
    {
        static void eval(In<"ts", TS<Str>> ts, In<"item", TS<Str>> item, Out<TS<Bool>> out)
        {
            out.set(ts.value().find(item.value()) != Str::npos);
        }
    };

    struct len_string
    {
        static void eval(In<"ts", TS<Str>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(ts.value().size()));
        }
    };

    struct is_empty_string
    {
        static void eval(In<"ts", TS<Str>> ts, Out<TS<Bool>> out)
        {
            out.set(ts.value().empty());
        }
    };

    struct len_tss
    {
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts, Out<TS<Int>> out)
        {
            container_impl_detail::set_if_changed(out, ts.valid() ? static_cast<Int>(ts.size()) : Int{0});
        }
    };

    struct is_empty_tss
    {
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts, Out<TS<Bool>> out)
        {
            container_impl_detail::set_if_changed(out, !ts.valid() || ts.empty());
        }
    };

    struct contains_tss_item
    {
        static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts,
                         In<"item", TS<ScalarVar<"K">>> item, Out<TS<Bool>> out)
        {
            container_impl_detail::set_if_changed(out, ts.valid() && ts.base().as_set().contains(item.base().value()));
        }
    };

    struct contains_tss_subset
    {
        static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts,
                         In<"item", TSS<ScalarVar<"K">>> item, Out<TS<Bool>> out)
        {
            Bool contains_all = ts.valid();
            if (contains_all)
            {
                const auto &item_set = item.base().as_set();
                for (const ValueView &value : item_set.values())
                {
                    if (!ts.base().as_set().contains(value))
                    {
                        contains_all = false;
                        break;
                    }
                }
            }
            container_impl_detail::set_if_changed(out, contains_all);
        }
    };

    struct len_tsd
    {
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts, Out<TS<Int>> out)
        {
            container_impl_detail::set_if_changed(out, ts.valid() ? static_cast<Int>(ts.size()) : Int{0});
        }
    };

    struct is_empty_tsd
    {
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts, Out<TS<Bool>> out)
        {
            container_impl_detail::set_if_changed(out, !ts.valid() || ts.empty());
        }
    };

    struct contains_tsd_key
    {
        static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
                         In<"item", TS<ScalarVar<"K">>> item, Out<TS<Bool>> out)
        {
            container_impl_detail::set_if_changed(out, ts.valid() && ts.base().as_dict().contains(item.base().value()));
        }
    };

    struct len_tsl
    {
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ts", TSL<TS<ScalarVar<"T">>, SIZE<"N">>, InputValidity::Unchecked> ts, Out<TS<Int>> out)
        {
            container_impl_detail::set_if_changed(out, static_cast<Int>(ts.size()));
        }
    };

    struct getitem_tsl_by_index
    {
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            if (context.args.size() != 2) { return false; }
            const TSValueTypeMetaData *schema = container_impl_detail::direct_tsl_schema(context.args[0]);
            if (schema == nullptr) { return false; }

            // Fixed-size scalar indexing is a structural wiring projection
            // (tsl_element). Only non-fixed TSLs should promote a scalar key
            // into the dynamic time-series index path.
            if (context.args[1].kind == WiringArg::Kind::Scalar && schema->fixed_size() != 0) { return false; }
            return true;
        }

        static void eval(In<"ts", TSL<TsVar<"E">, SIZE<"N">>, InputValidity::Unchecked> ts,
                         In<"key", TS<Int>> key, Out<REF<TsVar<"E">>> out)
        {
            // The ts input is ACTIVE: element/reference REBINDS (e.g. race
            // re-targeting its winner) must re-emit the item reference, not
            // only key ticks. Same-reference re-publishes are deduped so
            // ordinary element value ticks do not retrigger consumers.
            const std::size_t index =
                container_impl_detail::normalize_item_index(key.value(), ts.size(), "TSL");
            TimeSeriesReference reference = ts[index].base().reference();
            if (out.valid() && out.value().checked_as<TimeSeriesReference>() == reference) { return; }
            out.set(std::move(reference));
        }
    };

    struct getitem_tsd_by_key
    {
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.args.size() == 2 &&
                   container_impl_detail::direct_tsd_schema(context.args[0]) != nullptr;
        }

        static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
                         In<"key", TS<ScalarVar<"K">>> key, Out<REF<TsVar<"V">>> out)
        {
            const auto *schema = TypeRegistry::instance().dereference(ts.base().schema());
            const auto *target = schema != nullptr ? schema->element_ts() : nullptr;

            TimeSeriesReference reference{target};
            const auto         &dict = static_cast<const TSDInputView &>(ts);
            const std::size_t   slot = dict.find_slot(key.base().value());
            if (slot != TS_DATA_NO_CHILD_ID) { reference = ts.at_slot(slot).base().reference(); }

            if (!out.valid() || !(out.value().checked_as<TimeSeriesReference>() == reference))
            {
                out.set(reference);
            }
        }
    };

    struct index_of_tsl
    {
        static void eval(In<"ts", TSL<TS<ScalarVar<"T">>, SIZE<"N">>, InputValidity::Unchecked> ts,
                         In<"item", TS<ScalarVar<"T">>> item, Out<TS<Int>> out)
        {
            Int index = -1;
            for (std::size_t i = 0; i < ts.size(); ++i)
            {
                auto child = ts[i];
                if (child.valid() && child.base().value().equals(item.base().value()))
                {
                    index = static_cast<Int>(i);
                    break;
                }
            }
            container_impl_detail::set_if_changed(out, index);
        }
    };

    struct getitem_tsb_by_name
    {
        static constexpr auto name = "getitem_tsb_by_name";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (resolution.find_ts("__out__") != nullptr) { return; }
            if (context.args.size() != 2) { return; }

            const TSValueTypeMetaData *schema = container_impl_detail::direct_tsb_schema(context.args[0]);
            const Str                 *key    = context.scalar_as<Str>("key");
            if (schema == nullptr || key == nullptr) { return; }

            const TSValueTypeMetaData *field_schema = container_impl_detail::tsb_field_schema(*schema, *key);
            if (field_schema != nullptr) { resolution.bind_ts("__out__", field_schema); }
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            if (context.args.size() != 2) { return false; }
            const TSValueTypeMetaData *schema = container_impl_detail::direct_tsb_schema(context.args[0]);
            const Str                 *key    = context.scalar_as<Str>("key");
            return schema != nullptr && key != nullptr &&
                   container_impl_detail::find_tsb_field_index(*schema, *key).has_value();
        }

        static WiringPortRef compose(Wiring &, NamedPort<"ts", TsVar<"S">> ts, Scalar<"key", Str> key)
        {
            const TSValueTypeMetaData *schema = ts.erased().schema;
            std::size_t index = *container_impl_detail::find_tsb_field_index(*schema, key.value());
            return subgraph_wiring_detail::tsb_field_ref(ts.erased(), index, schema->fields()[index].type);
        }
    };

    struct getitem_tsb_by_index
    {
        static constexpr auto name = "getitem_tsb_by_index";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (resolution.find_ts("__out__") != nullptr) { return; }
            if (context.args.size() != 2) { return; }

            const TSValueTypeMetaData *schema = container_impl_detail::direct_tsb_schema(context.args[0]);
            const Int                 *key    = context.scalar_as<Int>("key");
            if (schema == nullptr || key == nullptr) { return; }

            const TSValueTypeMetaData *field_schema = container_impl_detail::tsb_field_schema(*schema, *key);
            if (field_schema != nullptr) { resolution.bind_ts("__out__", field_schema); }
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            if (context.args.size() != 2) { return false; }
            const TSValueTypeMetaData *schema = container_impl_detail::direct_tsb_schema(context.args[0]);
            const Int                 *key    = context.scalar_as<Int>("key");
            return schema != nullptr && key != nullptr &&
                   container_impl_detail::find_tsb_field_index(*schema, *key).has_value();
        }

        static WiringPortRef compose(Wiring &, NamedPort<"ts", TsVar<"S">> ts, Scalar<"key", Int> key)
        {
            const TSValueTypeMetaData *schema = ts.erased().schema;
            std::size_t index = *container_impl_detail::find_tsb_field_index(*schema, key.value());
            return subgraph_wiring_detail::tsb_field_ref(ts.erased(), index, schema->fields()[index].type);
        }
    };

    struct getattr_tsb
    {
        static constexpr auto name = "getattr_tsb";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (resolution.find_ts("__out__") != nullptr) { return; }
            if (context.args.size() != 2) { return; }

            const TSValueTypeMetaData *schema = container_impl_detail::direct_tsb_schema(context.args[0]);
            const Str                 *attr   = context.scalar_as<Str>("attr");
            if (schema == nullptr || attr == nullptr) { return; }

            const TSValueTypeMetaData *field_schema = container_impl_detail::tsb_field_schema(*schema, *attr);
            if (field_schema != nullptr) { resolution.bind_ts("__out__", field_schema); }
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            if (context.args.size() != 2) { return false; }
            const TSValueTypeMetaData *schema = container_impl_detail::direct_tsb_schema(context.args[0]);
            const Str                 *attr   = context.scalar_as<Str>("attr");
            return schema != nullptr && attr != nullptr &&
                   container_impl_detail::find_tsb_field_index(*schema, *attr).has_value();
        }

        static WiringPortRef compose(Wiring &, NamedPort<"ts", TsVar<"S">> ts, Scalar<"attr", Str> attr)
        {
            const TSValueTypeMetaData *schema = ts.erased().schema;
            std::size_t index = *container_impl_detail::find_tsb_field_index(*schema, attr.value());
            return subgraph_wiring_detail::tsb_field_ref(ts.erased(), index, schema->fields()[index].type);
        }
    };

    struct len_tsb
    {
        static constexpr auto name = "len_tsb";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.args.size() == 1 &&
                   container_impl_detail::direct_tsb_schema(context.args[0]) != nullptr;
        }

        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TsVar<"S">> ts)
        {
            return wire<const_, TS<Int>>(w, static_cast<Int>(ts.erased().schema->field_count()));
        }
    };

    struct is_empty_tsb
    {
        static constexpr auto name = "is_empty_tsb";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.args.size() == 1 &&
                   container_impl_detail::direct_tsb_schema(context.args[0]) != nullptr;
        }

        static Port<TS<Bool>> compose(Wiring &w, NamedPort<"ts", TsVar<"S">> ts)
        {
            return wire<const_, TS<Bool>>(w, ts.erased().schema->field_count() == 0);
        }
    };

    inline void register_container_operators()
    {
        register_overload<getitem_, getitem_string>();
        register_overload<contains_, contains_string>();
        register_overload<len_, len_string>();
        register_overload<is_empty, is_empty_string>();

        register_overload<len_, len_tss>();
        register_overload<len_, len_tsd>();
        register_overload<len_, len_tsl>();

        register_overload<is_empty, is_empty_tss>();
        register_overload<is_empty, is_empty_tsd>();

        register_overload<contains_, contains_tss_item>();
        register_overload<contains_, contains_tss_subset>();
        register_overload<contains_, contains_tsd_key>();

        register_overload<getitem_, getitem_tsl_by_index>();
        register_overload<getitem_, getitem_tsd_by_key>();
        register_overload<index_of, index_of_tsl>();

        register_graph_overload<getitem_, getitem_tsb_by_name>();
        register_graph_overload<getitem_, getitem_tsb_by_index>();
        register_graph_overload<getattr_, getattr_tsb>();
        register_graph_overload<len_, len_tsb>();
        register_graph_overload<is_empty, is_empty_tsb>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_CONTAINER_IMPL_H

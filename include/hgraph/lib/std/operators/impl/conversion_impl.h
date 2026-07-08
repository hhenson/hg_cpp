#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_CONVERSION_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_CONVERSION_IMPL_H

#include <hgraph/lib/std/operators/impl/type_resolution_helpers.h>
#include <hgraph/lib/std/operators/arithmetic.h>    // add_ / mul_ (zero_ op mapping)
#include <hgraph/lib/std/operators/collection.h>    // sum_     (zero_ op mapping)
#include <hgraph/lib/std/operators/comparison.h>    // min_ / max_ (zero_ op mapping)
#include <hgraph/lib/std/operators/conversion.h>    // const_ / zero_ / default_
#include <hgraph/runtime/node_scheduler.h>          // SingleShotScheduler
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/type_resolution.h>
#include <hgraph/types/wired_fn.h>
#include <hgraph/util/date_time.h>

#include <limits>
#include <deque>
#include <stdexcept>

namespace hgraph::stdlib
{
    /**
     * Implementations + registration for the conversion / utility operators. The abstract
     * markers are in ``<hgraph/lib/std/operators/conversion.h>``; this file provides the
     * concrete overloads and ``register_conversion_operators`` to register them. Only the
     * additive-zero source (``zero_``) and the constant source (``const_``) are implemented
     * so far.
     */

    /**
     * Shared output-type resolution for the ``const_`` overloads. The value type is a
     * ``ScalarVar`` inferred from the configured value; without an explicit output schema the
     * output resolves to ``TS<T>``, and with one the configured value's schema must match
     * that output's value schema.
     */
    inline void const_resolve_output(ResolutionMap &resolution)
    {
        const auto *value_schema  = resolution.scalar("T");
        const auto *output_schema = resolution.find_ts("S");
        if (output_schema == nullptr)
        {
            resolution.bind_ts("S", TypeRegistry::instance().ts(value_schema));
            return;
        }
        if (value_schema == nullptr || !current_value_schema_compatible(*output_schema, *value_schema))
        {
            throw std::logic_error("const: configured value schema does not match the resolved output value schema");
        }
    }

    /**
     * ``const_`` implementation — ``const(value)``: a single generic source that ticks the
     * configured ``value`` once at the start cycle (declared via ``schedule_on_start``).
     */
    struct const_source
    {
        static constexpr auto name              = "const";
        static constexpr bool schedule_on_start = true;

        static void resolve_default_types(ResolutionMap &resolution) { const_resolve_output(resolution); }

        static void eval(Scalar<"value", ScalarVar<"T">> value, Out<TsVar<"S">> out)
        {
            out.apply(value.value());  // erased copy of the configured value
        }
    };

    /**
     * ``const_`` implementation — ``const(value, delay)``: as ``const_source`` but the single
     * tick is delayed to ``start_time + delay``. The ``delay`` drives the one-shot schedule in
     * ``start``; ``eval`` applies the value (matching Python's ``yield start_time + delay, value``).
     */
    struct const_delayed
    {
        static constexpr auto name = "const";

        static void resolve_default_types(ResolutionMap &resolution) { const_resolve_output(resolution); }

        static void start(Scalar<"delay", TimeDelta> delay, SingleShotScheduler sched)
        {
            sched.schedule(delay.value());   // now + delay (now == start_time during start)
        }

        static void eval(Scalar<"value", ScalarVar<"T">> value, Scalar<"delay", TimeDelta> delay,
                         Out<TsVar<"S">> out)
        {
            static_cast<void>(delay);   // delay drives the start schedule; eval just applies the value
            out.apply(value.value());
        }
    };

    /**
     * ``zero_`` implementations — op-aware zero sources, mirroring Python
     * ``_impl/_operators/_zero.py``: the zero value depends on both the output
     * type and the operation (``add_``/``sum_`` -> identity of addition,
     * ``mul_`` -> identity of multiplication, ``min_``/``max_`` -> the
     * saturating bound). An unmapped operation is a wiring-time error, like
     * Python's ``KeyError``.
     */
    struct zero_int
    {
        static constexpr auto name = "zero_int";

        static Port<TS<Int>> compose(Wiring &w, Scalar<"op", WiredFn> op)
        {
            const WiredFn &f = op.value();
            Int            value{};
            if (f == fn<add_>() || f == fn<sum_>()) { value = Int{0}; }
            else if (f == fn<min_>()) { value = std::numeric_limits<Int>::max(); }
            else if (f == fn<max_>()) { value = std::numeric_limits<Int>::lowest(); }
            else if (f == fn<mul_>()) { value = Int{1}; }
            else { throw std::invalid_argument("zero: no TS<Int> zero value registered for the supplied operation"); }
            return wire<const_, TS<Int>>(w, value);
        }
    };

    struct zero_float
    {
        static constexpr auto name = "zero_float";

        static Port<TS<Float>> compose(Wiring &w, Scalar<"op", WiredFn> op)
        {
            const WiredFn &f = op.value();
            Float          value{};
            if (f == fn<add_>() || f == fn<sum_>()) { value = Float{0}; }
            else if (f == fn<min_>()) { value = std::numeric_limits<Float>::infinity(); }
            else if (f == fn<max_>()) { value = -std::numeric_limits<Float>::infinity(); }
            else if (f == fn<mul_>()) { value = Float{1}; }
            else
            {
                throw std::invalid_argument("zero: no TS<Float> zero value registered for the supplied operation");
            }
            return wire<const_, TS<Float>>(w, value);
        }
    };

    struct zero_str
    {
        static constexpr auto name = "zero_str";

        static Port<TS<Str>> compose(Wiring &w, Scalar<"op", WiredFn> op)
        {
            const WiredFn &f = op.value();
            if (f == fn<add_>() || f == fn<sum_>() || f == fn<mul_>())
            {
                return wire<const_, TS<Str>>(w, Str{});
            }
            throw std::invalid_argument("zero: no TS<Str> zero value registered for the supplied operation");
        }
    };

    /**
     * ``default`` implementation — the REF-forwarding node, mirroring Python
     * ``_impl/_operators/_graph_operators.py`` ``_default`` (``valid=()``):
     * while ``ts`` is invalid the output references ``default_value`` and ``ts``
     * is kept active (to notice its first tick); once valid the output
     * references ``ts`` and ``ts`` is made passive — downstream reads flow
     * *through* the reference without this node evaluating per tick. (Python
     * takes ``default_value`` as a ``REF`` input so value ticks never re-fire
     * the node; here it is a plain active input — for the ``const`` zeros it
     * ticks once, and a re-emit of the same reference is a cheap downstream
     * no-op rebind.)
     *
     * The output **is** a ``REF`` and its port keeps that computed schema —
     * the declared ``-> S`` contract holds because ``REF`` is transparent to
     * matching/unification (a variable binds the dereferenced type) and
     * consumers bind through the reference at runtime.
     */
    struct default_impl
    {
        static constexpr auto name = "default";

        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"default_value", TsVar<"S">, InputValidity::Unchecked> default_value,
                         Out<REF<TsVar<"S">>> out)
        {
            if (!ts.valid())
            {
                ts.make_active();
                out.set(default_value.reference());
            }
            else
            {
                ts.make_passive();
                out.set(ts.reference());
            }
        }
    };

    namespace convert_detail
    {
        /** The requested target: __out__ (bound from the explicit ``to`` /
            subscript by the py bridge or wire<> caller). */
        [[nodiscard]] inline const TSValueTypeMetaData *requested_out(const ResolutionMap &resolution)
        {
            return resolution.find_ts("__out__");
        }

        [[nodiscard]] inline const ValueTypeMetaData *ts_value(const TSValueTypeMetaData *schema)
        {
            return schema != nullptr && schema->kind == TSTypeKind::TS ? schema->value_schema : nullptr;
        }
    }  // namespace convert_detail

    /** convert(ts, to=SAME) - identical schemas pass the value through. */
    struct convert_identity_impl
    {
        static constexpr auto name = "convert_identity";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = convert_detail::requested_out(resolution);
            return out != nullptr &&
                   operator_impl_detail::time_series_schema_at(context, 0) == out;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.copy_value_from(ts.base().value()));
        }
    };

    /** Numeric/bool scalar conversions: TYPED kernels selected at node-
        selection time (the typed-kernel rule - no per-tick type branch). */
    template <typename From, typename To>
    struct convert_numeric_impl
    {
        static_assert(!std::same_as<From, To>);
        static constexpr auto name = "convert_numeric";

        // The concrete Out<TS<To>> is gated by the dispatcher's requested-
        // output match; requires_ only checks the INPUT.
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return convert_detail::ts_value(operator_impl_detail::time_series_schema_at(context, 0)) ==
                   scalar_descriptor<From>::value_meta();
        }

        static void eval(In<"ts", TS<From>> ts, Out<TS<To>> out)
        {
            if constexpr (std::same_as<To, Bool>) { out.set(ts.value() != From{}); }
            else { out.set(static_cast<To>(ts.value())); }
        }
    };

    /** Python-style str() conversions (py parity: 0.0 -> "0.0",
        True -> "True", tuples print as "(1, 2)"). */
    template <typename From>
    struct convert_to_str_impl
    {
        static constexpr auto name = "convert_to_str";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return convert_detail::ts_value(operator_impl_detail::time_series_schema_at(context, 0)) ==
                   scalar_descriptor<From>::value_meta();
        }

        static void eval(In<"ts", TS<From>> ts, Out<TS<Str>> out)
        {
            if constexpr (std::same_as<From, Bool>) { out.set(Str{ts.value() ? "True" : "False"}); }
            else if constexpr (std::same_as<From, Float>)
            {
                const Float value = ts.value();
                std::string text  = fmt::format("{}", value);
                if (text.find('.') == std::string::npos && text.find('e') == std::string::npos &&
                    text.find("inf") == std::string::npos && text.find("nan") == std::string::npos)
                {
                    text += ".0";   // python float repr always shows the point
                }
                out.set(Str{std::move(text)});
            }
            else { out.set(Str{fmt::format("{}", ts.value())}); }
        }
    };

    /** str() of a LIST-valued TS: python TUPLE style - "(1, 2)", "(1,)". */
    struct convert_list_to_str_impl
    {
        static constexpr auto name = "convert_list_to_str";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *in = convert_detail::ts_value(operator_impl_detail::time_series_schema_at(context, 0));
            return in != nullptr && in->kind == ValueTypeKind::List;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TS<Str>> out)
        {
            const auto  value = ts.base().value();
            auto        items = value.as_indexed_view();
            std::string text  = "(";
            for (std::size_t index = 0; index < items.size(); ++index)
            {
                if (index != 0) { text += ", "; }
                text += items.at(index).to_string();
            }
            text += items.size() == 1 ? ",)" : ")";
            out.set(Str{std::move(text)});
        }
    };

    /** bool() of a LIST-valued TS: python truthiness (non-empty). */
    struct convert_list_to_bool_impl
    {
        static constexpr auto name = "convert_list_to_bool";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *in = convert_detail::ts_value(operator_impl_detail::time_series_schema_at(context, 0));
            return in != nullptr && in->kind == ValueTypeKind::List;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TS<Bool>> out)
        {
            out.set(ts.base().value().as_indexed_view().size() > 0);
        }
    };

    /** date -> datetime (midnight). */
    struct convert_date_to_datetime_impl
    {
        static constexpr auto name = "convert_date_to_datetime";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return convert_detail::ts_value(operator_impl_detail::time_series_schema_at(context, 0)) ==
                   scalar_descriptor<Date>::value_meta();
        }

        static void eval(In<"ts", TS<Date>> ts, Out<TS<DateTime>> out)
        {
            out.set(DateTime{std::chrono::sys_days{ts.value()}});
        }
    };

    /** convert TS[T] -> TS[Set[T]] / TS[tuple[T,...]]: the SINGLETON
        collection value. */
    struct convert_ts_to_collection_impl
    {
        static constexpr auto name = "convert_ts_to_collection";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = convert_detail::ts_value(convert_detail::requested_out(resolution));
            const auto *in  = convert_detail::ts_value(operator_impl_detail::time_series_schema_at(context, 0));
            return out != nullptr && in != nullptr &&
                   (out->kind == ValueTypeKind::Set || out->kind == ValueTypeKind::List) &&
                   out->element_type == in;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto *meta   = erased.schema()->value_schema;
            const auto  value  = ts.base().value();
            Value       result;
            if (meta->kind == ValueTypeKind::Set)
            {
                SetBuilder builder{*ValuePlanFactory::instance().binding_for(meta->element_type)};
                static_cast<void>(builder.insert_copy(value.data()));
                result = builder.build();
            }
            else
            {
                ListBuilder builder{*ValuePlanFactory::instance().binding_for(meta->element_type)};
                builder.push_back_copy(value.data());
                result = builder.build();
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(result)));
        }
    };

    /** convert between COLLECTION-VALUED TS: tuple <-> set (same element). */
    struct convert_collection_to_collection_impl
    {
        static constexpr auto name = "convert_collection_to_collection";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = convert_detail::ts_value(convert_detail::requested_out(resolution));
            const auto *in  = convert_detail::ts_value(operator_impl_detail::time_series_schema_at(context, 0));
            return out != nullptr && in != nullptr && out != in &&
                   (out->kind == ValueTypeKind::Set || out->kind == ValueTypeKind::List) &&
                   (in->kind == ValueTypeKind::Set || in->kind == ValueTypeKind::List) &&
                   out->element_type == in->element_type;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto *meta   = erased.schema()->value_schema;
            const auto  value  = ts.base().value();
            auto        items  = value.as_indexed_view();
            Value       result;
            if (meta->kind == ValueTypeKind::Set)
            {
                SetBuilder builder{*ValuePlanFactory::instance().binding_for(meta->element_type)};
                for (std::size_t index = 0; index < items.size(); ++index)
                {
                    static_cast<void>(builder.insert_copy(items.at(index).data()));
                }
                result = builder.build();
            }
            else
            {
                ListBuilder builder{*ValuePlanFactory::instance().binding_for(meta->element_type)};
                for (std::size_t index = 0; index < items.size(); ++index)
                {
                    builder.push_back_copy(items.at(index).data());
                }
                result = builder.build();
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(result)));
        }
    };

    /** convert TSS[T] -> TS[Set[T]] / TS[tuple[T,...]]: the full membership
        as a scalar collection each tick. */
    struct convert_tss_to_collection_impl
    {
        static constexpr auto name = "convert_tss_to_collection";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = convert_detail::ts_value(convert_detail::requested_out(resolution));
            const auto *in  = operator_impl_detail::time_series_arg_of_kind(context, 0, TSTypeKind::TSS);
            return out != nullptr && in != nullptr &&
                   (out->kind == ValueTypeKind::Set || out->kind == ValueTypeKind::List) &&
                   out->element_type == in->value_schema->element_type;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto  &erased = static_cast<const TSOutputView &>(out);
            const auto  *meta   = erased.schema()->value_schema;
            TSSInputView set_input{ts.base().borrowed_ref()};
            auto         set    = set_input.data_view();
            Value        result;
            if (meta->kind == ValueTypeKind::Set)
            {
                SetBuilder builder{*ValuePlanFactory::instance().binding_for(meta->element_type)};
                for (const ValueView &element : set.values())
                {
                    static_cast<void>(builder.insert_copy(element.data()));
                }
                result = builder.build();
            }
            else
            {
                ListBuilder builder{*ValuePlanFactory::instance().binding_for(meta->element_type)};
                for (const ValueView &element : set.values()) { builder.push_back_copy(element.data()); }
                result = builder.build();
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(result)));
        }
    };

    /** convert TS[Set[T]] / TS[tuple[T,...]] -> TSS[T]: desired-membership
        writes (adds + removals fall out of the diff). */
    struct convert_collection_to_tss_impl
    {
        static constexpr auto name = "convert_collection_to_tss";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = convert_detail::requested_out(resolution);
            const auto *in  = convert_detail::ts_value(operator_impl_detail::time_series_schema_at(context, 0));
            return out != nullptr && out->kind == TSTypeKind::TSS && in != nullptr &&
                   (in->kind == ValueTypeKind::Set || in->kind == ValueTypeKind::List) &&
                   out->value_schema->element_type == in->element_type;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased  = static_cast<const TSOutputView &>(out);
            auto        set     = erased.as_set();
            auto        mutation = set.begin_mutation(erased.evaluation_time());
            const auto  value   = ts.base().value();
            auto        items   = value.as_indexed_view();

            const auto contains_in_desired = [&](const ValueView &element) {
                for (std::size_t index = 0; index < items.size(); ++index)
                {
                    if (items.at(index).equals(element)) { return true; }
                }
                return false;
            };
            std::vector<Value> stale;
            for (const ValueView &element : mutation.view().values())
            {
                if (!contains_in_desired(element)) { stale.emplace_back(element); }
            }
            for (const Value &element : stale) { static_cast<void>(mutation.remove(element.view())); }
            for (std::size_t index = 0; index < items.size(); ++index)
            {
                static_cast<void>(mutation.add(items.at(index)));
            }
        }
    };

    /** convert TSD[K, TS[V]] -> TS[Mapping[K, V]]: the full dictionary as a
        scalar map each tick. */
    struct convert_tsd_to_map_impl
    {
        static constexpr auto name = "convert_tsd_to_map";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = convert_detail::ts_value(convert_detail::requested_out(resolution));
            const auto *in  = operator_impl_detail::time_series_arg_of_kind(context, 0, TSTypeKind::TSD);
            if (out == nullptr || in == nullptr || out->kind != ValueTypeKind::Map) { return false; }
            const auto *element = TypeRegistry::instance().dereference(in->element_ts());
            return element != nullptr && element->kind == TSTypeKind::TS &&
                   out->key_type == in->key_type() && out->element_type == element->value_schema;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto  &erased = static_cast<const TSOutputView &>(out);
            const auto  *meta   = erased.schema()->value_schema;
            const TSDInputView dict{ts.base().borrowed_ref()};
            MapBuilder         builder{*ValuePlanFactory::instance().binding_for(meta->key_type),
                                       *ValuePlanFactory::instance().binding_for(meta->element_type)};
            for (auto &&[key, child] : dict.items())
            {
                if (!child.valid()) { continue; }
                builder.set_item_copy(key.data(), child.value().data());
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(builder.build()));
        }
    };

    /** convert TS[Mapping[K, V]] -> TSD[K, TS[V]]: desired-map writes. */
    struct convert_map_to_tsd_impl
    {
        static constexpr auto name = "convert_map_to_tsd";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = convert_detail::requested_out(resolution);
            const auto *in  = convert_detail::ts_value(operator_impl_detail::time_series_schema_at(context, 0));
            if (out == nullptr || out->kind != TSTypeKind::TSD || in == nullptr ||
                in->kind != ValueTypeKind::Map)
            {
                return false;
            }
            const auto *element = TypeRegistry::instance().dereference(out->element_ts());
            return element != nullptr && element->kind == TSTypeKind::TS &&
                   out->key_type() == in->key_type && element->value_schema == in->element_type;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            auto        dict   = erased.as_dict();
            auto        mutation = dict.begin_mutation(erased.evaluation_time());
            const auto  value  = ts.base().value();
            auto        map    = value.as_map();

            // Erase keys no longer present, then write every desired entry
            // (equal values dedup before the write - READ-side compare, the
            // flip_keys lesson).
            std::vector<Value> stale;
            for (const ValueView &key : mutation.view().keys())
            {
                if (!map.contains(key)) { stale.emplace_back(key); }
            }
            for (const Value &key : stale) { static_cast<void>(mutation.erase(key.view())); }
            for (const auto [key, entry_value] : map)
            {
                auto element = mutation.at(key);
                if (element.has_current_value() && element.value().equals(entry_value)) { continue; }
                auto element_mutation = element.begin_mutation(erased.evaluation_time());
                static_cast<void>(element_mutation.copy_value_from(entry_value));
            }
        }
    };

    /** convert TS[T] -> TSS[T]: the tick value becomes the SINGLETON set
        (write-desired semantics produce add-new/remove-old deltas). */
    struct convert_ts_to_tss_impl
    {
        static constexpr auto name = "convert_ts_to_tss";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = convert_detail::requested_out(resolution);
            const auto *in  = operator_impl_detail::time_series_arg_of_kind(context, 0, TSTypeKind::TS);
            return out != nullptr && out->kind == TSTypeKind::TSS && in != nullptr &&
                   out->value_schema->element_type == in->value_schema;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            auto        set    = erased.as_set();
            auto        mutation = set.begin_mutation(erased.evaluation_time());
            const auto  value  = ts.base().value();
            // Desired = {value}: remove everything else, insert the value.
            std::vector<Value> stale;
            for (const ValueView &element : mutation.view().values())
            {
                if (!element.equals(value)) { stale.emplace_back(element); }
            }
            for (const Value &element : stale) { static_cast<void>(mutation.remove(element.view())); }
            static_cast<void>(mutation.add(value));
        }
    };

    /** convert[TS[Mapping]](k, v): the SINGLETON mapping {k: v}. */
    struct convert_kv_to_map_impl
    {
        static constexpr auto name = "convert_kv_to_map";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = convert_detail::ts_value(convert_detail::requested_out(resolution));
            const auto *k   = convert_detail::ts_value(operator_impl_detail::time_series_schema_at(context, 0));
            const auto *v   = convert_detail::ts_value(operator_impl_detail::time_series_schema_at(context, 1));
            return out != nullptr && out->kind == ValueTypeKind::Map && k != nullptr && v != nullptr &&
                   out->key_type == k && out->element_type == v;
        }

        static void eval(In<"key", TsVar<"K">> key, In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto *meta   = erased.schema()->value_schema;
            MapBuilder  builder{*ValuePlanFactory::instance().binding_for(meta->key_type),
                                *ValuePlanFactory::instance().binding_for(meta->element_type)};
            builder.set_item_copy(key.base().value().data(), ts.base().value().data());
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(builder.build()));
        }
    };

    // ----- collect / emit ------------------------------------------------

    /** collect[TS[Set/Tuple]](ts, reset=...): accumulate ticks into a
        growing collection; a True reset clears BEFORE that cycle's tick.
        A collection-valued tick unions ALL its elements in. */
    struct collect_collection_impl
    {
        static constexpr auto name = "collect_collection";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = convert_detail::ts_value(convert_detail::requested_out(resolution));
            const auto *in  = convert_detail::ts_value(operator_impl_detail::time_series_schema_at(context, 0));
            if (out == nullptr || in == nullptr ||
                (out->kind != ValueTypeKind::Set && out->kind != ValueTypeKind::List))
            {
                return false;
            }
            const auto *element =
                in->kind == ValueTypeKind::Set || in->kind == ValueTypeKind::List ? in->element_type : in;
            return out->element_type == element;
        }

        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"reset", TS<Bool>, InputValidity::Unchecked> reset,
                         Out<TsVar<"__out__">> out)
        {
            if (!ts.modified() && !(reset.valid() && reset.modified())) { return; }
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto *meta   = erased.schema()->value_schema;
            const bool  fresh  = reset.valid() && reset.modified() && reset.value();

            const auto add_all = [&](auto &builder) {
                if (!fresh && erased.data_view().has_current_value())
                {
                    auto prior = erased.value().as_indexed_view();
                    for (std::size_t index = 0; index < prior.size(); ++index)
                    {
                        add_one(builder, prior.at(index));
                    }
                }
                if (ts.valid() && ts.modified())
                {
                    const auto value = ts.base().value();
                    const auto *in_meta = value.schema();
                    if (in_meta->kind == ValueTypeKind::Set || in_meta->kind == ValueTypeKind::List)
                    {
                        auto items = value.as_indexed_view();
                        for (std::size_t index = 0; index < items.size(); ++index)
                        {
                            add_one(builder, items.at(index));
                        }
                    }
                    else { add_one(builder, value); }
                }
            };

            Value result;
            if (meta->kind == ValueTypeKind::Set)
            {
                SetBuilder builder{*ValuePlanFactory::instance().binding_for(meta->element_type)};
                add_all(builder);
                result = builder.build();
            }
            else
            {
                ListBuilder builder{*ValuePlanFactory::instance().binding_for(meta->element_type)};
                add_all(builder);
                result = builder.build();
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(result)));
        }

      private:
        static void add_one(SetBuilder &builder, const ValueView &value)
        {
            static_cast<void>(builder.insert_copy(value.data()));
        }
        static void add_one(ListBuilder &builder, const ValueView &value)
        {
            builder.push_back_copy(value.data());
        }
    };

    /** collect[TS[Mapping]](k, v, reset=...): accumulate key/value pairs. */
    struct collect_map_impl
    {
        static constexpr auto name = "collect_map";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = convert_detail::ts_value(convert_detail::requested_out(resolution));
            const auto *k   = convert_detail::ts_value(operator_impl_detail::time_series_schema_at(context, 0));
            const auto *v   = convert_detail::ts_value(operator_impl_detail::time_series_schema_at(context, 1));
            return out != nullptr && out->kind == ValueTypeKind::Map && k != nullptr && v != nullptr &&
                   out->key_type == k && out->element_type == v;
        }

        static void eval(In<"key", TsVar<"K">, InputValidity::Unchecked> key,
                         In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"reset", TS<Bool>, InputValidity::Unchecked> reset,
                         Out<TsVar<"__out__">> out)
        {
            const bool ticked = (key.modified() || ts.modified()) && key.valid() && ts.valid();
            if (!ticked && !(reset.valid() && reset.modified())) { return; }
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto *meta   = erased.schema()->value_schema;
            const bool  fresh  = reset.valid() && reset.modified() && reset.value();

            MapBuilder builder{*ValuePlanFactory::instance().binding_for(meta->key_type),
                               *ValuePlanFactory::instance().binding_for(meta->element_type)};
            if (!fresh && erased.data_view().has_current_value())
            {
                for (const auto [k, v] : erased.value().as_map()) { builder.set_item_copy(k.data(), v.data()); }
            }
            if (ticked)
            {
                builder.set_item_copy(key.base().value().data(), ts.base().value().data());
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(builder.build()));
        }
    };

    /** collect[TSD](k, v, reset=...): accumulate into an owned dictionary. */
    struct collect_tsd_impl
    {
        static constexpr auto name = "collect_tsd";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *out = convert_detail::requested_out(resolution);
            const auto *k   = convert_detail::ts_value(operator_impl_detail::time_series_schema_at(context, 0));
            const auto *v   = convert_detail::ts_value(operator_impl_detail::time_series_schema_at(context, 1));
            if (out == nullptr || out->kind != TSTypeKind::TSD || k == nullptr || v == nullptr) { return false; }
            const auto *element = TypeRegistry::instance().dereference(out->element_ts());
            return out->key_type() == k && element != nullptr && element->kind == TSTypeKind::TS &&
                   element->value_schema == v;
        }

        static void eval(In<"key", TsVar<"K">, InputValidity::Unchecked> key,
                         In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"reset", TS<Bool>, InputValidity::Unchecked> reset,
                         Out<TsVar<"__out__">> out)
        {
            const bool ticked = (key.modified() || ts.modified()) && key.valid() && ts.valid();
            const bool fresh  = reset.valid() && reset.modified() && reset.value();
            if (!ticked && !fresh) { return; }
            const auto &erased  = static_cast<const TSOutputView &>(out);
            auto        dict    = erased.as_dict();
            auto        mutation = dict.begin_mutation(erased.evaluation_time());

            if (fresh)
            {
                std::vector<Value> stale;
                const auto key_value = key.base().value();
                for (const ValueView &existing : mutation.view().keys())
                {
                    if (!(ticked && existing.equals(key_value))) { stale.emplace_back(existing); }
                }
                for (const Value &existing : stale) { static_cast<void>(mutation.erase(existing.view())); }
            }
            if (ticked)
            {
                auto element          = mutation.at(key.base().value());
                auto element_mutation = element.begin_mutation(erased.evaluation_time());
                static_cast<void>(element_mutation.copy_value_from(ts.base().value()));
            }
        }
    };

    namespace convert_detail
    {
        struct EmitQueueState
        {
            std::deque<Value> buffer{};
        };
    }  // namespace convert_detail

}  // namespace hgraph::stdlib

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<stdlib::convert_detail::EmitQueueState>
    {
        static constexpr std::string_view value{"stdlib.emit_queue_state"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{
    /** emit(TS[Set/Tuple]): drain the collection ONE ELEMENT PER CYCLE
        (scheduler-driven; a new tick appends its elements). */
    struct emit_collection_impl
    {
        static constexpr auto name = "emit_collection";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *in = convert_detail::ts_value(operator_impl_detail::time_series_schema_at(context, 0));
            return in != nullptr && (in->kind == ValueTypeKind::Set || in->kind == ValueTypeKind::List);
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (operator_impl_detail::output_bound(resolution)) { return; }
            const auto *in = convert_detail::ts_value(operator_impl_detail::time_series_schema_at(context, 0));
            if (in == nullptr) { return; }
            operator_impl_detail::bind_output(resolution, TypeRegistry::instance().ts(in->element_type));
        }

        static void eval(In<"ts", TsVar<"S">> ts,
                         NodeScheduler scheduler,
                         State<convert_detail::EmitQueueState> state,
                         Out<TsVar<"__out__">> out)
        {
            auto current = state.get();
            if (ts.modified())
            {
                auto items = ts.base().value().as_indexed_view();
                for (std::size_t index = 0; index < items.size(); ++index)
                {
                    current.buffer.emplace_back(items.at(index));
                }
            }
            if (!current.buffer.empty())
            {
                const auto &erased = static_cast<const TSOutputView &>(out);
                Value       next   = std::move(current.buffer.front());
                current.buffer.pop_front();
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.move_value_from(std::move(next)));
                if (!current.buffer.empty()) { scheduler.schedule(MIN_TD); }
            }
            state.set(std::move(current));
        }
    };

    /** emit(TS[Mapping]) / emit(TSD): drain key/value PAIRS one per cycle
        into a {key, value} bundle (hgraph's KeyValue shape). A TSD input
        enqueues its DELTA entries; a mapping tick enqueues all entries. */
    struct emit_map_impl
    {
        static constexpr auto name = "emit_map";

        [[nodiscard]] static std::pair<const ValueTypeMetaData *, const ValueTypeMetaData *> kv_of(
            OperatorCallContext context)
        {
            const auto *surface = operator_impl_detail::time_series_schema_at(context, 0);
            if (surface == nullptr) { return {nullptr, nullptr}; }
            if (surface->kind == TSTypeKind::TSD)
            {
                const auto *element = TypeRegistry::instance().dereference(surface->element_ts());
                if (element == nullptr || element->kind != TSTypeKind::TS) { return {nullptr, nullptr}; }
                return {surface->key_type(), element->value_schema};
            }
            const auto *value = convert_detail::ts_value(surface);
            if (value == nullptr || value->kind != ValueTypeKind::Map) { return {nullptr, nullptr}; }
            return {value->key_type, value->element_type};
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return kv_of(context).first != nullptr;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (operator_impl_detail::output_bound(resolution)) { return; }
            const auto [k, v] = kv_of(context);
            if (k == nullptr) { return; }
            auto &registry = TypeRegistry::instance();
            operator_impl_detail::bind_output(
                resolution, registry.un_named_tsb({{"key", registry.ts(k)}, {"value", registry.ts(v)}}));
        }

        static void eval(In<"ts", TsVar<"S">> ts,
                         NodeScheduler scheduler,
                         State<convert_detail::EmitQueueState> state,
                         Out<TsVar<"__out__">> out)
        {
            auto current = state.get();
            if (ts.modified())
            {
                const auto *surface = ts.base().schema();
                if (surface->kind == TSTypeKind::TSD)
                {
                    const TSDInputView dict{ts.base().borrowed_ref()};
                    for (auto &&[key, child] : dict.items())
                    {
                        if (!child.modified() || !child.valid()) { continue; }
                        current.buffer.emplace_back(key);
                        current.buffer.emplace_back(child.value());
                    }
                }
                else
                {
                    for (const auto [key, value] : ts.base().value().as_map())
                    {
                        current.buffer.emplace_back(key);
                        current.buffer.emplace_back(value);
                    }
                }
            }
            if (current.buffer.size() >= 2)
            {
                const auto &erased = static_cast<const TSOutputView &>(out);
                Value key   = std::move(current.buffer.front());
                current.buffer.pop_front();
                Value value = std::move(current.buffer.front());
                current.buffer.pop_front();
                auto bundle = erased.as_bundle();
                {
                    auto field    = bundle.at(0);
                    auto mutation = field.data_view().begin_mutation(erased.evaluation_time());
                    static_cast<void>(mutation.copy_value_from(key.view()));
                }
                {
                    auto field    = bundle.at(1);
                    auto mutation = field.data_view().begin_mutation(erased.evaluation_time());
                    static_cast<void>(mutation.copy_value_from(value.view()));
                }
                if (!current.buffer.empty()) { scheduler.schedule(MIN_TD); }
            }
            state.set(std::move(current));
        }
    };

    struct str_impl
    {
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            // Fixed TSLs print tuple-style via str_tsl_impl.
            return operator_impl_detail::fixed_tsl_arg(context, 0) == nullptr;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TS<Str>> out)
        {
            out.set(ts.value().to_string());
        }
    };

    /** str_ over a fixed TSL prints TUPLE style - "(a, b)" not "[a, b]"
        (hgraph's python repr of the TSL value). */
    struct str_tsl_impl
    {
        static constexpr auto name = "str_tsl";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return operator_impl_detail::fixed_tsl_arg(context, 0) != nullptr;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TS<Str>> out)
        {
            const auto  value  = ts.value();
            auto        fields = value.as_indexed_view();
            std::string text   = "(";
            for (std::size_t index = 0; index < fields.size(); ++index)
            {
                if (index != 0) { text += ", "; }
                text += fields.at(index).to_string();
            }
            text += ")";
            out.set(Str{std::move(text)});
        }
    };

    /**
     * ``nothing`` implementation — a generic source of the requested output type that
     * **never ticks** (no inputs, not scheduled on start, ``eval`` never runs, so the
     * output stays perpetually invalid). Used as the rebindable placeholder for the
     * ``mesh_subscribe`` value input.
     */
    struct nothing_source
    {
        static constexpr auto name = "nothing";

        static void eval(Out<TsVar<"O">> out) { static_cast<void>(out); }  // never ticks
    };

    /** Register the conversion / utility operator overloads. */
    void register_conversion_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_CONVERSION_IMPL_H

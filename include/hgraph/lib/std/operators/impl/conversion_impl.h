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

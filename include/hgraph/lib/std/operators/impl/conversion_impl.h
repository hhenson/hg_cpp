#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_CONVERSION_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_CONVERSION_IMPL_H

#include <hgraph/lib/std/operators/conversion.h>   // const_ / zero_
#include <hgraph/runtime/node_scheduler.h>          // SingleShotScheduler
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/type_resolution.h>
#include <hgraph/util/date_time.h>

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
        if (output_schema->value_schema != value_schema)
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

        static void start(Scalar<"delay", engine_time_delta_t> delay, SingleShotScheduler sched)
        {
            sched.schedule(delay.value());   // now + delay (now == start_time during start)
        }

        static void eval(Scalar<"value", ScalarVar<"T">> value, Scalar<"delay", engine_time_delta_t> delay,
                         Out<TsVar<"S">> out)
        {
            static_cast<void>(delay);   // delay drives the start schedule; eval just applies the value
            out.apply(value.value());
        }
    };

    /** ``zero_`` — additive zero source for ``TS<Int>``. */
    struct zero_int
    {
        static constexpr bool schedule_on_start = true;
        static void           eval(Out<TS<Int>> out) { out.set(Int{0}); }
    };

    /** ``zero_`` — additive zero source for ``TS<Float>``. */
    struct zero_float
    {
        static constexpr bool schedule_on_start = true;
        static void           eval(Out<TS<Float>> out) { out.set(Float{0}); }
    };

    /** ``zero_`` — additive zero source for ``TS<Str>`` (the empty string). */
    struct zero_str
    {
        static constexpr bool schedule_on_start = true;
        static void           eval(Out<TS<Str>> out) { out.set(Str{}); }
    };

    /** Register the conversion / utility operator overloads. */
    inline void register_conversion_operators()
    {
        register_overload<const_, const_source>();    // const(value)         -> tick at start
        register_overload<const_, const_delayed>();   // const(value, delay)  -> tick at start + delay

        register_overload<zero_, zero_int>();
        register_overload<zero_, zero_float>();
        register_overload<zero_, zero_str>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_CONVERSION_IMPL_H

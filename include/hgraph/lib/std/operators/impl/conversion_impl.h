#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_CONVERSION_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_CONVERSION_IMPL_H

#include <hgraph/lib/std/operators/conversion.h>   // const_ / zero_
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/type_resolution.h>

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
     * ``const_`` implementation: a single generic source. The value type is a ``ScalarVar``
     * inferred from the configured value; without an explicit output schema it resolves to
     * ``TS<T>``, and with one the configured value's schema must match that output's value
     * schema. Ticks once at start (``PullSource``) and does not reschedule.
     */
    struct const_source
    {
        static constexpr auto name              = "const";
        static constexpr bool schedule_on_start = true;

        static void resolve_default_types(ResolutionMap &resolution)
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

        static void eval(Scalar<"value", ScalarVar<"T">> value, Out<TsVar<"S">> out)
        {
            out.apply(value.value());  // erased copy of the configured value, ticked at the start cycle
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
        register_overload<const_, const_source>();

        register_overload<zero_, zero_int>();
        register_overload<zero_, zero_float>();
        register_overload<zero_, zero_str>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_CONVERSION_IMPL_H

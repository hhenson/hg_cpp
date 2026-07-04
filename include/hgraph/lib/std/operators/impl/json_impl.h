#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_JSON_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_JSON_IMPL_H

#include <hgraph/lib/std/operators/json.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/json_codec.h>

#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    /**
     * ``to_json`` — erased implementations over any time-series
     * (schema-as-data: the interned ``JsonConverter`` for the value schema is
     * resolved on first use). The ``delta`` flag is a wiring-time constant,
     * so the value/delta split is resolved by OVERLOAD SELECTION
     * (``requires_`` on the flag) — each eval is branch-free on the hot path
     * (wiring-time resolution over run-time cost).
     */
    struct to_json_value_impl
    {
        static constexpr auto name = "to_json";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"delta", Value{Bool{false}}}};
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *delta = context.scalar_as<Bool>("delta");
            return delta != nullptr && !*delta;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"delta", Bool> delta, Out<TS<Str>> out)
        {
            static_cast<void>(delta);   // resolved at wiring; always false here
            out.set(to_json_string(ts.value()));
        }
    };

    /** ``to_json(ts, delta=true)`` — serialises the canonical per-tick delta. */
    struct to_json_delta_impl
    {
        static constexpr auto name = "to_json";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *delta = context.scalar_as<Bool>("delta");
            return delta != nullptr && *delta;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"delta", Bool> delta, Out<TS<Str>> out)
        {
            static_cast<void>(delta);   // resolved at wiring; always true here
            const Value tick_delta = capture_delta(ts.base());
            out.set(to_json_string(tick_delta.view()));
        }
    };

    /**
     * ``from_json`` — parses the incoming string into the resolved output's
     * VALUE schema and applies it as the tick's delta. The output type is
     * supplied at the wiring site: ``wire<from_json, TS<MySchema>>(w, ts)``.
     * (Whole-value application: suits ``TS`` over scalars/compounds — the
     * shapes whose delta and value schemas coincide, matching the Python
     * ``from_json`` support surface.)
     */
    struct from_json_impl
    {
        static constexpr auto name = "from_json";

        static void eval(In<"ts", TS<Str>> ts, Out<TsVar<"O">> out)
        {
            // ``Out``'s ``schema`` type alias hides the inherited accessor;
            // reach the runtime schema through the erased view.
            const auto &erased     = static_cast<const TSOutputView &>(out);
            const auto *value_meta = erased.schema() != nullptr ? erased.schema()->value_schema : nullptr;
            Value       parsed     = from_json_string(value_meta, ts.value());
            apply_delta(out, parsed.view());
        }
    };

    /** Register the JSON operator overloads. */
    inline void register_json_operators()
    {
        register_overload<to_json, to_json_value_impl>();
        register_overload<to_json, to_json_delta_impl>();
        register_overload<from_json, from_json_impl>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_JSON_IMPL_H

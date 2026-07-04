#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_JSON_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_JSON_IMPL_H

#include <hgraph/lib/std/operators/json.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/json_codec.h>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<JsonCodecState>
    {
        static constexpr std::string_view value{"JsonCodecState"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{
    /**
     * ``to_json`` — erased implementations over any time-series. The composed
     * ``JsonConverter`` is resolved ONCE in ``start`` and carried in node
     * State (the lifecycle form of the builder pattern); ``eval`` is a plain
     * converter invocation. The ``delta`` flag is a wiring-time constant, so
     * value vs delta is resolved by OVERLOAD SELECTION (``requires_`` on the
     * flag) — no hot-path branches.
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

        static void start(In<"ts", TsVar<"S">> ts, State<JsonCodecState> codec)
        {
            codec.set(JsonCodecState{&json_converter(ts.base().schema()->value_schema)});
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"delta", Bool> delta, State<JsonCodecState> codec,
                         Out<TS<Str>> out)
        {
            static_cast<void>(delta);   // resolved at wiring; always false here
            std::string text;
            codec.get().converter->write(ts.value(), text);
            out.set(std::move(text));
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

        static void start(In<"ts", TsVar<"S">> ts, State<JsonCodecState> codec)
        {
            codec.set(JsonCodecState{&json_converter(ts.base().schema()->delta_value_schema)});
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"delta", Bool> delta, State<JsonCodecState> codec,
                         Out<TS<Str>> out)
        {
            static_cast<void>(delta);   // resolved at wiring; always true here
            const Value tick_delta = capture_delta(ts.base());
            std::string text;
            codec.get().converter->write(tick_delta.view(), text);
            out.set(std::move(text));
        }
    };

    /**
     * ``from_json`` — parses the incoming string into the resolved output's
     * VALUE schema (converter resolved once in ``start``) and applies the
     * parsed value as the tick's delta. The output type is supplied at the
     * wiring site: ``wire<from_json, TS<MySchema>>(w, ts)``.
     */
    struct from_json_impl
    {
        static constexpr auto name = "from_json";

        static void start(Out<TsVar<"O">> out, State<JsonCodecState> codec)
        {
            // ``Out``'s ``schema`` type alias hides the inherited accessor;
            // reach the runtime schema through the erased view.
            const auto &erased = static_cast<const TSOutputView &>(out);
            codec.set(JsonCodecState{&json_converter(erased.schema()->value_schema)});
        }

        static void eval(In<"ts", TS<Str>> ts, State<JsonCodecState> codec, Out<TsVar<"O">> out)
        {
            Value parsed = from_json_string(*codec.get().converter, ts.value());
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

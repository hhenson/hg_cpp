#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_TABLE_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_TABLE_IMPL_H

#include <hgraph/lib/std/operators/table.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/table_codec.h>

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<TableCodecState>
    {
        static constexpr std::string_view value{"TableCodecState"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{
    /**
     * ``to_table`` — one bitemporal row per tick, straight into Arrow
     * builders. The composed ``TableConverter`` is resolved ONCE in ``start``
     * and carried in node State (the lifecycle form of the builder pattern).
     */
    struct to_table_impl
    {
        static constexpr auto name = "to_table";

        static void start(In<"ts", TsVar<"S">> ts, GlobalStateView gs, State<TableCodecState> codec)
        {
            const auto config = record_replay::config(gs);
            codec.set(TableCodecState{
                &table_converter(ts.base().schema()->value_schema, config.date_key, config.as_of_key)});
        }

        static void eval(In<"ts", TsVar<"S">> ts, GlobalStateView gs, State<TableCodecState> codec, DateTime now,
                         Out<TS<Frame>> out)
        {
            const TableConverter &converter = *codec.get().converter;
            const auto            as_of     = record_replay::config(gs).as_of.value_or(now);
            out.set(single_row_frame(converter, now, as_of, ts.value()));
        }
    };

    /**
     * ``from_table`` — applies each row of the incoming frame as this tick's
     * value (rows apply in order; for a whole-value ``TS`` the last row
     * wins). Column resolution is by name — the input-minimum rule. The
     * converter is resolved once in ``start`` from the resolved output.
     */
    struct from_table_impl
    {
        static constexpr auto name = "from_table";

        static void start(Out<TsVar<"O">> out, GlobalStateView gs, State<TableCodecState> codec)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto  config = record_replay::config(gs);
            codec.set(TableCodecState{
                &table_converter(erased.schema()->value_schema, config.date_key, config.as_of_key)});
        }

        static void eval(In<"ts", TS<Frame>> ts, State<TableCodecState> codec, Out<TsVar<"O">> out)
        {
            const TableConverter &converter = *codec.get().converter;
            const Frame          &frame     = ts.value();
            for (std::int64_t row = 0; row < frame_rows(frame); ++row)
            {
                Value value = read_row(converter, frame, row);
                apply_delta(out, value.view());
            }
        }
    };

    /**
     * ``from_table_const`` — const-evaluable (the const_fn ruling, P1): the
     * eager kernel extracts the frame's last row at the resolved output
     * schema; the wired form emits the same value once at start.
     */
    struct from_table_const_impl
    {
        static constexpr auto name              = "from_table_const";
        static constexpr bool schedule_on_start = true;

        static Value const_eval(const TSValueTypeMetaData *resolved_output, OperatorCallContext context)
        {
            const auto *frame = context.scalar_as<Frame>("value");
            if (frame == nullptr || !frame->has_value() || frame_rows(*frame) == 0) { return Value{}; }
            const auto  config = record_replay::config(context.global_state);
            const auto &converter =
                table_converter(resolved_output->value_schema, config.date_key, config.as_of_key);
            return read_row(converter, *frame, frame_rows(*frame) - 1);
        }

        static void eval(Scalar<"value", Frame> value, GlobalStateView gs, Out<TsVar<"O">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto &frame  = value.value();
            if (!frame.has_value() || frame_rows(frame) == 0) { return; }
            const auto  config = record_replay::config(gs);
            const auto &converter =
                table_converter(erased.schema()->value_schema, config.date_key, config.as_of_key);
            Value       row       = read_row(converter, frame, frame_rows(frame) - 1);
            out.apply(row.view());
        }
    };

    /** Register the table operator overloads. */
    void register_table_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_TABLE_IMPL_H

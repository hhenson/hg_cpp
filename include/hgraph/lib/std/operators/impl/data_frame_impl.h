#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_DATA_FRAME_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_DATA_FRAME_IMPL_H

#include <hgraph/lib/std/operators/data_frame.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/table_codec.h>

#include <string>
#include <string_view>
#include <vector>

namespace hgraph::stdlib
{
    using namespace hgraph::operator_type_resolution;

    /**
     * The data-frame convenience operators (design record: *Record/replay,
     * tables and const_fn*, step 6): ``from_data_frame`` replays a frame
     * VALUE by its date column, ``to_data_frame`` snapshots a time-series
     * into one-tick frames (a plain ``date`` column, no as-of), ``group_by``
     * partitions a Frame-valued TS into a TSD by key column(s). All ride
     * the tuple-row/frame codecs — no third serialisation path.
     */
    namespace data_frame_detail
    {
        struct FieldRead
        {
            std::string              column{};
            const ValueTypeMetaData *leaf{nullptr};
            std::size_t              field_index{0};
        };

        /** from_data_frame reading plan (heap handle via node State). */
        struct FromFramePlan
        {
            Frame                     frame{};
            std::string               dt_col{};
            TimeDelta                 offset{};
            std::int64_t              row{0};
            TSTypeKind                out_kind{};
            const ValueTypeMetaData  *key_meta{nullptr};      // TSD key
            std::string               key_col{};
            const ValueTypeMetaData  *bundle_meta{nullptr};   // TSB(-child) value bundle
            std::vector<FieldRead>    fields{};               // value cell reads
        };

        /** to_data_frame writing plan. */
        struct ToFramePlan
        {
            enum class Source : unsigned char { Date, Key, Field, ValueField, Whole };

            struct Column
            {
                Source      source{Source::Whole};
                std::size_t ts_field{0};   // TSB child index for Source::Field
            };

            const TableConverter     *converter{nullptr};   // over the OUT frame column schema
            const ValueTypeMetaData  *row_meta{nullptr};    // that column bundle
            std::vector<Column>       columns{};
            bool                      dict{false};          // input is a TSD
        };

        /** group_by partitioning plan. */
        struct GroupByPlan
        {
            const TableConverter                *converter{nullptr};   // over the frame's column schema
            std::vector<FieldRead>               key_cols{};
            const ValueTypeMetaData             *key_meta{nullptr};
            bool                                 tuple_key{false};
        };

        [[nodiscard]] const TSValueTypeMetaData *resolve_group_by_output(const TSValueTypeMetaData *ts,
                                                                         const ValueView          &by);

        /** The default snapshot-frame schema for ``ts``: an UN-NAMED column
            bundle [dt_col, (key_col,) *value columns] (upstream's
            compound_scalar resolvers). */
        [[nodiscard]] const TSValueTypeMetaData *resolve_to_frame_output(const TSValueTypeMetaData *ts,
                                                                         std::string_view dt_col,
                                                                         std::string_view key_col,
                                                                         std::string_view value_col);

        void start_from_frame(const Frame &frame, std::string_view dt_col, std::string_view key_col,
                              std::string_view value_col, TimeDelta offset, const TSOutputView &out,
                              SingleShotScheduler &sched, FromFramePlan *&plan_out);
        void eval_from_frame(FromFramePlan &plan, DateTime now, NodeScheduler &sched,
                             const TSOutputView &out);

        void start_to_frame(const TSInputView &ts, std::string_view dt_col, std::string_view key_col,
                            std::string_view value_col, const TSOutputView &out, ToFramePlan *&plan_out);
        void eval_to_frame(const ToFramePlan &plan, const TSInputView &ts, DateTime now,
                           const TSOutputView &out);

        void start_group_by(const TSInputView &ts, const ValueView &by, const TSOutputView &out,
                            GroupByPlan *&plan_out);
        void eval_group_by(const GroupByPlan &plan, const TSInputView &ts, const TSOutputView &out);

        // Frame targets for convert/combine (design record step 6).
        [[nodiscard]] bool value_is_frame(const ValueTypeMetaData *meta);
        [[nodiscard]] bool ts_value_is_frame(const TSValueTypeMetaData *ts);
        [[nodiscard]] std::string mapping_entry(const ValueView &mapping, std::string_view key);

        void start_convert_tsd_frame(const TSInputView &ts, const ValueView &mapping,
                                     const TSOutputView &out, ToFramePlan *&plan_out);
        void start_convert_value_frame(const TSInputView &ts, const TSOutputView &out,
                                       ToFramePlan *&plan_out);
        void eval_convert_value_frame(const ToFramePlan &plan, const TSInputView &ts,
                                      const TSOutputView &out);
        void eval_convert_frame_frame(const ValueView &mapping, const TSInputView &ts,
                                      const TSOutputView &out);
        void start_combine_frame(const TSInputView &ts, const TSOutputView &out, ToFramePlan *&plan_out);
        void eval_combine_frame(const ToFramePlan &plan, const TSInputView &ts, const TSOutputView &out);
    }  // namespace data_frame_detail

    struct FromDataFrameState
    {
        data_frame_detail::FromFramePlan *handle{nullptr};
    };

    struct ToDataFrameState
    {
        data_frame_detail::ToFramePlan *handle{nullptr};
    };

    struct GroupByState
    {
        data_frame_detail::GroupByPlan *handle{nullptr};
    };
}  // namespace hgraph::stdlib

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<stdlib::FromDataFrameState>
    {
        static constexpr std::string_view value{"FromDataFrameState"};
    };

    template <>
    struct scalar_name<stdlib::ToDataFrameState>
    {
        static constexpr std::string_view value{"ToDataFrameState"};
    };

    template <>
    struct scalar_name<stdlib::GroupByState>
    {
        static constexpr std::string_view value{"GroupByState"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{
    /** ``from_data_frame[OUT](df, ...)`` — a pull source replaying the frame
        by its date column (absolute scheduling, the replay precedent). */
    struct from_data_frame_impl
    {
        static constexpr auto name = "from_data_frame";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"dt_col", Value{Str{"date"}}},
                    {"key_col", Value{Str{"key"}}},
                    {"value_col", Value{Str{"value"}}},
                    {"offset", Value{TimeDelta{0}}}};
        }

        static void start(Scalar<"df", Frame> df, Scalar<"dt_col", Str> dt_col,
                          Scalar<"key_col", Str> key_col, Scalar<"value_col", Str> value_col,
                          Scalar<"offset", TimeDelta> offset, SingleShotScheduler sched,
                          State<FromDataFrameState> state, Out<TsVar<"O">> out)
        {
            data_frame_detail::FromFramePlan *plan = nullptr;
            data_frame_detail::start_from_frame(df.value(), dt_col.value(), key_col.value(),
                                                value_col.value(), offset.value(),
                                                static_cast<const TSOutputView &>(out), sched, plan);
            state.set(FromDataFrameState{plan});   // owned by node State until stop
        }

        static void eval(Scalar<"df", Frame> df, Scalar<"dt_col", Str> dt_col,
                         Scalar<"key_col", Str> key_col, Scalar<"value_col", Str> value_col,
                         Scalar<"offset", TimeDelta> offset, State<FromDataFrameState> state,
                         NodeScheduler sched, DateTime now, Out<TsVar<"O">> out)
        {
            static_cast<void>(df);
            static_cast<void>(dt_col);
            static_cast<void>(key_col);
            static_cast<void>(value_col);
            static_cast<void>(offset);
            data_frame_detail::eval_from_frame(*state.get().handle, now, sched,
                                               static_cast<const TSOutputView &>(out));
        }

        static void stop(State<FromDataFrameState> state)
        {
            std::unique_ptr<data_frame_detail::FromFramePlan> handle{state.get().handle};
            state.set(FromDataFrameState{});
        }
    };

    /** ``to_data_frame(ts, ...)`` — a per-tick snapshot frame; the output
        ``Frame[Schema]`` names the columns (dt_col first, TSD adds key_col). */
    struct to_data_frame_impl
    {
        static constexpr auto name = "to_data_frame";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"dt_col", Value{Str{"date"}}},
                    {"key_col", Value{Str{"key"}}},
                    {"value_col", Value{Str{"value"}}}};
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            const auto *schema  = time_series_schema_at(context, 0);
            const auto *dt_col  = context.scalar_as<Str>("dt_col");
            const auto *key_col = context.scalar_as<Str>("key_col");
            const auto *value_col = context.scalar_as<Str>("value_col");
            if (schema == nullptr || dt_col == nullptr || key_col == nullptr || value_col == nullptr)
            {
                return;
            }
            const auto *out = data_frame_detail::resolve_to_frame_output(
                schema, std::string_view{*dt_col}, std::string_view{*key_col},
                std::string_view{*value_col});
            if (out != nullptr) { bind_output(resolution, out); }
        }

        static void start(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts, Scalar<"dt_col", Str> dt_col,
                          Scalar<"key_col", Str> key_col, Scalar<"value_col", Str> value_col,
                          State<ToDataFrameState> state, Out<TsVar<"__out__">> out)
        {
            data_frame_detail::ToFramePlan *plan = nullptr;
            data_frame_detail::start_to_frame(ts.base(), dt_col.value(), key_col.value(),
                                              value_col.value(),
                                              static_cast<const TSOutputView &>(out), plan);
            state.set(ToDataFrameState{plan});
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"dt_col", Str> dt_col,
                         Scalar<"key_col", Str> key_col, Scalar<"value_col", Str> value_col,
                         State<ToDataFrameState> state, DateTime now, Out<TsVar<"__out__">> out)
        {
            static_cast<void>(dt_col);
            static_cast<void>(key_col);
            static_cast<void>(value_col);
            data_frame_detail::eval_to_frame(*state.get().handle, ts.base(), now,
                                             static_cast<const TSOutputView &>(out));
        }

        static void stop(State<ToDataFrameState> state)
        {
            std::unique_ptr<data_frame_detail::ToFramePlan> handle{state.get().handle};
            state.set(ToDataFrameState{});
        }
    };

    /** ``group_by(ts, by)`` — partition a Frame TS into TSD[key, TS[Frame]]
        (removed keys emit REMOVE; ``by`` is a column name or tuple of them). */
    struct group_by_impl
    {
        static constexpr auto name = "group_by";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            const auto *schema = time_series_schema_at(context, 0);
            const auto *by     = context.scalar("by");
            if (schema == nullptr || by == nullptr) { return; }
            const auto *out = data_frame_detail::resolve_group_by_output(schema, by->scalar_value.view());
            if (out != nullptr) { bind_output(resolution, out); }
        }

        static void start(In<"ts", TS<ScalarVar<"F">>, InputValidity::Unchecked> ts,
                          Scalar<"by", ScalarVar<"B">> by, State<GroupByState> state,
                          Out<TsVar<"__out__">> out)
        {
            data_frame_detail::GroupByPlan *plan = nullptr;
            data_frame_detail::start_group_by(ts.base(), by.value(),
                                              static_cast<const TSOutputView &>(out), plan);
            state.set(GroupByState{plan});
        }

        static void eval(In<"ts", TS<ScalarVar<"F">>> ts, Scalar<"by", ScalarVar<"B">> by,
                         State<GroupByState> state, Out<TsVar<"__out__">> out)
        {
            static_cast<void>(by);
            data_frame_detail::eval_group_by(*state.get().handle, ts.base(),
                                             static_cast<const TSOutputView &>(out));
        }

        static void stop(State<GroupByState> state)
        {
            std::unique_ptr<data_frame_detail::GroupByPlan> handle{state.get().handle};
            state.set(GroupByState{});
        }
    };

    namespace data_frame_impl_detail
    {
        [[nodiscard]] inline bool output_is_frame(const ResolutionMap &resolution)
        {
            return data_frame_detail::value_is_frame(output_ts_value_schema(resolution));
        }

        /** The no-mapping default: an empty STRING sentinel (mapping_entry
            treats any non-map as empty; a default-constructed Map value has
            no bindings and cannot hash for call-identity interning). */
        [[nodiscard]] inline Value empty_str_map() { return Value{Str{}}; }
    }  // namespace data_frame_impl_detail

    /** ``convert[TS[Frame*]](tsd)`` — snapshot a compound-valued TSD into a
        frame (mapping["key_col"] adds the key column). */
    struct convert_tsd_to_frame_impl
    {
        static constexpr auto name = "convert_tsd_to_frame";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"mapping", data_frame_impl_detail::empty_str_map()}};
        }

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *in = time_series_schema_at(context, 0);
            return data_frame_impl_detail::output_is_frame(resolution) && in != nullptr &&
                   in->kind == TSTypeKind::TSD && in->element_ts()->kind == TSTypeKind::TS &&
                   in->element_ts()->value_schema->kind == ValueTypeKind::Bundle;
        }

        static void start(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                          Scalar<"mapping", ScalarVar<"M">> mapping, State<ToDataFrameState> state,
                          Out<TsVar<"__out__">> out)
        {
            data_frame_detail::ToFramePlan *plan = nullptr;
            data_frame_detail::start_convert_tsd_frame(ts.base(), mapping.value(),
                                                       static_cast<const TSOutputView &>(out), plan);
            state.set(ToDataFrameState{plan});
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"mapping", ScalarVar<"M">> mapping,
                         State<ToDataFrameState> state, DateTime now, Out<TsVar<"__out__">> out)
        {
            static_cast<void>(mapping);
            data_frame_detail::eval_to_frame(*state.get().handle, ts.base(), now,
                                             static_cast<const TSOutputView &>(out));
        }

        static void stop(State<ToDataFrameState> state)
        {
            std::unique_ptr<data_frame_detail::ToFramePlan> handle{state.get().handle};
            state.set(ToDataFrameState{});
        }
    };

    /** ``convert[TS[Frame[X]]](frame_ts[, mapping])`` — pass-through /
        column-rename (input schemas are a minimum; output is exact). */
    struct convert_frame_to_frame_impl
    {
        static constexpr auto name = "convert_frame_to_frame";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"mapping", data_frame_impl_detail::empty_str_map()}};
        }

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            return data_frame_impl_detail::output_is_frame(resolution) &&
                   data_frame_detail::ts_value_is_frame(time_series_schema_at(context, 0));
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"mapping", ScalarVar<"M">> mapping,
                         Out<TsVar<"__out__">> out)
        {
            data_frame_detail::eval_convert_frame_frame(mapping.value(), ts.base(),
                                                        static_cast<const TSOutputView &>(out));
        }
    };

    /** ``convert[TS[Frame[X]]](ts)`` over a compound (one row per tick) or a
        tuple of compounds (one row per element). */
    struct convert_value_to_frame_impl
    {
        static constexpr auto name = "convert_value_to_frame";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *in = time_series_schema_at(context, 0);
            if (!data_frame_impl_detail::output_is_frame(resolution) || in == nullptr ||
                in->kind != TSTypeKind::TS)
            {
                return false;
            }
            const auto *value = in->value_schema;
            if (value->kind == ValueTypeKind::Bundle) { return true; }
            return value->kind == ValueTypeKind::List && value->element_type != nullptr &&
                   value->element_type->kind == ValueTypeKind::Bundle;
        }

        static void start(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                          State<ToDataFrameState> state, Out<TsVar<"__out__">> out)
        {
            data_frame_detail::ToFramePlan *plan = nullptr;
            data_frame_detail::start_convert_value_frame(ts.base(),
                                                         static_cast<const TSOutputView &>(out), plan);
            state.set(ToDataFrameState{plan});
        }

        static void eval(In<"ts", TsVar<"S">> ts, State<ToDataFrameState> state,
                         Out<TsVar<"__out__">> out)
        {
            data_frame_detail::eval_convert_value_frame(*state.get().handle, ts.base(),
                                                        static_cast<const TSOutputView &>(out));
        }

        static void stop(State<ToDataFrameState> state)
        {
            std::unique_ptr<data_frame_detail::ToFramePlan> handle{state.get().handle};
            state.set(ToDataFrameState{});
        }
    };

    /** ``combine[TS[Frame[X]]](a=col_ts, b=col_ts)`` — zip tuple-valued
        column time-series into a frame (fields matched by name). */
    struct combine_frame_impl
    {
        static constexpr auto name = "combine_frame";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *in = time_series_schema_at(context, 0);
            return data_frame_impl_detail::output_is_frame(resolution) && in != nullptr &&
                   in->kind == TSTypeKind::TSB;
        }

        static void start(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                          State<ToDataFrameState> state, Out<TsVar<"__out__">> out)
        {
            data_frame_detail::ToFramePlan *plan = nullptr;
            data_frame_detail::start_combine_frame(ts.base(),
                                                   static_cast<const TSOutputView &>(out), plan);
            state.set(ToDataFrameState{plan});
        }

        static void eval(In<"ts", TsVar<"S">> ts, State<ToDataFrameState> state,
                         Out<TsVar<"__out__">> out)
        {
            data_frame_detail::eval_combine_frame(*state.get().handle, ts.base(),
                                                  static_cast<const TSOutputView &>(out));
        }

        static void stop(State<ToDataFrameState> state)
        {
            std::unique_ptr<data_frame_detail::ToFramePlan> handle{state.get().handle};
            state.set(ToDataFrameState{});
        }
    };

    /** Register the data-frame operator overloads. */
    void register_data_frame_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_DATA_FRAME_IMPL_H

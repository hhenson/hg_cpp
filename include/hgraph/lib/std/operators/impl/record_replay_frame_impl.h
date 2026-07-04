#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_RECORD_REPLAY_FRAME_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_RECORD_REPLAY_FRAME_IMPL_H

#include <hgraph/lib/std/operators/io.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/table_codec.h>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    namespace record_replay_frame_detail
    {
        [[nodiscard]] inline std::string frame_key(const TraitsView &traits, std::string_view recordable_id,
                                                   std::string_view key)
        {
            return record_replay::fq_recordable_id(traits, recordable_id) + "." + std::string{key};
        }

        /** Heap recorder handle owned across start/eval/stop via node State. */
        struct RecorderHandle
        {
            FrameRecorder recorder;
            std::string   fq_key;
        };

        /** Heap replay cursor owned across start/eval/stop via node State. */
        struct ReplayHandle
        {
            const TableConverter *converter{nullptr};
            Frame                 frame{};
            std::int64_t          row{0};
        };
    }  // namespace record_replay_frame_detail

    /** Node-State payloads carrying the heap handles (start-lifecycle pattern). */
    struct FrameRecorderState
    {
        record_replay_frame_detail::RecorderHandle *handle{nullptr};
    };

    struct FrameReplayState
    {
        record_replay_frame_detail::ReplayHandle *handle{nullptr};
    };
}  // namespace hgraph::stdlib

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<stdlib::FrameRecorderState>
    {
        static constexpr std::string_view value{"FrameRecorderState"};
    };

    template <>
    struct scalar_name<stdlib::FrameReplayState>
    {
        static constexpr std::string_view value{"FrameReplayState"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{
    /**
     * The Arrow data-frame record/replay backend (design record, step 4;
     * model ``record_replay::DATA_FRAME``). ``record`` appends one bitemporal
     * row per tick straight into Arrow builders (the fused P4 path) and
     * writes the finished frame to the registered frame store (P6) at
     * ``stop`` under ``fq_recordable_id.key``; ``replay`` reads the frame and
     * re-emits each row at its recorded value time.
     */
    struct record_frame_impl
    {
        static constexpr auto name = "record";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"recordable_id", Value{Str{}}}};
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext)
        {
            return record_replay::model_is(record_replay::DATA_FRAME);
        }

        static void start(In<"ts", TsVar<"S">> ts, Scalar<"key", Str> key,
                          Scalar<"recordable_id", Str> recordable_id, TraitsView traits,
                          State<FrameRecorderState> state)
        {
            using record_replay_frame_detail::RecorderHandle;
            const auto &converter = table_converter(ts.base().schema()->value_schema);
            auto *handle          = new RecorderHandle{
                FrameRecorder{converter},
                record_replay_frame_detail::frame_key(traits, recordable_id.value(), key.value())};
            state.set(FrameRecorderState{handle});
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"key", Str> key,
                         Scalar<"recordable_id", Str> recordable_id, State<FrameRecorderState> state,
                         DateTime now)
        {
            static_cast<void>(key);
            static_cast<void>(recordable_id);
            const auto as_of = record_replay::config().as_of.value_or(now);
            state.get().handle->recorder.append(now, as_of, ts.value());
        }

        static void stop(State<FrameRecorderState> state)
        {
            auto *handle = state.get().handle;
            if (handle == nullptr) { return; }
            record_replay::store_write(handle->fq_key, handle->recorder.finish());
            delete handle;
            state.set(FrameRecorderState{});
        }
    };

    struct replay_frame_impl
    {
        static constexpr auto name = "replay";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"recordable_id", Value{Str{}}}};
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext)
        {
            return record_replay::model_is(record_replay::DATA_FRAME);
        }

        static void start(Scalar<"key", Str> key, Scalar<"recordable_id", Str> recordable_id, TraitsView traits,
                          State<FrameReplayState> state, SingleShotScheduler sched, Out<TsVar<"O">> out)
        {
            using record_replay_frame_detail::ReplayHandle;
            const auto  fq_key = record_replay_frame_detail::frame_key(traits, recordable_id.value(), key.value());
            Frame       frame  = record_replay::store_read(fq_key);
            if (!frame.has_value())
            {
                throw std::runtime_error("replay: no recorded frame under '" + fq_key + "'");
            }
            const auto &erased    = static_cast<const TSOutputView &>(out);
            const auto &converter = table_converter(erased.schema()->value_schema);
            auto       *handle    = new ReplayHandle{&converter, std::move(frame), 0};
            state.set(FrameReplayState{handle});
            if (frame_rows(handle->frame) > 0)
            {
                sched.schedule(frame_value_time(converter, handle->frame, 0));
            }
        }

        static void eval(Scalar<"key", Str> key, Scalar<"recordable_id", Str> recordable_id,
                         State<FrameReplayState> state, NodeScheduler sched, DateTime now, Out<TsVar<"O">> out)
        {
            static_cast<void>(key);
            static_cast<void>(recordable_id);
            auto      *handle = state.get().handle;
            const auto rows   = frame_rows(handle->frame);
            while (handle->row < rows && frame_value_time(*handle->converter, handle->frame, handle->row) == now)
            {
                Value value = read_row(*handle->converter, handle->frame, handle->row);
                apply_delta(out, value.view());
                ++handle->row;
            }
            if (handle->row < rows)
            {
                sched.schedule(frame_value_time(*handle->converter, handle->frame, handle->row));
            }
        }

        static void stop(State<FrameReplayState> state)
        {
            delete state.get().handle;
            state.set(FrameReplayState{});
        }
    };

    /** Register the data-frame record/replay backend overloads. */
    inline void register_record_replay_frame_operators()
    {
        register_overload<record, record_frame_impl>();
        register_overload<replay, replay_frame_impl>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_RECORD_REPLAY_FRAME_IMPL_H

#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_STREAM_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_STREAM_IMPL_H

#include <hgraph/lib/std/operators/stream.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/value.h>

#include <algorithm>
#include <cmath>
#include <deque>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace hgraph::stdlib
{
    namespace stream_impl_detail
    {
        struct DeltaQueueState
        {
            std::deque<Value> buffer{};
        };

        struct ThrottleState
        {
            DateTime  next_allowed{MIN_DT};
            TimeDelta period{MIN_TD};
            bool      has_period{false};
            bool      has_pending{false};
            Value     pending{};
        };

        inline void require_positive(Int value, const char *name)
        {
            if (value <= 0) { throw std::invalid_argument(std::string{name} + " must be positive"); }
        }

        inline void require_positive(TimeDelta value, const char *name)
        {
            if (value <= TimeDelta{}) { throw std::invalid_argument(std::string{name} + " must be positive"); }
        }

        inline void append_delta(DeltaQueueState &state, Value delta, std::size_t limit)
        {
            if (state.buffer.size() >= limit) { throw std::overflow_error("gate buffer length exceeded"); }
            state.buffer.push_back(std::move(delta));
        }
    }  // namespace stream_impl_detail
}  // namespace hgraph::stdlib

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<stdlib::stream_impl_detail::DeltaQueueState>
    {
        static constexpr std::string_view value{"stdlib.delta_queue_state"};
    };

    template <>
    struct scalar_name<stdlib::stream_impl_detail::ThrottleState>
    {
        static constexpr std::string_view value{"stdlib.throttle_state"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{

    struct sample_impl
    {
        static void eval(In<"signal", SIGNAL> signal,
                         In<"ts", TsVar<"S">, InputActivity::Passive, InputValidity::Unchecked> ts,
                         Out<TsVar<"S">> out)
        {
            static_cast<void>(signal);
            if (ts.valid()) { out.apply(ts.value()); }
        }
    };

    struct filter_impl
    {
        static void eval(In<"condition", TS<Bool>, InputValidity::Unchecked> condition,
                         In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         RecordableState<TS<Bool>> last_condition,
                         Out<TsVar<"S">> out)
        {
            const bool condition_true = condition.valid() && condition.value();
            const bool was_true = last_condition.valid() && last_condition.value().checked_as<Bool>();
            if (condition_true && ts.valid() && (ts.modified() || !was_true)) { out.apply(ts.value()); }
            last_condition.set(condition_true);
        }
    };

    struct lag_tick_impl
    {
        static void start(Scalar<"period", Int> period)
        {
            stream_impl_detail::require_positive(period.value(), "period");
        }

        static void eval(In<"ts", TsVar<"S">> ts,
                         Scalar<"period", Int> period,
                         State<stream_impl_detail::DeltaQueueState> state,
                         Out<TsVar<"S">> out)
        {
            auto current = state.get();
            current.buffer.push_back(capture_delta(ts.base()));

            const auto delay = static_cast<std::size_t>(period.value());
            if (current.buffer.size() > delay)
            {
                Value delta = std::move(current.buffer.front());
                current.buffer.pop_front();
                apply_delta(out, delta.view());
            }

            state.set(std::move(current));
        }
    };

    struct until_true_bool_impl
    {
        static void eval(In<"ts", TS<Bool>> ts, Out<TS<Bool>> out)
        {
            out.set(ts.value());
            if (ts.value()) { ts.make_passive(); }
        }
    };

    struct freeze_impl
    {
        static void eval(In<"predicate", TS<Bool>> predicate,
                         In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         Out<TsVar<"S">> out)
        {
            if (predicate.value())
            {
                if (ts.valid()) { out.apply(ts.value()); }
                predicate.make_passive();
                ts.make_passive();
                return;
            }

            if (ts.modified() && ts.valid()) { apply_delta(out, capture_delta(ts.base()).view()); }
        }
    };

    struct schedule_impl
    {
        static void start(Scalar<"delay", TimeDelta> delay, NodeScheduler scheduler)
        {
            stream_impl_detail::require_positive(delay.value(), "delay");
            scheduler.schedule(delay.value());
        }

        static void eval(Scalar<"delay", TimeDelta> delay, NodeScheduler scheduler, Out<TS<Bool>> out)
        {
            out.set(true);
            scheduler.schedule(delay.value());
        }
    };

    struct resample_impl
    {
        static void start(Scalar<"period", TimeDelta> period, NodeScheduler scheduler)
        {
            stream_impl_detail::require_positive(period.value(), "period");
            scheduler.schedule(period.value());
        }

        static void eval(In<"ts", TsVar<"S">, InputActivity::Passive, InputValidity::Unchecked> ts,
                         Scalar<"period", TimeDelta> period,
                         NodeScheduler scheduler,
                         Out<TsVar<"S">> out)
        {
            if (ts.valid()) { out.apply(ts.value()); }
            scheduler.schedule(period.value());
        }
    };

    struct gate_impl
    {
        static void start(Scalar<"buffer_length", Int> buffer_length)
        {
            stream_impl_detail::require_positive(buffer_length.value(), "buffer_length");
        }

        static void eval(In<"condition", TS<Bool>, InputValidity::Unchecked> condition,
                         In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         Scalar<"buffer_length", Int> buffer_length,
                         NodeScheduler scheduler,
                         State<stream_impl_detail::DeltaQueueState> state,
                         Out<TsVar<"S">> out)
        {
            auto       current        = state.get();
            const auto limit          = static_cast<std::size_t>(buffer_length.value());
            const bool condition_true = condition.valid() && condition.value();
            bool       emitted        = false;

            if (ts.modified() && ts.valid())
            {
                Value delta = capture_delta(ts.base());
                if (condition_true && current.buffer.empty())
                {
                    apply_delta(out, delta.view());
                    emitted = true;
                }
                else { stream_impl_detail::append_delta(current, std::move(delta), limit); }
            }

            if (!emitted && condition_true && !current.buffer.empty())
            {
                Value delta = std::move(current.buffer.front());
                current.buffer.pop_front();
                apply_delta(out, delta.view());
                emitted = true;
            }

            if (condition_true && !current.buffer.empty()) { scheduler.schedule(MIN_TD); }
            state.set(std::move(current));
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"buffer_length", Value{std::numeric_limits<Int>::max()}}};
        }
    };

    struct throttle_impl
    {
        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"period", TS<TimeDelta>, InputValidity::Unchecked> period,
                         NodeScheduler scheduler,
                         State<stream_impl_detail::ThrottleState> state,
                         DateTime now,
                         Out<TsVar<"S">> out)
        {
            auto current = state.get();
            if (period.modified() && period.valid())
            {
                stream_impl_detail::require_positive(period.value(), "period");
                current.period     = period.value();
                current.has_period = true;
            }

            bool emitted = false;
            if (scheduler.is_scheduled_now() && current.has_pending)
            {
                Value delta = std::move(current.pending);
                current.pending     = Value{};
                current.has_pending = false;
                apply_delta(out, delta.view());
                current.next_allowed = now + current.period;
                emitted              = true;
            }

            if (ts.modified() && ts.valid())
            {
                if (!current.has_period) { throw std::invalid_argument("throttle requires a valid period"); }

                Value delta = capture_delta(ts.base());
                if (!emitted && now >= current.next_allowed)
                {
                    apply_delta(out, delta.view());
                    current.next_allowed = now + current.period;
                    scheduler.reset();
                }
                else
                {
                    current.pending     = std::move(delta);
                    current.has_pending = true;
                    scheduler.schedule(current.next_allowed);
                }
            }

            state.set(std::move(current));
        }
    };

    struct take_impl
    {
        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"count", Int> count, State<Int> seen, Out<TsVar<"S">> out)
        {
            if (count.value() <= 0) { return; }
            const Int index = seen.get();
            if (index < count.value()) { out.apply(ts.value()); }
            seen.set(index + 1);
        }
    };

    struct drop_impl
    {
        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"count", Int> count, State<Int> seen, Out<TsVar<"S">> out)
        {
            const Int index = seen.get();
            if (index >= count.value()) { out.apply(ts.value()); }
            seen.set(index + 1);
        }
    };

    struct step_impl
    {
        static void start(Scalar<"step_size", Int> step_size)
        {
            stream_impl_detail::require_positive(step_size.value(), "step_size");
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"step_size", Int> step_size, State<Int> seen,
                         Out<TsVar<"S">> out)
        {
            const Int index = seen.get();
            if (index % step_size.value() == 0) { out.apply(ts.value()); }
            seen.set(index + 1);
        }
    };

    struct slice_impl
    {
        static void start(Scalar<"step_size", Int> step_size)
        {
            stream_impl_detail::require_positive(step_size.value(), "step_size");
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"start", Int> start, Scalar<"stop", Int> stop,
                         Scalar<"step_size", Int> step_size, State<Int> seen, Out<TsVar<"S">> out)
        {
            const Int index = seen.get();
            seen.set(index + 1);

            if (start.value() < 0) { return; }
            if (index < start.value()) { return; }
            if (stop.value() >= 0 && index >= stop.value()) { return; }
            if ((index - start.value()) % step_size.value() == 0) { out.apply(ts.value()); }
        }
    };

    struct count_impl
    {
        static void eval(In<"ts", SIGNAL> ts, State<Int> count, Out<TS<Int>> out)
        {
            static_cast<void>(ts);
            const Int next = count.get() + 1;
            count.set(next);
            out.set(next);
        }
    };

    struct dedup_scalar_impl
    {
        static void eval(In<"ts", TS<ScalarVar<"T">>> ts,
                         RecordableState<TS<ScalarVar<"T">>> last,
                         Out<TS<ScalarVar<"T">>> out)
        {
            const ValueView value = ts.base().value();
            if (!last.valid() || !last.value().equals(value)) { out.apply(value); }
            last.apply(value);
        }
    };

    struct diff_int_impl
    {
        static void eval(In<"ts", TS<Int>> ts, RecordableState<TS<Int>> last, Out<TS<Int>> out)
        {
            if (last.valid()) { out.set(ts.value() - last.value().checked_as<Int>()); }
            last.set(ts.value());
        }
    };

    struct diff_float_impl
    {
        static void eval(In<"ts", TS<Float>> ts, RecordableState<TS<Float>> last, Out<TS<Float>> out)
        {
            if (last.valid()) { out.set(ts.value() - last.value().checked_as<Float>()); }
            last.set(ts.value());
        }
    };

    struct clip_float_impl
    {
        static void start(Scalar<"min", Float> min, Scalar<"max", Float> max)
        {
            if (min.value() > max.value()) { throw std::invalid_argument("clip: min must be <= max"); }
        }

        static void eval(In<"ts", TS<Float>> ts, Scalar<"min", Float> min, Scalar<"max", Float> max, Out<TS<Float>> out)
        {
            out.set(std::clamp(ts.value(), min.value(), max.value()));
        }
    };

    struct ewma_float_impl
    {
        static void eval(In<"ts", TS<Float>> ts, Scalar<"alpha", Float> alpha,
                         RecordableState<TS<Float>> state, Out<TS<Float>> out)
        {
            const Float value = state.valid()
                                    ? alpha.value() * ts.value() +
                                          (Float{1.0} - alpha.value()) * state.value().checked_as<Float>()
                                    : ts.value();
            state.set(value);
            out.set(value);
        }
    };

    inline void register_stream_operators()
    {
        register_overload<sample, sample_impl>();
        register_overload<filter_, filter_impl>();
        register_overload<lag, lag_tick_impl>();
        register_overload<schedule, schedule_impl>();
        register_overload<resample, resample_impl>();
        register_overload<until_true, until_true_bool_impl>();
        register_overload<freeze, freeze_impl>();
        register_overload<gate, gate_impl>();
        register_overload<throttle, throttle_impl>();
        register_overload<take, take_impl>();
        register_overload<drop, drop_impl>();
        register_overload<step, step_impl>();
        register_overload<slice_, slice_impl>();
        register_overload<count, count_impl>();
        register_overload<dedup, dedup_scalar_impl>();
        register_overload<diff, diff_int_impl>();
        register_overload<diff, diff_float_impl>();
        register_overload<clip, clip_float_impl>();
        register_overload<ewma, ewma_float_impl>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_STREAM_IMPL_H

#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_STREAM_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_STREAM_IMPL_H

#include <hgraph/lib/std/operators/stream.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <string>

namespace hgraph::stdlib
{
    namespace stream_impl_detail
    {
        inline void require_positive(Int value, const char *name)
        {
            if (value <= 0) { throw std::invalid_argument(std::string{name} + " must be positive"); }
        }
    }  // namespace stream_impl_detail

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
        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"step_size", Int> step_size, State<Int> seen,
                         Out<TsVar<"S">> out)
        {
            stream_impl_detail::require_positive(step_size.value(), "step_size");
            const Int index = seen.get();
            if (index % step_size.value() == 0) { out.apply(ts.value()); }
            seen.set(index + 1);
        }
    };

    struct slice_impl
    {
        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"start", Int> start, Scalar<"stop", Int> stop,
                         Scalar<"step_size", Int> step_size, State<Int> seen, Out<TsVar<"S">> out)
        {
            stream_impl_detail::require_positive(step_size.value(), "step_size");

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
        static void eval(In<"ts", TS<Float>> ts, Scalar<"min", Float> min, Scalar<"max", Float> max, Out<TS<Float>> out)
        {
            if (min.value() > max.value()) { throw std::invalid_argument("clip: min must be <= max"); }
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

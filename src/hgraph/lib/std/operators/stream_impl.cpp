#include <hgraph/lib/std/operators/impl/stream_impl.h>

namespace hgraph::stdlib
{
    void register_stream_operators()
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

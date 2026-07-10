#ifndef HGRAPH_LIB_STD_OPERATORS_STREAM_H
#define HGRAPH_LIB_STD_OPERATORS_STREAM_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/util/date_time.h>

namespace hgraph::stdlib
{
    /**
     * Streaming / temporal-shaping operator **definitions** (markers only). Mirrors the
     * Python ``hgraph`` stream operators (``_stream.py``) and the analytical operators
     * (``_analytical_operators.py``). A ``period`` / ``count`` that Python types as
     * ``INT_OR_TIME_DELTA`` is modelled here as ``Int`` (ticks) — an implementation may also
     * accept ``TimeDelta``.
     */

    /** ``sample`` — snap ``ts`` on each tick of ``signal``. */
    struct sample : Operator<"sample", In<"signal", SIGNAL>, In<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** ``lag`` — delay delivery of ``ts`` by ``period`` ticks (or a time-delta). */
    struct lag : Operator<"lag", In<"ts", TsVar<"S">>, Scalar<"period", Int>, Out<TsVar<"S">>>
    {
    };

    /** ``__lag_proxy`` — the proxy-lag runtime node (internal; wired by the
        ``lag(ts, period, proxy)`` overloads): each tick of ``ts`` is cached
        under the live proxy count ``c`` and replayed when the LAGGED count
        ``lag_c`` reaches it. */
    struct lag_proxy_node
        : Operator<"__lag_proxy", In<"ts", TsVar<"S">>, In<"c", TS<Int>>, In<"lag_c", TS<Int>>, Out<TsVar<"S">>>
    {
    };

    /** ``schedule`` — a source ticking ``True`` every ``delay``. */
    struct schedule : Operator<"schedule", Scalar<"delay", TimeDelta>, Out<TS<Bool>>>
    {
    };

    /** ``resample`` — re-tick ``ts`` at ``period``, even when the input does not tick. */
    struct resample : Operator<"resample", In<"ts", TsVar<"S">>, Scalar<"period", TimeDelta>, Out<TsVar<"S">>>
    {
    };

    /** ``dedup`` — drop consecutive duplicate values. */
    struct dedup : Operator<"dedup", In<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** ``filter_`` — suppress ticks of ``ts`` while ``condition`` is ``False``. */
    struct filter_ : Operator<"filter_", In<"condition", TS<Bool>>, In<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** ``filter_by`` — filter ``ts`` by a predicate expression (supplied by the implementation). */
    struct filter_by : Operator<"filter_by", In<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** ``until_true`` — emit ``False`` until ``predicate`` first holds, then ``True`` (and passivate ``ts``). */
    struct until_true : Operator<"until_true", In<"ts", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``freeze`` — forward ``ts`` until ``predicate`` first holds, then passivate ``ts`` (stop forwarding). */
    struct freeze : Operator<"freeze", In<"predicate", TS<Bool>>, In<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** ``throttle`` — limit the tick rate of ``ts`` to ``period``. */
    struct throttle : Operator<"throttle", In<"ts", TsVar<"S">>, In<"period", TS<TimeDelta>>, Out<TsVar<"S">>>
    {
    };

    /** ``take`` — forward only the first ``count`` ticks of ``ts``. */
    struct take : Operator<"take", In<"ts", TsVar<"S">>, Scalar<"count", Int>, Out<TsVar<"S">>>
    {
    };

    /** ``drop`` — drop the first ``count`` ticks of ``ts``, then forward the rest. */
    struct drop : Operator<"drop", In<"ts", TsVar<"S">>, Scalar<"count", Int>, Out<TsVar<"S">>>
    {
    };

    /** ``window`` — buffer the last ``period`` values; result is a bundle (buffer + timestamps). */
    struct window : Operator<"window", In<"ts", TsVar<"S">>, Scalar<"period", Int>, Out<TsVar<"O">>>
    {
    };

    /** ``to_window`` — convert ``ts`` into a ``TSW`` time-series window of
        ``period`` ticks, valid once ``min_window_period`` ticks arrived. */
    struct to_window : Operator<"to_window", In<"ts", TsVar<"S">>, Scalar<"period", Int>,
                                Scalar<"min_window_period", Int>, Out<TsVar<"O">>>
    {
    };

    /** ``rolling_average`` — the trailing average of ``ts`` by tick count or
        duration (hgraph's window helper: ``(sum(ts) - sum(lag(ts, period)))``
        over the covered tick count). */
    struct rolling_average
        : Operator<"rolling_average", In<"ts", TS<ScalarVar<"T">>>, Scalar<"period", Int>, Out<TS<Float>>>
    {
    };

    /** ``gate`` — queue ticks while ``condition`` is ``False``, releasing them once it is ``True``. */
    struct gate : Operator<"gate", In<"condition", TS<Bool>>, In<"ts", TsVar<"S">>, Scalar<"buffer_length", Int>,
                           Out<TsVar<"S">>>
    {
    };

    /** ``batch`` — like ``gate`` but releases queued ticks in batches with ``delay`` between them. */
    struct batch : Operator<"batch", In<"condition", TS<Bool>>, In<"ts", TsVar<"S">>,
                            Scalar<"delay", TimeDelta>, Scalar<"buffer_length", Int>, Out<TsVar<"O">>>
    {
    };

    /** ``step`` — forward every ``step_size``-th tick of ``ts``. */
    struct step : Operator<"step", In<"ts", TsVar<"S">>, Scalar<"step_size", Int>, Out<TsVar<"S">>>
    {
    };

    /** ``slice_`` — ``drop`` + ``take`` + ``step`` combined over ``[start, stop)`` by ``step_size``. */
    struct slice_ : Operator<"slice_", In<"ts", TsVar<"S">>, Scalar<"start", Int>, Scalar<"stop", Int>,
                             Scalar<"step_size", Int>, Out<TsVar<"S">>>
    {
    };

    // ---- Analytical ----

    /** ``diff`` — the difference between the current and previous value of ``ts``. */
    struct diff : Operator<"diff", In<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** ``count`` — a running count of the ticks of ``ts`` (optional ``reset`` signal). */
    struct count : Operator<"count", In<"ts", SIGNAL>, Out<TS<Int>>>
    {
    };

    /** ``clip`` — clip ``ts`` into the ``[min, max]`` range. */
    struct clip : Operator<"clip", In<"ts", TsVar<"S">>, Scalar<"min", Float>, Scalar<"max", Float>, Out<TsVar<"S">>>
    {
    };

    /** ``ewma`` — an exponential moving average of ``ts`` with smoothing ``alpha``. */
    struct ewma : Operator<"ewma", In<"ts", TsVar<"S">>, Scalar<"alpha", Float>, Out<TsVar<"S">>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_STREAM_H

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
     * accept ``engine_time_delta_t``.
     */

    /** ``sample`` — snap ``ts`` on each tick of ``signal``. */
    struct sample : Operator<"sample", In<"signal", SIGNAL>, In<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** ``lag`` — delay delivery of ``ts`` by ``period`` ticks (or a time-delta). */
    struct lag : Operator<"lag", In<"ts", TsVar<"S">>, Scalar<"period", Int>, Out<TsVar<"S">>>
    {
    };

    /** ``schedule`` — a source ticking ``True`` every ``delay``. */
    struct schedule : Operator<"schedule", Scalar<"delay", engine_time_delta_t>, Out<TS<Bool>>>
    {
    };

    /** ``resample`` — re-tick ``ts`` at ``period``, even when the input does not tick. */
    struct resample : Operator<"resample", In<"ts", TsVar<"S">>, Scalar<"period", engine_time_delta_t>, Out<TsVar<"S">>>
    {
    };

    /** ``dedup`` — drop consecutive duplicate values. */
    struct dedup : Operator<"dedup", In<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** ``filter_`` — suppress ticks of ``ts`` while ``condition`` is ``False``. */
    struct filter_ : Operator<"filter", In<"condition", TS<Bool>>, In<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** ``filter_by`` — filter ``ts`` by a predicate expression (supplied by the implementation). */
    struct filter_by : Operator<"filter_by", In<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** ``throttle`` — limit the tick rate of ``ts`` to ``period``. */
    struct throttle : Operator<"throttle", In<"ts", TsVar<"S">>, In<"period", TS<engine_time_delta_t>>, Out<TsVar<"S">>>
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

    /** ``to_window`` — convert ``ts`` into a ``TSW`` time-series window of ``period``. */
    struct to_window : Operator<"to_window", In<"ts", TsVar<"S">>, Scalar<"period", Int>, Out<TsVar<"O">>>
    {
    };

    /** ``gate`` — queue ticks while ``condition`` is ``False``, releasing them once it is ``True``. */
    struct gate : Operator<"gate", In<"condition", TS<Bool>>, In<"ts", TsVar<"S">>, Scalar<"buffer_length", Int>,
                           Out<TsVar<"S">>>
    {
    };

    /** ``batch`` — like ``gate`` but releases queued ticks in batches with ``delay`` between them. */
    struct batch : Operator<"batch", In<"condition", TS<Bool>>, In<"ts", TsVar<"S">>,
                            Scalar<"delay", engine_time_delta_t>, Scalar<"buffer_length", Int>, Out<TsVar<"O">>>
    {
    };

    /** ``step`` — forward every ``step_size``-th tick of ``ts``. */
    struct step : Operator<"step", In<"ts", TsVar<"S">>, Scalar<"step_size", Int>, Out<TsVar<"S">>>
    {
    };

    /** ``slice_`` — ``drop`` + ``take`` + ``step`` combined over ``[start, stop)`` by ``step_size``. */
    struct slice_ : Operator<"slice", In<"ts", TsVar<"S">>, Scalar<"start", Int>, Scalar<"stop", Int>,
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

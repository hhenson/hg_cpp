#ifndef HGRAPH_LIB_STD_OPERATORS_TEMPORAL_H
#define HGRAPH_LIB_STD_OPERATORS_TEMPORAL_H

#include <hgraph/lib/std/operators/comparison.h>   // CmpResult (evaluation_time_in_range)
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/util/date_time.h>

namespace hgraph::stdlib
{
    /**
     * Date / time-series-property operator **definitions** (markers only). Mirrors the
     * Python ``hgraph`` date operators (``_date_operators.py``) and the time-series
     * introspection operators (``_time_series_properties.py``).
     */

    // ---- Date component extraction ----

    /** ``day_of_month`` — the day-of-month of a ``TS<Date>``. */
    struct day_of_month : Operator<"day_of_month", In<"ts", TS<Date>>, Out<TS<Int>>>
    {
    };

    /** ``month_of_year`` — the month-of-year of a ``TS<Date>``. */
    struct month_of_year : Operator<"month_of_year", In<"ts", TS<Date>>, Out<TS<Int>>>
    {
    };

    /** ``year`` — the year of a ``TS<Date>``. */
    struct year : Operator<"year", In<"ts", TS<Date>>, Out<TS<Int>>>
    {
    };

    /** ``explode`` — the (year, month, day) of a ``TS<Date>`` as a 3-element list. */
    struct explode : Operator<"explode", In<"ts", TS<Date>>, Out<TsVar<"O">>>
    {
    };

    // ---- Time-series introspection ----

    /** ``valid`` — ``True`` while ``ts`` is valid, ``False`` otherwise. */
    struct valid : Operator<"valid", In<"ts", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``modified`` — ``True`` in the cycle ``ts`` is modified (a live, ticking property). */
    struct modified : Operator<"modified", In<"ts", SIGNAL>, Out<TS<Bool>>>
    {
    };

    /** ``last_modified_time`` — the engine time ``ts`` was last modified. */
    struct last_modified_time : Operator<"last_modified_time", In<"ts", SIGNAL>, Out<TS<DateTime>>>
    {
    };

    /** ``last_modified_wall_clock_time`` — the wall-clock time ``ts`` was last modified. */
    struct last_modified_wall_clock_time
        : Operator<"last_modified_wall_clock_time", In<"ts", SIGNAL>, Out<TS<DateTime>>>
    {
    };

    /** ``last_modified_date`` — the date component of the last-modified time. */
    struct last_modified_date : Operator<"last_modified_date", In<"ts", SIGNAL>, Out<TS<Date>>>
    {
    };

    /** ``evaluation_time_in_range`` — whether the current evaluation time is LT / EQ / GT the range. */
    struct evaluation_time_in_range
        : Operator<"evaluation_time_in_range", In<"start_time", TsVar<"S">>, In<"end_time", TsVar<"S">>,
                   Out<TS<CmpResult>>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_TEMPORAL_H

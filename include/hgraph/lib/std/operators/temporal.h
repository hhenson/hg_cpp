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
    /** hgraph's date ATTRIBUTES (port.month / .day / .weekday / .isoweekday). */
    struct month : Operator<"month", In<"ts", TS<Date>>, Out<TS<Int>>>
    {
    };

    struct day : Operator<"day", In<"ts", TS<Date>>, Out<TS<Int>>>
    {
    };

    struct weekday : Operator<"weekday", In<"ts", TS<Date>>, Out<TS<Int>>>
    {
    };

    struct isoweekday : Operator<"isoweekday", In<"ts", TS<Date>>, Out<TS<Int>>>
    {
    };

    /** ``evaluation_time_in_range`` — where the evaluation time sits
        relative to [start, end]: LT / EQ / GT, self-scheduling at the
        boundaries (datetime / date / daily-recurring time overloads). */
    struct evaluation_time_in_range
        : Operator<"evaluation_time_in_range", In<"start_time", TsVar<"A">>, In<"end_time", TsVar<"B">>,
                   Out<TS<CmpResult>>>
    {
    };

    struct isoformat : Operator<"isoformat", In<"ts", TS<Date>>, Out<TS<Str>>>
    {
    };

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

    /** ``last_modified_time`` — the evaluation time ``ts`` was last modified. */
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

}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_TEMPORAL_H

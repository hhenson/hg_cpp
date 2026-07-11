#ifndef HGRAPH_LIB_STD_OPERATORS_DATA_FRAME_H
#define HGRAPH_LIB_STD_OPERATORS_DATA_FRAME_H

#include <hgraph/types/frame.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

namespace hgraph::stdlib
{
    /**
     * Data-frame convenience operators (design record: *Record/replay,
     * tables and const_fn*, step 6): the same layout/codec machinery as
     * ``to_table`` with a plain ``date`` column and no ``as_of``.
     *
     * ``from_data_frame[OUT](df, dt_col="date", key_col="key",
     * value_col="value", offset=0)`` replays a frame VALUE by its date
     * column (a pull source; TSD forms take the key from ``key_col``).
     * ``to_data_frame(ts, ...)`` snapshots the time-series per tick into a
     * one-tick frame whose columns come from the requested output
     * ``Frame[Schema]``. ``group_by(ts, by)`` partitions a Frame-valued TS
     * into ``TSD[key, TS[Frame]]`` by column name(s).
     */
    struct from_data_frame : Operator<"from_data_frame", Scalar<"df", Frame>, Scalar<"dt_col", Str>,
                                      Scalar<"key_col", Str>, Scalar<"value_col", Str>,
                                      Scalar<"offset", TimeDelta>, Out<TsVar<"O">>>
    {
    };

    struct to_data_frame : Operator<"to_data_frame", In<"ts", TsVar<"S">>, Scalar<"dt_col", Str>,
                                    Scalar<"key_col", Str>, Scalar<"value_col", Str>,
                                    Out<TsVar<"__out__">>>
    {
    };

    struct group_by
        : Operator<"group_by", In<"ts", TS<ScalarVar<"F">>>, Scalar<"by", ScalarVar<"B">>, Out<TsVar<"__out__">>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_DATA_FRAME_H

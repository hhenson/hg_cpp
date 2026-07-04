#ifndef HGRAPH_LIB_STD_OPERATORS_TABLE_H
#define HGRAPH_LIB_STD_OPERATORS_TABLE_H

#include <hgraph/types/frame.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

namespace hgraph::stdlib
{
    /**
     * Table serialization operators (design record: *Record/replay, tables
     * and const_fn*, P4). ``to_table`` converts each tick's value into a
     * one-row bitemporal ``Frame`` (``[date, as_of, *columns]``; Arrow-backed
     * — see ``types/value/table_codec.h``); ``from_table`` reverses it,
     * resolving frame columns BY NAME (the input-minimum rule: extra columns
     * pass through, missing required columns throw). The output type is
     * supplied at the wiring site: ``wire<from_table, TS<MySchema>>(w, ts)``.
     *
     * v1 covers ``TS`` over atomics and depth-1 compound bundles; TSD
     * partitioning and the Sample/Snap write modes land with the
     * record/replay backend (step 4).
     */
    struct to_table : Operator<"to_table", In<"ts", TsVar<"S">>, Out<TS<Frame>>>
    {
    };

    struct from_table : Operator<"from_table", In<"ts", TS<Frame>>, Out<TsVar<"O">>>
    {
    };

    /** ``from_table_const`` — the const-evaluable form of ``from_table``:
        extract the (last) row of a frame VALUE as the output type. */
    struct from_table_const : Operator<"from_table_const", Scalar<"value", Frame>, Out<TsVar<"O">>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_TABLE_H

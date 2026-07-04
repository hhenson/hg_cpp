#ifndef HGRAPH_LIB_STD_OPERATORS_IO_H
#define HGRAPH_LIB_STD_OPERATORS_IO_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

namespace hgraph::stdlib
{
    /**
     * I/O, logging and record/replay operator **definitions** (markers only). Mirrors the
     * Python ``hgraph`` debug tools (``_debug_tools.py``), graph-utility sinks
     * (``_graph_operators.py``) and record/replay operators (``_record_replay.py``). The
     * sinks (``debug_print`` / ``null_sink`` / ``print_`` / ``log_`` / ``assert_`` /
     * ``record`` / ``compare``) have no output.
     *
     * .. note::
     *
     *    The data-frame record/replay operators are deferred with the rest of the table /
     *    data-frame family (see *conversion*).
     */

    /** ``debug_print`` — print ``label: value`` on each tick of ``ts`` (a diagnostic sink).
        (Python also takes ``print_delta`` / ``sample`` — not yet modelled.) */
    struct debug_print : Operator<"debug_print", Scalar<"label", Str>, In<"ts", TsVar<"S">>>
    {
    };

    /** ``null_sink`` — consume ``ts`` and do nothing (a terminal sink). */
    struct null_sink : Operator<"null_sink", In<"ts", TsVar<"S">>>
    {
    };

    /** ``print_`` — format and write the supplied values to std-out (a sink, variadic args). */
    struct print_ : Operator<"print_", In<"fmt", TS<Str>>, In<"args", TsVar<"A">>>
    {
    };

    /** ``log_`` — format and log the supplied values at ``level`` (a sink, variadic args). */
    struct log_ : Operator<"log_", In<"fmt", TS<Str>>, In<"args", TsVar<"A">>, Scalar<"level", Int>>
    {
    };

    /** ``assert_`` — assert ``condition`` holds, raising ``error_msg`` otherwise (a sink). */
    struct assert_ : Operator<"assert_", In<"condition", TS<Bool>>, Scalar<"error_msg", Str>>
    {
    };

    /** ``record`` — record ``ts`` under ``key`` (a sink). */
    struct record : Operator<"record", In<"ts", TsVar<"S">>, Scalar<"key", Str>>
    {
    };

    /** ``replay`` — replay a recorded series for ``key`` as the requested output type (a source). */
    struct replay : Operator<"replay", Scalar<"key", Str>, Out<TsVar<"O">>>
    {
    };

    /** ``replay_const`` — replay the const value(s) at/under ``key`` valid up to the start time. */
    struct replay_const : Operator<"replay_const", Scalar<"key", Str>, Out<TsVar<"O">>>
    {
    };

    /** ``compare`` — the backtesting comparison sink (COMPARE mode): records
        per-tick equality of ``lhs`` vs ``rhs`` through the registered frame
        store (P6) under ``fq_recordable_id.__compare__``. */
    struct compare : Operator<"compare", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>,
                              Scalar<"recordable_id", Str>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IO_H

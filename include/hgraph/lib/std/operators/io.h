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
     * Python ``hgraph`` graph-utility sinks (``_graph_operators.py``) and record/replay
     * operators (``_record_replay.py``). The sinks (``print_`` / ``log_`` / ``assert_`` /
     * ``record`` / ``compare``) have no output.
     *
     * .. note::
     *
     *    ``null_sink`` / ``debug_print`` already exist as concrete nodes in ``std_nodes.h``
     *    and so are not redefined here as operators; reconciling node-vs-operator is a later
     *    step. The data-frame record/replay operators are deferred with the rest of the
     *    table / data-frame family (see *conversion*).
     */

    /** ``print_`` — format and write the supplied values to std-out (a sink, variadic args). */
    struct print_ : Operator<"print", In<"fmt", TS<Str>>, In<"args", TsVar<"A">>>
    {
    };

    /** ``log_`` — format and log the supplied values at ``level`` (a sink, variadic args). */
    struct log_ : Operator<"log", In<"fmt", TS<Str>>, In<"args", TsVar<"A">>, Scalar<"level", Int>>
    {
    };

    /** ``assert_`` — assert ``condition`` holds, raising ``error_msg`` otherwise (a sink). */
    struct assert_ : Operator<"assert", In<"condition", TS<Bool>>, Scalar<"error_msg", Str>>
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

    /** ``compare`` — compare two time-series (used when running in COMPARE mode; a sink). */
    struct compare : Operator<"compare", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IO_H

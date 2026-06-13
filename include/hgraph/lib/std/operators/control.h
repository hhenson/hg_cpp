#ifndef HGRAPH_LIB_STD_OPERATORS_CONTROL_H
#define HGRAPH_LIB_STD_OPERATORS_CONTROL_H

#include <hgraph/lib/std/operators/comparison.h>   // CmpResult (if_cmp)
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

namespace hgraph::stdlib
{
    /**
     * Flow-control / routing operator **definitions** (markers only). Mirrors the Python
     * ``hgraph`` flow-control operators (``_flow_control.py``). ``merge`` / ``race`` /
     * ``all_`` / ``any_`` are variadic over their inputs.
     */

    /** ``merge`` — forward the first of the inputs to tick this cycle (variadic). */
    struct merge : Operator<"merge", VarIn<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** ``race`` — forward the first *valid* of the inputs, falling through as they invalidate. */
    struct race : Operator<"race", In<"ts", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** ``all_`` — graph ``all``: ``True`` when every boolean input is ``True`` (variadic). */
    struct all_ : Operator<"all_", VarIn<"args", TS<Bool>>, Out<TS<Bool>>>
    {
    };

    /** ``any_`` — graph ``any``: ``True`` when any boolean input is ``True`` (variadic). */
    struct any_ : Operator<"any_", VarIn<"args", TS<Bool>>, Out<TS<Bool>>>
    {
    };

    /** ``if_`` — route ``ts`` to a ``true`` / ``false`` bundle output by ``condition``. */
    struct if_ : Operator<"if_", In<"condition", TS<Bool>>, In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``route_by_index`` — forward ``ts`` to the ``index``-th of a list of outputs. */
    struct route_by_index : Operator<"route_by_index", In<"index", TS<Int>>, In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``if_true`` — tick ``True`` when ``condition`` ticks ``True`` (optional ``tick_once_only``). */
    struct if_true : Operator<"if_true", In<"condition", TS<Bool>>, Scalar<"tick_once_only", Bool>, Out<TS<Bool>>>
    {
    };

    /** ``if_then_else`` — select ``true_value`` or ``false_value`` per ``condition``. */
    struct if_then_else : Operator<"if_then_else", In<"condition", TS<Bool>>, In<"true_value", TsVar<"S">>,
                                   In<"false_value", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** ``if_cmp`` — select ``lt`` / ``eq`` / ``gt`` according to a ``CmpResult``. */
    struct if_cmp : Operator<"if_cmp", In<"cmp", TS<CmpResult>>, In<"lt", TsVar<"O">>, In<"eq", TsVar<"O">>,
                             In<"gt", TsVar<"O">>, Out<TsVar<"O">>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_CONTROL_H

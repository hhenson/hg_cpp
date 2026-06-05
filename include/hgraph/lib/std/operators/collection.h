#ifndef HGRAPH_LIB_STD_OPERATORS_COLLECTION_H
#define HGRAPH_LIB_STD_OPERATORS_COLLECTION_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

namespace hgraph::stdlib
{
    /**
     * Collection operator **definitions** (markers only): aggregations / reductions, set
     * operations and ``TSD`` (dictionary) re-shaping. Mirrors the Python ``hgraph``
     * aggregation operators (``_operators.py``) and the TSD / mapping operators
     * (``_tsd_and_mapping.py``). The aggregations and set operations are **variadic**:
     * unary = over a collection / running, n-ary = element-wise.
     */

    // ---- Aggregations / reductions ----

    /** ``sum_`` — running / element-wise sum. */
    struct sum_ : Operator<"sum", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``mean`` — running / element-wise mean. */
    struct mean : Operator<"mean", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``std_`` — running / element-wise standard deviation. */
    struct std_ : Operator<"std", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``var_`` — running / element-wise variance. */
    struct var_ : Operator<"var", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    // ---- Set operations (variadic over the inputs) ----

    /** ``union_`` — set union of the inputs. */
    struct union_ : Operator<"union", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``intersection_`` — set intersection of the inputs. */
    struct intersection_ : Operator<"intersection", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``difference_`` — set difference (``lhs`` minus the rest). */
    struct difference_ : Operator<"difference", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``symmetric_difference_`` — set symmetric difference of the inputs. */
    struct symmetric_difference_ : Operator<"symmetric_difference", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    // ---- TSD / dictionary re-shaping ----

    /** ``keys_`` — the keys of a dictionary (as a ``TSS`` / set). */
    struct keys_ : Operator<"keys", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``values_`` — the values of a dictionary. */
    struct values_ : Operator<"values", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``rekey`` — re-key the input dictionary using a mapping time-series. */
    struct rekey : Operator<"rekey", In<"ts", TsVar<"S">>, In<"new_keys", TsVar<"K">>, Out<TsVar<"O">>>
    {
    };

    /** ``flip`` — swap keys and values of a dictionary. */
    struct flip : Operator<"flip", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``partition`` — split a ``TSD[K, V]`` into ``TSD[K1, TSD[K, V]]`` using a mapping. */
    struct partition : Operator<"partition", In<"ts", TsVar<"S">>, In<"partitions", TsVar<"P">>, Out<TsVar<"O">>>
    {
    };

    /** ``unpartition`` — merge a nested ``TSD[K1, TSD[K, V]]`` back into ``TSD[K, V]``. */
    struct unpartition : Operator<"unpartition", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``flip_keys`` — invert the outer/inner keys of a nested ``TSD[K, TSD[K1, V]]``. */
    struct flip_keys : Operator<"flip_keys", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``collapse_keys`` — collapse a nested ``TSD[K, TSD[K1, V]]`` to ``TSD[Tuple[K, K1], V]``. */
    struct collapse_keys : Operator<"collapse_keys", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``uncollapse_keys`` — the inverse of ``collapse_keys`` (optional ``remove_empty`` flag). */
    struct uncollapse_keys : Operator<"uncollapse_keys", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_COLLECTION_H

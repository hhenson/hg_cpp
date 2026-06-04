#ifndef HGRAPH_TYPES_TIME_SERIES_TS_DELTA_H
#define HGRAPH_TYPES_TIME_SERIES_TS_DELTA_H

namespace hgraph
{
    class TSInputView;
    class TSOutputView;
    class ValueView;
    class Value;

    /**
     * Runtime, type-erased per-cycle delta capture / apply — the schema-as-data
     * twin of the compile-time ``ts_delta<S>`` (``static_node.h``). Both dispatch
     * through the live endpoint's ``TSDataOps`` table and recurse through child
     * ops, so a single (non-templated) API serves every replayable time-series
     * schema — the basis for the erased ``replay`` / ``record`` utility nodes.
     *
     * ``capture_delta`` reads a live input and **rebuilds** the canonical delta
     * ``Value`` whose schema is ``in.schema()->delta_value_schema`` (via the
     * value-layer builders, so the result is owned and copyable — the runtime's
     * transient delta storage omits value-layer copy hooks, so a direct copy of
     * ``delta_value()`` is not safe). ``apply_delta`` is the inverse: it re-creates
     * output ticks from such a canonical delta ``Value``.
     *
     * The canonical per-kind delta shape (``type_registry.cpp``):
     *   ``TS<T>`` / ``SIGNAL`` / ``TSW<T>`` -> scalar; ``TSS<T>`` ->
     *   ``Bundle{added: Set<T>, removed: Set<T>}``; ``TSD<K,V>`` ->
     *   ``Bundle{removed: Set<K>, modified: Map<K, delta(V)>}``;
     *   ``TSL<C,N>`` -> ``Map<int64, delta(C)>``; ``TSB{f...}`` ->
     *   ``Bundle{f: delta(f)...}`` (recursive in children).
     *
     * ``REF`` throws a clear ``std::logic_error``: it is a separate
     * reference-binding surface rather than ordinary value replay.
     */
    [[nodiscard]] Value capture_delta(const TSInputView &in);

    void apply_delta(const TSOutputView &out, const ValueView &delta);
}  // namespace hgraph

#endif  // HGRAPH_TYPES_TIME_SERIES_TS_DELTA_H

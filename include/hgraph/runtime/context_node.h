#ifndef HGRAPH_RUNTIME_CONTEXT_NODE_H
#define HGRAPH_RUNTIME_CONTEXT_NODE_H

#include <hgraph/runtime/node.h>

#include <string>
#include <string_view>

namespace hgraph
{
    /**
     * Build the global-state key used for a captured context output.
     *
     * ``scope`` identifies the graph scope that owns the captured output, and
     * ``path`` identifies the context value within that scope. The Python runtime
     * uses keys shaped as ``context-{graph_id}-{path}``; this helper preserves
     * that namespace without prescribing the future C++ wiring syntax.
     */
    [[nodiscard]] HGRAPH_EXPORT std::string context_output_key(std::string_view scope,
                                                               std::string_view path);

    /**
     * Build a sink node that captures an input time-series reference into the
     * root graph's ``GlobalState`` during ``start`` and erases it during ``stop``.
     *
     * Runtime builder example:
     *
     * .. code-block:: cpp
     *
     *    auto key = context_output_key("root", "price");
     *    GraphBuilder gb;
     *    gb.add_node(price_source);
     *    gb.add_node(make_context_capture_node(key, *ts_price));
     *    gb.add_edge(GraphEdge{.source_node = 0, .target_node = 1, .target_path = {0}});
     *
     * This is an internal graph primitive. User-facing context wiring still
     * needs an approved C++ API.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_context_capture_node(std::string key,
                                                                      const TSValueTypeMetaData &target_schema);

    /**
     * Build a pull-source node that reads a captured context reference from
     * ``GlobalState`` and publishes it as ``REF<target_schema>``.
     *
     * Runtime builder example:
     *
     * .. code-block:: cpp
     *
     *    auto key = context_output_key("root", "price");
     *    auto stub = make_context_stub_source_node(key, *ts_price);
     *    gb.add_node(std::move(stub));
     *
     * The node publishes the reference token only; consumers bind through that
     * reference to the original output, so the referenced value is not copied.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_context_stub_source_node(std::string key,
                                                                          const TSValueTypeMetaData &target_schema);
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_CONTEXT_NODE_H

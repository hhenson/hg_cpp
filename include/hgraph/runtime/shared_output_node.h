#ifndef HGRAPH_RUNTIME_SHARED_OUTPUT_NODE_H
#define HGRAPH_RUNTIME_SHARED_OUTPUT_NODE_H

#include <hgraph/runtime/node.h>

#include <string>
#include <string_view>

namespace hgraph
{
    /** Build the global-state key used for a shared output reference. */
    [[nodiscard]] HGRAPH_EXPORT std::string output_key(std::string_view path);

    /** Build the global-state key reserved for shared-output change subscribers. */
    [[nodiscard]] HGRAPH_EXPORT std::string output_subscriber_key(std::string_view path);

    /**
     * Build a sink node that captures an input time-series reference under
     * ``output_key(path)`` and removes it during ``stop``.
     *
     * Runtime builder example:
     *
     * .. code-block:: cpp
     *
     *    auto path = "svc://prices/to_graph";
     *    gb.add_node(price_source);
     *    gb.add_node(make_shared_output_capture_node(path, *ts_price));
     *    gb.add_edge(GraphEdge{.source_node = 0, .target_node = 1, .target_path = {0}});
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_shared_output_capture_node(
        std::string path,
        const TSValueTypeMetaData &target_schema);

    /**
     * Build a pull-source node that reads ``output_key(path)`` from
     * ``GlobalState`` and publishes it as ``REF<target_schema>``.
     *
     * ``strict`` mirrors the Python helper's missing-output policy: when true,
     * absence is an error; when false, the node produces no reference for that
     * evaluation. Late-availability notification is a separate service/adaptor
     * runtime concern and is not implemented by this primitive.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_shared_output_stub_source_node(
        std::string path,
        const TSValueTypeMetaData &target_schema,
        bool strict = true);
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_SHARED_OUTPUT_NODE_H

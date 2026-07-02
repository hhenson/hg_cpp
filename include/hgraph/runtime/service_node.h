#ifndef HGRAPH_RUNTIME_SERVICE_NODE_H
#define HGRAPH_RUNTIME_SERVICE_NODE_H

#include <hgraph/runtime/node.h>
#include <hgraph/types/primitive_types.h>

#include <string>

namespace hgraph
{
    /**
     * Build a source node that owns a service subscription key set output.
     *
     * The output schema is ``TSS<key_schema>``. Paired capture nodes record
     * per-client key add/remove intents into this source's graph-local storage;
     * the source applies those intents to its own output during evaluation.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_subscription_key_source_node(
        std::string path,
        const ValueTypeMetaData &key_schema);

    /**
     * Build a sink node that captures one client's current subscription key.
     *
     * Input field ``key`` is ``TS<key_schema>`` and field ``subscriptions`` is
     * the paired source node's ``TSS<key_schema>`` output. Only ``key`` is active;
     * ``subscriptions`` is a passive binding used to locate the source node.
     *
     * Runtime builder example:
     *
     * .. code-block:: cpp
     *
     *    auto path = "svc://prices/subscriptions";
     *    gb.add_node(client_key_source);
     *    gb.add_node(make_subscription_key_source_node(path, *int_meta));
     *    gb.add_node(make_subscription_key_capture_node(path, *int_meta));
     *    gb.add_edge(GraphEdge{.source_node = 0, .target_node = 2, .target_path = {0}});
     *    gb.add_edge(GraphEdge{.source_node = 1, .target_node = 2, .target_path = {1}});
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_subscription_key_capture_node(
        std::string path,
        const ValueTypeMetaData &key_schema);

    /**
     * Build a source node that owns a request/reply service request dictionary.
     *
     * The output schema is ``TSD<int, request_schema>``. Paired capture nodes
     * update a source-owned request delta state; the source applies that delta
     * to its own output and resets it during evaluation.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_request_input_source_node(
        std::string path,
        const TSValueTypeMetaData &request_schema);

    /**
     * Build a sink node that captures one request/reply client's request.
     *
     * Input field ``request`` is ``request_schema`` and field ``requests`` is
     * the paired source node's ``TSD<int, request_schema>`` output. Only
     * ``request`` is active; ``requests`` is a passive binding used to locate
     * the source node. Captures schedule the source for the current engine time
     * during start, and the next engine tick during normal evaluation.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_request_input_capture_node(
        std::string path,
        const TSValueTypeMetaData &request_schema,
        Int request_id);
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_SERVICE_NODE_H

//
// Created by Howard Henson on 26/12/2024.
//

#ifndef GRAPH_BUILDER_H
#define GRAPH_BUILDER_H

#include <hgraph/builders/builder.h>
#include <hgraph/hgraph_forward_declarations.h>

#include <vector>

namespace hgraph
{

    struct Edge
    {
        int                  src_node;
        std::vector<int64_t> output_path;
        int                  dst_node;
        std::vector<int64_t> input_path;

        Edge(int src, std::vector<int64_t> out_path, int dst, std::vector<int64_t> in_path)
            : src_node(src), output_path(std::move(out_path)), dst_node(dst), input_path(std::move(in_path)) {}
    };

    struct Graph;

    struct GraphBuilder : public Builder
    {
        std::vector<node_ptr> node_builders;
        std::vector<Edge>     edges;

        /**
         * Construct an instance of a graph. The id provided is the id for the graph instance to be constructed.
         */
        virtual graph_ptr make_instance(const std::vector<int64_t> &graph_id, node_ptr parent_node = nullptr,
                                        const std::string &label = "") const = 0;

        /**
         * Make the nodes described in the node builders and connect the edges as described in the edges.
         * Return the iterable of newly constructed and wired nodes.
         * This can be used to feed into a new graph instance or to extend (or re-initialise) an existing graph.
         */
        virtual std::vector<node_ptr> make_and_connect_nodes(const std::vector<int64_t> &graph_id, int64_t first_node_ndx) const = 0;

        /**
         * Release resources constructed during the build process, plus the graph.
         */
        virtual void release_instance(graph_ptr item) const = 0;
    };
};  // namespace hgraph
#endif  // GRAPH_BUILDER_H

//
// Created by Howard Henson on 26/12/2024.
//

#ifndef NODE_BUILDER_H
#define NODE_BUILDER_H

#include "hgraph/types/node.h"

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/python/pyb.h>

#include <memory>
#include <optional>
#include <vector>

#include <hgraph/builders/builder.h>

namespace hgraph
{

    struct NodeBuilder : Builder
    {
        NodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                    std::optional<input_builder_ptr>  input_builder_            = std::nullopt,
                    std::optional<output_builder_ptr> output_builder_           = std::nullopt,
                    std::optional<output_builder_ptr> error_builder_            = std::nullopt,
                    std::optional<output_builder_ptr> recordable_state_builder_ = std::nullopt);

        virtual node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int node_ndx) = 0;

        virtual void release_instance(node_ptr &item) = 0;

      protected:
        node_signature_ptr                signature;
        nb::dict                          scalars;
        std::optional<input_builder_ptr>  input_builder;
        std::optional<output_builder_ptr> output_builder;
        std::optional<output_builder_ptr> error_builder;
        std::optional<output_builder_ptr> recordable_state_builder;
    };

    struct BaseNodeBuilder : NodeBuilder
    {
        using NodeBuilder::NodeBuilder;

    protected:
        void _build_inputs_and_outputs(node_ptr node);
    };
}  // namespace hgraph

#endif  // NODE_BUILDER_H

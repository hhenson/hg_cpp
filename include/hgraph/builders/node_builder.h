//
// Created by Howard Henson on 26/12/2024.
//

#ifndef NODE_BUILDER_H
#define NODE_BUILDER_H

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

        virtual node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int node_ndx) const = 0;

        virtual void release_instance(node_ptr &item) const {};

        static void register_with_nanobind(nb::module_ &m);

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
        void _build_inputs_and_outputs(node_ptr node) const;
    };

    struct PythonNodeBuilder : BaseNodeBuilder
    {
        PythonNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                    std::optional<input_builder_ptr>  input_builder_,
                    std::optional<output_builder_ptr> output_builder_,
                    std::optional<output_builder_ptr> error_builder_,
                    std::optional<output_builder_ptr> recordable_state_builder_,
                    nb::callable eval_fn, nb::callable start_fn, nb::callable stop_fn);

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int node_ndx) const override;

        nb::callable eval_fn;
        nb::callable start_fn;
        nb::callable stop_fn;
    };

    struct PythonGeneratorNodeBuilder : BaseNodeBuilder
    {
        PythonGeneratorNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
            std::optional<input_builder_ptr>  input_builder_,
            std::optional<output_builder_ptr> output_builder_,
            std::optional<output_builder_ptr> error_builder_,
            nb::callable eval_fn);

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int node_ndx) const override;

        nb::callable eval_fn;
    };
}  // namespace hgraph

#endif  // NODE_BUILDER_H

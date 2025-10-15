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

        virtual node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const = 0;

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
        PythonNodeBuilder(node_signature_ptr signature_, nb::dict scalars_, std::optional<input_builder_ptr> input_builder_,
                          std::optional<output_builder_ptr> output_builder_, std::optional<output_builder_ptr> error_builder_,
                          std::optional<output_builder_ptr> recordable_state_builder_, nb::callable eval_fn, nb::callable start_fn,
                          nb::callable stop_fn);

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;

        nb::callable eval_fn;
        nb::callable start_fn;
        nb::callable stop_fn;
    };

    struct PythonGeneratorNodeBuilder : BaseNodeBuilder
    {
        PythonGeneratorNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                                   std::optional<input_builder_ptr>  input_builder_,
                                   std::optional<output_builder_ptr> output_builder_,
                                   std::optional<output_builder_ptr> error_builder_, nb::callable eval_fn);

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;

        nb::callable eval_fn;
    };

    struct BaseTsdMapNodeBuilder : BaseNodeBuilder
    {
        BaseTsdMapNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                              std::optional<input_builder_ptr>                input_builder_            = std::nullopt,
                              std::optional<output_builder_ptr>               output_builder_           = std::nullopt,
                              std::optional<output_builder_ptr>               error_builder_            = std::nullopt,
                              std::optional<output_builder_ptr>               recordable_state_builder_ = std::nullopt,
                              graph_builder_ptr                               nested_graph_builder      = {},
                              const std::unordered_map<std::string, int64_t> &input_node_ids = {}, int64_t output_node_id = -1,
                              const std::unordered_set<std::string> &multiplexed_args = {}, const std::string &key_arg = {});

        graph_builder_ptr                          nested_graph_builder;
        const std::unordered_map<std::string, int64_t> input_node_ids;
        int64_t                                        output_node_id;
        const std::unordered_set<std::string>      multiplexed_args;
        const std::string                          key_arg;
    };

    template <typename T> struct TsdMapNodeBuilder : BaseTsdMapNodeBuilder
    {
        using BaseTsdMapNodeBuilder::BaseTsdMapNodeBuilder;

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;
    };

    struct BaseReduceNodeBuilder : BaseNodeBuilder
    {
        BaseReduceNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                             std::optional<input_builder_ptr>  input_builder_            = std::nullopt,
                             std::optional<output_builder_ptr> output_builder_           = std::nullopt,
                             std::optional<output_builder_ptr> error_builder_            = std::nullopt,
                             std::optional<output_builder_ptr> recordable_state_builder_ = std::nullopt,
                             graph_builder_ptr                 nested_graph_builder      = {},
                             const std::tuple<int64_t, int64_t> &input_node_ids = {}, int64_t output_node_id = -1);

        graph_builder_ptr               nested_graph_builder;
        std::tuple<int64_t, int64_t>    input_node_ids;
        int64_t                         output_node_id;
    };

    template <typename T> struct ReduceNodeBuilder : BaseReduceNodeBuilder
    {
        using BaseReduceNodeBuilder::BaseReduceNodeBuilder;

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;
    };

    struct ContextNodeBuilder : BaseNodeBuilder
    {
        using BaseNodeBuilder::BaseNodeBuilder;

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;
    };

    struct BaseNestedGraphNodeBuilder : BaseNodeBuilder
    {
        BaseNestedGraphNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                                   std::optional<input_builder_ptr>  input_builder_            = std::nullopt,
                                   std::optional<output_builder_ptr> output_builder_           = std::nullopt,
                                   std::optional<output_builder_ptr> error_builder_            = std::nullopt,
                                   std::optional<output_builder_ptr> recordable_state_builder_ = std::nullopt,
                                   graph_builder_ptr                 nested_graph_builder      = {},
                                   const std::unordered_map<std::string, int> &input_node_ids = {}, int output_node_id = -1);

        graph_builder_ptr                        nested_graph_builder;
        const std::unordered_map<std::string, int> input_node_ids;
        int                                      output_node_id;
    };

    struct NestedGraphNodeBuilder : BaseNestedGraphNodeBuilder
    {
        using BaseNestedGraphNodeBuilder::BaseNestedGraphNodeBuilder;

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;
    };

    struct ComponentNodeBuilder : BaseNestedGraphNodeBuilder
    {
        using BaseNestedGraphNodeBuilder::BaseNestedGraphNodeBuilder;

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;
    };

    struct TryExceptNodeBuilder : BaseNestedGraphNodeBuilder
    {
        using BaseNestedGraphNodeBuilder::BaseNestedGraphNodeBuilder;

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;
    };

    struct BaseSwitchNodeBuilder : BaseNodeBuilder
    {
        using BaseNodeBuilder::BaseNodeBuilder;
    };

    template<typename K> struct SwitchNodeBuilder : BaseSwitchNodeBuilder
    {
        using key_type = K;

        SwitchNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                          std::optional<input_builder_ptr>                                   input_builder_            = std::nullopt,
                          std::optional<output_builder_ptr>                                  output_builder_           = std::nullopt,
                          std::optional<output_builder_ptr>                                  error_builder_            = std::nullopt,
                          std::optional<output_builder_ptr>                                  recordable_state_builder_ = std::nullopt,
                          const std::unordered_map<K, graph_builder_ptr>                     &nested_graph_builders = {},
                          const std::unordered_map<K, std::unordered_map<std::string, int>> &input_node_ids = {},
                          const std::unordered_map<K, int>                                   &output_node_ids = {},
                          bool                                                               reload_on_ticked = false);

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;

        const std::unordered_map<K, graph_builder_ptr>                           nested_graph_builders;
        const std::unordered_map<K, std::unordered_map<std::string, int>> input_node_ids;
        const std::unordered_map<K, int>                                         output_node_ids;
        bool                                                                     reload_on_ticked;
    };

    struct BaseTsdNonAssociativeReduceNodeBuilder : BaseNodeBuilder
    {
        BaseTsdNonAssociativeReduceNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                                               std::optional<input_builder_ptr>  input_builder_            = std::nullopt,
                                               std::optional<output_builder_ptr> output_builder_           = std::nullopt,
                                               std::optional<output_builder_ptr> error_builder_            = std::nullopt,
                                               std::optional<output_builder_ptr> recordable_state_builder_ = std::nullopt,
                                               graph_builder_ptr                 nested_graph_builder      = {},
                                               const std::tuple<int64_t, int64_t> &input_node_ids = {},
                                               int64_t                            output_node_id   = -1);

        graph_builder_ptr            nested_graph_builder;
        std::tuple<int64_t, int64_t> input_node_ids;
        int64_t                      output_node_id;
    };

    struct TsdNonAssociativeReduceNodeBuilder : BaseTsdNonAssociativeReduceNodeBuilder
    {
        using BaseTsdNonAssociativeReduceNodeBuilder::BaseTsdNonAssociativeReduceNodeBuilder;

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;
    };

    struct BaseMeshNodeBuilder : BaseNodeBuilder
    {
        BaseMeshNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                            std::optional<input_builder_ptr>                input_builder_            = std::nullopt,
                            std::optional<output_builder_ptr>               output_builder_           = std::nullopt,
                            std::optional<output_builder_ptr>               error_builder_            = std::nullopt,
                            std::optional<output_builder_ptr>               recordable_state_builder_ = std::nullopt,
                            graph_builder_ptr                               nested_graph_builder      = {},
                            const std::unordered_map<std::string, int64_t> &input_node_ids = {}, int64_t output_node_id = -1,
                            const std::unordered_set<std::string> &multiplexed_args = {}, const std::string &key_arg = {},
                            const std::string &context_path = {});

        graph_builder_ptr                          nested_graph_builder;
        const std::unordered_map<std::string, int64_t> input_node_ids;
        int64_t                                        output_node_id;
        const std::unordered_set<std::string>      multiplexed_args;
        const std::string                          key_arg;
        const std::string                          context_path;
    };

    template <typename T> struct MeshNodeBuilder : BaseMeshNodeBuilder
    {
        using BaseMeshNodeBuilder::BaseMeshNodeBuilder;

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;
    };

    struct LastValuePullNodeBuilder : BaseNodeBuilder
    {
        using BaseNodeBuilder::BaseNodeBuilder;

        node_ptr make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const override;
    };

}  // namespace hgraph

#endif  // NODE_BUILDER_H

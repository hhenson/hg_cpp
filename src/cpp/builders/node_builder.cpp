
#include <hgraph/python/pyb_wiring.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>

#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/node_builder.h>
#include <hgraph/builders/output_builder.h>

#include <utility>

namespace hgraph
{

    NodeBuilder::NodeBuilder(node_signature_ptr signature_, nb::dict scalars_, std::optional<input_builder_ptr> input_builder_,
                             std::optional<output_builder_ptr> output_builder_, std::optional<output_builder_ptr> error_builder_,
                             std::optional<output_builder_ptr> recordable_state_builder_)
        : signature(std::move(signature_)), scalars(std::move(scalars_)), input_builder(std::move(input_builder_)),
          output_builder(std::move(output_builder_)), error_builder(std::move(error_builder_)),
          recordable_state_builder(std::move(recordable_state_builder_)) {}

    void NodeBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<NodeBuilder, Builder>(m, "NodeBuilder")
            .def("make_instance", &NodeBuilder::make_instance, "owning_graph_id"_a, "node_ndx"_a)
            .def("release_instance", &NodeBuilder::release_instance, "node"_a)
            .def_ro("signature", &NodeBuilder::signature)
            .def_ro("scalars", &NodeBuilder::scalars)
            .def_ro("input_builder", &NodeBuilder::input_builder)
            .def_ro("output_builder", &NodeBuilder::output_builder)
            .def_ro("error_builder", &NodeBuilder::error_builder)
            .def_ro("recordable_state_builder", &NodeBuilder::recordable_state_builder);

        nb::class_<BaseNodeBuilder, NodeBuilder>(m, "BaseNodeBuilder");

        nb::class_<PythonNodeBuilder, BaseNodeBuilder>(m, "PythonNodeBuilder")
            .def("__init__",
                 [](PythonNodeBuilder *self, nb::kwargs kwargs) {
                     auto                             signature_ = nb::cast<node_signature_ptr>(kwargs["signature"]);
                     auto                             scalars_   = nb::cast<nb::dict>(kwargs["scalars"]);
                     std::optional<input_builder_ptr> input_builder_ =
                         kwargs.contains("input_builder") ? nb::cast<std::optional<input_builder_ptr>>(kwargs["input_builder"])
                                                          : std::nullopt;
                     std::optional<output_builder_ptr> output_builder_ =
                         kwargs.contains("output_builder") ? nb::cast<std::optional<output_builder_ptr>>(kwargs["output_builder"])
                                                           : std::nullopt;
                     std::optional<output_builder_ptr> error_builder_ =
                         kwargs.contains("error_builder") ? nb::cast<std::optional<output_builder_ptr>>(kwargs["error_builder"])
                                                          : std::nullopt;
                     std::optional<output_builder_ptr> recordable_state_builder_ =
                         kwargs.contains("recordable_state_builder")
                             ? nb::cast<std::optional<output_builder_ptr>>(kwargs["recordable_state_builder"])
                             : std::nullopt;
                     auto eval_fn  = nb::cast<nb::callable>(kwargs["eval_fn"]);
                     auto start_fn = nb::cast<nb::callable>(kwargs["start_fn"]);
                     auto stop_fn  = nb::cast<nb::callable>(kwargs["stop_fn"]);
                     new (self) PythonNodeBuilder(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                                                  std::move(output_builder_), std::move(error_builder_),
                                                  std::move(recordable_state_builder_), std::move(eval_fn), std::move(start_fn),
                                                  std::move(stop_fn));
                 })
            .def_ro("eval_fn", &PythonNodeBuilder::eval_fn)
            .def_ro("start_fn", &PythonNodeBuilder::start_fn)
            .def_ro("stop_fn", &PythonNodeBuilder::stop_fn);

        nb::class_<PythonGeneratorNodeBuilder, BaseNodeBuilder>(m, "PythonGeneratorNodeBuilder")
            .def("__init__",
                 [](PythonGeneratorNodeBuilder *self, nb::kwargs kwargs) {
                     auto                             signature_ = nb::cast<node_signature_ptr>(kwargs["signature"]);
                     auto                             scalars_   = nb::cast<nb::dict>(kwargs["scalars"]);
                     std::optional<input_builder_ptr> input_builder_ =
                         kwargs.contains("input_builder") ? nb::cast<std::optional<input_builder_ptr>>(kwargs["input_builder"])
                                                          : std::nullopt;
                     std::optional<output_builder_ptr> output_builder_ =
                         kwargs.contains("output_builder") ? nb::cast<std::optional<output_builder_ptr>>(kwargs["output_builder"])
                                                           : std::nullopt;
                     std::optional<output_builder_ptr> error_builder_ =
                         kwargs.contains("error_builder") ? nb::cast<std::optional<output_builder_ptr>>(kwargs["error_builder"])
                                                          : std::nullopt;
                     std::optional<output_builder_ptr> recordable_state_builder_ =
                         kwargs.contains("recordable_state_builder")
                             ? nb::cast<std::optional<output_builder_ptr>>(kwargs["recordable_state_builder"])
                             : std::nullopt;
                     auto eval_fn = nb::cast<nb::callable>(kwargs["eval_fn"]);
                     new (self) PythonGeneratorNodeBuilder(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                                                           std::move(output_builder_), std::move(error_builder_),
                                                           std::move(recordable_state_builder_), std::move(eval_fn));
                 })
            .def_ro("eval_fn", &PythonGeneratorNodeBuilder::eval_fn);
    }

    void BaseNodeBuilder::_build_inputs_and_outputs(node_ptr node) {
        if (input_builder) {
            auto ts_input = (*input_builder)->make_instance(node);
            node->set_input(dynamic_cast_ref<TimeSeriesBundleInput>(ts_input));
        }

        if (output_builder) {
            auto ts_output = (*output_builder)->make_instance(node);
            node->set_output(ts_output);
        }

        if (error_builder) {
            auto ts_error_output = (*error_builder)->make_instance(node);
            node->set_error_output(ts_error_output);
        }

        if (recordable_state_builder) {
            auto ts_recordable_state = (*recordable_state_builder)->make_instance(node);
            node->set_recordable_state(dynamic_cast_ref<TimeSeriesBundleOutput>(ts_recordable_state));
        }
    }

    PythonNodeBuilder::PythonNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                                         std::optional<input_builder_ptr>  input_builder_,
                                         std::optional<output_builder_ptr> output_builder_,
                                         std::optional<output_builder_ptr> error_builder_,
                                         std::optional<output_builder_ptr> recordable_state_builder_, nb::callable eval_fn,
                                         nb::callable start_fn, nb::callable stop_fn)
        : BaseNodeBuilder(std::move(signature_), std::move(scalars_), std::move(input_builder_), std::move(output_builder_),
                          std::move(error_builder_), std::move(recordable_state_builder_)),
          eval_fn{std::move(eval_fn)}, start_fn{std::move(start_fn)}, stop_fn{std::move(stop_fn)} {}

    node_ptr PythonNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id, int node_ndx) {
        nb::ref<Node> node{new PythonNode{node_ndx, owning_graph_id, signature, scalars, eval_fn, start_fn, stop_fn}};

        _build_inputs_and_outputs(node);
        return node;
    }

    PythonGeneratorNodeBuilder::PythonGeneratorNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                                                           std::optional<input_builder_ptr>  input_builder_,
                                                           std::optional<output_builder_ptr> output_builder_,
                                                           std::optional<output_builder_ptr> error_builder_,
                                                           std::optional<output_builder_ptr> recordable_state_builder_,
                                                           nb::callable                      eval_fn)
        : BaseNodeBuilder(std::move(signature_), std::move(scalars_), std::move(input_builder_), std::move(output_builder_),
                          std::move(error_builder_), std::move(recordable_state_builder_)),
          eval_fn{std::move(eval_fn)} {}

    node_ptr PythonGeneratorNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id, int node_ndx) {
        nb::ref<Node> node{new PythonGeneratorNode{node_ndx, owning_graph_id, signature, scalars, eval_fn, {}, {}}};
        _build_inputs_and_outputs(node);
        return node;
    }

}  // namespace hgraph
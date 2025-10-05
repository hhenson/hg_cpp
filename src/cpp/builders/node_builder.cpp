
#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>

#include <hgraph/builders/graph_builder.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/node_builder.h>
#include <hgraph/builders/output_builder.h>

#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/nodes/reduce_node.h>

#include <utility>

namespace hgraph
{

    NodeBuilder::NodeBuilder(node_signature_ptr signature_, nb::dict scalars_, std::optional<input_builder_ptr> input_builder_,
                             std::optional<output_builder_ptr> output_builder_, std::optional<output_builder_ptr> error_builder_,
                             std::optional<output_builder_ptr> recordable_state_builder_)
        : signature(std::move(signature_)), scalars(std::move(scalars_)), input_builder(std::move(input_builder_)),
          output_builder(std::move(output_builder_)), error_builder(std::move(error_builder_)),
          recordable_state_builder(std::move(recordable_state_builder_)) {}

    template <typename T> auto create_tsd_map_node_builder(T *self, const nb::args &args) {
        // Expected Python signature (positional):
        // (signature, scalars, input_builder, output_builder, error_builder, recordable_state_builder,
        //  nested_graph, input_node_ids, output_node_id, multiplexed_args, key_arg)
        if (args.size() != 11) {
            throw nb::type_error("TsdMapNodeBuilder expects 11 positional arguments: "
                                 "(signature, scalars, input_builder, output_builder, error_builder, "
                                 "recordable_state_builder, nested_graph, input_node_ids, output_node_id, "
                                 "multiplexed_args, key_arg)");
        }

        auto signature_               = nb::cast<node_signature_ptr>(args[0]);
        auto scalars_                 = nb::cast<nb::dict>(args[1]);
        std::optional<input_builder_ptr> input_builder_ =
            args[2].is_none() ? std::nullopt : std::optional<input_builder_ptr>(nb::cast<input_builder_ptr>(args[2]));
        std::optional<output_builder_ptr> output_builder_ =
            args[3].is_none() ? std::nullopt : std::optional<output_builder_ptr>(nb::cast<output_builder_ptr>(args[3]));
        std::optional<output_builder_ptr> error_builder_ =
            args[4].is_none() ? std::nullopt : std::optional<output_builder_ptr>(nb::cast<output_builder_ptr>(args[4]));
        std::optional<output_builder_ptr> recordable_state_builder_ =
            args[5].is_none() ? std::nullopt : std::optional<output_builder_ptr>(nb::cast<output_builder_ptr>(args[5]));
        auto nested_graph_builder = nb::cast<graph_builder_ptr>(args[6]);
        auto input_node_ids = nb::cast<std::unordered_map<std::string, int64_t>>(args[7]);
        auto output_node_id = nb::cast<int64_t>(args[8]);
        auto multiplexed_args = nb::cast<std::unordered_set<std::string>>(args[9]);
        auto key_arg = nb::cast<std::string>(args[10]);

        return new (self) T(std::move(signature_), std::move(scalars_), std::move(input_builder_), std::move(output_builder_),
            std::move(error_builder_), std::move(recordable_state_builder_), std::move(nested_graph_builder),
            std::move(input_node_ids), std::move(output_node_id), std::move(multiplexed_args), std::move(key_arg));
    }

    template <typename T> auto create_reduce_node_builder(T *self, const nb::args &args) {
        // Expected Python signature (positional):
        // (signature, scalars, input_builder, output_builder, error_builder, recordable_state_builder,
        //  nested_graph, input_node_ids, output_node_id)
        if (args.size() != 9) {
            throw nb::type_error("ReduceNodeBuilder expects 9 positional arguments: "
                                 "(signature, scalars, input_builder, output_builder, error_builder, "
                                 "recordable_state_builder, nested_graph, input_node_ids, output_node_id)");
        }

        auto signature_               = nb::cast<node_signature_ptr>(args[0]);
        auto scalars_                 = nb::cast<nb::dict>(args[1]);
        std::optional<input_builder_ptr> input_builder_ =
            args[2].is_none() ? std::nullopt : std::optional<input_builder_ptr>(nb::cast<input_builder_ptr>(args[2]));
        std::optional<output_builder_ptr> output_builder_ =
            args[3].is_none() ? std::nullopt : std::optional<output_builder_ptr>(nb::cast<output_builder_ptr>(args[3]));
        std::optional<output_builder_ptr> error_builder_ =
            args[4].is_none() ? std::nullopt : std::optional<output_builder_ptr>(nb::cast<output_builder_ptr>(args[4]));
        std::optional<output_builder_ptr> recordable_state_builder_ =
            args[5].is_none() ? std::nullopt : std::optional<output_builder_ptr>(nb::cast<output_builder_ptr>(args[5]));
        auto nested_graph_builder = nb::cast<graph_builder_ptr>(args[6]);
        auto input_node_ids_tuple = nb::cast<std::tuple<int64_t, int64_t>>(args[7]);
        auto output_node_id = nb::cast<int64_t>(args[8]);

        return new (self) T(std::move(signature_), std::move(scalars_), std::move(input_builder_), std::move(output_builder_),
            std::move(error_builder_), std::move(recordable_state_builder_), std::move(nested_graph_builder),
            std::move(input_node_ids_tuple), std::move(output_node_id));
    }

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
                 [](PythonNodeBuilder *self, const nb::kwargs &kwargs) {
                     auto signature_ = nb::cast<node_signature_ptr>(kwargs["signature"]);
                     auto scalars_   = nb::cast<nb::dict>(kwargs["scalars"]);

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
                     auto eval_fn_  = nb::cast<nb::handle>(kwargs["eval_fn"]);
                     auto start_fn_ = nb::cast<nb::handle>(kwargs["start_fn"]);
                     auto stop_fn_  = nb::cast<nb::handle>(kwargs["stop_fn"]);

                     nb::callable eval_fn =
                         eval_fn_.is_valid() && !eval_fn_.is_none() ? nb::cast<nb::callable>(eval_fn_) : nb::callable{};
                     nb::callable start_fn =
                         start_fn_.is_valid() && !start_fn_.is_none() ? nb::cast<nb::callable>(start_fn_) : nb::callable{};
                     nb::callable stop_fn =
                         stop_fn_.is_valid() && !stop_fn_.is_none() ? nb::cast<nb::callable>(stop_fn_) : nb::callable{};

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
                 [](PythonGeneratorNodeBuilder *self, const nb::kwargs &kwargs) {
                     auto signature_obj = kwargs["signature"];
                     auto signature_    = nb::cast<node_signature_ptr>(signature_obj);
                     auto scalars_      = nb::cast<nb::dict>(kwargs["scalars"]);

                     std::optional<input_builder_ptr> input_builder_ =
                         kwargs.contains("input_builder") ? nb::cast<std::optional<input_builder_ptr>>(kwargs["input_builder"])
                                                          : std::nullopt;
                     std::optional<output_builder_ptr> output_builder_ =
                         kwargs.contains("output_builder") ? nb::cast<std::optional<output_builder_ptr>>(kwargs["output_builder"])
                                                           : std::nullopt;
                     std::optional<output_builder_ptr> error_builder_ =
                         kwargs.contains("error_builder") ? nb::cast<std::optional<output_builder_ptr>>(kwargs["error_builder"])
                                                          : std::nullopt;
                     auto eval_fn = nb::cast<nb::callable>(kwargs["eval_fn"]);
                     new (self)
                         PythonGeneratorNodeBuilder(std::move(signature_), std::move(scalars_), std::move(input_builder_),
                                                    std::move(output_builder_), std::move(error_builder_), std::move(eval_fn));
                 })
            .def_ro("eval_fn", &PythonGeneratorNodeBuilder::eval_fn);

        nb::class_<BaseTsdMapNodeBuilder, BaseNodeBuilder>(m, "BaseTsdMapNodeBuilder")
            .def_ro("nested_graph_builder", &BaseTsdMapNodeBuilder::nested_graph_builder)
            .def_ro("input_node_ids", &BaseTsdMapNodeBuilder::input_node_ids)
            .def_ro("output_node_id", &BaseTsdMapNodeBuilder::output_node_id)
            .def_ro("multiplexed_args", &BaseTsdMapNodeBuilder::multiplexed_args)
            .def_ro("key_arg", &BaseTsdMapNodeBuilder::key_arg);

        nb::class_<TsdMapNodeBuilder<bool>, BaseTsdMapNodeBuilder>(m, "TsdMapNodeBuilder_bool")
            .def("__init__", [](TsdMapNodeBuilder<bool> *self, const nb::args &args) { create_tsd_map_node_builder(self, args); });

        nb::class_<TsdMapNodeBuilder<int64_t>, BaseTsdMapNodeBuilder>(m, "TsdMapNodeBuilder_int")
            .def("__init__", [](TsdMapNodeBuilder<int64_t> *self, const nb::args &args) { create_tsd_map_node_builder(self, args); });

        nb::class_<TsdMapNodeBuilder<double>, BaseTsdMapNodeBuilder>(m, "TsdMapNodeBuilder_float")
            .def("__init__", [](TsdMapNodeBuilder<double> *self, const nb::args &args) { create_tsd_map_node_builder(self, args); });

        nb::class_<TsdMapNodeBuilder<engine_date_t>, BaseTsdMapNodeBuilder>(m, "TsdMapNodeBuilder_date")
            .def("__init__", [](TsdMapNodeBuilder<engine_date_t> *self, const nb::args &args) { create_tsd_map_node_builder(self, args); });

        nb::class_<TsdMapNodeBuilder<engine_time_t>, BaseTsdMapNodeBuilder>(m, "TsdMapNodeBuilder_date_time")
            .def("__init__", [](TsdMapNodeBuilder<engine_time_t> *self, const nb::args &args) { create_tsd_map_node_builder(self, args); });

        nb::class_<TsdMapNodeBuilder<engine_time_delta_t>, BaseTsdMapNodeBuilder>(m, "TsdMapNodeBuilder_time_delta")
            .def("__init__", [](TsdMapNodeBuilder<engine_time_delta_t> *self, const nb::args &args) { create_tsd_map_node_builder(self, args); });

        nb::class_<TsdMapNodeBuilder<nb::object>, BaseTsdMapNodeBuilder>(m, "TsdMapNodeBuilder_object")
            .def("__init__", [](TsdMapNodeBuilder<nb::object> *self, const nb::args &args) { create_tsd_map_node_builder(self, args); });

        nb::class_<BaseReduceNodeBuilder, BaseNodeBuilder>(m, "BaseReduceNodeBuilder")
            .def_ro("nested_graph_builder", &BaseReduceNodeBuilder::nested_graph_builder)
            .def_ro("input_node_ids", &BaseReduceNodeBuilder::input_node_ids)
            .def_ro("output_node_id", &BaseReduceNodeBuilder::output_node_id);

        nb::class_<ReduceNodeBuilder<bool>, BaseReduceNodeBuilder>(m, "ReduceNodeBuilder_bool")
            .def("__init__", [](ReduceNodeBuilder<bool> *self, const nb::args &args) { create_reduce_node_builder(self, args); });

        nb::class_<ReduceNodeBuilder<int64_t>, BaseReduceNodeBuilder>(m, "ReduceNodeBuilder_int")
            .def("__init__", [](ReduceNodeBuilder<int64_t> *self, const nb::args &args) { create_reduce_node_builder(self, args); });

        nb::class_<ReduceNodeBuilder<double>, BaseReduceNodeBuilder>(m, "ReduceNodeBuilder_float")
            .def("__init__", [](ReduceNodeBuilder<double> *self, const nb::args &args) { create_reduce_node_builder(self, args); });

        nb::class_<ReduceNodeBuilder<engine_date_t>, BaseReduceNodeBuilder>(m, "ReduceNodeBuilder_date")
            .def("__init__", [](ReduceNodeBuilder<engine_date_t> *self, const nb::args &args) { create_reduce_node_builder(self, args); });

        nb::class_<ReduceNodeBuilder<engine_time_t>, BaseReduceNodeBuilder>(m, "ReduceNodeBuilder_date_time")
            .def("__init__", [](ReduceNodeBuilder<engine_time_t> *self, const nb::args &args) { create_reduce_node_builder(self, args); });

        nb::class_<ReduceNodeBuilder<engine_time_delta_t>, BaseReduceNodeBuilder>(m, "ReduceNodeBuilder_time_delta")
            .def("__init__", [](ReduceNodeBuilder<engine_time_delta_t> *self, const nb::args &args) { create_reduce_node_builder(self, args); });

        nb::class_<ReduceNodeBuilder<nb::object>, BaseReduceNodeBuilder>(m, "ReduceNodeBuilder_object")
            .def("__init__", [](ReduceNodeBuilder<nb::object> *self, const nb::args &args) { create_reduce_node_builder(self, args); });
    }

    void BaseNodeBuilder::_build_inputs_and_outputs(node_ptr node) const {
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

    node_ptr PythonNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const {
        nb::ref<Node> node{new PythonNode{node_ndx, owning_graph_id, signature, scalars, eval_fn, start_fn, stop_fn}};

        _build_inputs_and_outputs(node);
        return node;
    }

    PythonGeneratorNodeBuilder::PythonGeneratorNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                                                           std::optional<input_builder_ptr>  input_builder_,
                                                           std::optional<output_builder_ptr> output_builder_,
                                                           std::optional<output_builder_ptr> error_builder_, nb::callable eval_fn)
        : BaseNodeBuilder(std::move(signature_), std::move(scalars_), std::move(input_builder_), std::move(output_builder_),
                          std::move(error_builder_), std::nullopt),
          eval_fn{std::move(eval_fn)} {}

    node_ptr PythonGeneratorNodeBuilder::make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const {
        nb::ref<Node> node{new PythonGeneratorNode{node_ndx, owning_graph_id, signature, scalars, eval_fn, {}, {}}};
        _build_inputs_and_outputs(node);
        return node;
    }

    BaseTsdMapNodeBuilder::BaseTsdMapNodeBuilder(
        node_signature_ptr signature_, nb::dict scalars_, std::optional<input_builder_ptr> input_builder_,
        std::optional<output_builder_ptr> output_builder_, std::optional<output_builder_ptr> error_builder_,
        std::optional<output_builder_ptr> recordable_state_builder_, graph_builder_ptr nested_graph_builder,
        const std::unordered_map<std::string, int64_t> &input_node_ids, int64_t output_node_id,
        const std::unordered_set<std::string> &multiplexed_args, const std::string &key_arg)
        : BaseNodeBuilder(std::move(signature_), std::move(scalars_), std::move(input_builder_), std::move(output_builder_),
                          std::move(error_builder_), std::move(recordable_state_builder_)),
          nested_graph_builder(std::move(nested_graph_builder)), input_node_ids(input_node_ids), output_node_id(output_node_id),
          multiplexed_args(multiplexed_args), key_arg(key_arg) {}

    template <typename T>
    node_ptr TsdMapNodeBuilder<T>::make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const {
        nb::ref<Node> node{new TsdMapNode<T>(node_ndx, owning_graph_id, signature, scalars, nested_graph_builder, input_node_ids,
                                             output_node_id, multiplexed_args, key_arg)};
        _build_inputs_and_outputs(node);
        return node;
    }

    BaseReduceNodeBuilder::BaseReduceNodeBuilder(node_signature_ptr signature_, nb::dict scalars_,
                                                 std::optional<input_builder_ptr>  input_builder_,
                                                 std::optional<output_builder_ptr> output_builder_,
                                                 std::optional<output_builder_ptr> error_builder_,
                                                 std::optional<output_builder_ptr> recordable_state_builder_,
                                                 graph_builder_ptr                 nested_graph_builder,
                                                 const std::tuple<int64_t, int64_t> &input_node_ids, int64_t output_node_id)
        : BaseNodeBuilder(std::move(signature_), std::move(scalars_), std::move(input_builder_), std::move(output_builder_),
                          std::move(error_builder_), std::move(recordable_state_builder_)),
          nested_graph_builder(std::move(nested_graph_builder)), input_node_ids(input_node_ids), output_node_id(output_node_id) {}

    template <typename T>
    node_ptr ReduceNodeBuilder<T>::make_instance(const std::vector<int64_t> &owning_graph_id, int64_t node_ndx) const {
        nb::ref<Node> node{new ReduceNode<T>(node_ndx, owning_graph_id, signature, scalars, nested_graph_builder, input_node_ids,
                                             output_node_id)};
        _build_inputs_and_outputs(node);
        return node;
    }

    // Explicit template instantiations
    template class ReduceNodeBuilder<bool>;
    template class ReduceNodeBuilder<int64_t>;
    template class ReduceNodeBuilder<double>;
    template class ReduceNodeBuilder<engine_date_t>;
    template class ReduceNodeBuilder<engine_time_t>;
    template class ReduceNodeBuilder<engine_time_delta_t>;
    template class ReduceNodeBuilder<nb::object>;

}  // namespace hgraph
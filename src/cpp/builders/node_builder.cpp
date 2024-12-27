

#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>

#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/node_builder.h>
#include <hgraph/builders/output_builder.h>

namespace hgraph
{

    NodeBuilder::NodeBuilder(node_signature_ptr signature_, nb::dict scalars_, std::optional<input_builder_ptr> input_builder_,
                             std::optional<output_builder_ptr> output_builder_, std::optional<output_builder_ptr> error_builder_,
                             std::optional<output_builder_ptr> recordable_state_builder_)
        : signature(std::move(signature_)), scalars(std::move(scalars_)), input_builder(std::move(input_builder_)),
          output_builder(std::move(output_builder_)), error_builder(std::move(error_builder_)),
          recordable_state_builder(std::move(recordable_state_builder_)) {}

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

}  // namespace hgraph
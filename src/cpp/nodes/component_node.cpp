#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/component_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph
{
    ComponentNode::ComponentNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
                                 nb::dict scalars, graph_builder_ptr nested_graph_builder,
                                 const std::unordered_map<std::string, int> &input_node_ids, int output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars)),
          m_nested_graph_builder_(std::move(nested_graph_builder)), m_input_node_ids_(input_node_ids),
          m_output_node_id_(output_node_id), m_active_graph_(nullptr) {}

    void ComponentNode::wire_graph() {
        // TODO: Implement wiring
    }

    void ComponentNode::write_inputs() {
        // TODO: Implement
    }

    void ComponentNode::wire_outputs() {
        if (m_output_node_id_) {
            auto node = m_active_graph_->nodes()[m_output_node_id_];
            node->set_output(&output());
        }
    }

    void ComponentNode::initialise() {
        std::string id_ = signature().name;

        m_active_graph_ = m_nested_graph_builder_->make_instance(node_id(), this, id_);
        m_active_graph_->traits().set_trait(RECORDABLE_ID_TRAIT, nb::cast(id_));
        m_active_graph_->set_evaluation_engine(new NestedEvaluationEngine(
            &graph().evaluation_engine(), new NestedEngineEvaluationClock(&graph().evaluation_engine_clock(), this)));

        initialise_component(*m_active_graph_);
        wire_graph();
    }

    void ComponentNode::do_start() {
        if (m_active_graph_ != nullptr) {
            start_component(*m_active_graph_);
        }
    }

    void ComponentNode::do_stop() {
        if (m_active_graph_ != nullptr) {
            stop_component(*m_active_graph_);
        }
    }

    void ComponentNode::dispose() {
        if (m_active_graph_ != nullptr) {
            dispose_component(*m_active_graph_);
            m_active_graph_ = nullptr;
        }
    }

    void ComponentNode::do_eval() {
        mark_evaluated();
        reinterpret_cast<NestedEngineEvaluationClock &>(m_active_graph_->evaluation_engine_clock())
            .reset_next_scheduled_evaluation_time();
        m_active_graph_->evaluate_graph();
        reinterpret_cast<NestedEngineEvaluationClock &>(m_active_graph_->evaluation_engine_clock())
            .reset_next_scheduled_evaluation_time();
    }

    std::unordered_map<int, graph_ptr> ComponentNode::nested_graphs() const {
        return m_active_graph_ ? std::unordered_map<int, graph_ptr>{{0, m_active_graph_}}
                              : std::unordered_map<int, graph_ptr>();
    }

    void ComponentNode::register_with_nanobind(nb::module_ &m) {
        nb::class_<ComponentNode, NestedNode>(m, "ComponentNode")
            .def_prop_ro("active_graph", [](ComponentNode &self) { return self.m_active_graph_; })
            .def_prop_ro("nested_graphs", &ComponentNode::nested_graphs);
    }

}  // namespace hgraph

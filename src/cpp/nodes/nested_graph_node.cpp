#include <hgraph/nodes/nest_graph_node.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/traits.h>

#include <utility>

namespace hgraph
{

    NestedGraphNode::NestedGraphNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
                                     nb::dict scalars, graph_builder_ptr nested_graph_builder,
                                     const std::unordered_map<std::string, int> &input_node_ids, int output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars)),
          m_nested_graph_builder_(std::move(nested_graph_builder)), m_input_node_ids_(input_node_ids),
          m_output_node_id_(output_node_id), m_active_graph_(nullptr) {}

    void NestedGraphNode::wire_graph() {
        write_inputs();
        wire_outputs();
    }

    void NestedGraphNode::write_inputs() {
        if (!m_input_node_ids_.empty()) {
            for (const auto &[arg, node_ndx] : m_input_node_ids_) {
                auto node = m_active_graph_->nodes()[node_ndx];
                node->notify();
                auto ts = input(arg);
                node->set_input(node->input().copy_with(node, ts));
                ts->re_parent(node->input());
            }
        }
    }

    void NestedGraphNode::wire_outputs() {
        if (m_output_node_id_) {
            auto node = m_active_graph_->nodes()[m_output_node_id_];
            node->set_output(output());
        }
    }

    void NestedGraphNode::initialise() {
        m_active_graph_ = m_nested_graph_builder_->make_instance(node_id(), this, signature()->name());
        m_active_graph_->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
            graph()->evaluation_engine(), std::make_shared<NestedEngineEvaluationClock>(graph()->engine_evaluation_clock(), this)));
        m_active_graph_->initialise();
        wire_graph();
    }

    void NestedGraphNode::start() { m_active_graph_->start(); }

    void NestedGraphNode::stop() { m_active_graph_->stop(); }

    void NestedGraphNode::dispose() {
        m_active_graph_->dispose();
        m_active_graph_ = nullptr;
    }

    void NestedGraphNode::do_eval() {
        mark_evaluated();
        m_active_graph_->evaluation_engine_clock().reset_next_scheduled_evaluation_time();
        m_active_graph_->evaluate_graph();
        m_active_graph_->evaluation_clock().reset_next_scheduled_evaluation_time();
    }

    std::unordered_map<int, graph_ptr> NestedGraphNode::nested_graphs() {
        return m_active_graph_ ? std::unordered_map<int, graph_ptr>{{0, m_active_graph_}} : std::unordered_map<int, graph_ptr>();
    }
}  // namespace hgraph
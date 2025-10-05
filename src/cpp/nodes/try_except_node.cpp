#include <hgraph/nodes/try_except_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsb.h>

namespace hgraph
{
    void TryExceptNode::wire_outputs() {
        if (m_output_node_id_) {
            auto node = m_active_graph_->nodes()[m_output_node_id_];
            // Simplified - just wire directly
            node->set_output(&output());
        }
    }

    void TryExceptNode::do_eval() {
        mark_evaluated();

        try {
            reinterpret_cast<NestedEngineEvaluationClock &>(m_active_graph_->evaluation_engine_clock())
                .reset_next_scheduled_evaluation_time();
            m_active_graph_->evaluate_graph();
            reinterpret_cast<NestedEngineEvaluationClock &>(m_active_graph_->evaluation_engine_clock())
                .reset_next_scheduled_evaluation_time();
        } catch (const std::exception &e) {
            // TODO: Implement error capture and wiring to exception output
            // For now, just rethrow
            throw;
        }
    }

    void TryExceptNode::register_with_nanobind(nb::module_ &m) {
        nb::class_<TryExceptNode, NestedGraphNode>(m, "TryExceptNode");
    }
}  // namespace hgraph

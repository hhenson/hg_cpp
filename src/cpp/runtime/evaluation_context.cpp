#include <hgraph/runtime/evaluation_context.h>

#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>

namespace hgraph
{

    EvaluationContext::EvaluationContext(EvaluationClock *evaluation_clock, Graph *graph)
        : _evaluation_clock{evaluation_clock}, _graph{graph}, _node{nullptr} {}

    EvaluationClock &EvaluationContext::evaluation_clock() const { return *_evaluation_clock; }
    Graph           &EvaluationContext::graph() const { return *_graph; }
    Node            &EvaluationContext::node() const { return *_node; }
    void             EvaluationContext::set_node(Node *node) { _node = node; }

}  // namespace hgraph

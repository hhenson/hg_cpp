
#include <hgraph/nodes/nested_node.h>
#include <hgraph/types/graph.h>

namespace hgraph
{

    engine_time_t NestedNode::last_evaluation_time() const { return _last_evaluation_time; }

    void NestedNode::mark_evaluated() { _last_evaluation_time = graph().evaluation_clock().evaluation_time(); }

}  // namespace hgraph
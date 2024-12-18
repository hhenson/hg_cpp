#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>

#include <utility>

namespace hgraph
{

    Graph::Graph(std::vector<int64_t> graph_id_, std::vector<Node::ptr> nodes_, std::optional<Node::ptr> parent_node_,
                 std::optional<std::string> label_, traits_ptr traits_)
        : ComponentLifeCycle(), _graph_id{std::move(graph_id_)}, _nodes{std::move(nodes_)}, _parent_node{std::move(parent_node_)},
          _label{std::move(label_)}, _traits{traits_} {
        auto it{std::find_if(nodes_.begin(), nodes_.end(),
                             [](const Node *v) { return v->signature().node_type != NodeTypeEnum::PUSH_SOURCE_NODE; })};
        _push_source_nodes_end = std::distance(_nodes.begin(), it);
        _schedule.resize(_nodes.size(), MIN_DT);
    }
    const std::vector<int64_t>  &Graph::graph_id() const { return _graph_id; }

    const std::vector<node_ptr> &Graph::nodes() const { return _nodes; }

    std::optional<node_ptr>    Graph::parent_node() const { return _parent_node; }

    std::optional<std::string> Graph::label() const {return _label; }

    EvaluationEngineApi &Graph::evaluation_engine_api() const { return *_evaluation_engine; }

    EvaluationClock &Graph::evaluation_clock() const { return _evaluation_engine->engine_evaluation_clock(); }

    EngineEvaluationClock &Graph::evaluation_engine_clock() { return _evaluation_engine->engine_evaluation_clock(); }

    EvaluationEngine &Graph::evaluation_engine() const { return *_evaluation_engine; }

    void Graph::set_evaluation_engine(EvaluationEngine::ptr value) { _evaluation_engine = value; }

    int64_t Graph::push_source_nodes_end() const { return _push_source_nodes_end; }

    void Graph::schedule_node(int64_t node_ndx, engine_time_t when) {}

    std::vector<engine_time_t> &Graph::schedule() { return _schedule; }

    void Graph::evaluate_graph() {}

    std::unique_ptr<Graph> Graph::copy_with(std::vector<Node::ptr> nodes) {}

    const Traits &Graph::traits() const { return *_traits; }

}  // namespace hgraph

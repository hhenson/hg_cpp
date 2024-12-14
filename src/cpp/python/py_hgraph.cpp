#include <hgraph/python/py_hgraph.h>

namespace hgraph
{
    namespace detail
    {
        engine_time_t EvaluationClock::evaluation_time() const { return nb::cast<engine_time_t>(attr("evaluation_time")()); }

        EvaluationClock::ptr Graph::evaluation_clock() const { return EvaluationClock(attr("evaluation_clock")); }

        Graph::ptr Node::owning_graph() { return Graph(attr("owning_graph")); }

        void Node::notify(engine_time_t modified_time) { attr("notify")(modified_time); }

    }  // namespace detail
}  // namespace hgraph
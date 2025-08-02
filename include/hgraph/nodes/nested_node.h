

#ifndef NESTED_NODE_H
#define NESTED_NODE_H

#include <hgraph/types/node.h>
#include <chrono>
#include <memory>
#include <vector>

namespace hgraph
{
    struct NestedNode :  Node
    {
        using ptr = nb::ref<NestedNode>;
        using Node::Node;

        engine_time_t last_evaluation_time() const;
        void          mark_evaluated();

      private:
        engine_time_t _last_evaluation_time {MIN_DT};
    };

    
}  // namespace hgraph

#endif  // NESTED_NODE_H

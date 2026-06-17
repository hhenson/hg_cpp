#ifndef HGRAPH_RUNTIME_NODE_FWD_H
#define HGRAPH_RUNTIME_NODE_FWD_H

#include <hgraph/types/metadata/type_binding.h>

namespace hgraph
{
    class NodeView;
    struct NodeOps;
    struct NodeTypeMetaData;

    using NodeTypeBinding = TypeBinding<NodeTypeMetaData, NodeOps>;
    using NodeStorageRef  = MemoryUtils::StorageRef<NodeTypeBinding>;
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_NODE_FWD_H

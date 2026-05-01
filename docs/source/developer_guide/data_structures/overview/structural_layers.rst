Structural Layers
=================

The runtime has five major structural layers:

``GraphExecutor``
    Owns or coordinates a complete graph run. It constructs the runtime graph, installs observers, starts and stops lifecycle processing, and drives the evaluation loop.

``EvaluationEngine``
    Provides the clock, run mode, lifecycle observer dispatch, one-shot evaluation callbacks, stop state, and optional push-message receiver state. Conceptually, this is the engine that evaluates a graph.

``Graph``
    Holds the flattened runtime node set, the graph schedule table, graph identity, boundary bindings, push-source partition, and links to parent or nested graph context.

``Node``
    Holds node identity, runtime operations, lifecycle state, time-series inputs and outputs, optional node-local scheduler, and optional node-local state.

``TimeSeries``
    Holds value validity, modification time, parent/child relationships, subscribers, structural delta state, and type-specific value storage.

The high-level relationship is:

.. mermaid::

   flowchart TD
      Executor[GraphExecutor]
      Engine[EvaluationEngine]
      Clock[EvaluationClock]
      Observers[Lifecycle Observers]
      Callbacks[Before/After Callback Queues]
      Receiver[Push Message Receiver]
      Graph[Graph]
      Schedule[Graph Schedule Table]
      Nodes[Flattened Node Array]
      Boundaries[Graph Boundaries]
      Node[Node]
      NodeScheduler[Optional Node Scheduler]
      Inputs[Inputs]
      Outputs[Outputs]
      State[Node State]
      TS[TimeSeries Tree]
      Value[Value Storage]

      Executor --> Engine
      Executor --> Graph
      Engine --> Clock
      Engine --> Observers
      Engine --> Callbacks
      Engine --> Receiver
      Graph --> Schedule
      Graph --> Nodes
      Graph --> Boundaries
      Nodes --> Node
      Node --> NodeScheduler
      Node --> Inputs
      Node --> Outputs
      Node --> State
      Inputs --> TS
      Outputs --> TS
      TS --> Value
      Receiver -.push source node index + message.-> Graph
      NodeScheduler -.earliest event.-> Schedule
      Schedule -.scheduled time + node index.-> Nodes

The Execution Layer above ``Graph`` is described in its own page (see
*Execution Layer*). The remaining structural layers — Graph, Node, and
the scheduling primitives — are expanded below. The ``TimeSeries`` layer
contents are described in *Schemas* (kinds, tick semantics) and
*Allocation, Plans and Ops* (memory layout, slot stores).

Graph Layer
~~~~~~~~~~~

The graph layer is the hot path for evaluation. It should be compact and directly indexed.

Core elements:

``GraphIdentity``
    Stable graph id, optional label, and optional parent-node link for nested graphs.

``FlattenedNodeArray``
    The fully ordered node set. The array order is rank order and is the evaluation order.

``GraphScheduleTable``
    Parallel array indexed by node index. Each entry is the next visible scheduled time for that node.

``PushSourcePartition``
    Index boundary that separates push-source nodes from the normal rank-ordered scan. Push-source nodes occupy the prefix ``[0, push_source_nodes_end)``.

``BoundaryBindings``
    Runtime bindings for graph inputs, outputs, nested graph boundaries, and parent/child graph communication.

``GraphStorage``
    Memory owned by the graph instance. This may eventually be an arena or packed allocation that contains nodes, node-local schedulers, node-local state, and time-series objects.

The graph's most important paired structure is:

.. code-block:: text

   FlattenedNodeArray[index] -> Node
   GraphScheduleTable[index] -> scheduled_time

Together these form the scheduled time-node relationship used by evaluation. The schedule table does not own scheduler events; it only exposes the next time each node may need to run.

Node Layer
~~~~~~~~~~

Each node is a runtime object in the flattened graph. The node object should be small enough to scan efficiently and should point to heavier state only when needed.

Core elements:

``NodeHeader``
    Node index, rank position, owning graph pointer, lifecycle state, and schema/runtime metadata needed for dispatch.

``RuntimeOps``
    Function table or equivalent dispatch object for start, stop, evaluate, dispose, and error handling.

``InputSet``
    Ordered inputs used by readiness checks and evaluation dispatch. Inputs may be active subscribers, passive samples, or context inputs.

``OutputSet``
    Ordered outputs produced by the node. Outputs propagate modification notifications to subscribers.

``NodeState``
    Optional node-local state. This is present only for nodes whose schema or implementation requires state.

``NodeScheduler``
    Optional node-local scheduler. This is present only when the schema declares that the node requires scheduling.

``ErrorOutput``
    Optional output used to capture evaluation errors when the schema provides one.

The optional elements are important. A node that does not require a scheduler must not allocate scheduler state. A stateless node must not pay for state storage.

Scheduling Structures
~~~~~~~~~~~~~~~~~~~~~

Scheduling uses two layers:

``GraphScheduleEntry``
    A single ``scheduled_time`` stored in the graph schedule table at ``node_index``.

``NodeScheduledEvent``
    A node-local event containing ``scheduled_time`` and optional ``tag``. Real-time scheduling may also be backed by a wall-clock alarm handle.

``NodeScheduler``
    A per-node structure that stores multiple ``NodeScheduledEvent`` values, supports tag replacement and cancellation, and exposes the earliest pending event to the graph schedule table.

The intended relationship is:

.. mermaid::

   flowchart LR
      Event1[NodeScheduledEvent time=t1 tag=a]
      Event2[NodeScheduledEvent time=t2 tag=b]
      Event3[NodeScheduledEvent time=t3]
      NodeScheduler[NodeScheduler]
      GraphEntry[GraphScheduleTable node_index = min time]
      Clock[EvaluationClock next scheduled time]

      Event1 --> NodeScheduler
      Event2 --> NodeScheduler
      Event3 --> NodeScheduler
      NodeScheduler --> GraphEntry
      GraphEntry --> Clock

The node scheduler owns the complete set of pending events. The graph schedule table owns only the next visible time for the node.

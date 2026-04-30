Runtime Architecture
====================

The C++ runtime should be organized around stable ownership boundaries:

- graph definition and wiring artifacts,
- runtime graph instances,
- node instances,
- time-series storage,
- scheduler and evaluation clock,
- lifecycle hooks,
- optional Python integration.

Clock And Execution Mode
------------------------

HGraph is a time-series processor, so time is part of the runtime contract rather than an incidental utility. All HGraph runtime code must use the HGraph clock abstraction. Runtime code must not read process wall-clock time directly except inside the clock implementation.

The clock exposes three related time concepts:

``evaluation_time``
    The activation time for the current evaluation cycle. This is the event time that caused the graph to run. It can also be thought of as the graph's current logical time.

``now``
    The runtime's current approximation of "now".

    In ``REAL_TIME`` mode, ``now`` is derived from the computer clock in UTC.

    In ``SIMULATION`` mode, ``now`` is an implementation-specific approximation of what ``now`` could have been in real-time mode if the event had triggered at wall-clock time. The implementation should preserve that intuition, but the exact mechanism is not part of the semantic contract.

``cycle_time``
    The elapsed time from the beginning of the current evaluation cycle to the current point in that cycle. This represents processing lag within the cycle.

Execution Modes
~~~~~~~~~~~~~~~

The engine has two primary execution modes: ``REAL_TIME`` and ``SIMULATION``.

In ``SIMULATION`` mode, time is compressed. The engine does not wait between scheduled events. It processes events as quickly as possible while preserving event-time ordering.

In ``REAL_TIME`` mode, the engine attempts to align event processing with wall-clock time. If an event is scheduled for the future, the engine waits until wall-clock time reaches that event time. If the engine is already behind, it evaluates immediately. Waiting would only increase the lag.

The engine does not skip scheduled events. If event coalescing or collapsing is required, that behavior belongs to a source node. A collapsing source node may choose to combine external events before introducing them into the runtime, but that is source-node behavior, not scheduler behavior.

Scheduling Semantics
~~~~~~~~~~~~~~~~~~~~

Scheduling is time based. All events scheduled for the same time are evaluated at the same ``evaluation_time``.

``evaluation_time`` is monotonic non-decreasing across evaluation cycles. Runtime time must not move backwards.

Events are normally introduced through source nodes, but any node may schedule itself for future evaluation. Self-scheduling is a performance optimization that avoids creating stub source nodes only to represent timers.

Graph Flattening And Order
~~~~~~~~~~~~~~~~~~~~~~~~~~

Wiring produces a graph structure, but runtime evaluation operates over a flattened node set. The flattened set is fully ordered in rank order. That order is the evaluation order for the graph.

The order is part of the runtime invariant. During a cycle, a node may cause later-ranked nodes to become scheduled at the current ``evaluation_time``. That is valid because the later node has not yet been reached by the scan. A node must not schedule an earlier-ranked node for the current ``evaluation_time`` after that earlier node has already been passed. The graph is constructed so this cannot happen; if it does happen, the runtime should treat it as an invalid graph or scheduler bug rather than silently deferring or skipping work.

Graph Scheduler
~~~~~~~~~~~~~~~

The graph owns the schedule table used by the evaluation loop. This table stores at most one visible scheduled time per flattened node: the next time that node may need evaluation.

The graph scheduler follows these rules:

- scheduling before the current ``evaluation_time`` is invalid,
- scheduling at the current ``evaluation_time`` means "evaluate during the current cycle" if the node has not yet been reached,
- a node schedule is replaced when the existing time is already current or consumed,
- a node schedule is replaced when the new time is earlier than the existing time,
- an explicit force operation may replace the visible time when a node-local scheduler changes its earliest event,
- each accepted future time is folded into the clock's next scheduled evaluation time.

The clock must not advance to a same-cycle schedule entry. When it tracks the next scheduled evaluation time, it ignores ``evaluation_time`` and clamps advancement to at least ``next_cycle_evaluation_time``.

Evaluation Cycle
~~~~~~~~~~~~~~~~

Each evaluation cycle runs at one ``evaluation_time``.

The normal cycle shape is:

1. run one-shot before-evaluation callbacks,
2. notify lifecycle observers before graph evaluation,
3. process push-source messages that are ready for this cycle,
4. scan flattened nodes in rank order,
5. evaluate nodes whose graph schedule equals ``evaluation_time``,
6. fold future node schedules into the clock's next scheduled evaluation time,
7. notify lifecycle observers after graph evaluation,
8. run one-shot after-evaluation callbacks,
9. advance the engine clock.

Push-source processing happens before the normal rank-order scan. If a push-source message cannot be applied, the message is returned to the front of the queue and push-source scheduling is requested again. The engine does not coalesce those messages; any collapsing behavior belongs to the source node implementation.

Node Evaluation
~~~~~~~~~~~~~~~

A scheduled node still has to satisfy its node-level readiness rules. The node must have the required valid inputs, and nodes that require all-valid input state must enforce that before invoking user code or system-node code.

A node that uses a scheduler may evaluate because its scheduler fired at the current ``evaluation_time`` or because a time-series input was modified. If neither is true, the node should not pay evaluation cost for a scheduler merely being present.

Errors raised during evaluation should be surfaced deterministically. If the node schema provides an error output, the node may capture the error there. Otherwise, the exception is propagated as an evaluation failure.

Node Scheduler
~~~~~~~~~~~~~~

The per-node scheduler remains part of the runtime contract. It supports multiple pending events, optional tags, replacement by tag, cancellation, and advancement after due events have fired.

The scheduler is optional at runtime. Schema construction decides whether a node requires a scheduler. Nodes that do not require scheduling must not allocate or carry scheduler state.

The graph scheduler only needs the node scheduler's earliest pending event. After node evaluation, the node scheduler consumes all events due at or before ``evaluation_time`` and registers the next future event with the graph scheduler.

The C++ API must preserve the scheduling features currently exposed by the Python API, including real-time scheduling concepts. In real-time mode, a node scheduler may register wall-clock alarms that later materialize as node-local scheduled events. In simulation mode, relative and absolute event-time scheduling remain compressed and deterministic.

Notification Propagation
~~~~~~~~~~~~~~~~~~~~~~~~

Outputs track their last modification time. An output is modified for the current cycle when its last modification time equals ``evaluation_time``.

Marking an output modified propagates modification state through parent outputs and notifies active subscribers. Inputs deduplicate notifications by modification time and schedule their owning node for that time.

Lifecycle Ordering
~~~~~~~~~~~~~~~~~~

Graph lifecycle processing follows the flattened node order, but start and stop use opposite directions.

Start processing runs in rank order:

1. notify before graph start,
2. for each node in flattened rank order, notify before node start, start the node, then notify after node start,
3. notify after graph start.

Stop processing runs in reverse rank order:

1. notify before graph stop,
2. for each node in reverse flattened rank order, notify before node stop, stop the node, then notify after node stop,
3. notify after graph stop.

Reverse stop order is intentional. Stop is a teardown operation and is similar to disposal: downstream nodes should be stopped before the upstream nodes they depend on. Subgraph lifecycle processing follows the same directional rule within the selected flattened range.

Observer And Callback Draining
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Lifecycle observer lists should be processed consistently across handlers in registration order. This describes observer ordering for a single lifecycle event. It does not override the node ordering rule above: stop-node lifecycle events are still emitted while walking nodes in reverse rank order.

One-shot before-evaluation and after-evaluation callbacks are drained until complete. If a callback registers additional callbacks of the same phase while draining, the newly registered callbacks must also run before that drain operation returns.

The current Python and C++ reference branches process after-evaluation callbacks in reverse order while most lifecycle handlers process observers in registration order. The intended C++ contract should normalize this so handlers are consistent; if reverse after-evaluation ordering is required for compatibility, that exception should be documented explicitly before implementation.

Expected Runtime Phases
-----------------------

Wiring
    Build the logical graph, resolve types and schemas, and produce runtime construction data.

Construction
    Allocate runtime objects, bind graph boundaries, initialize time-series storage, and prepare node lifecycle state.

Execution
    Advance time, schedule nodes, evaluate in dependency order, and propagate modifications.

Shutdown
    Stop nodes, release resources, and surface errors deterministically.

Open Design Items
-----------------

- Define the exact graph IR passed from wiring to runtime construction.
- Define how nested graphs share clocks, schedulers, and memory resources.
- Decide whether after-evaluation callback ordering must preserve the current reference-branch reverse ordering or move to uniform registration order.

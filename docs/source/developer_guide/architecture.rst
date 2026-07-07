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

    In ``RealTime`` mode, ``now`` is derived from the computer clock in UTC.

    In ``Simulation`` mode, ``now`` is an implementation-specific approximation of what ``now`` could have been in real-time mode if the event had triggered at wall-clock time. The implementation should preserve that intuition, but the exact mechanism is not part of the semantic contract.

``cycle_time``
    The elapsed time from the beginning of the current evaluation cycle to the current point in that cycle. This represents processing lag within the cycle.

C++ user-authored nodes receive this through ``EvaluationClockView``, a borrowed
read-only type-erased view. The engine/runtime owns and mutates the clock state;
node code only observes it.

Execution Modes
~~~~~~~~~~~~~~~

The engine has two primary execution modes: ``RealTime`` and ``Simulation``
(``GraphExecutorMode`` in ``runtime/executor.h``).

In ``Simulation`` mode, time is compressed. The engine does not wait between scheduled events. It processes events as quickly as possible while preserving event-time ordering.

In ``RealTime`` mode, the engine attempts to align event processing with wall-clock time. If an event is scheduled for the future, the engine waits until wall-clock time reaches that event time. If the engine is already behind, it evaluates immediately. Waiting would only increase the lag.

The engine does not skip scheduled events. If event coalescing or collapsing is required, that behavior belongs to a source node. A collapsing source node may choose to combine external events before introducing them into the runtime, but that is source-node behavior, not scheduler behavior.

Scheduling Semantics
~~~~~~~~~~~~~~~~~~~~

Scheduling is time based. All events scheduled for the same time are evaluated at the same ``evaluation_time``.

``evaluation_time`` is monotonic non-decreasing across evaluation cycles. Evaluation time must not move backwards.

Events are normally introduced through source nodes, but any node may schedule itself for future evaluation. Self-scheduling is a performance optimization that avoids creating stub source nodes only to represent timers.

Graph Flattening And Order
~~~~~~~~~~~~~~~~~~~~~~~~~~

Wiring produces a graph structure, but runtime evaluation operates over a flattened node set. The flattened set is fully ordered in rank order. That order is the evaluation order for the graph.

The order is part of the runtime invariant. During a cycle, a node may cause later-ranked nodes to become scheduled at the current ``evaluation_time``. That is valid because the later node has not yet been reached by the scan. A node must not schedule an earlier-ranked node for the current ``evaluation_time`` after that earlier node has already been passed. The graph is constructed so this cannot happen; if it does happen, the runtime should treat it as an invalid graph or scheduler bug rather than silently deferring or skipping work.

Graph Scheduler
~~~~~~~~~~~~~~~

The graph owns the schedule table used by the evaluation loop. This table
stores at most one visible scheduled time per flattened node: the next time
that node may need evaluation. A per-node slot starts at ``MIN_DT`` when the
node has no visible graph schedule. ``MIN_DT`` is the smallest runtime time,
below ``MIN_ST``, so it can never become due in a normal evaluation cycle.

The graph scheduler follows these rules:

- scheduling before the current ``evaluation_time`` is invalid and is the only
  hot-path validation check,
- scheduling at the current ``evaluation_time`` means "evaluate during the current cycle" if the node has not yet been reached,
- a node schedule is replaced when the existing time is already current or consumed,
- a node schedule is replaced when the new time is earlier than the existing time,
- the cached next scheduled time is updated only when the accepted time is
  strictly greater than the current ``evaluation_time`` and earlier than the
  current cache value.

``GraphView::next_scheduled_time()`` returns ``MAX_DT`` when no pending
scheduled time is cached. The graph stores this as an incrementally maintained
cached value rather than scanning the full schedule table on every executor
query. After graph start, the cache is seeded from the schedule table so
``schedule(now())`` can drive the first cycle. During evaluation, the cache is
reset and rebuilt by folding only schedule entries strictly after the current
``evaluation_time``. The scheduler does not rewrite a same-cycle request to a
future time; ordering errors are graph/scheduler bugs, not implicit deferrals.
The runtime trusts the compiled wiring topology to prevent invalid same-cycle
backward scheduling. That contract should be protected with wiring/compiler
tests rather than additional production checks in the scheduling hot path.

The graph schedule is the only activation gate. There is deliberately no
eval-level bypass flag: if a node is scheduled at the current evaluation time,
the graph calls its eval operation; if it is not scheduled, it is not
considered. Active time-series inputs, node-local schedulers, ``schedule_on_start``,
and explicit graph scheduling APIs all express work by writing the graph
schedule table.

The evaluation cycle, end to end — every activation path funnels into the one
schedule table:

.. mermaid::

   flowchart TD
      advance["executor ops: advance evaluation_time<br/>(Simulation jumps; RealTime waits on the wall clock)"]
      drain["drain pending push values into push-source outputs<br/>(real-time root graphs only)"]
      scan["graph: scan nodes in rank order"]
      gate{"schedule[node] == evaluation_time?"}
      eval["node eval"]
      tick["output tick: record_modified"]
      notify["TSData observer set: notify"]
      sched["input notifier: schedule_node(downstream, now)"]
      selfsched["NodeScheduler / schedule_on_start<br/>also write the schedule table"]
      next["fold next_scheduled_time (MAX_DT = idle);<br/>executor advances or sleeps until then"]

      advance --> drain --> scan
      scan --> gate
      gate -- "no" --> scan
      gate -- "yes" --> eval
      eval --> tick --> notify --> sched --> scan
      eval -.-> selfsched
      scan --> next --> advance

Lifecycle Teardown
~~~~~~~~~~~~~~~~~~

**Rank invariant:** an input target link (including a ``REF``-adapted binding)
may only point at a **lower**-ranked node's output. Exactly three sanctioned
exceptions exist, each declared rather than accidental:

- ``feedback``'s delayed value edge (``rank_dependency = false``; one-cycle
  semantics);
- **output forwarding** — projection state such as the REF alternative store,
  whose links follow a *dynamic* target (whatever the reference currently
  designates) and therefore cannot be rank-ordered structurally;
- the boundary **capture's paired-source recovery input**
  (``rank_dependency = false``): the port-level link never constrains rank so
  the pairing itself cannot create a wiring cycle. How the pair is ordered and
  scheduled then splits by kind — the boundary scheduling matrix:

  - **Shared-output relays** (adaptor ``from_graph``/``to_graph``, reference
    service outputs, service response outputs, context/shared outputs) are
    **rank-correct and same-cycle**: pairs are declared with
    ``Wiring::add_same_cycle_pair``, which rank-constrains the paired source
    *after* every capture; ``Wiring::finish`` re-ranks the whole graph once
    **all** captures are known and then **validates** every pair's final order
    (this is what keeps chains of multiple adaptors/services correct — see the
    chained-adaptor test). Because the ordering is proven at wiring time, the
    runtime trusts it: a capture schedules its source for the **current**
    evaluation time with no hot-path checks (debug asserts only) — wiring-time
    validation over run-time cost, and never a next-cycle deferral papering
    over a ranking bug.
  - **Request stubs** (subscription keys, request/reply requests) are the
    sanctioned **next-cycle** forwarders: the pairing is rank-free (no rank
    dependency at all), and the capture forwards to the service source at
    ``evaluation_time + MIN_TD`` (current time during ``start``). The temporal
    break — not a wiring edge — is what allows a client's request to derive
    from the service's own response without creating a wiring cycle, exactly
    like ``feedback``.

Anything else pointing backward is a wiring bug.

Subscription teardown is a **stop-time** responsibility: ``stop`` unbinds the
edge-established input links (``unbind_edges``, the dual of ``bind_edges``) and
releases output alternative-store subscriptions/links
(``release_alternative_subscriptions``) while **every producer's storage is
still alive**; by dispose time no cross-node references remain, and the
destructors' tolerant unbind paths are no-ops. The rank invariant makes
reverse-rank storage destruction safe for ordinary links; the stop-time
release is what makes it safe for the three sanctioned backward categories
above. A graph destroyed while still started is stopped by ``GraphValue``'s
destructor first (best-effort) for the same reason.

Edge bindings are established at graph **construction** (so a built graph is
inspectable before its first start). **Restart is not supported by design**
(ruling 2026-07-04): a stopped node/graph is never restarted — ``stop`` is a
step toward erase, run to guarantee the instance is clean before disposal.
No restart-aware rebind pass exists or is planned.

Runtime Diagnostics
~~~~~~~~~~~~~~~~~~~

Two hooks exist for the "which node is misbehaving?" question
(``tests/cpp/test_graph_introspection.cpp``):

- **Node identity on escaping exceptions.** An exception that leaves a node's
  ``start`` / ``evaluate`` / ``stop`` and would escape the **root** graph is
  re-thrown as ``std::runtime_error`` prefixed with
  ``node[<index> '<label-or-display-name>'] <phase> failed:``. The annotation
  happens only at the root boundary: exceptions inside nested graphs reach
  ``try_except_`` / per-node error capture unmodified, so ``NodeError``
  payloads keep the original message (see :doc:`error_handling`).
- **``GraphView::dump()``** returns a human-readable snapshot — graph name,
  lifecycle state, ``evaluation_time`` / ``next_scheduled_time``, then one
  line per node with its index, display name/label, and current
  graph-schedule entry (``-`` = not scheduled). ``GraphView`` also exposes
  ``node_scheduled_time(index)`` for programmatic inspection. Intended for
  logging and debugger use, not as a stable machine format.

Evaluation Cycle
~~~~~~~~~~~~~~~~

Each evaluation cycle runs at one ``evaluation_time``.

Graph instances are currently treated as run-and-dispose objects. ``stop()``
is cleanup before disposal; restarting a stopped graph instance is not part of
the runtime contract.

The normal cycle shape is:

1. run one-shot before-evaluation callbacks,
2. notify lifecycle observers before graph evaluation,
3. scan flattened nodes in rank order,
4. evaluate nodes whose graph schedule equals ``evaluation_time``, notifying
   lifecycle observers immediately before and after each node's evaluation,
5. fold future node schedules into the clock's next scheduled evaluation time,
6. notify lifecycle observers after graph evaluation,
7. run one-shot after-evaluation callbacks,
8. advance the engine clock.

This shape is uniform for root and nested graphs alike: a nested graph's own
``evaluate`` call brackets its scheduled nodes with the same before/after
graph- and node-evaluation notifications, on the same shared observer list
(see "Lifecycle Observers" below) — an observer registered once on the
executor sees every graph in the run, not just the root.

Push-source nodes are specialized node implementations, but they are still
evaluated through the normal node evaluation interface. A push-source node owns
its queue, exposes a sender during ``start``, and drains/applies queued messages
from its ``eval`` implementation. In real-time mode, the evaluation clock owns
the condition-variable wake-up state used to notice queued push messages; in
simulation mode, push-source nodes are not supported. The graph/evaluator must
not call a generic ``apply_message`` node operation directly.

Node Evaluation
~~~~~~~~~~~~~~~

A scheduled node still has to satisfy its node-level readiness rules. The node
must have the required valid inputs, and nodes that require all-valid input
state must enforce that before invoking user code or system-node code. Eval
does not re-poll active input ``modified`` flags; those inputs schedule the node
through their notification path when a relevant source ticks, binds, rebinds, or
unbinds.

A node that uses a scheduler consumes due scheduler events after its eval
operation and then re-arms the graph schedule from the next pending event, if
one remains. Cancellation happens during evaluation, so a cancelled event simply
does not re-arm a future graph schedule entry.

Nested graph evaluation uses the same cache. At the end of a nested graph
evaluation block, the nested graph schedules its parent node at the cached next
scheduled time, when one exists. That is the pull half of nested scheduling
delegation; out-of-band child schedules still push to the parent immediately
through ``nested_schedule_node_impl``.

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

Lifecycle Observers
~~~~~~~~~~~~~~~~~~~

``LifecycleObserver`` (``runtime/lifecycle_observer.h``) is the interface behind
every "notify" step above: a fixed set of before/after hooks for graph/node
start, stop, and evaluation, all defaulting to no-ops so an implementation
overrides only what it needs.

.. code-block:: cpp

   struct LifecycleObserver
   {
       virtual void on_before_start_graph(const GraphView &);
       virtual void on_after_start_graph(const GraphView &);
       virtual void on_before_start_node(const NodeView &);
       virtual void on_after_start_node(const NodeView &);

       virtual void on_before_graph_evaluation(const GraphView &);
       virtual void on_after_graph_evaluation(const GraphView &);
       virtual void on_before_node_evaluation(const NodeView &);
       virtual void on_after_node_evaluation(const NodeView &);

       virtual void on_before_stop_node(const NodeView &);
       virtual void on_after_stop_node(const NodeView &);
       virtual void on_before_stop_graph(const GraphView &);
       virtual void on_after_stop_graph(const GraphView &);
   };

There is exactly **one** ``LifecycleObserverList`` per executor run — it lives
in the executor's runtime storage (run-level state folds into the executor's
ops table rather than a separate engine object; see ``runtime/executor.h``),
not on any individual graph. Root and nested graphs
alike reach it through a raw ``LifecycleObserverList *`` cached once in their
own runtime storage at construction: the root graph caches it directly from
its executor, and a nested graph caches it with one hop to its parent graph's
already-cached pointer. This keeps the hot path (start/stop/evaluate) to a
plain field read — no ops-table dispatch, no walk up the nested-parent chain,
no allocation — while still giving a single registration on the executor
visibility into every graph and node in the run, at any nesting depth.

Following ``types/utils/slot_observer.h``'s ``SlotObserver``/``SlotObserverList``
idiom (not a shared generic template with it — two small concrete lists for
two distinct event sets), registration is by raw non-owning pointer: the
caller owns the ``LifecycleObserver`` instance and is responsible for
unregistering it before either side is destroyed. Two registration points:

- **Build time**: ``GraphExecutorBuilder::add_lifecycle_observer(observer)``,
  seeded into the executor's list at construction.
- **Runtime**: ``GraphExecutorView::lifecycle_observers()`` /
  ``GraphView::lifecycle_observers()`` both return the same
  ``LifecycleObserverList &``; call ``.add(observer)`` / ``.remove(observer)``
  directly at any point before or during the run. Removal is safe from within
  an observer's own callback (deferred compaction while a notification is in
  progress, matching ``SlotObserverList``'s reentrancy guard).

Observer And Callback Draining
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Lifecycle observer lists should be processed consistently across handlers in registration order. This describes observer ordering for a single lifecycle event. It does not override the node ordering rule above: stop-node lifecycle events are still emitted while walking nodes in reverse rank order.

One-shot before-evaluation and after-evaluation callbacks are drained until complete. If a callback registers additional callbacks of the same phase while draining, the newly registered callbacks must also run before that drain operation returns.

After-evaluation callbacks follow the same registration-order rule as other
lifecycle observer lists (the open question about ordering is resolved —
see below for the resolved exception-safety rule).

**Resolved: exception safety for "after" notifications.** After-graph-evaluation,
after-node-evaluation, and after-node-stop notifications fire **best-effort** —
even when the operation they bracket throws — so an observer sees a matching
before/after pair for every attempt, not just successful ones:

- **Graph evaluation**: "after" fires exactly once per cycle, on whichever
  call to ``evaluate`` either completes the cycle or lets an exception
  propagate out of the node loop. It does **not** fire when a node pauses the
  cycle (a resumed call later gets the same "after" treatment) — a paused
  cycle is not a completed one.
- **Node evaluation**: "after" always fires once the node's ``evaluate`` call
  returns or throws, regardless of pause or exception — the node itself did
  run once, independent of whether the enclosing graph cycle completed.
- **Node stop**: "after" always fires once the node's ``stop`` call returns
  or throws — consistent with stop's existing best-effort contract (every
  node gets a stop attempt even if an earlier one failed).
- **Node/graph start are the one asymmetric case**: "after start" is plain
  sequential and is *not* guaranteed on exception. A node that fails to start
  never really started, so no matching after-start notification fires for
  it; nodes that failed to start are rolled back through the normal stop
  path (and so do get before/after-stop notifications for that rollback).

An observer's own exception during a best-effort "after" notification is
swallowed rather than allowed to propagate — otherwise a buggy observer could
either mask the real failure it was firing on, or terminate the process
outright (a second exception escaping a destructor while the first is still
unwinding calls ``std::terminate``). Observers should treat their own
notification methods as diagnostic/logging code, not as a place to signal
failure back into the runtime.

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

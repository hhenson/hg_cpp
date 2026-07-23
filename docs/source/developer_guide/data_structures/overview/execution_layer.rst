Execution Layer
===============

The execution layer contains run-level state. It is not the graph topology itself, but it controls how graph topology is evaluated.

.. note::

   **Status & direction.** The current runtime implements only the
   ``GraphExecutor`` described under *Current implementation*. The separate
   ``EvaluationEngine`` / ``EvaluationClock`` objects under *Alternative
   design* are **one candidate**, **not** a committed target. With the
   type-erased design the current leaning is to **fold
   run-level state and the evaluation loop into the executor's ops** rather
   than introduce separate engine/clock objects. Document execution machinery
   here as it lands, and keep this note honest.

Current implementation
----------------------

``GraphExecutor`` (``runtime/executor.h``)
    The only execution-layer object that exists today. ``GraphExecutorValue``
    owns the built ``Graph`` and a ``GraphExecutorTypeMetaData`` carrying the
    run ``mode`` (``Simulation`` / ``RealTime``), ``start_time``, and
    ``end_time``. ``GraphExecutorView::run()`` drives the loop directly:
    ``graph.start(start_time)``, then evaluate the graph at its
    ``next_scheduled_time`` until that time reaches ``end_time``, then
    ``graph.stop()``. There is no separate engine or clock object yet; nodes
    receive ``evaluation_time`` explicitly, and "next scheduled time" is the
    minimum over the graph's per-node schedule entries.

    **End-of-run enforcement.** ``end_time`` bounds the run in *evaluation*
    time — the loop exits once the next evaluation time reaches it. The
    real-time executor additionally enforces it against the **wall clock**:
    ``advance_realtime`` ends the run as soon as the wall clock passes
    ``end_time``, even if earlier-scheduled work remains. Without this, a
    node that re-schedules itself every ``MIN_TD`` while each cycle burns
    real wall time (the shape of a permanently failing adaptor retry loop)
    advances evaluation time by only one microsecond per cycle and can
    starve the logical bound almost indefinitely at 100% CPU. This is a
    recorded deviation from the ``ext/main`` Python runtime, whose run loop
    bounds only evaluation time and shares the starvation. Work scheduled
    before ``end_time`` but not yet evaluated when the wall clock reaches it
    is dropped, exactly as ``request_stop()`` would drop it.

Pause / resume (the cursor protocol)
------------------------------------

Evaluation can **pause** mid-cycle and **resume** later in the same cycle. This is
the substrate the ``mesh_`` engine is built on: a ``mesh_(func)[k]`` node that
needs the not-yet-computed result of sibling instance ``k`` pauses its instance,
lets the mesh evaluate ``k`` (in dependency-rank order), then resumes the instance
so the ``[k]`` read succeeds — all within one engine cycle.

The protocol is a single boolean threaded through the eval API plus one cursor:

- **Node eval returns ``bool``** (``NodeOps::evaluate_impl`` / ``NodeView::evaluate``):
  ``true`` = completed, ``false`` = *the node requested a pause* and must be
  re-evaluated to make progress. Ordinary nodes always return ``true``; this
  boolean **is** the C++ pause-request API (a later Python frontend gets an
  equivalent hook). On a pause the node has arranged for its own re-trigger
  (e.g. the mesh records the dependency and creates/evaluates it).
- **Graph eval returns ``bool``** (``GraphOps::evaluate_impl`` /
  ``GraphView::evaluate``) and carries a **cursor in graph state** — the existing
  ``evaluation_cursor`` (the node_id), now the loop counter itself rather than a
  local. When a node returns ``false`` the cursor is already sitting on it, so the
  graph returns ``false`` immediately. The next ``evaluate`` at the same time sees
  a non-zero cursor, **skips the per-cycle setup** (``next_scheduled_time`` reset,
  push-source pass) and resumes the node loop at the cursor — re-running the paused
  node and continuing. A completed cycle resets the cursor to ``0``. No extra state
  variable is introduced.
- **Nested handlers propagate** (``single_nested_graph``, ``switch_``, ``map_``,
  ``reduce_``): each forwards a child's ``false`` upward, recording what it was
  evaluating so a resume re-drives the same child. ``single_nested`` / ``switch_``
  have a single active child, so they just relay its bool (its own graph cursor does
  the rest); ``map_`` / ``reduce_`` evaluate many children, so they hold a **per-child
  cursor** in node storage (``map_``: the entry slot; ``reduce_``: the combiner
  position) — on resume they skip the per-cycle setup (key reconciliation / structure
  rebuild) and continue the child loop from that cursor, resetting it on completion.
  Re-binding on resume is idempotent. The **mesh node is the pause boundary**: it
  *resolves* a paused instance instead of propagating, returning ``true`` to its own
  graph once the instance set has settled. Everything between a ``mesh_(func)[k]`` and
  its mesh just relays the pause.
- **The root graph never pauses** (a pausing node only exists inside a mesh scope),
  so ``GraphExecutorView::run()`` treats a ``false`` from the root as a logic error.

Alternative design: a separate engine and clock
------------------------------------------------

The split below is recorded here as an *alternative*, **not** the current
direction. It lifts run-level state into a separate
``EvaluationEngine`` so real-time mode, observers, and push-sources have a
dedicated home. The current preference is instead to fold these
responsibilities into the executor's type-erased ops (see the status note
above); revisit this split only if that proves insufficient.

``EvaluationEngine``
    Stores ``start_time``, ``end_time``, run mode, stop-request state, the engine clock, lifecycle observers, and one-shot callback queues.

``EvaluationClock``
    Stores ``evaluation_time`` and the next scheduled evaluation time. Real-time clocks additionally store wall-clock alarm state and push-source wake-up state.

``LifecycleObserverList``
    Ordered list of observer handles. Observers receive lifecycle events but do not own graph runtime state.

``EvaluationCallbackQueues``
    One-shot before-evaluation and after-evaluation callback queues. These queues are drained until complete.

``PushMessageReceiver``
    Queue of external messages addressed to push-source nodes. Each queued item is logically ``(node_index, message)``.


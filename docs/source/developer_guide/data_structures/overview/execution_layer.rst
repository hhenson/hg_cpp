Execution Layer
===============

The execution layer contains run-level state. It is not the graph topology itself, but it controls how graph topology is evaluated.

.. note::

   **Status & direction.** The current runtime implements only the
   ``GraphExecutor`` described under *Current implementation*. The separate
   ``EvaluationEngine`` / ``EvaluationClock`` objects under *Alternative
   design* are **one candidate** (the ``ext/2603`` shape), **not** a committed
   target. With the type-erased design the current leaning is to **fold
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

Alternative design: a separate engine and clock
------------------------------------------------

The split below is the ``ext/2603`` shape, recorded here as an *alternative*,
**not** the current direction. It lifts run-level state into a separate
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


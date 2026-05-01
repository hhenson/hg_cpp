Execution Layer
===============

The execution layer contains run-level state. It is not the graph topology itself, but it controls how graph topology is evaluated.

Core elements:

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


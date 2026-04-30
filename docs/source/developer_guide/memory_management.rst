Memory Management
=================

Objectives
----------

- Make ownership explicit.
- Avoid Python-owned lifetime as a runtime requirement.
- Keep time-series storage cache friendly.
- Support deterministic teardown.
- Make graph-level allocation strategies visible and testable.

Ownership Model
---------------

The runtime should distinguish:

- long-lived graph topology,
- per-run runtime state,
- node-owned resources,
- time-series value storage,
- transient evaluation state.

Initial Direction
-----------------

Graph topology should be immutable after construction. Runtime state can then refer to topology through stable identifiers or references without needing defensive synchronization for wiring changes.

Time-series values should be stored in structures that make validity, modification state, and value payload access cheap during evaluation. Collection-shaped time-series values need clear delta ownership rules to avoid accidental copies.

Topics To Specify
-----------------

- allocator strategy,
- arena or pool ownership for graph instances,
- stable handles versus raw pointers,
- intrusive versus external reference counting,
- Python object ownership at bridge boundaries,
- teardown order,
- observer and trace lifetimes.

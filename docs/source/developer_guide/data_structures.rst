Data Structures
===============

The C++ runtime will need compact representations for graph topology, time-series state, scheduling, and schema metadata.

Graph Structures
----------------

Potential structures:

- node id and port id tables,
- adjacency lists for propagation,
- rank-ordered evaluation queues,
- graph boundary maps,
- nested graph ownership records.

Time-Series Structures
----------------------

Potential structures:

- scalar time-series state,
- bundle and record storage,
- keyed dictionary storage,
- set storage and deltas,
- list/window storage,
- reference values.

Scheduling Structures
---------------------

Potential structures:

- dirty queues,
- rank queues,
- timer queues,
- source event queues,
- lifecycle work queues.

Topics To Specify
-----------------

- stable identifiers and invalidation rules,
- mutation and delta semantics,
- iteration order guarantees,
- memory locality expectations,
- thread-safety assumptions.

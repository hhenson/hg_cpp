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
- Define runtime error propagation and observer hooks.

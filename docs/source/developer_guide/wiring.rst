Wiring
======

Wiring is graph construction. It turns user-authored graph descriptions into a runtime graph definition.

Project Boundary
----------------

Wiring remains Python first for compatibility with the current HGraph ecosystem. The C++ runtime should still expose enough native graph-building API to support C++ authored graphs and nodes directly.

Responsibilities
----------------

- collect node declarations,
- resolve overloads and generic types,
- bind graph inputs and outputs,
- create graph edges,
- construct nested graph boundaries,
- lower wiring artifacts into C++ runtime construction data.

C++ Runtime Contract
--------------------

The runtime should receive a graph definition that is already type-checked and structurally valid. Runtime construction should validate invariants that protect memory safety, but it should not depend on Python-level wiring behavior for correctness.

Topics To Specify
-----------------

- graph IR shape,
- boundary binding rules,
- nested graph representation,
- C++ graph builder API,
- Python wiring to C++ lowering,
- diagnostics and source location mapping.

Schema Management
=================

Schema management describes how scalar types, time-series types, bundles, records, and generic signatures are represented and resolved.

Responsibilities
----------------

- represent scalar and time-series type metadata,
- validate node input and output schemas,
- support generic resolution,
- expose enough metadata for Python wiring compatibility,
- keep runtime evaluation independent from Python type objects.

Expected Concepts
-----------------

Scalar Schema
    Describes the non-time-series value shape.

Time-Series Schema
    Describes how scalar values participate in runtime state, validity, modification, and propagation.

Node Signature
    Describes input and output ports, type variables, constraints, and defaults.

Graph Boundary Schema
    Describes how nested graphs connect to parent graphs.

Topics To Specify
-----------------

- canonical schema representation,
- schema interning and comparison,
- Python type metadata conversion,
- C++ template integration,
- error messages for failed resolution,
- serialization or debugging representation.

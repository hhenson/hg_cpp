Python Integration
==================

Python integration exists to preserve the current ecosystem and to support Python user-authored nodes. It should not define the core runtime architecture.

Supported Roles
---------------

- Python graph wiring,
- compatibility with existing Python HGraph user code,
- Python user nodes executed by the C++ runtime,
- packaging of optional bindings.

Boundaries
----------

Normal CMake builds should not require Python. Python-specific code should live behind optional CMake targets and be enabled only through:

.. code-block:: bash

   -DHGRAPH_BUILD_PYTHON_BINDINGS=ON
   -DHGRAPH_ENABLE_PYTHON_USER_NODES=ON

GIL And Runtime Locks
---------------------

The C++ runtime must assume that it does **not** hold the Python GIL unless a
local scope has explicitly acquired it. Any path that calls Python code or uses
Python C API objects must acquire the GIL at that boundary. This includes Python
node ``start`` / ``eval`` / ``stop`` callbacks, lifecycle observers implemented
in Python, Python notification callbacks, Python-backed sender functions, and
exception translation that inspects Python exception state.

Conversely, the real-time engine must not hold the GIL while waiting on runtime
condition variables or other blocking primitives. It also must not hold graph,
node, sender, receiver, or clock mutexes while entering Python. The ordering
rule is:

1. release/acquire runtime locks only for C++ state,
2. drop those locks before calling Python,
3. acquire the GIL immediately around the Python call,
4. release the GIL before a blocking wait.

This is especially important for push-source nodes: external threads enqueue
through a sender and wake the real-time evaluation clock, while the evaluator may
be sleeping on a condition variable. The implementation must avoid GIL/runtime
lock inversion in both directions.

Topics To Specify
-----------------

- GIL ownership during node evaluation,
- Python object lifetime inside C++ runtime state,
- exception translation,
- Python callback scheduling,
- conversion between Python type metadata and C++ schemas,
- packaging and ABI policy.


Cross-boundary type identity
----------------------------

The C++ runtime identifies schemas by **pointer equality of interned metadata**:
two equivalent schemas resolve to the same ``const TSValueTypeMetaData*`` /
``const ValueTypeMetaData*`` because the ``TypeRegistry`` interns them. Anything
that matches types across the boundary — notably *operator* overload dispatch
(see *Operators*) — relies on this: a Python ``TS[int]`` and a C++ ``TS<Int>``
must produce the **same** interned pointer.

The invariant that guarantees it: there is exactly **one canonical scalar per
logical type**, and every name a Python type uses is an **alias** onto that
canonical scalar (via ``register_value_alias``), never a separately-interned
synthetic. Concretely, ``value_type("int")`` (the Python lookup) must return the
*same* pointer as ``register_scalar<int>("int")`` (the C++ registration); since
``TypeRegistry::ts(value_meta)`` interns on the value pointer, identity then
composes upward automatically (``TS``, ``TSL``, ``TSD``, …). The standard-types
seed must run **before** any overload is registered on either path; the C++ test
listener seeds via ``register_standard_types()`` and the Python module seeds at
import via ``register_builtin_value_types()`` — the two must agree on names and
aliases.

Hosting a Python node
---------------------

A Python user node is hosted without a new node *kind*: ``NodeCallbacks``
(``include/hgraph/runtime/node.h``) is already a type-erased ``std::function``
triple, so a Python implementation produces the same ``(NodeTypeMetaData,
NodeCallbacks, TSEndpointSchema)`` that ``NodeBuilder::native`` consumes, with
``NodeCallbacks::evaluate`` acquiring the GIL, marshalling the ``NodeView`` to
Python, and writing the result back through the ``TSOutputView``. This is the
mechanism behind a Python *operator* implementation registering as an ordinary
candidate (see *Operators > The Python implementation path*), built only under
``HGRAPH_ENABLE_PYTHON_USER_NODES``.

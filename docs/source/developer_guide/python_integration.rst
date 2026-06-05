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

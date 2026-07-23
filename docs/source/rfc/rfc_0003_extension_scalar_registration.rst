RFC 0003: Downstream Python Scalar Registration
================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-07-23
:Target: Unscheduled

Summary
-------

Provide a public bridge API through which a downstream native extension can
associate one of its Python classes with the native scalar schema it owns.
This removes the need for extensions to mutate ``hgraph._types`` internals in
order to use ``TS[ExtensionType]`` in Python wiring.

Motivation
----------

The C++ static schema machinery already lets an extension register a custom
scalar with the shared ``TypeRegistry``. A nanobind extension can also expose
the corresponding Python class. Today there is no public operation connecting
those two facts, so Python type resolution falls back to the opaque ``object``
schema unless the extension edits the private ``_SCALAR_NAMES`` map.

Proposed contract
-----------------

The bridge should expose an idempotent function conceptually equivalent to:

.. code-block:: python

   register_native_scalar_type(PythonType, native_value_type)

The association is process-wide, holds a strong reference to the Python type,
and participates in both Python-annotation-to-schema and
schema-to-Python-type reflection. Re-registering the same pair succeeds;
attempting to associate either side with a conflicting counterpart fails.

The C++ helper supplied to downstream modules should accept the native scalar
type and Python class, ensure the scalar is registered in the shared
``TypeRegistry``, and install the same association without importing private
Python modules.

Ownership and constraints
-------------------------

The registry and public hook belong in ``hg_cpp`` because every extension with
native scalar types needs them. The extension owns the class, conversion
traits, scalar name, lifecycle, and package import order. Registration must not
create a second runtime/type registry or require Python for pure C++ use.

Compatibility and performance
-----------------------------

Built-in mappings remain unchanged. Lookup is performed during wiring and
reflection, not during node evaluation. The design must be safe across repeated
module imports and must preserve the single process-wide native schema pointer
required by downstream ABI contracts.

Acceptance criteria
-------------------

* A separately built test extension registers a custom C++ scalar and Python
  class using only public installed headers and bridge functions.
* ``TS[ExtensionType]`` resolves to the extension's native scalar schema.
* Native values round-trip through a Python-authored graph and reflect back to
  the registered class.
* Duplicate identical registration is harmless and conflicting registration
  is rejected deterministically.
* Pure C++ consumers remain independent of the Python runtime.

Implementation status
---------------------

No core implementation is included. A downstream test extension that currently
requires a private type mapping will provide the proving integration for this
RFC.

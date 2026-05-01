Scalar Plans and Ops
====================

The scalar (value) layer is the simplest concrete instantiation of the
Plan/Schema/Ops/Builder/Value/View vocabulary: each role maps onto one
named runtime element, with no time-series state or per-tick
bookkeeping.

Mapping to Core Concepts
------------------------

================  ============================================================
Concept role      Value-layer name
================  ============================================================
Schema            ``ValueTypeMetaData`` — kind, capability flags, fields,
                  element/key types. *No layout information; that lives on
                  the matching* ``StoragePlan``.
Plan              ``MemoryUtils::StoragePlan`` — memory layout (size,
                  alignment, field offsets) plus a ``LifecycleOps`` table
                  (alloc, construct, destruct, dealloc)
Plan factory      ``ValuePlanFactory`` — schema → plan mapping, with
                  results cached against the schema. Atomic plans are
                  pre-registered by ``TypeRegistry`` from
                  ``MemoryUtils::plan_for<T>()``; tuple / bundle / fixed
                  list plans are synthesised on first use.
Ops               ``ValueOps`` — ``hash``, ``equals``, ``compare``,
                  ``to_string`` function pointers
Binding           ``ValueTypeBinding`` — interned ``(ValueTypeMetaData,
                  StoragePlan, ValueOps)`` triple; the canonical handle the
                  rest of the layer shares
Builder           ``ValueBuilder`` — wraps a binding; constructs ``Value``
                  instances; cached in a ``ValueBuilderRegistry`` keyed by
                  schema
Value             ``Value`` — owning handle over storage + binding + allocator
View              ``ValueView`` — two-word reference: ``(binding, data)``.
                  Specialized adapters extend it for composite kinds.
================  ============================================================

The schema-side details (``ValueTypeMetaData`` fields, kinds, composite
synthesis) are described in *Schemas > Scalar Schemas*. The erased view
surface, multi-implementation rationale, view casting, and mutable-view
contract are described in *Erased Types*. The remaining pieces — plan
factory, owning value, storage, buffer/Arrow interop, nullability — are
covered below.

Plan Factory
------------

``ValuePlanFactory`` is the schema → plan mapping. It is the value
layer's answer to "the schema is independent of plan, but a builder
needs to pick a plan": the factory hands back the canonical plan for a
given schema, with results cached against the schema for stable
addresses.

- **Atomic schemas** are paired with their canonical plan at
  registration time. ``TypeRegistry::register_scalar<T>()`` calls
  ``ValuePlanFactory::register_atomic(schema, &MemoryUtils::plan_for<T>())``,
  so subsequent ``plan_for(atomic_schema)`` calls return the
  function-local-static plan without recomputation.
- **Tuple, bundle, and fixed-size list schemas** are synthesised on
  first use. ``plan_for`` recursively resolves component schemas to
  their plans and feeds them into ``MemoryUtils::tuple_plan`` /
  ``named_tuple_plan`` / ``array_plan``. Because those builders intern
  their results, the factory's cache lines up with the global plan
  cache.
- **Container kinds (Set, Map, CyclicBuffer, Queue, dynamic List)**
  require the value-layer's container storage shapes
  (``DynamicListStorage``, ``SetStorage``, ``MapStorage``, etc.). Until
  that layer is ported, ``plan_for`` throws for these kinds; the
  interface is in place so callers can be written against the final
  shape.

The time-series layer has the matching ``TSValuePlanFactory``. It is
declared with the same surface today; its synthesis logic is deferred
until the value-layer container shapes and the TS-layer state-tree
storage are both in place.

A builder may use the default factory to obtain a default plan, or
swap in an alternative factory for a specialised implementation
(e.g. a delta-tracking factory for time-series consumers wrapping the
same set/map shape). The schema itself remains untouched in both
cases.

Owning Value
------------

``Value`` is the owning handle:

- ``Value(const ValueTypeMetaData&)`` — constructs an owning value for the
  given schema, in a typed-null state. Schema is preserved even when the
  payload is absent.
- ``Value(T&&)`` — convenience for scalars; resolves to the canonical
  scalar binding via ``value::scalar_type_meta<T>()``.
- ``has_value()`` / ``reset()`` — manage top-level payload presence.
- ``view()`` — produces a ``ValueView`` over the live payload.
- ``hash()`` / ``equals()`` / ``compare()`` / ``to_string()`` — route
  through ``view()`` and the bound ``ValueOps``. ``Value`` itself only
  carries the minimum behaviour needed to live in a container.

Anything richer than container-membership operations is exposed through
``ValueView`` or one of its specialized adapters (see *Erased Types*),
never directly on ``Value``.

Storage and Allocation
----------------------

Memory is owned by ``MemoryUtils::StorageHandle``, parameterised by an
inline-storage policy and the binding type. Lifecycle hooks are
delegated to ``LifecycleOps`` on the binding's ``StoragePlan``:

- ``construct``, ``copy_construct``, ``move_construct``
- ``copy_assign``, ``move_assign``
- ``destroy``
- ``allocate``, ``deallocate``

Small payloads use inline (SBO) storage; larger payloads heap-allocate
with schema alignment. Container kinds have their own internal storage
shapes that keep element addresses stable across growth and
reconciliation; those shapes feed directly into the time-series
representation and are described under *Time-Series Plans and Ops*.

A scalar ``StoragePlan`` does not use a slot store. Slot stores carry
the delayed-erase and per-slot-bit machinery the time-series layer
needs to expose deltas; for scalars, that overhead is wasted — a flat
``StorageHandle`` is sufficient. The slot store family is introduced
separately in *Time-Series Plans and Ops*.

Nullability
-----------

Null is a *state*, not a schema or type. There is no null
``ValueTypeMetaData``.

- **Top-level**: ``Value::has_value()`` distinguishes a typed-null
  ``Value`` (schema known, payload absent) from a populated one.
- **Nested**: composite kinds track per-child validity — per-field for
  tuples and bundles, per-element for fixed and dynamic lists, per-slot
  for map values. Map keys and set elements are non-null by design.

Top-level null Values map to Python ``None`` at the bridge boundary;
``from_python(None)`` calls ``reset()``.

Buffer and Arrow Interop
------------------------

Exposing value data as a buffer is a first-class concern of the value
layer. Internal storage is not required to be Arrow-native, but every
kind whose payload can be sensibly viewed as a contiguous buffer must
expose a path to do so:

- atomic and fixed-size composite kinds expose a direct buffer view
  over their payload memory.
- dynamic list and queue kinds expose buffer views over their element
  storage, with a separate validity buffer where nested nullability
  applies.
- map and set kinds expose key, value, and validity buffers in
  parallel.

Validity bits follow Arrow conventions (``1 = valid``, ``0 = null``)
so a buffer view is interchangeable with an Arrow-compatible consumer
without a translation step. This applies whether the buffer is
exported for analytics, sent over an external bridge, or consumed by
an Arrow-aware adaptor.

Buffer access is read-only at the value layer. Mutation continues to
go through the typed view APIs.

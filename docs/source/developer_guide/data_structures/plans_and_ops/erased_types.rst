Type-Erased Data Structures
===========================

Both scalar and time-series implementations expose their values through
type-erased handles rather than concrete C++ templates. This chapter
describes the erased surface — the value-side handle, the view, and the
ops vtable — that every kind reuses. The concrete layouts for scalar
and time-series kinds are described in the next two pages.

Why type erasure
----------------

A single kind can have several implementations, chosen by the caller's
needs and bound at the value layer's resolution step.

Pure-data callers — those that only need to store, hash, compare, and
serialise values — use **compact** implementations. A pure-data Set is
a hash-backed key store; a pure-data Map is a paired key/value slot
store. These minimise memory and skip bookkeeping the caller does not
need.

Time-series callers — those that need to observe insertions, removals,
and modifications across ticks — use **delta-tracking**
implementations. The delta-tracking Set is the storage substrate for
``TSS``; the delta-tracking Map is the substrate for ``TSD``. They
carry per-slot observers and pending-erase state so the time-series
layer can produce coherent deltas.

Both implementations expose the same ``SetView`` / ``MapView`` shape.
Code that walks a Set or a Map through its view does not need to know
which implementation it is looking at. The choice is made by the
schema's bound ``StoragePlan`` and ``ValueOps``; the view contract is
unchanged.

This is the load-bearing reason values are type-erased rather than
generic-templated: it lets the time-series layer reuse the value-layer
container vocabulary without forking the surface.

Binding: the shared anchor
--------------------------

Type erasure is held together by a *binding*: an interned triple of
schema, plan, and ops that every erased handle dereferences:

.. code-block:: cpp

   template <typename TypeMeta, typename Ops>
   struct TypeBinding {
       const TypeMeta*                 type_meta;
       const MemoryUtils::StoragePlan* storage_plan;
       const Ops*                      ops;
   };

   using ValueTypeBinding = TypeBinding<ValueTypeMetaData, ValueOps>;

A ``Value``, a ``ValueView``, or a ``Builder`` stores one borrowed
pointer to its binding and reaches schema, plan, lifecycle, and ops
uniformly through it.

Bindings are interned by ``(type, plan, ops)`` triple. Builders are
interned by schema. Each registry exists only to expose a typed
convenience API on top of one ``InternTable`` instance, so the
interning contract is uniform across all erased shapes.

Composite bindings populate lazily. The first request for a tuple,
list, map, or other container schema synthesises its plan from the
element/key bindings, interns the resulting binding, and stores the
builder.

The View
--------

A view is the type-erased non-owning handle:

.. code-block:: cpp

   struct ValueViewContext {
       const ValueTypeBinding* binding;
       void*                   data;
   };

The binding gives the view its schema, plan, lifecycle, and ops; the
data pointer addresses the live payload. Because the context is two
pointers, a view is cheap to copy and pass through internal traversal
code.

A view exposes:

- type interrogation: ``is_atomic()``, ``is_tuple()``, ``is_list()``,
  …;
- typed access: ``as<T>()``, ``try_as<T>()``, ``checked_as<T>()`` for
  atomic kinds;
- generic ops: ``hash()``, ``equals()``, ``compare()``, ``to_string()``
  — always routed through the bound ops table;
- read access for composite kinds via specialised adapters described
  below.

Mutation, including atomic ``set<T>`` and structural assignment,
requires a mutable view obtained via ``begin_mutation()`` (see
*Read-Only and Mutable Views*).

View Casting
------------

The erased layer supports direct casting from one view shape to
another so callers do not need to chain calls to reach a typed
handle. Two cast families exist:

- **Kind-specialised view casts**: ``as_tuple()``, ``as_bundle()``,
  ``as_list()``, ``as_set()``, ``as_map()``, ``as_cyclic_buffer()``,
  ``as_queue()``, with ``try_as_*`` counterparts that return
  ``std::optional`` and throw nothing.
- **Atomic typed casts**: ``as<T>()``, ``try_as<T>()``,
  ``checked_as<T>()`` reach the underlying scalar in one call.

Both families are mirrored on the owning ``Value`` itself
(``Value::as_list()``, ``Value::as<T>()``, …) so callers holding a
``Value`` do not need to dereference into a ``ValueView`` and then
cast again.

These casts only re-interpret the existing binding's view shape. They
do not change the underlying schema or copy the payload. Cross-schema
adaptation — exposing one schema's value through a different schema —
is a time-series concern, not a value-layer concern.

Read-Only and Mutable Views
---------------------------

Views come in read-only and mutable variants. The distinction is part
of the public contract, not just C++ ``const`` discipline:

- A **read-only view** exposes inspection and iteration: typed
  access, ``hash``, ``equals``, ``compare``, ``to_string``, buffer
  exposure, and structural reads.
- A **mutable view** adds the kind-specific mutation operations:
  ``set`` for atomics, field mutation on bundles and tuples,
  ``push_back`` / ``resize`` on lists, ``add`` / ``remove`` on sets,
  key insertion and value updates on maps, and so on.

A mutable view is obtained from an existing view by calling
``begin_mutation()``. The transition is explicit so that consumers can
reason about when mutation is in scope — the time-series layer in
particular needs to know precisely when changes start and end so its
delta accounting stays coherent.

The mutation is closed by calling ``end_mutation()`` on the mutable
view. If the caller does not call it, the mutable view's destructor
closes the mutation when the view goes out of scope. Explicit
``end_mutation()`` gives the caller deterministic control over when
the runtime sees the mutation complete; RAII closure is the safety net
that guarantees no mutation is left dangling if a caller forgets or an
exception unwinds the stack.

One specific cross-kind read-only view is worth calling out:
``MapView`` exposes ``key_set()``, which returns a read-only
``SetView`` over the map's keys. Callers can iterate or query keys
with the same surface they would use for a standalone set, without
copying or materialising a second container. The view is read-only
because the keys belong to the map; structural changes go through the
map's mutable view.

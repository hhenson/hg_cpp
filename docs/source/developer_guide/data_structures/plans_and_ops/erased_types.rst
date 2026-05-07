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
serialise values atomically (read whole, replace whole) — use
**compact** implementations. A pure-data Set is a hash-backed key
store; a pure-data Map is a contiguous keyed map; pure-data
List / CyclicBuffer / Queue use contiguous element arrays. These
forms are described in *Scalar Plans and Ops > Container Storage
Shapes*. They minimise memory and skip the per-element bookkeeping
the value layer never needs.

Time-series callers — those that need to observe insertions,
removals, and modifications across ticks — use **slot-store-based**
implementations. The slot-store-based Set is the storage substrate
for ``TSS``; the slot-store-based Map is the substrate for ``TSD``.
They carry per-slot observers, stable-address slots, and pending-
erase state so the time-series layer can produce coherent deltas.
These forms are described in *Time-Series Plans and Ops > The Slot
Store Family*.

Both modes expose the same ``SetView`` / ``MapView`` shape, but with
different mutation surfaces: the pure-data view does whole-container
replace; the slot-store view does per-element insert / remove. Code
that *reads* a Set or a Map through its view does not need to know
which implementation it is looking at. The choice is made by the
schema's bound ``StoragePlan`` and ``ValueOps``; the read contract
is unchanged.

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
  — routed through the binding; ``compare()`` returns
  ``std::partial_ordering`` as the common erased representation of
  ``operator<=>`` results. Compact containers use their bound ops table,
  while structured tuple/bundle/fixed-list views recurse through child
  views;
- read access for composite kinds via specialised adapters described
  below.

Mutation, including atomic ``set<T>`` and structural assignment, is
reserved for mutable views obtained via ``begin_mutation()`` (see
*Read-Only and Mutable Views*). The compact value-layer views are
read-only; whole-value replacement happens through ``Value``.

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

Status: the read-only cast family is implemented for tuple, bundle,
list, set, map, cyclic buffer, and queue views. Mutable-view casts land
with the slot-store-backed time-series views.

Read-Only and Mutable Views
---------------------------

Views conceptually come in read-only and mutable variants. The
distinction is part of the public contract, not just C++ ``const``
discipline:

- A **read-only view** exposes inspection and iteration: typed
  access, ``hash``, ``equals``, ``compare``, ``to_string``, buffer
  exposure, and structural reads.
- A **mutable view** adds the kind-specific mutation operations:
  ``set`` for atomics, field mutation on bundles and tuples,
  ``push_back`` / ``resize`` on lists, ``add`` / ``remove`` on sets,
  key insertion and value updates on maps, and so on.

The current compact value-layer implementation exposes the read-only
variants. A mutable view is obtained from an existing view by calling
``begin_mutation()`` once the slot-store-backed time-series variants
land. The transition is explicit so that consumers can reason about
when mutation is in scope — the time-series layer in particular needs
to know precisely when changes start and end so its delta accounting
stays coherent.

The mutation is closed by calling ``end_mutation()`` on the mutable
view. If the caller does not call it, the mutable view's destructor
closes the mutation when the view goes out of scope. Explicit
``end_mutation()`` gives the caller deterministic control over when
the runtime sees the mutation complete; RAII closure is the safety net
that guarantees no mutation is left dangling if a caller forgets or an
exception unwinds the stack.

.. note::

   **Expected use: one outer mutation per engine cycle.** The runtime
   contract assumes that a given collection enters at most one
   *outermost* ``begin_mutation()`` per evaluation cycle. Nested
   ``begin_mutation()`` calls within that outer scope are fine — they
   are tracked as a depth counter and only the outermost call performs
   start-of-mutation work — but the design does not anticipate a
   collection completing one full mutation and then opening a second
   independent mutation in the same cycle. Pending-erase cleanup runs
   at the start of an outermost mutation, so a second outermost
   mutation in the same cycle would re-run the cleanup against the
   delta state still in scope for the current tick and discard
   information consumers may still want to read.

   If a use case ever needs multiple disjoint outer mutations per
   cycle, the fix is not to disable cleanup but to change mutation
   tracking from a depth counter to an *evaluation-time* stamp: record
   the cycle in which cleanup last ran, and let the start-of-mutation
   logic skip work whenever that stamp matches the current
   ``evaluation_time``. Subsequent outer mutations within the same
   cycle would then be no-ops for cleanup while still flushing user-
   visible changes on close. This is a deliberate fallback path; the
   current depth-counter implementation is correct as long as the
   one-outer-mutation-per-cycle assumption holds.

One specific cross-kind read-only view is worth calling out:
``MapView`` exposes ``key_set()``, which returns a read-only
``SetView`` over the map's keys. Callers can iterate or query keys
with the same surface they would use for a standalone set, without
copying or materialising a second container. The view is read-only
because the keys belong to the map; structural changes go through the
map's mutable view.

Specialised Views
-----------------

Each kind has a specialised **read-only** view that adds kind-specific
access on top of ``ValueView``. Views are still two-word handles —
they hold the same ``(binding, data)`` context as ``ValueView`` — and
most share an ``IndexedValueView`` base for the kinds that are
addressed positionally.

The base specialised views never expose mutation methods. Mutation
goes through a separate **mutable** view obtained from the read-only
view by calling ``begin_mutation()`` (see *Read-Only and Mutable
Views* above). The mutable view is closed with ``end_mutation()`` (or
its destructor as the RAII safety net) and is the only place
per-element ``set`` / ``insert`` / ``remove`` / ``push_back`` style
operations exist. Mutable views are only meaningful for the
slot-store-backed time-series variants; for the compact value-layer
storage there is no mutable counterpart — replacement happens at the
``Value`` level (whole-container copy/move).

Read-only views
~~~~~~~~~~~~~~~

``IndexedValueView``
    Base for tuple, bundle, list, cyclic buffer, and queue views.
    Adds ``size()``, ``at(index)``, ``operator[](index)``, and a
    forward iterator over child ``ValueView`` handles. Resolves the
    per-element ``ValueTypeMetaData`` either from the field array
    (tuple, bundle) or from the homogeneous element type (list,
    cyclic buffer, queue). Read-only — per-index ``set`` is on the
    mutable variant.

``TupleView``
    Index-addressed positional fields. Field types may differ.

``BundleView``
    Named tuple. Adds ``at(name)`` / ``operator[](name)`` for name-
    addressed access on top of the indexed surface.

``ListView``
    ``size()``, ``at(index)``, iteration. Read-only.

``CyclicBufferView``
    Read surface plus ``capacity()``, ``empty()`` / ``full()`` and
    ``head`` (the ring's logical start). Iteration is in ring order.

``QueueView``
    Read surface plus ``size()``, ``empty()`` / ``full()``,
    ``front()`` (returns a child view of the front element).

``SetView``
    ``contains(key)``, iteration over members. ``contains`` is part of
    the erased ops contract and must be average O(1) for set
    implementations. Set comparison orders by size when sizes differ;
    same-sized sets compare equivalent when their members match and
    unordered otherwise.

``MapView``
    ``contains(key)``, ``at(key)`` / ``operator[](key)``, iteration
    over ``(key, value)`` entries, ``key_set()`` returning a read-only
    ``SetView`` over the live keys. ``contains`` and ``at`` are part
    of the erased ops contract and must be average O(1) for map
    implementations.

Mutable views (slot-store-backed only)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each mutable counterpart is obtained from its read-only view via
``begin_mutation()`` and adds the mutation methods listed below; the
read surface stays available throughout the mutation scope. The
methods listed are *additions* — read-only methods on the base view
remain accessible through the mutable view.

Status: these mutable counterparts are design/API targets for the
time-series layer and are not implemented by the compact value-layer
storage.

``MutableTupleView``
    Adds ``set(index, value)``.

``MutableBundleView``
    Adds ``set(index, value)``, ``set(name, value)``.

``MutableListView``
    Adds ``set(index, value)``, ``push_back(value)``, ``resize(n)``.

``MutableCyclicBufferView``
    Adds ``push_back(value)`` (replaces oldest when full),
    ``set(index, value)``.

``MutableQueueView``
    Adds ``push(value)``, ``pop()``.

``MutableSetView``
    Adds ``insert(key)``, ``remove(key)``.

``MutableMapView``
    Adds ``set_item(key, value)`` (insert-or-replace), ``remove(key)``.
    The keys remain owned by the map; structural changes flow through
    the mutable view rather than through the ``key_set()`` accessor
    (which always returns a read-only ``SetView``).

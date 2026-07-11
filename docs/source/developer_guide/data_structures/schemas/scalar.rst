Scalar Schemas
==============

A scalar schema is the value-layer's concept identity for a non-time-
series payload. Scalar schemas are represented at runtime by
``ValueTypeMetaData``. The struct carries:

- ``kind`` — the value kind (one of the nine listed below),
- capability flags — for example, whether the value is hashable,
  comparable, or buffer-exposable,
- component fields — for composite kinds, the field list (bundle/tuple)
  or the element/key type pointers (list/set/map),
- the canonical name — used for Python-bridge interop and diagnostic
  output.

There are no layout fields on the schema. Size, alignment, and field
offsets live on the matching ``StoragePlan``. That separation lets two
implementations of the same kind share one schema while keeping their
own storage strategies (see *Allocation, Plans and Ops > Erased
Types* — the multi-implementation rationale).

Value Kinds
-----------

A scalar schema has one of nine kinds, recorded on its
``ValueTypeMetaData``:

``Atomic``
    A single scalar: integer, floating-point, boolean, string, or one of the
    date/time scalars.

    **Enums** are named atomic scalars (ruling 2026-07-10: a first-class
    value kind, never a python-object ride-along). An enum schema is
    interned NOMINALLY by its type name — like a named bundle — and
    carries its ordered member table on the schema's ``fields`` array
    (member name plus the member's ASSIGNED integer value; the
    ``ValueTypeFlags::Enum`` trait marks the meta). The stored payload is
    the assigned integer value itself, so the generic comparison ops
    order enums exactly as hgraph does (`.value` comparison) with no
    enum-specific kernels; ``to_string`` renders the member NAME, and
    the python conversion maps to the REGISTERED python ``Enum`` class
    (the bridge registers each class against its interned meta — the
    ``CmpResult``/``DivideByZero`` slot mechanism generalised).
    Re-registering the same name requires an identical member table.

``Tuple``
    Fixed-arity ordered fields, accessed by index. Field types may differ.

``Bundle``
    Named tuple. Fields are accessed by name; field-name metadata is
    preserved in the storage plan.

    A bundle is either **un-named** or **named**. The schema content
    (the ordered ``(field_name, field_type)`` list) is stored on the
    un-named form; a named bundle layers a name on top of it and
    holds a borrowed pointer to the un-named schema. The two forms
    have distinct identity semantics:

    - Two un-named bundles with the same field list are the **same
      schema** (structural equality, single interned pointer).
    - Two named bundles with the same field list but different names
      are **distinct schemas** (nominal equality — name is part of
      identity).
    - A named bundle can be assigned values from an un-named bundle
      with the same field list, since the named form's structural
      content is exactly the un-named schema it wraps. The reverse
      direction is also allowed; only named ↔ named with different
      names is rejected.

    This split exists so structural shapes used by the runtime
    (delta bundles, tuple-of-fields algebra) can stay anonymous and
    de-duplicate freely, while user-defined records (``Trade``,
    ``Quote``) keep nominal identity even when their fields happen
    to coincide.

    *Python correspondence.* The Python ``CompoundScalar`` class
    (``hgraph._types._scalar_types.CompoundScalar``) maps onto the
    C++ ``Bundle`` kind. A user-defined ``CompoundScalar`` subclass
    — a class with ``__meta_data_schema__`` declaring its fields —
    is the *named* C++ ``Bundle`` whose name is the Python class
    name; Python's ``UnNamedCompoundScalar`` (anonymous compounds
    produced by ``compound_scalar(**kwargs)``) is the *un-named*
    form. The nominal-vs-structural identity rule holds on both
    sides: two ``CompoundScalar`` subclasses with the same fields
    but different class names are distinct schemas, and any
    ``UnNamedCompoundScalar`` is structurally equal to any same-
    shape compound. The Python→C++ bridge for an expanded compound
    goes through ``_hgraph.value.get_compound_scalar_type_meta(
    fields, py_type, name)``, which interns the named ``Bundle`` in
    the C++ ``TypeRegistry``.

``List``
    Ordered sequence of one element type. May be ``fixed_size`` or dynamic.
    A dynamic list is **immutable** (built once) by default; a list schema
    carrying ``ValueTypeFlags::Mutable`` (from ``TypeRegistry::mutable_list``)
    is instead **structurally mutable** — backed by growable slot-store
    storage and supporting ``push_back`` / ``set`` / ``erase`` / ``pop_back``
    / ``clear`` through ``MutableListView``. Mutability is an explicit schema
    axis, so the two forms intern separately and never collide.

``Set``
    Unordered collection of unique elements of one type. Immutable (built once)
    by default; a set schema carrying ``ValueTypeFlags::Mutable`` (from
    ``TypeRegistry::mutable_set``) is **structurally mutable** — backed by a
    ``KeySlotStore`` and supporting ``add`` / ``remove`` / ``clear`` through
    ``MutableSetView``, with order-independent hash-based equality. Mutability is
    an explicit schema axis that interns separately. A mutable set is the field
    type of a ``TSS`` delta bundle in the testing toolkit.

``Map``
    Key/value mapping with one key type and one value type. Immutable
    (built once) by default; a map schema carrying ``ValueTypeFlags::Mutable``
    (from ``TypeRegistry::mutable_map``) is **structurally mutable** — backed
    by a ``KeySlotStore`` + co-indexed ``ValueSlotStore`` and supporting
    ``set_item`` / ``remove`` / ``clear`` through ``MutableMapView``. As with
    the list, mutability is an explicit schema axis that interns separately.
    A mutable ``Map<string, Any>`` is the backing for the runtime
    ``GlobalState`` injectable; an **immutable** ``Map<int, T>`` (built once
    via ``MapBuilder``) is the delta value of a ``TSL`` time-series in the
    testing toolkit. Map **values** support element validity: an entry may
    carry an UNSET value (``MapBuilder::set_item_unset``) — Python's
    ``None``-valued mapping entry, JSON ``null`` — read back as an absent
    value view / ``None``; keys are never nullable.

``CyclicBuffer``
    Fixed-capacity ring buffer of one element type.

``Queue``
    FIFO queue with capacity and ordering.

``Any``
    A type-erased "any value" box. **Compile-time schema knowledge ends
    here**: an ``Any`` carries no element/key/field references — it is
    unconstrained — and its storage is an embedded owning ``Value`` whose
    own schema is only known at run time. The box is empty until a value is
    assigned; assigning copies the value in, after which the contained
    ``Value`` carries its own schema and owns its own memory. ``Any`` is the
    singleton schema returned by ``TypeRegistry::any()``.

    ``Any`` is the slot type that lets value-layer containers hold
    heterogeneous contents (a ``List<Any>`` or ``Map<K, Any>`` where each
    element may have a different concrete schema), and is the eventual
    analogue of a generic / Python ``object``. ``hash`` / ``equals`` /
    ``compare`` / ``to_string`` delegate to the contained value, with an
    explicit empty state (empty equals empty; empty orders before any
    non-empty value; an empty box stringifies as ``"None"``).

Each kind has a corresponding specialised view at the runtime layer
(``TupleView``, ``BundleView``, ``ListView``, ``SetView``, ``MapView``,
``CyclicBufferView``, ``QueueView``, ``AnyView`` / ``MutableAnyView``);
those views are described under *Allocation, Plans and Ops > Erased Types*.

Composite Schema Synthesis
--------------------------

Composite schemas are synthesised on first use. ``TypeRegistry``
exposes typed factory methods — ``tuple(...)``, ``un_named_bundle(...)``,
``bundle(...)``, ``list(...)``, ``set(...)``, ``map(...)``,
``cyclic_buffer(...)``, ``queue(...)`` — each backed by an internal
``InternTable<TypedKey, ValueTypeMetaData>`` keyed by structural
inputs. Calling ``map(string_meta, int_meta)`` twice returns the
same pointer; the second call resolves directly off the intern
table.

The bundle pair reflects the named/un-named distinction described
under *Value Kinds*:

``un_named_bundle({(name, type)...})``
    Interns by the structural key (the ordered field list). Returns
    the canonical un-named schema for that field list.

``bundle(name, {(name, type)...})``
    Requires a name. Internally synthesises the un-named bundle for
    the field list, then interns a named wrapper keyed by
    ``(name, un_named_pointer)``. The named schema records its name
    and a borrowed pointer to the un-named schema rather than a
    second copy of the field list, so the field-list content lives
    in exactly one place.

The typed keys are deliberately structural rather than string-based.
For example, the map registry keys on ``MapKey{key_meta_ptr,
value_meta_ptr}`` rather than a synthesised ``"Map<string,int>"``
name. This keeps key construction cheap and avoids stringification
mismatches across composition orders.

The typed keys are deliberately structural rather than string-based.
For example, the map registry keys on ``MapKey{key_meta_ptr,
value_meta_ptr}`` rather than a synthesised ``"Map<string,int>"`` name.
This keeps key construction cheap and avoids stringification mismatches
across composition orders.

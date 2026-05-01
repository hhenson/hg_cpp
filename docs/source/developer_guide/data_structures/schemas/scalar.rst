Scalar Schemas
==============

A scalar schema is the value-layer's concept identity for a non-time-
series payload. Scalar schemas are represented at runtime by
``ValueTypeMetaData``. The struct carries:

- ``kind`` — the value kind (one of the eight listed below),
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

A scalar schema has one of eight kinds, recorded on its
``ValueTypeMetaData``:

``Atomic``
    A single scalar: integer, floating-point, boolean, string, or one of the
    engine time/date scalars.

``Tuple``
    Fixed-arity ordered fields, accessed by index. Field types may differ.

``Bundle``
    Named tuple. Fields are accessed by name; field-name metadata is
    preserved in the storage plan.

``List``
    Ordered sequence of one element type. May be ``fixed_size`` or dynamic.

``Set``
    Unordered collection of unique elements of one type.

``Map``
    Key/value mapping with one key type and one value type.

``CyclicBuffer``
    Fixed-capacity ring buffer of one element type.

``Queue``
    FIFO queue with capacity and ordering.

Each kind has a corresponding specialised view at the runtime layer
(``TupleView``, ``BundleView``, ``ListView``, ``SetView``, ``MapView``,
``CyclicBufferView``, ``QueueView``); those views are described under
*Allocation, Plans and Ops > Erased Types*.

Composite Schema Synthesis
--------------------------

Composite schemas are synthesised on first use. ``TypeRegistry`` exposes
typed factory methods — ``tuple(...)``, ``bundle(...)``, ``list(...)``,
``set(...)``, ``map(...)``, ``cyclic_buffer(...)``, ``queue(...)`` —
each backed by an internal ``InternTable<TypedKey, ValueTypeMetaData>``
keyed by structural inputs. Calling ``map(string_meta, int_meta)`` twice
returns the same pointer; the second call resolves directly off the
intern table.

The typed keys are deliberately structural rather than string-based.
For example, the map registry keys on ``MapKey{key_meta_ptr,
value_meta_ptr}`` rather than a synthesised ``"Map<string,int>"`` name.
This keeps key construction cheap and avoids stringification mismatches
across composition orders.

Schemas
=======

A *schema* is the runtime's concept-level description of a type. Schemas
answer the question *what is this thing?* — they record the kind, the
component types, capability flags, and the pre-computed properties used
during wiring and evaluation. They do **not** describe how that thing is
laid out in memory or how it behaves at runtime; those concerns live in
*Allocation, Plans and Ops*.

This chapter covers the two schema families:

- **Scalar schemas** describe the value-layer payload kinds (atomic,
  tuple, bundle, list, set, map, cyclic buffer, queue).
- **Time-series schemas** describe the runtime-side wrappers (TS, TSS,
  TSD, TSL, TSW, TSB, REF, SIGNAL) that participate in graph evaluation.

The two families are separate types but share the same vocabulary
introduced in *Core Concepts*: a schema is a Plan-independent identity,
interned through the registry, and consulted everywhere a value or
time-series flows through the system.

Responsibilities
----------------

The schema layer is responsible for:

- representing scalar and time-series type identity,
- validating node input and output schemas during wiring,
- supporting generic resolution (template-style schema variables),
- exposing enough metadata for Python wiring compatibility,
- keeping runtime evaluation independent of Python type objects.

Schemas are immutable once registered. Two structurally identical
schemas always resolve to the same interned pointer, so equality checks
on schemas are pointer comparisons.

Registries and Interning
------------------------

Schemas are kept alive — and de-duplicated — by registries that wrap the
generic ``InternTable<Key, Value>`` primitive introduced in
*Core Concepts*. Each registry exists only to expose a typed
convenience API on top of one ``InternTable`` instance, so the interning
contract is uniform: structurally equivalent inputs always resolve to
the same stable pointer, and any consumer can hold that pointer for the
artifact's lifetime without managing ownership.

The two relevant registries are:

``TypeRegistry``
    The single registry for both scalar and time-series schemas.
    Internally it owns one ``InternTable<TypedKey, TypeMetaData>`` per
    composite shape (atomic, tuple, bundle, list, set, map, sized, TSD,
    TSL, TSW, TSB) and a flat name-keyed map for atomic scalar
    registrations. Calling ``ts(int_meta)`` or ``tsd(string_meta,
    ts_int_meta)`` returns the same pointer no matter how the request
    was assembled.

``ValuePlanFactory`` / ``TSValuePlanFactory``
    Schema → plan mapping. Atomic plans are paired with their schema at
    registration; composite plans are synthesised on first use and
    cached. The factory is logically a registry of plans keyed by
    schema, so it is treated as part of the schema infrastructure even
    though plans themselves belong to the next chapter.

A schema's identity is its pointer. Two schemas with the same kind,
component types, and metadata fields produce the same key tuple, hash
to the same bucket, and resolve to the same ``TypeMetaData *``. This
holds for nested compositions: ``TSD<string, TSL<TS<double>>>`` is a
single interned schema, and the registry never re-synthesises any of
its components on subsequent lookups.

What this chapter does *not* cover
----------------------------------

The schema layer is intentionally narrow. It does not own:

- node signatures (input/output port lists, type variables, defaults) —
  these will be described under *Wiring* once that chapter is filled
  out;
- graph boundary descriptors (parent/nested graph connection shapes) —
  same;
- canonical Python type metadata conversion — covered separately under
  *Python Integration*;
- error messaging for failed resolution and serialisation/debug
  representation — open topics, see *Refinement Topics*.

.. toctree::
   :maxdepth: 2

   scalar
   time_series

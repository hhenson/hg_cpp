Time-Series Schemas
===================

A time-series schema describes the runtime-side wrapper that lets a
value participate in graph evaluation. It carries the universal
time-series contract — modification time, validity, delta tracking — on
top of a value-layer payload. The schema is represented at runtime by
``TSValueTypeMetaData``; the actual memory layout and storage strategy
live on the matching plan and are described in *Allocation, Plans and
Ops > Time-Series Plans and Ops*.

This chapter describes:

- the eight time-series kinds and what they wrap,
- the tick-level contract every kind exposes,
- the per-element modification model,
- the schema-level pre-computation of ``value_schema`` and
  ``delta_value_schema`` for each kind.

Time-Series Kinds
-----------------

``TS[T]``
    Scalar time-series of one atomic type.

``TSB``
    Named bundle of time-series fields.

``TSL``
    Ordered list of one time-series type, fixed-size or dynamic.

``TSS``
    Unordered set of one scalar type.

``TSD``
    Keyed dictionary with scalar keys and time-series values.

``TSW``
    Sliding window of one scalar type.

``REF``
    Reference to a time-series target. The target is resolved through
    binding state rather than carried inline.

``SIGNAL``
    Boolean tick — used purely as a notification primitive.

Tick Semantics
--------------

Every time-series, regardless of kind, exposes the same five tick-level
properties. These are the universal time-series contract. Inputs and
outputs both expose them; on an input, the values reflect the bound
output. The kind-specific view APIs add container-specific access on
top of this base, but the base is always present.

``value``
    The cumulative current state of the time-series. For a ``TS[T]`` this
    is the scalar that has been written; for a ``TSB`` it is the bundle
    of all currently valid fields; for a ``TSD`` it is the map of all
    live keys to their current values. ``value`` persists across ticks
    until it is replaced or extended.

``delta_value``
    What changed in the current tick. ``delta_value`` is ``None`` when
    the time-series did not tick in the current engine cycle. When it
    did tick, the shape depends on the kind:

    - ``TS[T]`` — ticks are atomic value replacements with no diffing,
      so ``delta_value`` is the new scalar (the same object as
      ``value``). Setting a ``TS[T]`` to its current value still
      produces a delta.
    - ``TSB`` — modified fields only.
    - ``TSL`` — modified elements with their indices.
    - ``TSS`` — added and removed sets.
    - ``TSD`` — added entries, removed keys, and per-key modified values.
    - ``TSW`` — the element added in the current tick (if any).

    ``delta_value`` is logically present only for the engine cycle of
    the change. The runtime is not required to physically reset it at
    the end of the cycle, and in fact prefers lazy cleanup: removed
    ``TSS`` and ``TSD`` slot entries live past their logical destruction
    so the current cycle's delta can be read, and physical erase happens
    later at the next outermost ``begin_mutation()``. A subsequent
    engine cycle querying ``delta_value`` returns ``None`` if no new
    change has occurred, or the new change if one has.

``modified``
    Whether the time-series ticked in the current evaluation cycle. For
    container kinds this is recursive: a container is modified if any
    of its elements is modified. Equivalent to
    ``last_modified_time == evaluation_time``. Must be an O(1) query —
    consumers test it on every active input on every tick, so it is
    read directly from per-TS state, never derived by scanning children.

``valid``
    Whether the time-series has ever been ticked. Equivalent to
    ``last_modified_time != MIN_DT`` (``MIN_DT`` is the sentinel for
    never-modified). Note that ``valid`` does *not* imply the
    time-series holds any content: a ``TSS`` or ``TSD`` that ticked
    with no members is valid but empty. Also an O(1) query.

``last_modified_time``
    The engine time at which the time-series was last modified. Stored
    once per time-series instance in the per-TS runtime state tree.
    Defaults to ``MIN_DT`` until the first tick.

Per-Element Modification
~~~~~~~~~~~~~~~~~~~~~~~~

For composite kinds, a consumer often needs to ask not just *did this
container tick?* but *which children ticked?*. The answer comes from
each child's own time-series state: every time-series instance — at
every level of nesting — carries its own ``last_modified_time`` as
part of the per-TS runtime state tree. A collection like TSD holds a
vector of slots where each slot contains the full time-series state of
that element, including the child's ``last_modified_time``. Walking
the slots and inspecting each child's ``last_modified_time`` is how
the container reports which elements changed in the current cycle —
there is no separate side structure tracking this.

The container's own ``last_modified_time`` is updated whenever any
child ticks, which is what makes the recursive container-level
``modified`` query a single field read rather than a tree walk. A
slot id is therefore both a path identifier and the key into the
child's full time-series state.

Value and Delta-Value Schemas
-----------------------------

The conceptual ``value`` and ``delta_value`` shapes described under
Tick Semantics surface as two pre-computed properties on every TS
schema (``TSValueTypeMetaData``):

- ``value_schema`` — the value-layer schema of the time-series's
  runtime ``value``.
- ``delta_value_schema`` — the value-layer schema of its
  ``delta_value`` (the per-tick change set).

Both are populated during schema registration and read as plain field
accesses — consumers never need to recompute them. The same composite
schemas the value layer interns back the underlying storage, so two TS
schemas with the same value/delta shapes share the same value-layer
schema pointers.

Per-kind mapping:

.. list-table::
   :header-rows: 1
   :widths: 20 35 45

   * - Kind
     - ``value_schema``
     - ``delta_value_schema``
   * - ``TS<T>``
     - ``T``
     - ``T``
   * - ``TSS<T>``
     - ``Set<T>``
     - ``Bundle{added: Set<T>, removed: Set<T>}``
   * - ``TSD<K, V>``
     - ``Map<K, V.value_schema>``
     - ``Bundle{added: Map<K, V.delta>, removed: Set<K>, modified: Map<K, V.delta>}``
   * - ``TSL<T>``
     - ``List<T.value_schema, fixed_size>``
     - ``Map<int64, T.delta_value_schema>``
   * - ``TSW<T>`` (tick)
     - ``List<T, period>``
     - ``T``
   * - ``TSW<T>`` (duration)
     - ``List<T, 0>`` (dynamic)
     - ``T``
   * - ``TSB{f...}``
     - ``Bundle{f: f.value_schema...}``
     - ``Bundle{f: f.delta_value_schema...}``
   * - ``REF<T>``
     - ``TimeSeriesReference``
     - ``TimeSeriesReference``
   * - ``SIGNAL``
     - ``bool``
     - ``bool``

A few notes on the cells that aren't immediate:

- ``REF<T>`` is conceptually ``TS<TimeSeriesReference>``: the reference
  token itself is the value, and dereferencing to read the target's
  value is a runtime concern handled through binding state, not
  through ``value_schema`` or ``delta_value_schema``. Note the
  asymmetry: every ``REF<T>`` *schema* (the ``TSValueTypeMetaData``)
  is unique per ``T`` and continues to carry the wrapped target
  schema via ``referenced_ts()`` — useful for binding validation,
  introspection, and ``dereference()``. What is shared across all
  ``REF<T>`` metadata pointers is only the ``value_schema`` and
  ``delta_value_schema``: both alias the canonical
  ``TimeSeriesReference`` atomic, the C++ value type that backs a
  reference token at runtime.
- ``TSL`` keys its delta on ``int64`` because the slot id (an integer
  index) is the universal path identifier into a slot store. The
  registry auto-registers an ``int64`` scalar the first time it
  synthesises a TSL delta schema.
- ``TSD`` and ``TSS`` deltas are bundles because the per-tick change
  set has multiple categories (added / removed / modified). Each
  category is its own value-layer container.
- ``TSW`` (tick) has a fixed-size list as ``value_schema`` because the
  rolling window length is known up front; duration-based windows fall
  back to a dynamic list since the count of elements per window varies
  with tick rate.

Recursion is automatic: the metadata for a nested ``TSD<string,
TSL<TS<double>>>`` reads its inner ``TSL``'s ``delta_value_schema``
directly off the inner schema, so the registry never has to recompose
known schemas.

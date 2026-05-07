Static Schema
=============

Static schema is the C++-friendly way to describe a schema. The
"dynamic" path described in *Scalar Schemas* and *Time-Series Schemas*
goes through ``TypeRegistry`` factory calls (``ts(int_meta)``,
``tsd(string_meta, ts_int_meta)``, …) and returns interned metadata
pointers at runtime. Static schema lets a C++ developer express the
same shapes as **types** at compile time:

.. code-block:: cpp

   using PriceTick = TSB<"PriceTick",
                         Field<"bid",   TS<double>>,
                         Field<"ask",   TS<double>>,
                         Field<"size",  TS<int>>>;

   using QuoteFeed = TSD<std::string, PriceTick>;

The compiler carries the shape; a small set of *descriptor* traits
turn that compile-time type into the matching runtime metadata
pointer (``TSValueTypeMetaData *``) by calling into ``TypeRegistry``.
Descriptors deliberately do **not** maintain their own static caches:
all canonicalisation and pointer stability comes from the registry's
intern tables. A node implementation written against these markers
gets both type safety at the call site and one canonical interned
schema at runtime.

This page describes the compile-time vocabulary, the descriptor
bridge to the runtime registry, and the design choices that follow
from the C++-first goal. The selector layer that pairs markers with
runtime views (``In<>``, ``Out<>``, ``State<>``, ``RecordableState<>``)
is sketched at the end and will land alongside the view types.

Why a compile-time vocabulary
-----------------------------

The dynamic registry path is the source of truth at runtime, but it
has two costs at the call site:

1. **Untyped pointers.** The factory returns ``const
   TSValueTypeMetaData *``. The compiler cannot tell whether two
   pointers describe the same shape; two callers must agree by
   convention.
2. **Order-of-construction sensitivity.** The factory must be
   reachable, the metadata must already be registered, and the
   composition must be assembled correctly each time.

Static schema folds both away. A type alias like ``QuoteFeed``
*is* the schema. The compiler enforces shape compatibility on
function signatures and returns, and the descriptor traits ensure
the runtime metadata pointer is canonical and shared with every
other use of the same alias.

Compile-time vocabulary
-----------------------

Every static-schema type is a small, mostly-empty struct template
whose template parameters carry the shape. They never instantiate at
runtime — they exist purely to drive the descriptor traits.

``fixed_string<N>``
    Compile-time string literal usable as a non-type template
    parameter. Used to embed names (field names, bundle names, type-
    variable names) directly into a schema type.

``TS<T>``
    Scalar time-series of one atomic ``T``. Mirrors the runtime ``TS``
    kind.

``TSS<T>``
    Time-series set of ``T``.

``TSD<K, V>``
    Time-series dict. ``K`` is a scalar (``int``, ``std::string``,
    …); ``V`` is itself a static-schema time-series type.

``TSL<T, N = 0>``
    Time-series list of ``T`` (a static-schema time-series type).
    ``N == 0`` is the dynamic form; ``N > 0`` is fixed-size.

``TSW<T, period, min_period = 0>``
    Tick-based sliding window. (Duration-based windows are not yet
    expressible as a compile-time type; use the runtime registry for
    that case.)

``REF<TSchema>``
    Reference to a target time-series schema.

``SIGNAL``
    The signal kind. Has no parameters.

``Field<Name, TSchema>``
    Named field used inside ``Bundle`` / ``UnNamedBundle`` /
    ``TSB`` / ``UnNamedTSB``. ``Name`` is a ``fixed_string``; the
    schema is either a value-layer type (for ``Bundle``) or a
    time-series schema (for ``TSB``).

``Bundle<Name, Fields...>`` / ``UnNamedBundle<Fields...>``
    Value-layer compound. ``Bundle`` has a name (nominal identity);
    ``UnNamedBundle`` is structural. Maps to the runtime
    ``bundle(name, fields)`` and ``un_named_bundle(fields)``
    factories respectively, with the same nominal-vs-structural
    identity rules described in *Scalar Schemas > Bundle*.

``TSB<Name, Fields...>`` / ``UnNamedTSB<Fields...>``
    Time-series compound. Same named/un-named split as ``Bundle``.

``ScalarVar<Name, Constraints...>`` / ``TsVar<Name, Constraints...>``
    Type variables. Used when a node implementation is generic over a
    scalar or time-series type. Resolution happens at wiring time;
    the descriptor traits report ``is_concrete() == false`` for
    schemas that contain unresolved variables.

Bridge to the runtime registry
------------------------------

The descriptor traits are the single bridge between the compile-time
vocabulary above and the runtime ``TypeRegistry``. They are the only
place a static-schema type touches the registry.

``scalar_descriptor<T>``
    For a scalar type ``T``, exposes:

    - ``is_concrete()`` — ``true`` for concrete scalar types, ``false``
      for ``ScalarVar``.
    - ``value_meta()`` — returns the canonical
      ``ValueTypeMetaData *`` for the scalar by calling
      ``TypeRegistry::register_scalar<T>(...)``.

``schema_descriptor<TSchema>``
    For a time-series schema type, exposes:

    - ``is_concrete()`` — recurses through component schemas.
    - ``ts_meta()`` — returns the canonical ``TSValueTypeMetaData *``
      for ``TSchema`` by resolving the shape through ``TypeRegistry``.

``field_descriptor<Field<Name, TSchema>>``
    Exposes the field's static name and forwards
    ``ts_meta()`` (for TSB fields) / ``value_meta()`` (for Bundle
    fields) to the appropriate inner descriptor.

The descriptor specialisations cover every marker type. Adding a new
schema marker is a matter of adding a corresponding descriptor
specialisation; the rest of the registry path is unchanged.

Example (rendered as the doc lands alongside an actual implementation):

.. code-block:: cpp

   using PriceTick = TSB<"PriceTick",
                         Field<"bid",  TS<double>>,
                         Field<"ask",  TS<double>>>;

   const TSValueTypeMetaData *meta =
       schema_descriptor<PriceTick>::ts_meta();
   // meta is the same pointer as
   //   registry.tsb("PriceTick", {{"bid", registry.ts(...)}, ...})

The registry intern tables are the only cache. Repeated descriptor
calls may re-enter ``TypeRegistry``, but equivalent shapes resolve to
the same canonical metadata pointer. This keeps static descriptors
correct across test-only registry resets and avoids a second lifetime
model outside the registry.

Generic schemas
---------------

A schema that contains a ``TsVar`` or ``ScalarVar`` is *generic*. The
descriptor's ``is_concrete()`` returns ``false`` and ``ts_meta()`` /
``value_meta()`` returns ``nullptr``. Wiring resolves the variable to
a concrete type before constructing a runtime node, at which point a
*resolved* schema (with the variable replaced by the concrete type)
becomes concrete and looks up its registry pointer normally.

This is the C++ counterpart of Python type-variable resolution. The
binding from generic to concrete is the *resolution substitution*
described in *Schemas > Node Schemas > Generic Resolution*.

Selector wrappers (planned)
---------------------------

The selector layer wraps a static-schema marker with a runtime view,
so a node ``eval`` can take a typed handle as a parameter:

.. code-block:: cpp

   struct SumNode
   {
       static constexpr auto name = "sum";

       static void eval(In<"lhs", TS<int>> lhs,
                        In<"rhs", TS<int>> rhs,
                        Out<TS<int>>      out)
       {
           out.set(lhs.value() + rhs.value());
       }
   };

The selectors planned are:

- ``In<Name, TSchema, Policies...>`` — typed input view, with
  optional ``InputActivity`` and ``InputValidity`` policy flags.
- ``Out<TSchema>`` — typed output view, plus the current
  ``evaluation_time`` for marking modifications.
- ``State<TSchema, Name>`` — typed handle into node-local state.
- ``RecordableState<TSchema, Name>`` — typed handle into the
  recordable-state output.
- ``ScalarArg<Name, T>`` — named scalar parameter injected from the
  wiring layer.

These depend on the ``TSInputView`` / ``TSOutputView`` machinery and
the node-builder, neither of which has been ported yet. The marker +
descriptor layer described above is independent of those and is
landing first.

Status
------

Today: ``fixed_string``, marker types (``TS``, ``TSS``, ``TSD``,
``TSL``, ``TSW`` tick-only, ``REF``, ``SIGNAL``, ``Field``, named &
un-named ``Bundle`` / ``TSB``, ``ScalarVar``, ``TsVar``), and the
``scalar_descriptor`` / ``schema_descriptor`` / ``field_descriptor``
traits. These build on top of the live ``TypeRegistry`` API
(including the named/un-named bundle split) so static schemas
register and resolve identically to schemas constructed by direct
factory calls.

Deferred until the relevant runtime layer lands: selector wrappers,
duration-based ``TSW``, Python-export bridge, generic-resolution
substitution machinery.

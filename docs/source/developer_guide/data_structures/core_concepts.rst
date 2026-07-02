Core Concepts
=============

Every data structure in the runtime is built from the same six-element
vocabulary. Defining it once lets each layer below describe itself by naming
the concrete Plan, Schema, Ops, and Builder it uses, instead of restating the
pattern.

The vocabulary is grouped into three roles: a *concept* group that describes
what something is, a *resolution* group that picks an implementation for it,
and a *data* group that holds and exposes the actual instance.

.. mermaid::

   flowchart LR
      subgraph interned["Interned — stable addresses, program lifetime"]
         Schema["Schema<br/>(layout-free type identity)"]
         Plan["Plan<br/>(size/alignment/offsets + lifecycle ops)"]
         Ops["Ops<br/>(struct of fn-ptrs)"]
      end
      Builder["Builder<br/>(the only place Schema binds to a Plan + Ops)"]
      Value["Value<br/>(owns memory; constructed in place by the Plan)"]
      View["View<br/>(borrows memory + Ops)"]

      Schema --> Builder
      Plan --> Builder
      Ops --> Builder
      Builder -->|"constructs into pre-allocated storage"| Value
      Value -->|"borrow"| View

Concept Group
~~~~~~~~~~~~~

``Plan``
    Memory-layout primitive. A Plan describes the size, alignment, and
    field layout for one data structure, and pairs that with the
    construction, copy / move, and destruction operations needed to
    bring an instance to life in already-allocated memory. A Plan does
    **not** allocate or deallocate memory and carries no reference to
    any allocator; allocators consume a Plan's size and alignment
    separately to acquire storage. A Plan does not carry semantic
    meaning beyond memory layout and lifecycle.

``Schema``
    Independent, generic description of a concept. A Schema describes what a
    time-series, node, or higher-level structure *is*. It is not tied to any
    particular memory layout or behaviour, so multiple implementations of the
    same concept share one Schema.

``Ops``
    Struct of function pointers that exposes behaviour for a data structure.
    The first parameter of every operation is always a pointer to the memory
    representing the structure; remaining parameters follow as needed. An
    Ops table is the type-erasure vehicle for one implementation of a
    concept.

Plan, Schema, and Ops are plain C++ structs and are immutable once
constructed.

Resolution Group
~~~~~~~~~~~~~~~~

``Builder``
    Construction role for a concept. A Builder resolves, or is given, the
    Schema / Plan / Ops / Binding needed to construct a concrete runtime
    object. The lifetime of a Builder depends on which layer it belongs to:

    - A **reusable builder** is a cached blueprint. It binds the resolved
      implementation once and can construct multiple runtime instances.
      Time-series value builders, node builders, and graph builders are in
      this category. Graph builders also pull double duty for nested graphs:
      the parent graph can retain the child graph builder as the reusable
      template for each nested instance.
    - A **value builder** is an instance assembler. It owns mutable scratch
      storage while a specific scalar/value-layer value is being assembled,
      then produces one ``Value`` on ``build()``. It is not a canonical
      implementation object and is not intended to be cached for repeated
      construction of the same value.

    A Builder may itself be type-erased so that generic construction code can
    hold builders for unrelated concepts uniformly.

Anything that needs a runtime object for a given Schema goes through the
appropriate Builder category for that layer.

Data Group
~~~~~~~~~~

``Value``
    Holds the data described by a Plan. By definition, Values are
    type-erased storage. A Value carries only the minimum behaviour needed
    to live in a container: destruction, copy and move construction and
    assignment, equality, and hash. Reads, writes, comparisons over content,
    and iteration are exposed through a View, never directly on the Value.
    A Value owns its allocator separately from its Plan: the Plan
    contributes size and alignment, the allocator owns the storage,
    and the Plan's lifecycle ops construct and destruct into that
    storage.

``View``
    Constructed from an existing Value. A View references the Value's
    memory and pairs it with the corresponding Ops table so that behaviour
    can be invoked. A View carries no payload of its own; it is a
    lightweight reference.

Interning
~~~~~~~~~

Plans, Schemas, Ops tables, Bindings, and reusable Builders are *interned*:
there is only ever one true instance of each, kept alive by an intern table
that returns stable references for structurally equivalent inputs. Any
consumer—a View, a Builder, a node, a graph—holds a borrowed pointer to its
Plan, Schema, Ops, Binding, or reusable Builder for the artifact's whole
lifetime without managing ownership.

The generic vehicle is ``InternTable<Key, Value>``, which guarantees stable
addresses for the values it owns.

Values and value builders are *not* interned. Each ``Value`` instance is
independent, and each value builder is local scratch state for one build.

The Universal Pattern
~~~~~~~~~~~~~~~~~~~~~

Every layer below instantiates the same shape:

.. mermaid::

   flowchart LR
      Schema[Schema<br/>concept]
      Plan[Plan<br/>memory layout]
      Ops[Ops<br/>function pointers]
      Builder[Builder<br/>resolved blueprint or instance assembler]
      Value[Runtime instance<br/>Value / time-series / node / graph]
      View[View<br/>exposes behaviour]

      Schema --> Builder
      Plan --> Builder
      Ops --> Builder
      Builder -->|constructs| Value
      Value -.referenced by.-> View
      Ops -.bound into.-> View

A Builder is fed by a Schema (what it is), a Plan (how its memory is laid
out), and an Ops table (how it behaves). Reusable builders cache that
resolved construction recipe and construct many instances; value builders
hold one in-progress value's scratch data and are consumed by ``build()``.
A View is built from an instance and pulls in the same Ops table so that
callers can act on the instance's data where that layer exposes views.

Subsequent sections name the concrete Schema, Plan, Ops, and Builder used
for each layer rather than re-introduce the pattern.

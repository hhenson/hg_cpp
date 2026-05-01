Core Concepts
=============

Every data structure in the runtime is built from the same six-element
vocabulary. Defining it once lets each layer below describe itself by naming
the concrete Plan, Schema, Ops, and Builder it uses, instead of restating the
pattern.

The vocabulary is grouped into three roles: a *concept* group that describes
what something is, a *resolution* group that picks an implementation for it,
and a *data* group that holds and exposes the actual instance.

Concept Group
~~~~~~~~~~~~~

``Plan``
    Memory-layout primitive. A Plan describes how to allocate, construct,
    destruct, and deallocate the memory for one data structure. It does not
    carry semantic meaning beyond memory lifecycle.

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
    The only place a Schema is bound to a specific Plan and Ops instance.
    A Builder resolves a Schema into a concrete implementation, then
    constructs Value instances. A Builder may itself be type-erased so that
    generic graph-construction code can hold builders for unrelated concepts
    uniformly.

Anything that needs a Value for a given Schema goes through a Builder.

Data Group
~~~~~~~~~~

``Value``
    Holds the data described by a Plan. By definition, Values are
    type-erased storage. A Value carries only the minimum behaviour needed
    to live in a container: deallocation, copy and move construction and
    assignment, equality, and hash. Reads, writes, comparisons over content,
    and iteration are exposed through a View, never directly on the Value.

``View``
    Constructed from an existing Value. A View references the Value's
    memory and pairs it with the corresponding Ops table so that behaviour
    can be invoked. A View carries no payload of its own; it is a
    lightweight reference.

Interning
~~~~~~~~~

Plans, Schemas, Ops tables, and Builders are *interned*: there is only ever
one true instance of each, kept alive by an intern table that returns stable
references for structurally equivalent inputs. Any consumer—a View, a
Builder, a node, a graph—holds a borrowed pointer to its Plan, Schema, Ops,
or Builder for the artifact's whole lifetime without managing ownership.

The generic vehicle is ``InternTable<Key, Value>``, which guarantees stable
addresses for the values it owns.

Values are *not* interned. Each instance is independent.

The Universal Pattern
~~~~~~~~~~~~~~~~~~~~~

Every layer below instantiates the same shape:

.. mermaid::

   flowchart LR
      Schema[Schema<br/>concept]
      Plan[Plan<br/>memory layout]
      Ops[Ops<br/>function pointers]
      Builder[Builder<br/>resolves Schema with chosen Plan + Ops]
      Value[Value<br/>holds data]
      View[View<br/>exposes behaviour]

      Schema --> Builder
      Plan --> Builder
      Ops --> Builder
      Builder -->|constructs| Value
      Value -.referenced by.-> View
      Ops -.bound into.-> View

A Builder is fed by a Schema (what it is), a Plan (how its memory is laid
out), and an Ops table (how it behaves). The Builder constructs Values.
A View is built from a Value and pulls in the same Ops table so that
callers can act on the Value's data.

Subsequent sections name the concrete Schema, Plan, Ops, and Builder used
for each layer rather than re-introduce the pattern.


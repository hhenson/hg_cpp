RFC 0004: Python-Owned Structured Scalars
=========================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-07-23
:Target: Next ``hg_cpp`` minor release

Summary
-------

Add a schema-bearing scalar category for values that remain ordinary Python
objects throughout graph execution. A Python dataclass used as a scalar type
has a nominal hgraph schema and supports reflection, typed attribute access,
construction, dispatch, equality, and the usual scalar graph operations, but
its runtime storage is one strong reference to the original Python object. It
is not materialised as a C++ ``Bundle`` and its fields are not exposed through
``BundleView``.

``CompoundScalar`` remains the C++-addressable structured scalar. This RFC
adds a separate Python-owned representation for application models whose
constructors, descriptors, inheritance, equality, or deliberately permissive
field values rely on Python behaviour.

Motivation
----------

The original Python hgraph treated a ``CompoundScalar`` value as a Python
object. Existing applications therefore rely on behaviours that a native
field-by-field representation cannot preserve without either rejecting the
value or silently changing it. Examples include:

* a dataclass annotation being descriptive rather than a runtime validator;
* custom ``__init__``, ``__post_init__``, property, descriptor, ``__eq__``,
  and ``__hash__`` implementations;
* Python subclasses and additional non-schema attributes;
* fields containing values accepted by the application even when their exact
  runtime class is narrower or wider than the annotation; and
* object identity being retained while Python code holds or forwards a value.

The current arbitrary-object fallback preserves a Python object but maps every
class to the same native ``object`` schema. It cannot provide reliable nominal
dispatch, field reflection, or wiring-time output resolution for
``getattr_``. Conversely, mapping the class to ``Bundle`` provides those
features by eagerly converting every declared field into native storage, which
is the source of the compatibility problem.

The required model therefore separates logical schema from physical layout:
C++ can know that a value is a particular record with particular declared
attributes, while the Python bridge alone owns and interprets its payload.

Goals
-----

* Preserve the exact Python instance, including its concrete runtime class and
  Python-defined behaviour.
* Extract an ordered, typed schema suitable for hgraph wiring, reflection, and
  operator resolution.
* Provide the structured scalar conveniences users expect, especially
  ``getattr_`` and construction from field time series.
* Avoid eager, recursive field validation or conversion when the whole object
  enters a time series.
* Keep native ``CompoundScalar`` semantics, storage, and C++ access unchanged.
* Make the ownership and performance boundary visible: C++ can inspect the
  logical schema but cannot directly address Python-owned fields.

Non-goals
---------

This RFC does not:

* make a Python-owned object usable in a Python-free process;
* expose ``PyObject`` or bridge-internal storage in the public C++ SDK;
* make an opaque record a ``Bundle`` or permit ``BundleView`` access;
* infer a portable wire format, Arrow layout, or JSON representation;
* observe in-place mutation as an hgraph tick;
* perform general runtime validation from Python type annotations; or
* change the representation of an existing ``CompoundScalar``.

Terminology and ownership
-------------------------

**Python-owned structured scalar**
   The Python-facing feature proposed by this RFC. Its declared class and
   fields participate in hgraph's type system, while its value remains one
   Python object.

**Opaque record**
   The binding-neutral core schema capability used to describe a nominal
   record whose fields are logical attributes rather than addressable storage
   components.

The opaque-record metadata and its interning rules belong to the hgraph core.
The class registry, ``PyObject`` lifetime, schema extraction, Python
conversion, and attribute evaluation belong to the optional Python bridge.
Pure C++ builds contain no Python class registrations and retain no dependency
on the Python runtime.

Python contract
---------------

Dataclasses
~~~~~~~~~~~

A concrete standard-library dataclass that does not derive from
``CompoundScalar`` is recognised lazily when used in an hgraph scalar
annotation:

.. code-block:: python

   from dataclasses import dataclass
   from hgraph import TS, graph

   @dataclass(frozen=True)
   class Quote:
       instrument: str
       bid: float
       ask: float

       @property
       def mid(self) -> float:
           return (self.bid + self.ask) / 2.0

   @graph
   def spread(quote: TS[Quote]) -> TS[float]:
       return quote.ask - quote.bid

``TS[Quote]`` is a distinct nominal hgraph type. Values read by Python nodes
are the original ``Quote`` instances; hgraph does not replace them with a
dictionary, namespace, reconstructed dataclass, or proxy.

``CompoundScalar`` detection takes precedence over dataclass detection. A
dataclass deriving from ``CompoundScalar`` therefore keeps the existing native
``Bundle`` representation.

Explicit registration
~~~~~~~~~~~~~~~~~~~~~

Non-dataclass application classes are opt-in:

.. code-block:: python

   from hgraph import register_python_object_type

   class LegacyQuote:
       instrument: str
       bid: float
       ask: float

   register_python_object_type(LegacyQuote)

``register_python_object_type`` accepts a class and, optionally, an ordered
``fields`` mapping for classes whose public attributes are not completely
described by resolved annotations. It returns the class, so it may also be
used as a decorator. Registration is process-wide and idempotent for the same
class and field schema. Registering the same class or qualified schema identity
with a conflicting field schema fails deterministically.

An arbitrary, unregistered, non-dataclass class retains the existing
``TS[object]``-style fallback. Merely adding ``__annotations__`` to a class
does not opt it into structured semantics.

Schema extraction
~~~~~~~~~~~~~~~~~

For a dataclass, field order and membership come from ``dataclasses.fields``.
This includes inherited and ``init=False`` data fields and excludes
``ClassVar`` and ``InitVar`` pseudo-fields. Resolved type hints provide each
field's logical hgraph scalar schema. Dataclass defaults, default factories,
keyword-only status, and constructor participation are retained as Python
construction metadata but do not change the field's scalar type.

The class is the nominal identity. Two classes with identical fields remain
different hgraph types. The diagnostic name is based on ``module`` and
``qualname``; the registry uses the retained class identity rather than the
rendered name as the Python-side key.

Direct self references are permitted because they are descriptive metadata and
do not imply recursive native storage. Parameterised generic dataclasses,
mutually recursive registrations, and automatic recognition of
PEP 681 dataclass-like frameworks are deferred. Such classes may use explicit
registration once an unambiguous resolved field mapping is available.

Reflection provides:

.. code-block:: python

   scalar_type(TS[Quote]) is Quote
   fields(Quote) == {
       "instrument": str,
       "bid": float,
       "ask": float,
   }
   fields(TS[Quote]) == fields(Quote)

The Python class is retained strongly for as long as its schema registration is
live. Test-only registry reset clears the association alongside the schema and
storage-plan registries.

Runtime type checking
~~~~~~~~~~~~~~~~~~~~~

A value entering ``TS[Quote]`` must be an instance of ``Quote``; a Python
subclass is accepted and remains that subclass. The bridge does not walk the
object's fields, compare their runtime types with annotations, copy them, or
normalise them when the whole object is assigned.

The field annotation is a wiring contract, not an eager object validator.
Conversion to a native field type happens only when an operator actually
extracts that attribute into its typed output. An incompatible extracted value
then fails at that boundary with the field name, declared type, actual type,
and owning class in the diagnostic.

``None`` retains hgraph's existing scalar meaning: it does not form a valid
value tick. For attribute access, a missing attribute or ``None`` produces no
tick unless the default-valued ``getattr_`` form is used.

C++ schema contract
-------------------

The core adds a nominal opaque-record schema facility. An opaque record:

* is scalar/atomic for storage and time-series purposes;
* carries ``ValueTypeFlags::OpaqueRecord``;
* may carry ordered ``ValueFieldMetaData`` entries as logical attributes;
* has nominal identity and cannot be structurally assigned to another opaque
  record merely because its fields match;
* is paired explicitly with an atomic storage plan and ``ValueOps`` table; and
* never provides indexed, tuple, or bundle views over those fields.

The metadata contract must no longer describe ``fields`` as exclusive to
``Tuple`` and ``Bundle``. Fields on an enum remain enum members, fields on a
``Bundle`` remain physical components, and fields on an opaque record are
descriptive attributes. Consumers must check the kind and capability flag
before interpreting the array.

The registry operation is conceptually:

.. code-block:: cpp

   opaque_record(
       qualified_name,
       ordered_fields,
       atomic_storage_binding);

It interns by nominal name and field schema, verifies idempotent
re-registration, and associates the result with the supplied atomic storage
binding. The Python bridge uses its private Python-object binding; that binding
is not installed as a public C++ payload type.

A C++ consumer can use the schema in generic wiring, inspect its name and
logical fields, and pass or compare its erased value through registered
operations. It cannot call ``checked_as<PythonClass>()``, obtain field offsets,
or use ``BundleView``. Operators that execute Python behaviour are registered
only when Python support is enabled.

This facility is separate from :doc:`rfc_0003_extension_scalar_registration`.
RFC 0003 associates a Python facade with extension-owned native scalar storage.
This RFC does the inverse: the Python class owns the value semantics and the
core sees only an opaque nominal schema plus bridge-owned operations.

Runtime representation
----------------------

The canonical value plan is one bridge-owned Python object handle. Assignment
retains the exact object with a strong reference; copy and destruction adjust
Python reference counts under the GIL; conversion back to Python returns the
same object. Nested attributes remain part of that object and create no
independent hgraph storage until extracted.

The declared schema controls a typed boundary. Schema-free inference uses an
exact registered class first and then the first registered class in the
runtime type's Python MRO. It never chooses an opaque-record schema by
inspecting field values. When a target schema is already known, ``isinstance``
against its registered class is authoritative.

Equality follows Python ``==`` and propagates Python exceptions. Hash support
is advertised only when the registered class is hashable; hashing follows
Python ``hash`` and must not silently fall back to object identity. Ordering is
not advertised merely because Python could attempt ``<``. These rules preserve
Python's equality/hash contract for ``TSS`` and ``TSD`` keys.

Operator semantics
------------------

Attribute access
~~~~~~~~~~~~~~~~

``getattr_(ts, attr)`` and ``ts.attr`` select a Python-owned-object overload
when the input has the opaque-record capability. ``attr`` remains a wiring-time
string. For a declared field, the output is resolved from the field schema,
then the runtime node acquires the GIL, performs ordinary Python attribute
lookup, and converts only the result to the output schema.

This is a distinct operator implementation selected during wiring; native
``Bundle`` attribute access retains its direct field-projection
implementation. No per-tick storage-policy branch is introduced.

Properties and other descriptors use Python lookup. A descriptor not listed in
the record schema requires an explicit output specialisation, for example:

.. code-block:: python

   getattr_[SCALAR: float](quote, "mid")

An unknown attribute without an explicit output type is a wiring error.
Attribute lookup exceptions are translated with the class and attribute name.
The default-valued form uses the default when lookup reports absence or
returns ``None``; it does not swallow other descriptor exceptions.

Construction
~~~~~~~~~~~~

``combine[TS[Quote]](...)`` is implemented by a Python bridge node, not by a
native Bundle builder. Wiring checks field names and input time-series types.
At evaluation the node calls the registered class with keyword arguments and
publishes the resulting object unchanged.

For dataclasses, omitted ``init=True`` fields must have a default or default
factory; required constructor fields must be wired. ``init=False`` fields are
not constructor arguments. Python's constructor, default factory, and
``__post_init__`` behaviour remain authoritative. Supplied field inputs must
be valid before the first construction; their last values are reused when
another supplied field ticks.

The initial implementation does not define a generic field-update or
``setattr_`` operation. A future operator may use ``dataclasses.replace`` or a
registered class-specific factory, but it must return a new emitted object and
must not mutate the held value in place.

Other scalar operations
~~~~~~~~~~~~~~~~~~~~~~~

Pass-through, merge, switch, sampling, ``type_``, equality, string conversion,
and explicit deduplication use the ordinary scalar framework and the
Python-object ``ValueOps``. There is no implicit field-wise merge. Operators
that require native scalar, tuple, map, list, or Bundle storage do not match an
opaque record merely because its logical fields have compatible schemas.

Mutation and time-series semantics
----------------------------------

Frozen dataclasses are strongly recommended but not required. Hgraph stores a
reference, not a defensive copy. Mutating an object after emission:

* does not schedule a graph evaluation or create a tick;
* may change what later Python reads observe through the retained reference;
* may invalidate equality, deduplication, or container-key assumptions; and
* may make previously recorded values non-reproducible.

An application requiring value semantics must emit a new object. The
documentation and diagnostics should call this **snapshot-by-emission**
semantics. Hgraph does not monitor ``__setattr__`` or deep-copy arbitrary
object graphs.

Inheritance and assignability
-----------------------------

The declared opaque-record schema is nominal. A runtime subclass instance is
valid for a base-class target and remains a subclass object. A port declared as
``TS[Child]`` may bind to ``TS[Base]`` through an opaque up-cast that changes
only the logical schema and retains the same object handle. Down-casts require
the existing explicit dispatch/cast mechanisms and an ``isinstance`` check.

Opaque-record ancestry is captured when a graph is wired so that later class
definitions do not change an existing graph's overload set. Unlike closed
``CompoundScalar`` unions, storage size is unaffected by descendants and no
discriminator is required: the Python object already carries its concrete
class.

Serialization and persistence
-----------------------------

The presence of logical fields does not opt an opaque record into the native
Bundle, JSON, Arrow, Frame, record/replay, or table codecs. Pickle is not an
implicit interchange format. Persistence requires an explicit adapter or a
future schema-directed Python-object codec with a separately reviewed version
and reconstruction contract.

This restriction prevents descriptive annotations from being mistaken for
proof that arbitrary constructors, descriptors, hidden state, subclasses, and
field values can round-trip field by field.

Compatibility and migration
---------------------------

Existing native ``CompoundScalar`` classes are unchanged. Code that requires
C++ nodes to inspect fields, native field projection, native codecs, or
Python-free execution should continue to use ``CompoundScalar``.

A legacy model that requires Python object semantics migrates by removing the
``CompoundScalar`` base while retaining its dataclass definition:

.. code-block:: python

   # Native, field-addressable C++ value
   @dataclass(frozen=True)
   class NativeQuote(CompoundScalar):
       bid: float
       ask: float

   # Python-owned value with a reflected logical schema
   @dataclass
   class LegacyQuote:
       bid: float
       ask: float

Both support typed Python wiring and attribute access, but their storage and
C++ capabilities are intentionally different.

Dataclasses currently falling through to the shared ``object`` schema gain a
distinct nominal schema. Python source behaviour is compatible, but code that
compares raw native schema handles or intentionally relies on all application
classes sharing ``TS[object]`` must use an explicit ``object`` annotation.

The proposal deliberately does not put a mutable ``cpp_native`` policy on one
class. Representation is part of a time-series type's cross-language contract;
using separate Python-owned and ``CompoundScalar`` types keeps schema identity,
operator selection, persistence capability, and installed-extension behaviour
unambiguous.

Performance and memory
----------------------

The time-series slot contains one owning handle rather than storage for every
declared field. Whole-object flow avoids native decomposition and later
dataclass reconstruction. It still incurs Python reference counting, and
copy/destruction, equality, hashing, attribute access, construction, and
conversion require the GIL.

Each extracted attribute incurs Python lookup plus conversion to its output
schema. A graph that repeatedly performs field-level work in C++ should use a
native ``CompoundScalar`` instead. Benchmarks in the implementation PR must
compare:

* whole-object pass-through for Python-owned and native structured scalars;
* one and several ``getattr_`` projections per tick;
* construction from field time series; and
* equality/deduplication for representative frozen dataclasses.

No performance claim should compare the two forms without identifying whether
the workload is whole-object Python processing or field-level native
processing.

Alternatives considered
-----------------------

Use the shared ``object``/``Any`` schema
   This already preserves the object, but loses nominal schema identity and
   field types at the C++ wiring boundary. It cannot reliably resolve typed
   ``getattr_`` or native overloads.

Represent the value as a ``Bundle`` with Python-object storage
   Rejected because ``Bundle`` promises addressable field storage.
   ``BundleView``, codecs, comparison, and table operators would interpret the
   object pointer as a composite layout.

Restore opaque storage for every ``CompoundScalar``
   This would weaken the established C++-first contract and make existing C++
   consumers representation-dependent. A distinct type category makes the
   trade-off explicit.

Add a new ``ValueTypeKind``
   A dedicated kind is more visibly distinct but requires changes to every
   exhaustive value-kind switch. An atomic schema with an
   ``OpaqueRecord`` capability accurately describes the storage and follows
   the existing precedent of nominal atomic schemas carrying supplementary
   metadata. A later generalisation can introduce a kind if non-atomic
   semantics emerge.

Validate all annotated fields on assignment
   Rejected because it recreates the compatibility failure this RFC addresses
   and contradicts standard dataclass semantics, where annotations are not
   runtime validators. Validation belongs in the application's constructor,
   ``__post_init__``, descriptor, or an explicit validation node.

Deep-copy on emission
   Rejected as a default because arbitrary objects may be non-copyable, may own
   external resources, and may define identity-sensitive behaviour. It also
   hides unbounded cost in a scalar assignment.

Acceptance criteria and test plan
---------------------------------

Core metadata and storage
~~~~~~~~~~~~~~~~~~~~~~~~~

* Opaque records have nominal identity, ordered logical fields, an atomic
  storage binding, and no indexed/Bundle view.
* Equal registration is idempotent; name, field, class, or binding conflicts
  fail deterministically.
* Metadata consumers distinguish enum members, physical composite fields, and
  opaque logical attributes.
* A Python-free build and the installed C++ SDK remain independent of Python.

Python typing and conversion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* A plain dataclass resolves as ``TS[Class]`` without deriving from an hgraph
  base and reflects its ordered fields.
* A non-dataclass can be explicitly registered; an unregistered class keeps
  the generic object fallback.
* A native ``CompoundScalar`` remains a Bundle and does not take this path.
* Typed assignment retains object identity and accepts subclasses without
  eagerly reading or validating fields.
* Schema-free inference is deterministic across exact classes and MRO bases.
* Registry reset and repeated import do not leave stale class or schema
  pointers.

Operators
~~~~~~~~~

* Declared ``getattr_`` fields infer the correct output and use normal Python
  descriptor semantics.
* ``None``, missing attributes, defaults, incompatible extracted values, and
  descriptor exceptions have focused tests and diagnostics.
* Explicitly typed computed properties work without being declared storage
  fields.
* ``combine[TS[Class]]`` honours dataclass required fields, defaults, default
  factories, keyword-only fields, ``init=False``, and ``__post_init__``.
* Equality and hashing follow Python. Unhashable classes are rejected as
  ``TSS``/``TSD`` keys rather than receiving an identity-hash fallback.
* Native Bundle operators do not accidentally select for opaque records.

Semantics and robustness
~~~~~~~~~~~~~~~~~~~~~~~~

* Tests document that in-place mutation creates no tick and that emitting a new
  object does.
* Recursive and inherited dataclass fields do not imply native recursive
  storage.
* Python exceptions cross the bridge without losing the owning class,
  attribute, or operator context.
* Full C++ and Python suites, limited-API builds, and an installed-package
  Python consumer pass.
* Benchmarks cover the workloads listed in `Performance and memory`_.

Implementation status
---------------------

No implementation is included with this proposal. The RFC remains
``Proposed`` until its implementation and conformance tests are accepted for
merge. The implementation PR must update this section with any contract changes
and change the status to ``Accepted`` only when the code is accepted.

References
----------

* :doc:`rfc_0003_extension_scalar_registration`
* :doc:`../developer_guide/data_structures/schemas/scalar`
* :doc:`../developer_guide/python_bridge`
* `PEP 557 -- Data Classes <https://peps.python.org/pep-0557/>`_
* `Python dataclasses documentation
  <https://docs.python.org/3/library/dataclasses.html>`_
* `Python data model: customising attribute access
  <https://docs.python.org/3/reference/datamodel.html#customizing-attribute-access>`_
* `PEP 681 -- Data Class Transforms <https://peps.python.org/pep-0681/>`_

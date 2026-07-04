Record/replay, tables and ``const_fn`` — architectural review
==============================================================

.. note::
   **Status: analysis for an architectural session — NOT an approved design.**
   This page maps how the Python machinery actually works (``ext/main``),
   isolates the design issues that make a direct port questionable, and
   proposes options with recommendations. Rulings from the session get folded
   back in and the page becomes the design record. (Ruling already recorded:
   the Frame/table specification maps onto **Apache Arrow**, not Polars —
   Polars sits on Arrow and stays reachable zero-copy.)

How the Python machinery works
------------------------------

The pieces and their dependency order (each depends on those above it):

1. **``const_fn``** (``_wiring/_decorators.py``) — a function over scalar
   inputs producing a constant value, declared with a time-series return
   type. Its trick is **dual-mode calling**: inside a graph it wires as a
   const source; outside a graph it evaluates eagerly and returns a value
   (``my_const(1, 2).value``). It participates in operator overload
   resolution (``overloads=`` / ``requires=``), so a ``const_fn`` can be a
   backend-selected implementation of an operator.

2. **Table schema derivation** (``_to_table_dispatch_impl.py``) — the part
   worth keeping as-is in spirit. ``extract_table_schema`` is a recursive
   ``singledispatch`` over type metadata producing a ``PartialSchema``: the
   flattened column ``keys``/``types`` plus **composed closures**
   ``to_table`` / ``to_table_sample`` / ``to_table_snap`` / ``from_table``,
   assembled child-by-child and cached per type. A row is
   ``(last_modified_time, as_of, *values)``; TSD adds partition-key columns
   and a removed flag; three write modes (Tick / Sample / Snap).
   ``table_schema`` is exposed as a ``const_fn`` so wiring code can read the
   schema as a value.

3. **``to_table`` / ``from_table`` operators** (``_to_table_impl.py``) —
   thin nodes over the composed closures; the ``TABLE`` type is a nested
   Python tuple (``tuple[*row]`` or ``tuple[tuple[*row], ...]``).
   ``from_table_const`` is a ``const_fn``. The date/as-of **column names and
   the as-of time come from GlobalState** (``set_table_schema_date_key`` /
   ``set_as_of``).

4. **``to_json`` / ``from_json``** (``_to_json.py``) — the same composed
   converter-builder pattern (``to_json_builder(tp, delta)`` returns a
   cached closure pipeline), independent of tables; output is ``TS[str]``
   (the separate ``json`` *scalar* type is only needed by
   ``json_encode``/``json_decode``).

5. **Record/replay** (``_record_replay.py`` + backends) —

   - ``record(ts, key, recordable_id=None)`` / ``replay(key, tp)`` /
     ``replay_const(key, tp, tm, as_of)`` / ``compare(lhs, rhs)`` operators;
   - a **model registry**: the active backend is a *string in GlobalState*
     (``set_record_replay_model``), and each backend registers its operator
     overloads guarded by ``record_replay_model_restriction`` — a
     ``requires`` predicate that reads that global **at wiring time**;
   - **recordable ids** resolve through **graph traits**: a parent-chained
     key-value store on graphs (``get_fq_recordable_id`` walks
     ``parent.child`` chains);
   - ``RecordReplayContext`` — a wiring-time context-manager stack carrying
     ``(mode, recordable_id)``;
   - the **data-frame backend** (``adaptors/data_frame``) implements the
     operators on ``to_table``/``table_schema`` + Polars frames, keyed by
     recordable id, with bitemporal as-of filtering (this is the part the
     Arrow ruling re-targets);
   - ``replay_const`` **must be a const_fn**: RECOVER mode needs recorded
     state *as a wiring-time value* to seed graphs.

6. **``@component``** (``_component_node_class.py``) — wraps every input in
   ``input_wrapper`` and the output in ``output_wrapper``, which consult the
   ambient ``RecordReplayContext`` mode and conditionally wire
   ``merge(ts, replay_const(...))`` (RECOVER), ``replay`` (REPLAY/COMPARE),
   ``record`` (RECORD), ``compare`` (COMPARE). **The graph's shape is a
   function of the ambient mode.**

The design issues
-----------------

**I1 — ``const_fn`` as a concept.** Python needs it because wiring code is
Python and node bodies are Python: one function usable both as a plain call
and as a graph source requires a dedicated node class. In C++ the wiring
language *is* C++ — any ordinary function is already callable at wiring time,
and ``const_(fn(args...))`` turns its result into a source. What ``const_fn``
*additionally* provides is (a) overload participation and (b) a uniform shape
the Python bridge can evaluate eagerly.

**I2 — overload resolution coupled to mutable global state.** The backend
restriction predicate reads ``record_replay_model()`` from GlobalState during
wiring. Resolution outcomes therefore depend on ambient mutable state — this
collides with hg_cpp's interning model (node identity derives from explicit
arguments and resolved schemas) and makes wiring non-reproducible.

**I3 — mode-dependent graph shape.** ``@component``'s wrappers change the
wired topology per ambient mode. Conditional wiring is fine in itself
(compose code is code); the issue is that the *ambient* mode is invisible to
the call site and to interning identity.

**I4 — stringly-typed global configuration.** Date/as-of column names, the
as-of override, the model string, and replay seeds all live in GlobalState
under ``::magic::`` keys — set imperatively, read at wiring or eval time.

**I5 — the TABLE value.** Nested per-tick tuples are idiomatic Python and
wasteful C++: materialising a row value per tick, then accumulating rows into
a frame in the recorder, costs two copies and a type (``TABLE``) with no C++
identity. The Arrow ruling changes the natural target: columnar builders.

What maps cleanly (keep)
------------------------

- **The builder pattern is our ops-table pattern.** ``PartialSchema`` — a
  struct of composed per-type closures, cached per type — is precisely an
  interned per-schema **serializer ops table** (struct of fn-ptrs synthesized
  recursively over ``TSValueTypeMetaData``, interned like plans/ops). The
  part of the design Howard likes survives as the part hg_cpp already does
  everywhere.
- **Bitemporal rows** (``(value_time, as_of, *columns)``), partition keys +
  removed flags for TSD, and the Tick/Sample/Snap mode triad — sound
  semantics, keep as-is.
- **Fully-qualified recordable ids** via parent-chained traits — sound; needs
  a small graph-traits primitive.
- ``to_json``/``from_json`` are independent of tables and need only the
  serializer-ops synthesis + ``TS<Str>``; they can land first as the
  proving ground for the synthesis machinery.

Proposed C++ shape (for discussion)
-----------------------------------

**P1 — no ``const_fn`` node kind.** A wiring-time computation is a plain
function; a const source is ``const_``. For the two roles that remain:

- *Overload participation*: allow registering an operator implementation as
  **const-evaluable** — an ``OperatorImpl`` flag plus an eager kernel
  ``Value(std::span<const Value>)``. ``resolve`` works unchanged; a caller
  (C++ wiring code or the Python bridge) may invoke the eager kernel instead
  of wiring a node. ``table_schema`` / ``from_table_const`` /
  ``replay_const`` register this way.
- *Python-bridge parity*: the bridge exposes eager evaluation of
  const-evaluable operators — Python's dual-mode behaviour reproduced
  without a node class.

**P2 — explicit configuration over ambient globals.** The record/replay
model, date/as-of keys, and as-of override become a **RecordReplayConfig**
value seeded at wiring (``Wiring::global_state()`` already exists for
runtime; the *model choice* should be an explicit wiring argument or
build-time configuration, not a mid-wiring mutable). Backend selection uses
the existing ``requires_predicate`` machinery reading that explicit config —
reproducible and interning-safe. Parity with Python's imperative setters is
a bridge-level shim.

**P3 — modes via the context machinery, folded into identity.** The
``RecordReplayContext`` maps onto the landed wiring-scope machinery (a
dedicated stack beside the context stack on ``OperatorRegistry``), carrying
``(RecordReplayMode, recordable_id)``. The component/graph that consults it
must fold the consulted mode into its intern key (the ``MapCallConfig``
precedent) so identical calls under different modes are distinct instances.

**P4 — Arrow-native tables.** ``TableSchema`` maps to an Arrow schema
(``value_time``/``as_of`` as timestamp columns; partition keys; removed as a
bool column). The serializer ops write **directly into Arrow array builders**
(append per tick), so the recorder accumulates a ``RecordBatch`` without a
per-tick row value; ``from_table`` walks Arrow arrays. The user-visible
``to_table`` operator (a ``TS`` of row values) stays for parity, but
record/replay backends bypass it and drive the serializer ops directly —
one copy, no intermediate tuples. Arrow becomes an optional dependency
(``HGRAPH_ENABLE_ARROW``), never in the default build path.

**P5 — graph traits primitive.** A small parent-chained key-value store on
graph storage (build-time write, runtime read), carrying ``recordable_id``
and later the rest of Python's traits surface. Prerequisite for
``@component``.

**Sequencing** (each lands green independently):

1. Serializer-ops synthesis + ``to_json``/``from_json`` (proves the pattern,
   no new dependencies).
2. Graph traits + RecordReplayConfig (+ mode scope machinery).
3. Arrow value integration + ``table_schema``/``to_table``/``from_table``.
4. Data-frame (Arrow) record/replay backend; ``replay_const`` + RECOVER.
5. ``@component`` on top of all of it.

Questions for the session
-------------------------

1. **Q-const_fn:** agree to *not* port ``const_fn`` as a node kind, replacing
   it with const-evaluable operator registration (P1)? If not, what should
   its C++ shape be?
2. **Q-model:** is making the record/replay model an explicit wiring-time
   configuration (P2) acceptable, with the Python-compat imperative setters
   as bridge shims? This deliberately drops "change the model mid-wiring".
3. **Q-modes:** should the mode scope fold into intern identity per P3, or
   should components take the mode as an explicit argument (even more
   explicit, but diverges further from Python's ``with`` ergonomics)?
4. **Q-table-surface:** keep the user-visible ``TS[TABLE]``-style operator
   (rows as values) alongside the fused builder path (P4), or make tabular
   conversion recorder-internal until a concrete need appears?
5. **Q-arrow-dep:** Arrow as opt-in dependency fetched like fmt/Catch2, or
   system-provided only? What is TS-value form of a table — an owned
   ``RecordBatch`` scalar kind, or do we avoid tables-as-values entirely?
6. **Q-compare:** ``compare`` (backtesting comparison sink) writes results
   to "a comparison result file" in Python — what should the C++ result
   channel be (GlobalState buffer, logger, file via adaptor)?
7. **Q-recover:** RECOVER seeds inputs by ``merge(ts, replay_const(...))``.
   Is merge-based seeding the C++ design too, or should recovery use the
   recordable-state machinery (``RecordableState``) directly?

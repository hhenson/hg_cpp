Operators (overload dispatch)
=============================

An **operator** is a named operation with a *general* signature that collects
many concrete **implementations** and, when wired with concrete ports/scalars,
selects the **best-matching** one. It is the C++ counterpart of Python
``hgraph``'s ``@operator`` (and ``@compute_node(overloads=…)`` /
``@graph(overloads=…)``): one logical name (``add_``) stands for a family of
implementations, and the wiring layer resolves which to use.

This is the multi-candidate generalisation of ordinary node wiring. A plain
``wire<Node>(w, …)`` names one implementation directly; ``wire<add_>(w, a, b)``
names an operator and lets the **operator registry** choose among the
implementations registered under ``"add"``, ranked by how *specific* their
declared types are to the supplied arguments. Resolution is entirely
**wiring-time** — exactly like *Graph Wiring*, there is no runtime dispatch; the
chosen implementation is baked into the graph.

.. note::

   **Status.** Phase 1 is **implemented**: the runtime ``TypePattern`` interpreter
   (``include/hgraph/types/type_pattern.h`` + ``.cpp``), the ``OperatorRegistry`` /
   ``OperatorImpl`` / ``WiringArg`` and the ``wire<>`` operator arm
   (``include/hgraph/types/operator_dispatch.h`` + ``.cpp``), with the reset-listener
   hook — proven by ``tests/cpp/test_operators.cpp`` (specific-beats-generic, generic
   fallback, no-match and ambiguity errors). The ``TypePattern`` AST and its
   ``match`` / ``rank`` / ``resolve`` already cover every kind (``TS`` / ``TSS`` /
   ``TSL`` / ``TSD`` / ``REF`` / ``Signal`` and the scalar variants); the remaining
   slices wire more of it into dispatch. Phases 2–3 are **also implemented**:
   scalar-argument matching, a ``static bool requires_(const ResolutionMap &)``
   veto, and a ``lib/std`` operator family (``add_`` / ``eq_`` in
   ``include/hgraph/lib/std/std_operators.h``), proven by
   ``tests/cpp/test_std_operators.cpp`` and the scalar / ``requires`` / nested-``TSL``
   cases in ``tests/cpp/test_operators.cpp``. Still to come (see *Roadmap*): Phase 4 —
   the Python implementation path (behind ``HGRAPH_ENABLE_PYTHON_USER_NODES``). (The
   operator path's scalar-configuration
   bundle is assembled by the same ``BundleBuilder`` + ``scalar_schema(map)``
   mechanism the generic ``wire<>`` path uses — an erased-``Value`` field source on
   the operator side, a typed one on ``wire<>``'s; one mechanism, not a second
   resolver.)


One runtime model, not a second resolver
----------------------------------------

The guardrail for this subsystem (*CLAUDE.md* §3) is **one runtime model, no
parallel abstraction**. The operator registry is therefore an **index over
candidates plus a ranking loop** — it adds selection, *not* a second way to
resolve and build a generic node:

- a candidate **resolves** into the **same** ``ResolutionMap``
  (``include/hgraph/types/type_resolution.h``) the generic ``wire<>`` path uses;
- a candidate **builds** through the **same**
  ``NodeBuilder::implementation<Impl>(map)`` call (*Wiring*,
  ``include/hgraph/types/static_node.h``);
- the resolved node is interned through the **same** ``Wiring::add_node`` (so a
  resolved operator dedups and ranks identically to any other node).

A single-implementation generic node (``const_``, ``replay``, a passthrough) is
**not** an operator and stays on the ordinary ``wire<>`` path; an operator is the
case where *more than one* implementation competes for one name. Conceptually a
non-overloaded node is just an operator with exactly one candidate and no ranking
step — the two share the resolve/build machinery and differ only in whether the
ranking loop runs.

What the operator subsystem genuinely adds: a name → candidates **index**, a
**matcher/ranker** that can compare a candidate's (possibly generic) declared
types against the supplied *runtime* schemas, and a **selection** rule.


Why runtime dispatch (and why it is Python-ready)
-------------------------------------------------

``wire<Node>`` is a compile-time dispatcher: the implementation type is known at
the call site. An operator cannot be: the candidate set is **open** (registered
from anywhere, including a future Python module), the winner depends on the
**runtime-interned** schemas of the supplied ports, and an operator may be wired
with an **erased** ``Port<void>`` whose schema is only known at wiring time. So
operator matching/ranking is a **runtime** computation over interned metadata
pointers.

That single decision is what makes Python drop-in (Phase 4): because matching,
ranking and building are expressed over **runtime data** behind a type-erased
candidate, a Python-defined implementation registers as just another candidate —
the dispatcher cannot tell it from a C++ one. The C++ compile-time reflection
only *feeds* the runtime representation; it is not on the resolution path.


``TypePattern`` — the one matcher
---------------------------------

Matching needs to compare a candidate's declared type — which may contain type
**variables** (``TsVar`` / ``ScalarVar``) — against a concrete interned schema.
A ``TypePattern`` is the runtime form of such a (possibly generic) schema, and a
single set of runtime functions over it is the **one** matcher/ranker shared by
C++ and Python candidates:

.. code-block:: text

   TypePattern   = Var(name)                        # a TsVar / TIME_SERIES_TYPE variable
                 | Concrete(const TSValueTypeMetaData*)   # a fully-interned TS leaf
                 | TS(ScalarPattern)  | TSS(ScalarPattern)
                 | TSL(TypePattern, N) | TSD(ScalarPattern key, TypePattern value)
                 | REF(TypePattern)   | Signal
   ScalarPattern = ScalarVar(name) | ScalarConcrete(const ValueTypeMetaData*)

Three runtime functions are the source of truth:

- ``bool match(pattern, const TSValueTypeMetaData* concrete, ResolutionMap&)`` —
  walks the pattern against a concrete schema. A ``Var`` / ``ScalarVar`` leaf
  **binds** into the map (reusing ``ResolutionMap::bind_ts`` / ``bind_scalar``,
  which already reject an inconsistent re-bind — so a shared variable such as
  ``add(TS<S>, TS<S>)`` is enforced for free). A ``Concrete`` leaf is **verified**
  (``time_series_schema_equivalent`` / pointer identity), which is exactly the
  check the compile-time ``ts_unifier`` deliberately omits (it trusts the
  static path). Composite kinds check ``concrete->kind`` then recurse, short-
  circuiting on failure.
- ``int rank(pattern)`` — an integer **specificity** score; **lower is more
  specific** and wins. See *Ranking*.
- ``const TSValueTypeMetaData* resolve(pattern, const ResolutionMap&)`` —
  substitutes bound variables to produce the concrete meta. Used to compute (and
  verify the resolvability of) a candidate's output schema, and by the Phase-4
  Python build.

C++ candidates obtain their patterns by **lowering** their selector schema types
at compile time: ``to_pattern<S>()`` / ``to_scalar_pattern<T>()`` mirror the
shape of ``ts_resolver`` but emit ``Var`` nodes for ``TsVar`` / ``ScalarVar`` and
``Concrete(schema_descriptor<S>::ts_meta())`` for concrete leaves. Python
candidates build the same ``TypePattern`` directly from their type metadata. One
pattern representation, one matcher — no drift.

Note the division of labour: the ``TypePattern`` interpreter is the *matcher*
across candidates. Building the chosen C++ candidate still goes through
``NodeBuilder::implementation<Impl>(map)``, whose internal ``ts_resolver`` is the
*builder's* compile-time schema substitution for that one known ``Impl`` — it is
not a second matcher.


``WiringArg`` — erased arguments
--------------------------------

The operator-dispatch entry point erases every ``wire<Operator>`` argument into a
``std::vector<WiringArg>`` — **the only compile-time step**; everything after is
runtime and type-erased (which is what lets Python candidates participate):

.. code-block:: text

   WiringArg = TimeSeries(WiringPortRef)            # carries const TSValueTypeMetaData* schema
             | ScalarValue(Value, const ValueTypeMetaData*)   # an owned, self-describing value

Scalars are erased to an owned ``Value`` immediately, so they are self-describing;
the chosen candidate sets bundle fields from the ``Value`` by schema without
needing the original typed argument.


``OperatorImpl`` — a type-erased candidate
------------------------------------------

A candidate is a runtime record — constructible from runtime data, **not only**
from a C++ template (this is the load-bearing Python seam):

.. code-block:: cpp

   struct OperatorImpl {
       std::string                params_label;   // signature text, for error messages
       std::vector<TypePattern>   params;          // one per In / Scalar position
       int                        rank;            // precomputed from params
       std::function<bool(std::span<const WiringArg>, ResolutionMap&, std::string& why)> try_resolve;
       std::function<NodeBuilder(const ResolutionMap&, std::span<const WiringArg>)>       build;
       enum class Source { Cpp, Python } source;
   };

- ``make_operator_impl<Impl>()`` is **one** factory: it lowers
  ``StaticNodeSignature<Impl>``'s selectors into ``params`` via ``to_pattern``,
  precomputes ``rank``, and wraps a ``build`` closure that assembles the scalar
  bundle (the shared ``assemble_scalars<Impl>`` helper, factored out of
  ``wire<>``) and delegates to ``NodeBuilder::implementation<Impl>(map)``.
- ``try_resolve`` runs ``match`` for each parameter, then runs
  ``Impl::resolve_default_types(map)`` if present (the same hook the generic
  ``wire<>`` path uses), then checks the **output** pattern resolves. A still-
  unbound output variable is a **non-match** (reject with a reason), never an
  exception — an operator overload whose output is a free variable with no
  resolver can never produce a typed port and is ill-formed.
- A **Python** candidate (Phase 4) fills the *same* struct: ``params`` built
  directly as ``TypePattern``s over shared interned metas, and a ``build`` that
  produces a ``NodeBuilder`` whose ``NodeCallbacks::evaluate`` (already a
  type-erased ``std::function`` — ``include/hgraph/runtime/node.h``) calls the
  retained Python callable. The registry and dispatcher are unchanged.


``OperatorRegistry`` and resolution
-----------------------------------

A process-wide singleton maps an operator name to its candidates:

.. code-block:: cpp

   class OperatorRegistry {
     public:
       static OperatorRegistry &instance();                    // plain static; single-threaded, no locks
       void register_overload(std::string name, OperatorImpl);
       // Pick the unique best candidate and the ResolutionMap it produced.
       std::pair<const OperatorImpl*, ResolutionMap>
            resolve(std::string_view name, std::span<const WiringArg> args) const;
       void reset();                                           // test isolation
   };

``resolve`` ports ``ext/2603``'s
``OverloadedWiringNodeHelper.get_best_overload``
(``ext/2603/hgraph/_wiring/_wiring_node_class/_operator_wiring_node.py``):

1. **Filter by arity** — drop candidates whose parameter count does not match.
2. **Try-resolve each** — run the candidate's ``try_resolve``; survivors carry
   their precomputed ``rank`` and the ``ResolutionMap`` they produced. Rejected
   candidates keep a human-readable reason.
3. **Rank** — sort survivors ascending by ``rank``.
4. **Select** — the unique lowest-rank survivor wins. **No survivor** → an error
   listing the rejected candidates and why. **A tie at the lowest rank** → an
   ambiguity error listing the tied candidates and their ranks.

Resolution is deterministic: rank is a precomputed total order, the candidate
vector preserves registration order, and no decision is ever made from
hash-map iteration order. Rich rejected-candidate messages are produced from
Phase 1 — operator misuse is the most common wiring error, and a bare "no
overload" message is hostile.


Ranking (specificity)
----------------------

Lower rank = more specific = preferred. The integer model reproduces the
**partial order** of ``ext/2603`` (``_generic_rank_util.py`` + ``_calc_rank``)
without its floating-point constants:

.. code-block:: text

   rank(Concrete scalar leaf) = 0
   rank(ScalarVar)            = 100
   rank(Concrete TS | Signal) = rank(value)            # 0 for a concrete leaf
   rank(TS(p))                = 1 + rank(p)
   rank(TSS(p))               = 1 + rank(p)
   rank(TSL(p, N))            = 1 + rank(p)
   rank(TSD(k, v))            = 1 + rank(k) + rank(v)
   rank(REF(s))               = rank(s)
   rank(Var)                  = LARGE                   # a bare top-level variable is least specific

   candidate rank = Σ over its In / Scalar parameter ranks

This yields ``TS<Int>`` < ``TS<ScalarVar>`` < a bare ``TsVar``, recursively
(``TSL<TS<Int>,N>`` < ``TSL<TS<ScalarVar>,N>`` < ``TSL<Var,N>``) — the
``ext/2603`` rule that a top-level generic is always less specific than a generic
*inside* a collection. The abstract operator signature is **not** a candidate
(it has no ``eval``), so unlike ``ext/2603`` there is no need for a sentinel
"never win" rank.

*Deferred:* ``ext/2603``'s per-variable ``min`` combine, which de-duplicates the
genericness of a variable shared across multiple inputs. The integer sum induces
the same order for non-shared-variable signatures; the combine is a later
refinement if a real overload set needs it.


Defining and registering an operator
-------------------------------------

An operator is a marker struct carrying a name and an abstract (documentary)
signature; implementations are ordinary stateless node structs registered under
that operator:

.. code-block:: cpp

   // The operator: a name + general signature. Not executable (no eval).
   struct add_ : Operator<"add",
                          In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TsVar<"S">>> {};

   // Two implementations.
   struct add_ts_int {
       static void eval(In<"lhs", TS<Int>> a, In<"rhs", TS<Int>> b, Out<TS<Int>> o) {
           o.set(a.value() + b.value());
       }
   };
   struct add_ts_scalar {                                   // generic fallback
       static void eval(In<"lhs", TS<ScalarVar<"T">>> a, In<"rhs", TS<ScalarVar<"T">>> b,
                        Out<TS<ScalarVar<"T">>> o) { /* generic add */ }
   };

   // Registration — explicit, grouped into a seed function.
   void register_standard_operators() {
       register_overload<add_, add_ts_int>();
       register_overload<add_, add_ts_scalar>();
   }

Like ``ext/2603``, an implementation is **not** required to structurally conform
to the operator's abstract signature; the abstract signature documents the
operator and supplies its name and nominal arity for error messages. Matching is
by rank against the supplied arguments, not against the abstract signature.

**Registration is explicit, never static-init.** The test harness wipes every
registry between cases (see *Test isolation*), so a static-initialiser overload
would register once and vanish after the first test; and a candidate's patterns
reference interned schema pointers that exist only after the standard types are
seeded. Standard operators are therefore registered by an explicit
``register_standard_operators()`` — called by an application / the Python module at
startup, or by a test that needs them. Unlike ``register_standard_types`` (seeded
for *every* test, as it is foundational), the standard operators are **not**
auto-seeded by the reset listener: doing so would collide with a test's own ad-hoc
operator registered under the same name (e.g. ``"add"``). Tests register the
overloads they want in the test body (after reset, before wiring).


Calling an operator — the ``wire<>`` operator arm
-------------------------------------------------

``wire<X>`` dispatches on the shape of ``X`` (see *Graph Wiring*); operators add a
third arm:

.. code-block:: text

   wire<X>:  X has compose  ->  inline sub-graph (flatten)                  (existing)
             X has eval     ->  add node (concrete or generic resolution)   (existing)
             else (Operator) ->  erase args -> registry.resolve -> winner.build -> add_node

The operator arm erases the call's arguments to ``std::vector<WiringArg>``, calls
``OperatorRegistry::resolve(name, args)``, builds the winner via its ``build``
closure, and lowers to ``w.add_node(typeid(winner-impl), …)`` so the resolved
node inherits the ordinary interning and ranking. It returns the **erased**
``Port<void>`` carrying the resolved output schema (the winner's output type is
only known at wiring time); downstream ``wire<>`` accepts an erased port exactly
as it does today for generic-node outputs.


Test isolation
--------------

``OperatorRegistry`` is a process-wide singleton holding **borrowed** interned
schema pointers inside its candidates' patterns. The Catch2 reset listener
(``tests/cpp/registry_test_listener.cpp``) must:

1. call ``OperatorRegistry::instance().reset()`` **before**
   ``TypeRegistry::instance().reset()`` — clear the borrowers before the owner,
   the same ordering the other registries already follow (and the
   ``plan-registries-clear-on-reset`` rule).

It does **not** re-seed standard operators (that would collide with a test's own
ad-hoc operators); a test that needs the ``lib/std`` family calls
``register_standard_operators()`` itself, after the reset, before wiring.


The Python implementation path (Phase 4)
----------------------------------------

Python overloads are drop-in because the candidate is type-erased and built from
runtime data. A Python overload supplies the **same** ``OperatorImpl``:

- **patterns** — built directly as ``TypePattern``s over schemas interned in the
  shared ``TypeRegistry``;
- **rank** — computed by the same ``rank(TypePattern)`` (identical to C++);
- **build** — produces a ``NodeBuilder`` whose ``NodeCallbacks::evaluate``
  (a type-erased ``std::function``) acquires the GIL, marshals the ``NodeView``
  inputs to Python, calls the retained callable, and writes the result back
  through the ``TSOutputView`` — behind ``HGRAPH_ENABLE_PYTHON_USER_NODES``.

**Cross-boundary type identity is the precondition.** Matching is pointer
equality of interned metadata, so a Python ``TS[int]`` and a C++ ``TS<Int>`` must
resolve to the **same** ``const TSValueTypeMetaData*``. That holds only if there
is exactly **one canonical scalar per logical type** and every name a Python type
uses is an **alias** onto it (not a separately-interned synthetic). The standard
types registration (and primitive aliases) must guarantee
``value_type("int") == register_scalar<int>("int")`` (same pointer), and the
standard-types seed must run before any overload is registered on either path.
See *Python Integration*.

The default build never depends on Python: Phases 1–3 include no nanobind header,
and the Python candidate path is compiled only under the opt-in flag.


Roadmap
-------

1. **Phase 1 — core dispatch (C++) — done.** The full ``TypePattern`` AST +
   ``match`` / ``rank`` / ``resolve`` / ``to_pattern``; ``Operator<>`` /
   ``WiringArg`` / ``OperatorImpl`` / ``OperatorRegistry`` / ``register_overload``;
   the ``wire<>`` operator arm; the reset-listener hook. Proven by ``add_``
   (``TS<Int>`` specific beats the generic; the generic wins for ``TS<Str>``) plus
   no-match and ambiguity errors. Time-series arguments only; scalar arguments are
   accepted by the API but their *matching* lands in Phase 2.
2. **Phase 2 — scalars, predicates — done.** ``WiringArg`` scalar matching wired
   into dispatch; an optional per-candidate ``static bool requires_(const
   ResolutionMap &)`` veto that rejects a candidate after its types resolve.
3. **Phase 3 — ``lib/std`` operator family — done.** ``add_`` (per-type
   implementations) and ``eq_`` (``-> TS<Bool>``) in
   ``include/hgraph/lib/std/std_operators.h``, selected by the supplied operand
   types, with an explicit ``register_standard_operators()``.
4. **Phase 4 — Python implementation path.** Runtime-data ``OperatorImpl`` from a
   Python signature; ``NodeCallbacks`` hosting a Python callable; cross-boundary
   identity asserted. Behind ``HGRAPH_ENABLE_PYTHON_USER_NODES``.

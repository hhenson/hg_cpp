Nested Graphs
=============

This page is the design record for **non-flattening nested graphs** — the
substrate and operators (``nested_``, then ``switch_`` / ``map_`` / ``reduce``)
where a node owns and drives a child graph at runtime instead of inlining it at
wiring time. It builds on the runtime nested-graph node described in *Graph
Wiring > Nested graphs* and supersedes the prior C++ attempt's design where the
two conflict (see *Reconciliation with the 2603 RFC* below).

Scope of this page: the wiring-side sub-graph compilation (done), and the design
decisions the upcoming operators implement. Sections are added in the same change
as the code they describe.


The model
---------

Three artifacts, one per phase:

- **Sub-graph definition** (authoring): an ordinary graph struct — a static
  ``compose(Wiring &, Port..., Scalar...)`` body. The same definition can be
  inlined by ``wire<G>`` (flattening) or compiled by ``compile_subgraph<G>``
  (non-flattening); nothing in the definition chooses.
- **``CompiledSubGraph``** (compile-time template;
  ``include/hgraph/types/subgraph_wiring.h``): the ranked child ``GraphBuilder``
  plus the **boundary binding specs** (``NestedGraphInputBinding`` /
  ``NestedGraphOutputBinding``) and boundary schemas. Produced once at wiring
  time; owned by the nested node's (interned, program-lifetime) context. This is
  the C++ realisation of the RFC's *child-graph template*.
- **Child graph instance** (runtime): a ``GraphValue`` created by
  ``GraphBuilder::make_nested_graph(NodeStorageRef)``, living in the owning
  node's storage. ``nested_`` owns exactly one; ``switch_`` will own at most one
  (the active branch); ``map_`` will own one per key. There is no separate
  instance class — per-instance state is the operator's own storage struct.


Boundary compilation: placeholders, not stubs
---------------------------------------------

A sub-graph's time-series parameters are compiled as **boundary placeholder
sources** — a ``WiringPortRef`` source kind (``BoundarySource{arg_index, path}``)
alongside peered/structural/null. ``compile_subgraph<G>`` runs ``G::compose``
against a fresh ``Wiring`` whose ``Port`` parameters carry boundary sources;
``Wiring::finish_subgraph`` then runs the ordinary rank pass and converts every
boundary-sourced input edge into a ``NestedGraphInputBinding``
(``source_path = {arg_index} + path`` from the outer node's input root,
``target`` = the child node input endpoint). The returned output port becomes the
``NestedGraphOutputBinding`` (root forwarding).

**No stub nodes exist at any layer.** The 2603 RFC's Option B (keep wiring-stub
nodes, compile them away) assumed Python-side stub wiring nodes; since the C++
wiring core owns boundaries, we adopt the RFC's own "cleanest end state"
(Option C) directly: boundaries are wiring-only values, compiled into binding
specs at ``finish_subgraph``.

A boundary source composes with the rest of wiring uniformly: it can feed any
child node input directly or appear as a child of a structural source (the
binding target path is extended into the structural slot).

**Pass-through outputs (``alias_parent_input``).** A sub-graph ``compose`` may
return a boundary input directly. This compiles to the ``ParentInput`` output
binding kind: at runtime the outer node's forwarding output aliases whatever
upstream output the outer *input* is bound to (re-resolved each cycle, cleared
while the upstream is unbound). This is the RFC's ``alias_parent_input`` mode
and is what identity branches of ``switch_`` / ``map_`` and component-style
wrappers rely on.

**Structural boundary args.** An outer structural initializer
(``nested_<G>(w, {a, b})``, or named TSB form) is mirrored into the child
compile as a structural source whose **leaves are boundary refs**. The child
consumer's input endpoint therefore derives as non-peered with bindable leaf
slots, and ``finish_subgraph`` emits one leaf-wise input binding per peered
leaf (null leaves stay unbound) — the runtime binds each leaf position, which
is the only bindable granularity (``is_bindable`` = target-link position).

Rejected explicitly at compilation, with a clear error:

- error / recordable-state roots as the sub-graph output;
- structural (non-port) sub-graph *outputs*;
- seeding ``GlobalState`` inside a sub-graph ``compose`` — nested graphs
  delegate global state to the **root** graph at runtime, so a sub-graph-seeded
  store would be silently discarded; seed it on the outer wiring instead.


``nested_<G>`` — the wiring entry
---------------------------------

``nested_<G>(w, ports..., scalars...)`` (compose-parameter order, same call
shape as ``wire<G>``, including ``{…}`` structural initializers) compiles ``G``
and adds **one** ``single_nested_graph_node`` that owns the child graph:

- outer node input schema = un-named TSB over the boundary arg schemas (field
  per arg, in order); output schema = the sub-graph output schema (forwarding
  at the output root, or the ``ParentInput`` alias for pass-through);
- scalar arguments are baked into the compiled child nodes (they configure the
  child graph), and additionally folded into the outer node's interning key —
  so two ``nested_<G>`` calls with equal inputs **and** equal scalars dedup to
  one nested node, distinct scalars do not. Interning is **lazy**: the child
  ``GraphBuilder`` and its program-lifetime node context are only created on an
  intern miss (the deferred-builder ``Wiring::add_node`` overload), so deduped
  calls leak nothing;
- a sink sub-graph (``compose`` returns ``void``) wires as a nested sink node
  (not interned, like every output-less node).

Scheduling follows the existing substrate: the child's
``next_scheduled_time()`` is propagated to the parent after start/evaluate, so
a source-only sub-graph drives the outer graph (proven by
``tests/cpp/test_nested_wiring.cpp``). Child-driven **push** propagation (a
child node scheduled by notification while the parent is idle) is the next
increment.

Tests: ``tests/cpp/test_nested_wiring.cpp``.


Higher-order constructs are operators
-------------------------------------

``reduce`` — and ``map_`` / ``switch_`` when they land — are **ordinary
operators**, not bespoke wiring functions. This mirrors the ``ext/main``
direction (``map_`` is an ``@operator`` whose old implementation became the
default registered overload): the default implementation is one registry
candidate, the future dynamic kernels (e.g. ``reduce`` over ``TSD``) are
further overloads of the same name, and user specialisations — including ones
gated on the supplied function's identity via ``requires_`` — are selected by
the standard best-match machinery. The callable argument is the ``WiredFn``
scalar (``fn<X>()``; see *Operators > Higher-order operators*). Their markers
and default overloads live in their own ``lib/std`` family files
(``operators/higher_order.h`` + ``impl/higher_order_impl.h``) — there is
nothing special about them now that sub-graph compilation is standardised.


``reduce``
----------

``reduce(func, ts[, zero])`` reduces a time-series **collection** into one
time-series with the (associative) combiner — any wirable function: an
operator (``add_``), a node, or an ``(lhs, rhs)`` sub-graph (flattened at
every reduction node). The operator's contract covers any collection kind
(``TSL`` / ``TSD``; ``TSS`` once the Python reference grows one) — each kind
is its own registered overload selected by pattern rank. Implemented today:
the **fixed-size TSL** overloads
(``wire<stdlib::reduce_>(w, fn<add_>(), tsl_port)``).

The default overloads lay the reduction out **statically at wiring time**,
mirroring Python ``_reduce_tsl``: every leaf is ``default(ts[i], zero)`` — an
element that has not ticked yet counts as ``zero`` — then a linear chain below
four elements, otherwise balanced binary pairing with an odd-element carry
(``over_run``). No nested node is involved. Two arities, like Python:

- ``reduce(func, ts)`` — the zero is derived from the operation,
  ``zero(item_tp, func)`` (the op-aware ``zero_`` operator: ``add_`` -> 0,
  ``mul_`` -> 1, ``min_`` -> the max bound, …). A combiner with no registered
  zero (e.g. a custom sub-graph) is a wiring-time error, like Python's
  ``KeyError``;
- ``reduce(func, ts, zero)`` — the explicit zero value, wired as
  ``const(zero)`` at the element schema.

The leaves dispatch ``zero`` / ``default`` / ``const`` **through the registry
at the resolved element schema** (``wire_operator`` — the runtime-schema
counterpart of ``wire<Op, OutSchema>``). ``default`` itself is the
REF-forwarding implementation mirroring Python's ``_default`` (substitute
while invalid, then forward the reference and go passive). Element access uses
the erased ``tsl_element_ref`` projection (typed form ``tsl_element(port, i)``
in ``subgraph_wiring.h``), which works on any source kind: a peered output
path, a structural child, or a **sub-graph boundary** (so ``reduce`` composes
inside a sub-graph ``compose`` over a boundary TSL). The overloads'
``requires_`` accept fixed-size TSL inputs only, leaving dynamic-TSL/TSD
reductions to future overloads of the same name (the gated milestone
increment — see roadmap). Still deferred: a time-series (port) ``zero``
argument, and non-associative/linear-forced reduction.

Tests: ``tests/cpp/test_reduce.cpp`` (including a user overload gated on the
wired function's identity, mirroring ``ext/main``'s ``test_map_overload``).


Scheduling delegation
---------------------

The RFC's clock invariant — *the parent must wake no later than the child's
next scheduled work* — is realised as two halves folded into the existing
graph ops (no separate engine/clock object; see the recorded decision):

- **Pull** (``single_nested_graph_propagate_schedule``): after the child is
  started or evaluated, the owning node force-schedules itself at the child's
  ``next_scheduled_time()``. This covers every schedule created while the
  parent is already driving the child (self-rescheduling sources, scheduler
  calls inside child ``start``/``eval``).
- **Push** (``nested_schedule_node_impl`` in ``graph.cpp``): any
  ``schedule_node`` recorded on a child graph **while it is idle**
  (``started && !evaluating``) immediately schedules the parent node at the
  same time — the path a notification or wall-clock alarm takes between
  parent evaluations, and the safety net the keyed operators (``switch_`` /
  ``map_``) rely on when a child is not evaluated every parent cycle. The
  push is deliberately gated off during child evaluation (pull covers it, and
  pushing mid-evaluate would schedule a spurious extra parent cycle) and
  while the child is stopped.

The parent graph's own same-cycle clamp supplies the ``+MIN_TD`` behaviour,
and multi-level nesting recurses up to the root naturally. The boundary
*binding* helpers shared by all nested node implementations
(``walk_ts_path`` / ``bind_input_to_source`` /
``bind_forwarding_output_to_source``) live in
``include/hgraph/runtime/nested_bindings.h`` — no bespoke bind/unbind logic
per operator.


``switch_``
-----------

``switch_(key, cases[, ts])`` routes through **one** child graph at a time,
selected by ``key`` — an operator like the rest of the family
(``wire<stdlib::switch_>(w, key, switch_cases({{k, fn<G>()}, …}[, fn<Default>]),
ts…)``).

- **Branches are ``WiredFn`` values** (graphs, nodes, or operators). Each is
  compiled into a ``SingleNestedGraphNodeSpec`` via the function's **compile
  thunk** (``WiredFn::compile`` — the same value either inlines via ``wire``
  or compiles into a child graph; the caller chooses). All branches must
  produce the same output schema.
- **The key is just a boundary input.** The outer switch node's inputs are
  ``[key, ts…]``, so a key-consuming branch (arity = ts-count + 1, key first)
  binds outer input ``0`` through the standard binding mechanism — none of
  the Python key-stub plumbing exists. A non-key branch's binding paths
  simply shift past the key input. Source-style branches (arity 0) take no
  bindings at all.
- **Runtime** (``runtime/switch_node.{h,cpp}``, on the shared
  ``nested_bindings`` helpers): at most one live child in node storage. On a
  key change (or any key tick with ``reload_on_ticked`` — runtime-supported,
  not yet exposed by the operator) the active child is stopped and destroyed,
  the new branch (else the default, else a runtime error) is built, bound and
  started, and the forwarding output **re-points**.
- **Sampled semantics** (the sampled-runtime contract; the recorded
  divergence from Python's ``value = None`` reset): the freshly selected
  branch evaluates with the *current* upstream values even when they did not
  tick that cycle — after the child starts, every consumer whose bound source
  is already valid is force-scheduled at the switch time. Pinned by test
  (a swap while the ts input holds emits the new branch's value immediately).
- **Lifecycle**: switching away destroys the child ``GraphValue`` (immediate
  destroy is safe — the output is re-pointed first); switching back rebuilds
  a fresh instance (per-branch state resets — pinned by test). The output's
  resolver discovers the schema by compiling the first branch
  (``resolve_default_types`` on the overloads).
- Deferred: variadic time-series arguments (overloads cover none/one until
  variadic operator parameters exist), exposing ``reload_on_ticked``, and
  all-sink switches.

Tests: ``tests/cpp/test_switch.cpp``.


Reconciliation with the 2603 RFC
--------------------------------

The 2603 design corpus (``ext/2603/docs/design/2026-04-v2-nested-graphs-rfc.md``,
the implementation notes, and the sampled-runtime contract) defines the intent;
where it conflicts with decisions already recorded for this codebase, the
current code wins:

- **No ``NestedEvaluationEngine`` / clock-delegate objects.** The separate
  engine/clock split was rejected for this runtime (run-level state folds into
  the executor ops). The RFC's clock invariant — the parent must wake no later
  than the child's next scheduled time — is realised as (a) pull-style
  propagation after child start/evaluate (exists) and (b) push propagation from
  the nested graph's ``schedule_node`` to the parent node (next increment). The
  parent graph's existing same-cycle clamp provides the ``+MIN_TD`` behaviour.
- **No ``ChildGraphTemplate`` / ``ChildGraphInstance`` classes.** Template =
  ``CompiledSubGraph``; instance = ``GraphValue`` in node storage.
- **Binding modes.** Of the RFC's binding-mode catalogue only the modes the
  current milestone needs are implemented: direct input binding (exists,
  incl. leaf-wise for structural boundary args), root output forwarding
  (exists), ``alias_parent_input`` pass-through (exists, the ``ParentInput``
  output-binding kind), key-value injection (with ``switch_``/``map_``), and a
  tactical copy-merge for ``map_`` output (see below). Everything else
  (context import/export, REF adaptation across the boundary, recordable-state
  pass-through) is rejected explicitly at wiring time until designed here.
- **Sampled semantics on rebind.** Per the sampled-runtime contract, when
  ``switch_`` retargets the forwarding output at time *t* the output samples the
  new branch at *t*; we deliberately diverge from Python's ``value = None``
  reset and will pin the behaviour with tests when ``switch_`` lands.
- **No slab payload stores yet.** ``map_`` will use
  ``unordered_dense::map<Value, std::unique_ptr<PerKeyState>>`` — pointer-stable
  per-key state (a relocated live ``GraphValue`` would break notifier
  subscriptions). The RFC's ``StablePayloadStore`` is a recorded refinement.


Roadmap (this milestone)
------------------------

1. **Done — sub-graph compilation.** ``BoundarySource`` +
   ``Wiring::finish_subgraph`` + ``compile_subgraph<G>`` + ``nested_<G>``
   (this page, above).
2. **Done — ``reduce`` over a fixed-size ``TSL``, as an operator** — the
   wiring-time linear/tree flatten mirroring Python ``_reduce_tsl`` is the
   default registered overload of the ``reduce`` operator; the combiner is the
   ``WiredFn`` scalar (see its section above); leaves are
   ``default(ts[i], zero)`` with the op-aware ``zero_`` (derived) or the
   explicit-zero arity. ``map_`` / ``switch_`` follow the same operator shape
   when they land.
3. **Done — scheduling push delegation + shared binding helpers.** See
   *Scheduling delegation* above; helpers extracted to
   ``runtime/nested_bindings.h``.
4. **Done — ``switch_``** (see its section above): one active branch child,
   sampled retarget on key change, key consumption as an ordinary boundary
   input. ASAN/UBSAN-verified (branch teardown/rebuild churn).
5. ``map_`` over ``TSD`` — keyed child instances driven by the input dict
   delta; child outputs **copy-merged** into the owned ``TSD`` output (child
   outputs cannot alias into TSD elements while ``TSDSlotStorage`` has no
   link children — recorded as the tactical mode, with proxy/link-based
   aliasing as the refinement).
6. *(gated)* associative ``reduce`` over ``TSD`` — only after the 2603 reduce
   design is ported into this page and reconciled.

Non-goals for the milestone: ``mesh_``, ``try_except``, services/contexts,
push sources inside nested graphs, explicit ``__keys__`` / ``pass_through`` /
``no_key`` wrappers, TSD link-children aliasing, non-associative or dynamic-TSD
reduce kernels, graph-level generic (``TsVar``) sub-graphs.

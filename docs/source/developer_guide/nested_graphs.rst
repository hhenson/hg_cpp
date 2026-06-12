Nested Graphs
=============

This page is the design record for **non-flattening nested graphs** — the
substrate and operators (``nested_``, then ``switch_`` / ``map_`` / ``reduce``)
where a node owns and drives a child graph at runtime instead of inlining it at
wiring time. It builds on the runtime nested-graph node described in *Graph
Wiring > Nested graphs* and supersedes the prior C++ attempt's design where the
two conflict (see *Reconciliation with the 2603 RFC* below).

Scope of this page: the wiring-side sub-graph compilation and the runtime
design decisions for the nested operators implemented so far. Sections are added
in the same change as the code they describe.


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

Scheduling follows the existing substrate: child start can schedule the parent
from the child's cached ``next_scheduled_time()``, and child evaluation
propagates the cached next time to the parent before the nested evaluation
block returns. A source-only sub-graph therefore drives the outer graph
(proven by ``tests/cpp/test_nested_wiring.cpp``). Child-driven **push**
propagation covers a child node scheduled by notification while the parent is
idle.

Tests: ``tests/cpp/test_nested_wiring.cpp``.


Higher-order constructs are operators
-------------------------------------

``reduce``, ``switch_`` and the current ``map_`` subset are **ordinary
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
(``wire<stdlib::reduce_>(w, fn<add_>(), tsl_port)``) described here, and the
**dynamic TSD** kernel (next section).

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
``requires_`` accept fixed-size TSL inputs only; the dynamic-TSD overloads
(next section) gate on a TSD input the same way. Still deferred: dynamic-TSL
reduction, a time-series (port) ``zero`` argument, and non-associative linear
reduction.

Tests: ``tests/cpp/test_reduce.cpp`` (including a user overload gated on the
wired function's identity, mirroring ``ext/main``'s ``test_map_overload``).


``reduce`` over dynamic ``TSD``
-------------------------------

The dynamic kernel is a further registered overload of ``reduce`` — one
**runtime node** owning a balanced binary tree of combiner child graphs over
the live keys of its TSD input. This section is the port of the 2603 design
record (``ext/2603/docs/design/2026-04-v2-reduce-design.md``), reconciled to
the current substrate; where the two differ, this section is authoritative.

**Vocabulary reconciliation** (2603 → current): ``ChildGraphTemplate`` /
``ChildGraphInstance`` → ``CompiledSubGraph`` (compiled once at wiring from
the ``WiredFn``, two boundary args ``(lhs, rhs)``) / ``GraphValue`` via
``make_nested_graph``; ``OutputLink`` root publication → the node's
**forwarding output** + ``bind_forwarding_output_to_source``;
``StablePayloadStore`` + per-graph slabs → ``unique_ptr`` combiner entries
(pointer-stable; the slab store remains the recorded refinement, as for
``map_``); ``MapViewDispatch`` slot observers → **current-key
reconciliation** when the TSD input modifies, re-points, becomes invalid, or
is first observed (the same lifecycle lesson as ``map_``; observer-driven
mirroring is the recorded refinement, settling the 2603 doc's open question 1
for v1).

**Core decisions kept from 2603** (the load-bearing ones):

- **Leaves are aliases, not child graphs.** A leaf position references a live
  source element output (``tsd.at(key)``); only **internal combine points**
  instantiate the combiner child graph, and only when **both** subtrees are
  non-empty. ``n`` live keys cost at most ``n - 1`` live combiners; 0 or 1
  keys cost none.
- **``zero`` is the empty-collection result, not odd-branch padding.** The
  Python implementation pads odd branches with ``zero`` (the known
  spurious-zero-tick wart); the dynamic kernel never does. Root publication:
  no live keys → forward to the ``zero`` input's bound output; one →
  forward to that element directly; more → forward to the root combiner's
  output. (The **fixed-TSL** overloads above retain Python's
  ``default(ts[i], zero)`` semantics — a principled difference: a fixed TSL
  has not-yet-valid *positions*, a TSD key is live by existence.)
- **Dense leaves over a power-of-two capacity.** ``key ↔ dense leaf``
  mappings; erasing a key moves the **last** live leaf into the vacated
  position (bounded rebalancing). Internal nodes are heap-indexed
  (``0`` = root, children of ``i`` at ``2i+1`` / ``2i+2``), so deeper nodes
  have higher indices.
- **Aggregate resolution** (per position): empty leaf → ``Empty``; live leaf
  → ``Leaf``; internal: both children empty → ``Empty``, exactly one
  non-empty → alias that child's aggregate, both → ``Node`` (the combiner).

**Binding discipline (the map_ lesson, restated):** value ticks normally flow
through standing bindings: a leaf tick notifies its combiner's child node
directly (the input is bound to the upstream element output), the combiner's
output tick notifies its parent combiner, and the cascade resolves within one
evaluation pass by processing combiners **deepest-first** (descending heap
index — a parent has a lower index than its children, so it evaluates after
them). When the TSD root modifies or re-points, v1 conservatively reconciles
the current key set and refreshes live combiner/root bindings; this covers
ordinary add/remove deltas and forwarding/REF retargets, including same-key
different-source retargets. Combiner evaluation is **unconditional** per
pass, like the other nested operators: a same-cycle notification lands in a
child's schedule array but not its cached next time, and an idle child
evaluate is a cheap scan. Re-binding samples held values (``bind_output``
notifies on valid-source binds), and a root **re-point to an already-valid
source is a tick of the reduce output at the publication time** (the aliased
value changed without the target writing — ``mark_modified`` through the
forwarding link), per the sampled-runtime contract.

**v1 simplifications (recorded, with the refinement path):** structural
events recompute the internal bindings in one ``O(capacity)`` pass (per-path
dirty propagation is the refinement); ``leaf_capacity`` is monotonic
(conservative shrink is the refinement). Structural recompute is **three
phases ordered so a link is never bound to or unbound from a dead target**:
create needed combiners deepest-first (parents can reference fresh
children), bind every live combiner and publish the root while every old
target is still alive, then retire displaced combiners **root-first**
(ascending heap index — a doomed parent unbinds from its still-alive doomed
child; the same teardown-direction reasoning as the ``map_`` storage-plan
ordering, and also the explicit ``ReduceNodeStorage`` destructor order).
The node's own output is a forwarding endpoint linking into field-held
combiner outputs, so the reduce field uses the default *before-output* plan
placement (the ``nested_``/``switch_`` direction).

**Exception / unwind policy:** reduce rebuild failures are not intended to
recover and continue execution. If child construction, binding, starting, root
publication, or evaluation throws, the exception propagates to the graph
evaluator; an outer error-capture policy may catch it, but the graph is
considered failed and should be stopped. The nested node must still be safe to
stop and destroy after such an unwind: all link mutations and retired-child
handling must preserve pointer stability, keep old targets alive until
subscribers have moved off them, and use rollback/cleanup guards so no input
subscription or forwarding output is left pointing at dead child storage. Future
optimisations to the rebuild path must keep that stop-after-failure safety
property even if they make the key/combiner update more transactional.

**Deferred:** non-associative ordered reduction (a linear chain over
``TSD[int]``; ``zero`` as a true lhs seed) and tuple reduction through it,
``TSS`` reduction, pass-through combiner outputs for the dynamic kernel, and
unifying the fixed-TSL overload onto this kernel (2603 §10 recommends keeping
TSL static-shaped first — we do).

Runtime: ``runtime/reduce_node.{h,cpp}``. Tests: ``tests/cpp/test_reduce.cpp``
(dynamic-TSD cases, including the no-spurious-zero regression from 2603
phase 3).


Scheduling delegation
---------------------

The RFC's clock invariant — *the parent must wake no later than the child's
next scheduled work* — is realised as two halves folded into the existing
graph ops (no separate engine/clock object; see the recorded decision):

- **Pull** (nested graph evaluation in ``graph.cpp``): child graph evaluation
  maintains a cached ``next_scheduled_time()`` as it scans and evaluates child
  nodes. Before the nested evaluation block returns, it schedules the parent
  node at that cached time. ``single_nested_graph_propagate_schedule`` remains
  for the start path because child start can schedule work before any child
  evaluation block exists.
- **Push** (``nested_schedule_node_impl`` in ``graph.cpp``): any
  ``schedule_node`` recorded on a child graph **while it is idle**
  (``started && !evaluating``) immediately schedules the parent node at the
  same time — the path a notification or wall-clock alarm takes between
  parent evaluations, and the safety net the keyed operators (``switch_`` /
  ``map_``) rely on when a child is not evaluated every parent cycle. The
  push is deliberately gated off during child evaluation (pull covers it, and
  pushing mid-evaluate would schedule a spurious extra parent cycle) and
  while the child is stopped.

Multi-level nesting recurses up to the root naturally. The boundary *binding*
helpers shared by all nested node implementations
(``walk_ts_path`` / ``bind_input_to_source`` /
``bind_forwarding_output_to_source``) live in
``include/hgraph/runtime/nested_bindings.h`` — no bespoke bind/unbind logic
per operator.

An evaluating nested graph always has a parent node. Missing parent state is a
construction/test setup error, not a scheduling case to branch around in the
nested evaluation path.


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
  ``[key, ts…]``. A branch consumes outer input ``0`` as the key only when its
  first parameter is named ``key``; non-key branch binding paths simply shift
  past that outer input. Source-style branches (arity 0) take no bindings at
  all.
- **Runtime** (``runtime/switch_node.{h,cpp}``, on the shared
  ``nested_bindings`` helpers): at most one live child in node storage. On a
  key change (or any key tick with ``reload_on_ticked`` — runtime-supported,
  not yet exposed by the operator) the active child is stopped and retired,
  the new branch (else the default, else a runtime error) is built, bound and
  started, and the forwarding output **re-points**. Retired branches are kept
  alive for at least one outer evaluation cycle so downstream consumers that
  were bound through the previous branch can unsubscribe in normal topological
  order.
- **Sampled semantics** (the sampled-runtime contract; the recorded
  divergence from Python's ``value = None`` reset): the freshly selected
  branch evaluates with the *current* upstream values even when they did not
  tick that cycle. Binding or rebinding an active child input to an
  already-valid source schedules the child through the normal input
  notification path at the switch time. Pinned by test (a swap while the ts
  input holds emits the new branch's value immediately).
- **Lifecycle**: switching away stops and retires the child ``GraphValue``;
  switching back rebuilds a fresh instance (per-branch state resets — pinned
  by test). The previous retired set is released on the next switch
  evaluation, and on node stop. The output's resolver discovers the schema by
  compiling the first branch (``resolve_default_types`` on the overloads).
- ``switch_(key, cases, *ts, **kwargs)`` takes any number of positional
  and **keyword** time-series arguments (see *Operators > Variadic operator
  parameters* and *Named arguments…*). Branches bind positional args in
  order (optionally preceded by the key) and keyword args **per branch by
  the branch's own parameter names** (``NamedPort`` / ``In<"name">`` —
  branches may declare the same names in different orders, like Python).
  Deferred: all-sink switches.

Tests: ``tests/cpp/test_switch.cpp``.


``map_``
--------

``map_(func, *args, **kwargs)`` owns **one child graph instance per key**
of its multiplexed ``TSD`` input(s) — an operator like the rest of the family
(``wire<stdlib::map_>(w, fn<G>(), tsd_port[, ts…])``).

- **Key lifecycle is driven by ``__keys__: TSS[K]``**. Wiring supplies either
  the explicit ``__keys__`` argument or an inferred key set from the
  multiplexed inputs. The runtime treats the key-set slot layout as
  authoritative: first bind and source re-point rebuild from live key slots;
  ordinary key ticks create or destroy children from ``slot_added`` /
  ``slot_removed``. Multiplexed TSD membership only affects element binding,
  not child existence. An invalid or unbound ``__keys__`` source stops all
  children and clears the owned output.
- **Per-key state** lives in node storage as a ``ValueSlotStore`` indexed by
  the current ``__keys__`` slot id. Each constructed slot owns the copied key
  ``Value``, an optional per-key ``TS<K>`` output, and the child ``GraphValue``.
  The owned key is used for output erasure across removals and re-points. Entry
  member order is load-bearing: the child graph (subscriber) tears down before
  the key output it observes.
- **Child boundary args** are sourced per ordinal (``MapArgSource``): the
  **element** binds to the parent TSD input's bound output child *at the
  entry's key*; if that key is absent from a multiplexed TSD, the child input
  is left unbound until the key appears there. The **key** (when ``func``'s
  first parameter is NAMED ``key`` — ``ndx`` on TSL maps; the Python rule,
  applied per branch on ``switch_`` too; ``arg<"__key_arg__">(Str{…})``
  renames it, ``""`` disables — arity detection is gone) binds to an
  entry-owned
  ``TS<K>`` output written once at creation; **broadcast** args bind whole to
  the corresponding outer input. Any outer input re-point refreshes the
  existing child bindings through the shared ``nested_bindings`` helpers.
- **Output (write-through, no copy)**: the output is an *owned*
  ``TSD<K, OUT>`` — for every key a **real element is instantiated** in it
  (``TSDDataMutationView::operator[]`` at entry creation), and the child's
  terminal node is built with a **forwarding output endpoint**
  (``NodeBuilder::output_endpoint`` override, set by the operator on the
  compiled template's terminal) that the map node binds onto the parent's
  element. The child node then **writes the parent's storage directly**
  through the link (``target_link_copy_value_from`` resolves the target and
  runs the standard mutation path, so modified tracking and dict parent
  recording are exactly the owned-write path). Bindings are established at
  entry creation and refreshed when an outer input's bound output re-points
  (an upstream forwarding/REF retarget), detected with one handle compare per
  outer input per cycle. Output forwarding retargets mark the
  forwarding endpoint modified so active downstream consumers schedule for the
  retarget cycle, including retargets to an invalid source. Removals destroy
  the child (its link unbinds) and then ``erase`` the element (publishing the
  removed delta). Wiring rejects ``OUT`` shapes that cannot embed as TSD
  elements (``TSD`` / dynamic ``TSL``), reference-valued outputs, and
  pass-through/sub-path outputs. On node stop, ``map_`` stops every child,
  erases the owned TSD elements, and clears the output; this differs from
  ``nested_`` / ``switch_`` forwarding aliases, where the last forwarded value
  can remain observable after stop.
- **Storage-plan ordering is load-bearing**: the map field is placed *after*
  ``output`` in the node storage plan (``node_storage_plan_for``'s
  ``extra_fields_after_output``) so reverse-order destruction tears the
  children (whose links point INTO the output) down before the output —
  the mirror image of ``nested_``/``switch_``, whose outer forwarding output
  links INTO the field-held child and therefore destroys first.
- **Scheduling**: an evaluated child propagates its own next time to the map
  node (the graph-level pull); children left unevaluated this cycle pull
  their pending schedule up explicitly; out-of-band child schedules push
  through the nested-graph delegation as everywhere else.
- The output schema resolver discovers ``TSD<K, OUT>`` by compiling ``func``
  at the element schema (``resolve_default_types``, like ``switch_``).
- **TSL multiplexing is a wiring-time expansion**, not a runtime node —
  Python's ``_map_no_index``: the fixed-TSL overloads inline one application
  of ``func`` per index (key = ``const(i)`` at ``TS<Int>`` when ``func``
  takes it first, broadcasts passed whole), validate the per-index output
  schemas agree, and assemble a **structural TSL** output. Selected by the
  same ``map_`` name (``requires_`` gates on a fixed-size TSL input); the
  resolver discovers ``TSL<OUT, SIZE>`` by compiling ``func`` once.
- **The variadic tail is classified, Python-style**: every TSD argument is
  multiplexed alongside the anchor (key types must agree) — the live key set
  is the **union** of their key sets; a key absent from one dict leaves that
  child input unbound (invalid) until it appears there (the phantom-element
  behaviour), and the output entry is removed only when the key has left
  every multiplexed input. Non-TSD args broadcast whole; in the TSL form a
  tail arg that is a fixed TSL of the same size multiplexes per index.
  Positional arguments map onto ``func`` parameters in order (after the
  key) and **keyword arguments resolve by the function's parameter names**
  (``NamedPort`` on sub-graph ports, ``In<"name">`` on nodes — the
  ``WiredFn`` carries its parameter names); multiplexed-ness never reorders.
  There is no fixed anchor parameter (the Python shape): the call is
  ``map_(func, *args, **kwargs)``. Inputs are first resolved onto ``func``'s
  parameter order; the first collection in that ordered list selects the
  TSD/TSL kernel, and the first TSD provides the key type. Membership changes
  anywhere are structural events: they create/destroy children and re-bind
  surviving entries.
- **The lifecycle is always keys-driven.** The ``__keys__`` ``TSS[K]``
  outer input (``MapNodeSpec::keys_input_index``) alone creates and destroys
  children; there is no in-node union scan. When not supplied explicitly
  (``arg<"__keys__">(tss)``, Python's ``__keys__``), the wiring derives it —
  ``keys_(tsd)`` for one multiplexed input, ``union(keys_(tsd)…)`` for
  several — exactly Python's ``__keys__ = union(*key_sets)``. ``keys_`` is a
  **zero-copy projection**: ``TSDOutputView::key_set()`` exposes the dict's
  key set as a bindable ``TSS`` view over the same storage, addressed in
  edges/bindings by the ``ts_key_set_path_component`` path sentinel (no node
  is wired; works through forwarding links — the link layer carries its own
  key-set binding). The key-set surface is a **first-class time series**:
  it carries its own modified tracking AND its own observer set, recorded and
  notified only by membership changes in the dict slot storage — subscribers
  to the key set are never woken by the dictionary's value ticks, with no
  consumer- or link-side special-casing. The ``union`` node reconciles
  removals against the full
  current membership, so an input going invalid (a switched-away source)
  drops its exclusive members. An explicit set is validated as a ``TSS`` of
  the mapped key type, split from the kwargs before ``func``-parameter
  binding (it is ``map_``'s argument, not ``func``'s), and rejected on TSL
  maps. Children for keys absent from a dict keep phantom/invalid element
  inputs, as before.
- **Argument tags — Python's ``pass_through`` / ``no_key`` wrappers.**
  ``stdlib::pass_through(port)`` and ``stdlib::no_key(port)`` return the same
  ``Port`` carrying a wiring-time tag (``WiringPortRef::ArgTag``), exactly how
  the Python wiring marks arguments — the tag is **never part of graph
  structure**: it rides through ``arg<"name">``, variadic and keyword
  arguments, is ignored by edge/source interning, and instead joins the map
  node's interning identity through the ``MapCallConfig`` scalar (function +
  ``__key_arg__`` + per-argument tags), so equal inputs with different tags
  never dedup to one node. Classification honours the tags: ``pass_through``
  forces the argument to bind **whole** (broadcast, whatever its kind — the
  only way a child can consume a full ``TSD``, or keep a same-size ``TSL``
  whole in the TSL form, and such an argument never anchors the kernel/size
  selection); ``no_key`` keeps a TSD multiplexed but **excludes it from
  key-set inference** (its key *type* still participates) — if every
  multiplexed input is ``no_key`` an explicit ``__keys__`` is required, and
  ``no_key`` is rejected on non-TSD arguments and on TSL maps.
- Deferred: dynamic-TSL multiplexing and sink maps.

Tests: ``tests/cpp/test_map.cpp``. ASAN/UBSAN-verified (keyed
create/destroy churn).


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
  the nested graph's ``schedule_node`` to the parent node.
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
  ``switch_`` retargets the active branch at time *t*, the new child samples any
  already-valid bound inputs at *t*. That happens by scheduling the child
  through active input bind/rebind notification, not by bypassing normal graph
  scheduling or forcing eval directly. We deliberately diverge from Python's
  ``value = None`` reset.
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
   explicit-zero arity. ``map_`` / ``switch_`` follow the same operator shape.
3. **Done — scheduling push delegation + shared binding helpers.** See
   *Scheduling delegation* above; helpers extracted to
   ``runtime/nested_bindings.h``.
4. **Done — ``switch_``** (see its section above): one active branch child,
   sampled retarget on key change, key consumption as an ordinary boundary
   input. ASAN/UBSAN-verified (branch teardown/rebuild churn).
5. **Done — ``map_`` over ``TSD``** (see its section above): keyed child
   instances driven by current-key reconciliation on mapped-source
   modification/repoint; child terminals write through forwarding outputs into
   the owned ``TSD``'s instantiated elements (no copy). ASAN/UBSAN-verified
   (keyed create/destroy churn).
6. **Done — associative ``reduce`` over ``TSD``** (see its section above):
   the 2603 design ported/reconciled first, then the runtime kernel —
   alias leaves, minimal combiner tree, zero as the empty result only.
   ASAN/UBSAN-verified.

Non-goals for the milestone: ``mesh_``, ``try_except``, services/contexts,
push sources inside nested graphs, TSD link-children aliasing,
non-associative reduce, dynamic-TSL reduction, graph-level generic
(``TsVar``) sub-graphs. (Explicit ``__keys__`` and the ``pass_through`` /
``no_key`` wrappers, originally deferred, landed within the milestone —
see the ``map_`` section.)

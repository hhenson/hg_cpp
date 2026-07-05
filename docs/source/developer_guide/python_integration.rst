Python Integration
==================

Python integration exists to preserve the current ecosystem and to support Python user-authored nodes. It should not define the core runtime architecture.

Supported Roles
---------------

- Python graph wiring,
- compatibility with existing Python HGraph user code,
- Python user nodes executed by the C++ runtime,
- packaging of optional bindings.

Boundaries
----------

Normal CMake builds should not require Python. Python-specific code should live behind optional CMake targets and be enabled only through:

.. code-block:: bash

   -DHGRAPH_BUILD_PYTHON_BINDINGS=ON
   -DHGRAPH_ENABLE_PYTHON_USER_NODES=ON

GIL And Runtime Locks
---------------------

The C++ runtime must assume that it does **not** hold the Python GIL unless a
local scope has explicitly acquired it. Any path that calls Python code or uses
Python C API objects must acquire the GIL at that boundary. This includes Python
node ``start`` / ``eval`` / ``stop`` callbacks, lifecycle observers implemented
in Python, Python notification callbacks, Python-backed sender functions, and
exception translation that inspects Python exception state.

Conversely, the real-time engine must not hold the GIL while waiting on runtime
condition variables or other blocking primitives. It also must not hold graph,
node, sender, receiver, or clock mutexes while entering Python. The ordering
rule is:

1. release/acquire runtime locks only for C++ state,
2. drop those locks before calling Python,
3. acquire the GIL immediately around the Python call,
4. release the GIL before a blocking wait.

This is especially important for push-source nodes: external threads enqueue
through a sender and wake the real-time evaluation clock, while the evaluator may
be sleeping on a condition variable. The implementation must avoid GIL/runtime
lock inversion in both directions.

Topics To Specify
-----------------

- GIL ownership during node evaluation,
- Python object lifetime inside C++ runtime state,
- exception translation,
- Python callback scheduling,
- conversion between Python type metadata and C++ schemas,
- packaging and ABI policy.


Cross-boundary type identity
----------------------------

The C++ runtime identifies schemas by **pointer equality of interned metadata**:
two equivalent schemas resolve to the same ``const TSValueTypeMetaData*`` /
``const ValueTypeMetaData*`` because the ``TypeRegistry`` interns them. Anything
that matches types across the boundary — notably *operator* overload dispatch
(see *Operators*) — relies on this: a Python ``TS[int]`` and a C++ ``TS<Int>``
must produce the **same** interned pointer.

The invariant that guarantees it: there is exactly **one canonical scalar per
logical type**, and every name a Python type uses is an **alias** onto that
canonical scalar (via ``register_value_alias``), never a separately-interned
synthetic. Concretely, ``value_type("int")`` (the Python lookup) must return the
*same* pointer as ``register_scalar<int>("int")`` (the C++ registration); since
``TypeRegistry::ts(value_meta)`` interns on the value pointer, identity then
composes upward automatically (``TS``, ``TSL``, ``TSD``, …). The standard-types
seed must run **before** any overload is registered on either path; the C++ test
listener seeds via ``register_standard_types()`` and the Python module (when it
is built) must seed at import via a ``register_builtin_value_types()`` entry
point — the two must agree on names and aliases.

Hosting a Python node
---------------------

A Python user node is hosted without a new node *kind*: ``NodeCallbacks``
(``include/hgraph/runtime/node.h``) is already a type-erased ``std::function``
triple, so a Python implementation produces the same ``(NodeTypeMetaData,
NodeCallbacks, TSEndpointSchema)`` that ``NodeBuilder::native`` consumes, with
``NodeCallbacks::evaluate`` acquiring the GIL, marshalling the ``NodeView`` to
Python, and writing the result back through the ``TSOutputView``. This is the
mechanism behind a Python *operator* implementation registering as an ordinary
candidate (see *Operators > The Python implementation path*), built only under
``HGRAPH_ENABLE_PYTHON_USER_NODES``.

The Bridge (Slice 1 — Landed)
-----------------------------

``bindings/python/`` (opt-in: ``-DHGRAPH_BUILD_PYTHON_BINDINGS=ON``; the
default build never needs Python) holds the nanobind module ``_hgraph``.
Slice 1 proves the contract end to end from Python:

- **Wiring by name**: ``Wiring.wire(name, args, kwargs, output_type=None)``
  builds erased ``WiringArg`` lists (ports and scalars, positional and
  keyword) and goes through the exact three calls the template-free proof
  established (``OperatorRegistry::resolve`` → ``impl->wire``); ports come
  back as opaque handles. ``ts_type("TS[int]")`` resolves expected-output
  schemas by registry name.
- **Running**: ``Wiring.run()`` finishes the wiring, builds a simulation
  executor and runs it; ``Wiring.set_replay(key, [values|None])`` seeds the
  replay buffer at wiring, ``Run.recorded(key)`` reads recordings back as
  Python values — the eval_node harness shape, from Python.
- **Eager const**: ``evaluate_const(name, args, kwargs, output_type)``
  exposes the P1 kernel.
- **Scalar conversion** (slice 1): bool/int/float/str + datetime/timedelta
  via the vendored nanobind chrono casters. Containers, bundles, Frame and
  the remaining atoms are the next conversion slice.
- Registration happens at module import; ``reset_registries()`` re-seeds.
  Type metadata handles must be re-looked-up after a reset.

Landed with a load-bearing fix: the core registries (TypeRegistry,
ValuePlanFactory, TSDataPlanFactory, OperatorRegistry) are **immortal**
(leaked) singletons — in a shared module, cross-TU static destruction
order destroyed interned bindings before the operator impls' default
``Value``\ s that reference them (a segfault at interpreter exit).
Registries hold process-lifetime immutable artifacts; they are never
destroyed. Tests: ``bindings/python/tests/test_bridge.py`` (registered
with ctest under the option).

The hgraph Package (Slices 2-4 — Landed)
----------------------------------------

``python/hgraph`` is the package that will eventually be **the** hgraph
package (Howard's direction); it mirrors the Python hgraph surface over
the ``_hgraph`` bridge module (built from ``python/module.cpp``):

- **Types**: ``TS[int]``, ``TSS[str]``, ``TSD[str, TS[int]]``,
  ``TSL[TS[int], Size[N]]``, ``TSB[SchemaClass]`` (``TimeSeriesSchema``
  annotations) — each subscription resolves to an interned C++ type handle.
- **Operator surface**: every registered operator (113) is a module-level
  function via PEP 562; ``WiringPort`` carries hgraph's dunder sugar
  (arithmetic/comparison/bitwise/unary, ``[]``, ``.field`` via
  ``getattr_``); ``const`` takes hgraph's ``tp=``.
- **Composition/evaluation**: ``@graph`` (nested graphs inline by calling),
  ``run_graph(fn, *args, start_time=, end_time=)`` returning
  ``[(time, value), ...]``, ``eval_node(fn, *vectors, __start_time__=,
  __end_time__=)`` with schema-directed test vectors (TSS from sets, TSD
  from ``{key: value}`` dicts with ``None`` = removal, TSL from per-index
  lists) and friendly delta read-back (``REMOVED`` sentinel). No implicit
  run bound is injected — a test that cannot quiesce sets ``__end_time__``
  explicitly and says why.
- **Higher-order**: ``map_``/``reduce``/``switch_`` over **named operator
  callables** — the bridge pre-instantiates ``fn<X>()`` erasures for the
  stdlib markers (``wired_op``); ``switch_`` builds the ``SwitchCases``
  scalar; ``feedback(tp, initial)`` replicates the C++ feedback wiring
  erased (same node tags → same interning).
- **Value conversion**: all atoms (incl. date/time/bytes) + recursive
  containers both ways; ``evaluate_const`` exposes the P1 kernel.

Recorded divergences / gaps (the morning-summary list):

- REF is **value-only** (agreed): no output dereferencing from Python.
- Python ``@graph`` functions are full ``WiredFn`` citizens (the ruled
  type-erased context+ops backend): ``map_``/``switch_`` COMPILE them as
  C++ sub-graphs, ``reduce`` accepts raw lambdas (un-annotated callables
  assume an output; only an explicit ``-> None`` marks a sink). Identity
  is the user function object; records are immortal (WiredFn contexts).
- **Python user nodes landed** (the ruling realised): ``@compute_node`` /
  ``@sink_node`` / ``@generator`` run Python functions as runtime nodes —
  graph-thread only, both modes, no side effects beyond their output. The
  GIL is RELEASED the instant the run loop starts and ACQUIRED around each
  Python-node call. Inputs arrive as plain Python VALUES (a recorded
  divergence from Python hgraph's TimeSeries view objects); a compute
  node's return value ticks its output (``None`` = no tick); a generator
  yields ``(datetime, value)`` pairs emitted at their absolute times. The
  bridge registers internal erased operators (``__py_compute`` /
  ``__py_sink`` / ``__py_generator``) over an immortal callable-record
  scalar. Argument ports pack into ONE structural un-named TSB and
  wiring-time SCALARS ride a list-of-Any scalar, with a LAYOUT string
  (part of node identity) mapping the python call positions — any arity,
  one operator (Howard's review of the per-arity first cut).
  ``STATE`` / ``CLOCK`` / ``SCHEDULER``-annotated parameters are injected
  and MUST default to ``None`` (the hgraph convention, enforced at
  decoration - graph code never supplies them):
  STATE is a lazily-created per-node namespace preserved across ticks,
  CLOCK exposes ``evaluation_time``, SCHEDULER exposes
  ``schedule(datetime)`` / ``schedule_delta(timedelta)``. Each
  ``@generator`` call is a distinct source node.
- ``passive(port)`` landed (both languages): the feedback idiom is
  ``a + passive(fb())``, and such loops quiesce naturally. ACTIVE feedback
  consumption still needs an explicit end time.
- ``run_graph`` output times are cycle-aligned from the start time in
  ``MIN_TD`` steps (the simulation clock convention).
- ``@component`` + the record/replay modes are surfaced (all through the
  ``hgraph`` package — ``_hgraph`` is internal and never user-imported):
  ``record_replay_scope(RecordReplayEnum.RECORD | ...)`` is the context
  manager over the C++ RAII scope; the Python ``@component`` decorator
  replicates the C++ wrapping rules by name (Record / Replay /
  ReplayOutput / Recover / Compare); ``comparison_summary`` reads Compare
  results and ``frame_store_contains`` probes the store;
  ``recovering_pass_through`` is registry-wirable as
  ``__recovering_pass_through``. The eval_node/run_graph harness wires
  ungated ``__harness_record``/``__harness_replay`` aliases so the active
  record/replay MODEL never captures the test harness itself.
- **Real-time + push sources** are surfaced with hgraph's shapes:
  ``run_graph(..., run_mode=EvaluationMode.REAL_TIME)`` runs the
  wall-clock executor (the GIL is released for the whole run), and
  ``@push_queue(tp, conflate=False)`` wraps a function that IS the node's
  start lifecycle hook — called with the thread-safe sender callable
  (plus wiring-time scalars) once the graph runs; values convert
  schema-directed on the sending thread and cross the sanctioned C++
  boundary. Wiring the decorated function returns its port.
- **Frame ↔ pyarrow**: Frames cross the boundary as ``pyarrow.Table``\ s
  through the Arrow C stream protocol (``__arrow_c_stream__`` capsules —
  zero copy, version-independent): ``frame_store_read`` returns Tables,
  Tables convert back to Frame values, and ``to_table``/``from_table``
  are fully usable from Python.
- **Contexts** are surfaced BOTH ways (Howard: existing python code must
  keep working). The hgraph-compatible API: ``with port:`` publishes (the
  wiring port is a context manager; ``as name`` binds the context name via
  frame-local resolution), and a ``CONTEXT[TS[X]] = None`` parameter on a
  user node resolves by type/name — ``REQUIRED`` / ``REQUIRED["name"]`` /
  a call-site ``context="name"`` override, ``WiringError`` on failure; the
  resolved context VALUE is entered (context-manager protocol) around each
  eval, exactly hgraph's semantics. The C++-design-record API also stands:
  ``with hg.context("name", port)`` + ``hg.context.get/has`` over the
  string-keyed scope stack. Underpinning both: **arbitrary python objects
  are first-class scalars** — ``TS[AnyClass]`` maps onto the new
  ``object`` value kind (a GIL-safe refcounted ``PyObj`` scalar; value ops
  acquire the GIL around refcount changes since the run loop releases it),
  and ``const`` infers it without ``tp=``.
- **Services** are surfaced per the runtime-identity rulings
  (services.rst *Runtime service identity*): ``@reference_service`` /
  ``@subscription_service`` / ``@request_reply_service`` decorate
  interface stubs (annotations give the schemas; calling the stub wires a
  client with ``path=``); implementations are
  ``@service_impl(interfaces=...)``-decorated (hgraph's shape — the
  declared interfaces validate the impl's signature per flavour at
  decoration and drive ``register_service(path, impl)``, path first;
  undecorated impls are refused); interfaces may be stubs or the NAMES of
  C++-defined interfaces (the ruled direction). MULTI-INTERFACE
  implementations work per the C++ ``register_services`` shape: the impl
  takes no wired inputs and uses ``impl_input(stub, path)`` /
  ``impl_output(stub, out, path)`` per interface inside its body (the
  erased ``service_impl_input``/``service_impl_output`` flows +
  ``register_multi_service_impl`` with the combined required-endpoint
  scope). The erased core
  (``types/service_runtime.{h,cpp}``) shares
  the role markers, path grammar and node makers with the templates, so
  an erased registration UNIFIES with a template client on the same path
  (proven in ``test_service_runtime.cpp``). **Adaptors and mesh** are surfaced too:
  ``@adaptor`` stubs (first TS param = graph-side input, return = output,
  both optional), ``@adaptor_impl(interfaces=...)`` +
  ``register_adaptor(path, impl)`` with impl-side ``from_graph``/
  ``to_graph`` — the four adaptor markers de-templated to roles like the
  services; and ``mesh_(func, ...)`` + ``mesh_ref(key[, name])`` with the
  func's element type taken from the python fn's return annotation
  (records carry their annotated output schema).
- **TimeSeries view objects** (Howard's rulings: proper C++ objects, all
  kinds, strictly lazy): user nodes receive a C++-bound ``TimeSeries``
  view over the LIVE input — nothing converts unless accessed.
  Universal: ``.value`` / ``.delta_value`` / ``.modified`` / ``.valid`` /
  ``.all_valid`` / ``.last_modified_time``. Kind-dispatched: TSS
  ``added()``/``removed()``; TSD ``[]``/``keys()``/``modified_keys()``/
  ``modified_items()``/``removed_keys()``/``in``; TSL ``[i]``/``len``;
  TSB ``.field`` / ``[]``. Child access returns child views sharing the
  parent's lifetime guard: a view stored past its node's evaluation
  raises rather than dangling. ``delta_value`` builds hgraph's friendly
  shapes natively from the dict/set views (no canonical-delta
  intermediate).

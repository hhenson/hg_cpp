Services, adaptors, shared outputs and contexts
===============================================

This page is the **authoritative design record** for the graph-boundary layer:
shared outputs, the context runtime primitive, adaptors, and the three service
flavours (**reference**, **subscription**, and **request/reply**). It records the model
that has landed — the "Boundary design decisions" originally drafted on the
:doc:`roadmap` are restated here as the canonical form. The Python reference is
``ext/main/hgraph`` (``@reference_service`` / ``@subscription_service`` /
``@request_reply_service`` / ``@service_impl``); the C++ adaptations are
deliberate and noted where they diverge.

Code: ``include/hgraph/types/service_wiring.h`` (wiring surface),
``include/hgraph/runtime/service_node.h`` + ``src/hgraph/runtime/service_node.cpp``
(runtime source/capture nodes), ``include/hgraph/runtime/shared_output_node.h``
and ``include/hgraph/runtime/context_node.h`` (the shared primitive). Tests:
``tests/cpp/test_service_wiring.cpp``, ``test_service_node.cpp``,
``test_shared_output_node.cpp``, ``test_context_node.cpp``.


The boundary model (design decisions)
-------------------------------------

Everything on this page is built from one runtime shape — a **feedback-style
source/capture pair** — applied with different payloads:

.. mermaid::

   flowchart LR
      client["client / producer node"]
      cap["capture node (sink)<br/>records intent into the paired source's state"]
      src["boundary source (pull source)<br/>owns the shared output + graph-local state"]
      out["owned output<br/>REF&lt;T&gt; / TSS&lt;K&gt; / TSD&lt;int, request&gt;"]
      cons["consumers bind here and are woken by<br/>ordinary output notification"]

      client --> cap
      cap -->|"write state; schedule_node<br/>(start: now / eval: +MIN_TD)"| src
      src -->|"applies state in one mutation<br/>of its own output"| out
      out --> cons

- Runtime boundary outputs follow the normal graph rule: every non-sink node
  owns its output, and boundary machinery forwards into that output instead of
  copying values between outputs.
- The **source** is a pull-source node that owns the shared output and
  graph-local state. The **capture** node is a sink that records intent (a
  producer reference, a key add/remove, a request value) into the paired
  source's state and schedules the source. The source mutates only its own
  output, so client changes never copy values between outputs.
- No global-state subscribers and no shared ownership for graph-local state:
  consumers bind to the source node's output and are woken by ordinary output
  notification. This keeps the boundary inside the single scheduling model
  (everything funnels through ``schedule_node``).
- **Scheduling:** capture during ``start`` schedules the paired source for the
  current engine time; capture during graph evaluation schedules it for
  ``evaluation_time + MIN_TD`` so a source that has already passed in rank
  order is observed on the next engine cycle (the same one-cycle rule as
  ``feedback``).
- **Lifecycle:** the source clears its captured state on ``stop``. A restarted
  graph must republish through capture before the source can produce a live
  shared output.
- **Teardown:** because boundary links are retargeted at runtime to outputs the
  ranker never saw, they may point at higher-ranked nodes — which reverse-rank
  storage destruction frees first. The graph therefore tears all subscriptions
  down at **stop**, while every producer is alive (edge unbind + alternative-
  store release; see *Lifecycle Teardown* in :doc:`architecture`); disposal
  must find no references.

The per-flavour payloads:

- **Shared output** (``runtime/shared_output_node.h``): the source owns a
  ``REF<T>`` output and graph-local REF state; the capture sink records the
  producer's reference (``make_shared_output_source_node`` /
  ``make_shared_output_capture_node``, addressed by ``output_key(path)``).
- **Context** (``runtime/context_node.h``): the same primitive with
  wiring-scoped keys — ``context_output_key(scope, …)`` plus
  ``make_context_source_node`` / ``make_context_capture_node``. Context keys
  are wiring-time identifiers for scope resolution; they are **not** runtime
  ``GlobalState`` storage locations for copied reference values. (The
  user-facing ``@graph``-level context wiring API is still pending approval —
  see *Status* below.)
- **Subscription service**: the source owns a ``TSS<K>`` output and graph-local
  reference counts; capture sinks enqueue key add/remove intents and schedule
  the source (``make_subscription_key_source_node`` /
  ``make_subscription_key_capture_node``). Keys published by the source appear
  the cycle after capture; releases are reference-counted across captures
  (``tests/cpp/test_service_node.cpp``).
- **Request/reply service**: the source owns ``TSD<int, request_schema>``.
  Each client has a **stable wiring-time request id**; capture sinks update
  source-local mutable delta state; the source applies that delta in one
  mutation when scheduled and then resets it. Multiple captures can update
  before the source emits, so the final request delta is **cumulative**
  (``make_request_input_source_node`` / ``make_request_input_capture_node``;
  proven by "request/reply source emits cumulative client requests" in
  ``test_service_wiring.cpp``).

Related decision recorded with this layer: real-time wall-clock scheduler
alarms use the normal graph schedule queue — ``NodeScheduler(...,
on_wall_clock=true)`` is enabled only for real-time graph executors, where
engine time is wall-clock-aligned; simulation rejects wall-clock alarms because
simulated time cannot be advanced by host time.


Wiring surface
--------------

A service is declared as a plain struct naming its schemas. The type aliases a
descriptor declares are its **flavour tag** — exactly one of the three alias
sets must be present, and the sets are mutually exclusive (checked by concepts
at compile time: ``output_schema`` = reference, ``key_type`` +
``value_schema`` = subscription, ``request_schema`` + ``response_schema`` =
request/reply):

.. code-block:: cpp

   struct ReferencePricesService          // reference service
   {
       static constexpr std::string_view name{"reference_prices"};
       using output_schema = TSD<Int, TS<Int>>;
   };

   struct PricesService                   // subscription service
   {
       static constexpr std::string_view name{"prices"};
       using key_type     = Int;
       using value_schema = TS<Int>;
   };

   struct AddOneService                   // request/reply service
   {
       static constexpr std::string_view name{"add_one"};
       using request_schema  = TS<Int>;
       using response_schema = TS<Int>;
   };

An implementation is an ordinary node or sub-graph whose signature matches the
flavour's contract — a subscription implementation receives the service's key
set (``In<"keys", TSS<K>>``), a request/reply implementation receives the
request dictionary (``In<"requests", TSD<Int, request_schema>>``) and produces
the keyed response dictionary, and a reference implementation simply produces
``output_schema``. An implementation may declare ``Scalar<"path", Str>`` to
receive the service path it was registered under (path injection).

Registration binds ``Impl`` to the service at a path. Consumption goes through
the **ordinary** ``wire<>`` verb: the service descriptor is passed where a node
or graph type would go, and a wiring customization point (gated on the flavour
concepts above) dispatches to the right boundary machinery. Registration is
**separate** from client use — clients resolve whatever implementation was
registered:

.. code-block:: cpp

   using namespace hgraph::service;

   // register (usually once, at an outer graph):
   register_reference_service<ReferencePricesService, ReferencePricesImpl>(w);
   register_subscription_service<PricesService, PricesImpl>(w);
   register_request_reply_service<AddOneService, AddOneImplNode>(w, path("premium"));

   // client side — the flavour tag selects the call shape:
   auto prices = wire<ReferencePricesService>(w);                   // reference: no argument
   auto quote  = wire<PricesService>(w, instrument);                // subscription: the key
   auto reply  = wire<AddOneService>(w, path("premium"), request);  // request/reply: the request

When a non-default path is used, ``service::path("…")`` is always the **first**
argument after ``w`` (for registration and consumption alike). Passing an
explicit output schema (``wire<Service, Schema>``) is rejected — a service's
output schema is defined by its descriptor.


Implementing a service
----------------------

An implementation is an ordinary node (``eval``) or graph (``compose``); the
registration machinery feeds it the flavour's input and captures its output.
There are two routes.

**Single interface** — ``register_*_service<Service, Impl>(w[, path])``: the
implementation's **first time-series parameter** receives the flavour input and
its output (node ``Out<>`` / graph return port) is captured as the service
output. The per-flavour contract, with the complications each one must handle:

- **Reference** (``output_schema``): produce the output — there is no service
  input. The implementation is source-shaped, so it must initiate itself
  (typically ``schedule_on_start = true``, or a scheduler).
- **Subscription** (``key_type`` + ``value_schema``): input is the subscribed
  key set ``TSS<key_type>``; output is ``TSD<key_type, value_schema>``.
  Complications: the key set is **invalid until the first subscription
  arrives** — declare the input ``InputValidity::Unchecked`` and guard
  ``if (!keys.valid()) return;``. Keys come and go: on each tick erase
  ``keys.removed()`` from the output and (re)publish entries for the live keys
  through one ``begin_mutation``.
- **Request/reply** (``request_schema`` + ``response_schema``): input is the
  request dictionary ``TSD<Int, request_schema>`` keyed by a **stable per-client
  request id**; output is ``TSD<Int, response_schema>`` keyed by the **same
  id** — that is how a reply finds its client. Complications: declare the input
  ``InputValidity::Unchecked`` and gate on ``requests.modified()``; erase
  ``removed_items()``; a request element that ticked **invalid** means the
  client's request went away — erase that reply too.

.. code-block:: cpp

   struct AddOneImplNode
   {
       static constexpr auto name = "add_one_impl_node";

       static void eval(In<"requests", TSD<Int, TS<Int>>, InputValidity::Unchecked> requests,
                        Out<TSD<Int, TS<Int>>> out)
       {
           if (!requests.modified()) { return; }

           auto mutation = out.begin_mutation(out.evaluation_time());
           for (const auto &[request_id, request] : requests.removed_items())
           {
               static_cast<void>(mutation.erase(request_id));
           }
           for (const auto &[request_id, request] : requests.modified_items())
           {
               if (!request.valid()) { static_cast<void>(mutation.erase(request_id)); continue; }
               Value response{request.value() + Int{1}};
               mutation.set(request_id, response.view());
           }
       }
   };

**Path injection**: an implementation (node or graph) may declare a scalar
parameter named exactly ``path`` (``Scalar<"path", Str>``); it receives the
path the implementation was registered under, so one implementation can serve
several paths with path-dependent behaviour.

**Multiple interfaces** — one implementation graph can serve several service
interfaces at once. Register it with
``register_services<Impl, Services…>(w, path[, args…])`` and, inside its
``compose``, address each interface's boundary explicitly:

- ``service::impl_input<Service>(w[, path])`` returns the flavour input —
  ``Port<TSS<key_type>>`` for a subscription service,
  ``Port<TSD<Int, request_schema>>`` for request/reply (a reference service
  has no input);
- ``service::impl_output<Service>(w[, path], port)`` publishes the
  implementation's result as that service's shared output (all three
  flavours).

.. code-block:: cpp

   struct MultiRequestReplyImpl
   {
       static void compose(Wiring &w, Scalar<"path", Str> path)
       {
           const auto custom = service::path(path.value());

           auto add_one_requests = service::impl_input<AddOneService>(w, custom);
           auto add_ten_requests = service::impl_input<AddTenService>(w, custom);

           auto add_one_replies = wire<AddOneImplNode>(w, add_one_requests).as<TSD<Int, TS<Int>>>();
           auto add_ten_replies = wire<AddTenImplNode>(w, add_ten_requests).as<TSD<Int, TS<Int>>>();

           service::impl_output<AddOneService>(w, custom, add_one_replies);
           service::impl_output<AddTenService>(w, custom, add_ten_replies);
       }
   };

   // registration — the interface list is asserted against the descriptors:
   service::register_services<MultiRequestReplyImpl, AddOneService, AddTenService>(w, custom);

``impl_input`` / ``impl_output`` bind directly to the flavour's boundary source
for the given path, so the graph may freely mix flavours (e.g. consume a
subscription key set and publish both a subscription output and a reference
output). The implementation graph is wired once per registration; clients then
consume each interface with the ordinary ``wire<Service>`` calls.


Adaptors
--------

An adaptor is the graph's boundary to the *outside world*: one implementation
owns the external interaction, and client code exchanges time-series with it
through an interface descriptor. Where a **service** descriptor is tagged by
its schema aliases, an **adaptor** descriptor is tagged by deriving from
``adaptor::interface``, plus ``input_schema`` (what clients send in) and/or
``output_schema`` (what clients receive) — omit one for a sink-only or
source-only adaptor:

.. code-block:: cpp

   struct LoopbackAdaptor : adaptor::interface
   {
       static constexpr std::string_view name{"loopback"};
       using input_schema  = TS<Int>;
       using output_schema = TS<Int>;
   };

Clients use the ordinary ``wire<>`` verb, exactly like services:
``wire<LoopbackAdaptor>(w, input)`` returns the adaptor output (an
``output_schema``-only adaptor takes no port; an ``input_schema``-only adaptor
returns nothing). ``adaptor::path("…")`` — first argument after ``w`` — binds a
non-default instance.

**Implementing an adaptor.** The implementation is a graph registered with
``register_adaptor<Interface, Impl>(w[, path])``. Inside its ``compose`` it
addresses the client-facing boundary through two helpers (the adaptor-side
mirror of ``impl_input`` / ``impl_output``):

- ``adaptor::from_graph<Interface>(w[, path])`` — the merged client **input**
  stream (what clients passed to ``wire<Interface>``);
- ``adaptor::to_graph<Interface>(w[, path], port)`` — publish the adaptor
  **output** back to clients.

.. code-block:: cpp

   struct LoopbackAdaptorImpl
   {
       static void compose(Wiring &w)
       {
           auto input  = adaptor::from_graph<LoopbackAdaptor>(w);
           auto output = wire<EchoNode>(w, input);
           adaptor::to_graph<LoopbackAdaptor>(w, output);
       }
   };

   // in the consuming graph:
   adaptor::register_adaptor<LoopbackAdaptor, LoopbackAdaptorImpl>(w);
   auto out = wire<LoopbackAdaptor>(w, some_input);

The same complications as services apply, plus the external-world ones the
adaptor exists to own: an implementation that talks to an external system is
where a **push source** (real-time injection of external events) and the
external resource's lifecycle live — client graphs never see them. Path
injection works as for services (``Scalar<"path", Str>``), and one
implementation can serve **multiple interfaces** via
``register_adaptors<Impl, Interfaces…>(w, path)`` — e.g. a sink-only interface
in, a source-only interface out:

.. code-block:: cpp

   struct MultiAdaptorImpl
   {
       static void compose(Wiring &w, Scalar<"path", Str> path)
       {
           const auto custom = adaptor::path(path.value());
           auto input  = adaptor::from_graph<MultiInAdaptor>(w, custom);   // sink-only in
           auto output = wire<EchoNode>(w, input);
           adaptor::to_graph<MultiOutAdaptor>(w, custom, output);          // source-only out
       }
   };

   adaptor::register_adaptors<MultiAdaptorImpl, MultiInAdaptor, MultiOutAdaptor>(w, custom);

Adaptor paths support the same scalar-qualified form as service paths
(``adaptor::path("typed", arg<"side">(Str{"primary"}))``), the same
``default_path`` descriptor override, and the same duplicate-registration
rejection.

Under the hood adaptors reuse the shared-output source/capture substrate
(``runtime/shared_output_node.h``) — the same boundary model as everything
else on this page. Code: ``include/hgraph/types/adaptor_wiring.h``; tests:
``tests/cpp/test_adaptor_wiring.cpp``.

Service adaptors — per-client keyed adaptors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A plain adaptor merges all clients into one stream and broadcasts one output.
A **service adaptor** applies the request/reply multiplexing model to an
adaptor boundary: every client gets its **own** keyed exchange with the
implementation. The descriptor derives from ``service_adaptor::interface``
(with the same ``input_schema``/``output_schema`` aliases); clients call
``wire<Interface>(w[, path], input)`` and receive their own reply.

The implementation-side helpers change shape accordingly:

- ``service_adaptor::from_graph<Interface>(w[, path])`` returns
  ``Port<TSD<Int, input_schema>>`` — **all** client inputs, keyed by a stable
  per-client id;
- ``service_adaptor::to_graph<Interface>(w[, path], port)`` publishes
  ``Port<TSD<Int, output_schema>>`` — replies keyed by the **same id**.

The implementation therefore carries exactly the request/reply service
complications (gate on ``modified()``, erase ``removed_items()``, treat an
invalid element as a departed client — see *Implementing a service* above):

.. code-block:: cpp

   struct AddTwentyServiceAdaptor : service_adaptor::interface
   {
       static constexpr std::string_view name{"add_twenty_adaptor"};
       using input_schema  = TS<Int>;
       using output_schema = TS<Int>;
   };

   struct AddTwentyServiceAdaptorImpl
   {
       static void compose(Wiring &w, Scalar<"path", Str> path)
       {
           const auto custom   = service_adaptor::path(path.value());
           auto       requests = service_adaptor::from_graph<AddTwentyServiceAdaptor>(w, custom);
           auto       replies  = wire<AddTwentyServiceAdaptorImplNode>(w, requests)
                                     .as<TSD<Int, TS<Int>>>();
           service_adaptor::to_graph<AddTwentyServiceAdaptor>(w, custom, replies);
       }
   };

Registration mirrors the other kinds: ``register_service_adaptor<Interface,
Impl>(w[, path])`` and multi-interface ``register_service_adaptors<Impl,
Interfaces…>(w, path)``; ``service_adaptor::path`` accepts the qualified form
and ``ServiceAdaptorPath`` is an alias of ``ServicePath``. Proven by
"service adaptors collect multiple client requests" in
``tests/cpp/test_service_wiring.cpp`` (two clients, each receiving its own
keyed reply through one implementation).

**Paths.** Services are addressed by ``ServicePath`` (``service::path("…")``;
the default when omitted, overridable per descriptor via a
``static constexpr std::string_view default_path`` member). Path resolution
derives the concrete storage keys (``reference_output_path`` /
``subscriptions_path`` / ``request_input_path`` /
``request_reply_output_path`` …) so distinct paths keep independent shared
outputs — the same service can be registered with different implementations
under different paths and each client binds to the implementation at its own
path.

Paths may be **qualified with scalar arguments**:
``service::path("prices", arg<"tier">(Str{"premium"}))``. The arguments are
folded (escaped) into the path string, so each distinct argument set is an
independent instance, and the implementation's ``Scalar<"path", Str>``
receives the full qualified value. This is also how **template service
descriptors** are kept apart: a ``template <typename T> struct Service`` binds
each instantiation as its own concrete interface, with a qualified path
carrying the type tag (e.g. ``arg<"T">(Str{"Int"})``).

**Duplicate registration is a wiring error.** Every registration records its
base path on the ``Wiring`` (``register_built_service_path``); registering a
second implementation for the same service kind + path throws
``std::invalid_argument`` when the graph is built.

**Semantics proven by tests** (``test_service_wiring.cpp``): a reference client
reads the implementation output by reference (no copy); paths keep shared
outputs separate; a subscription client's keys reach the implementation on the
next cycle and the response flows back keyed; request/reply replies are keyed
by the client's request id; two clients' requests reach the implementation as
one cumulative delta.


How a client expression lowers
------------------------------

``register_*_service`` compiles the implementation (node or sub-graph) and the
flavour's source node; ``wire<Service>(w, …)`` wires the capture sink for the
client's input side (subscription key / request value) and binds the client's
output to the source-owned output (reference / response) — all with the
ordinary ``wire<>`` machinery, entered through the ``wire_customization``
extension point. Interning follows the normal rules: the service struct,
path, and implementation identity fold into the node intern keys, so a service
registered once and consumed many times shares one source/impl instance per
path. Service markers (``…_source_marker`` / ``…_capture_marker`` in
``service_wiring.h``) provide the interning identities for the synthesized
nodes.


Status / deferred
-----------------

Landed and green: all three service flavours end-to-end (with path injection,
explicit and scalar-qualified paths, template descriptors,
duplicate-registration rejection, and multi-interface implementations via
``register_services`` + ``impl_input``/``impl_output``), adaptor foundations
(source/sink/duplex interfaces, ``from_graph``/``to_graph``, multi-interface
``register_adaptors``), **service adaptors** (per-client keyed exchange),
shared outputs, the context source/capture primitive, and wall-clock alarms.

Deferred (see :doc:`roadmap` Priority 1):

- the user-facing **context wiring API** (graph-level capture/lookup, nested
  graph context import/export) — the runtime primitive exists; the C++ surface
  needs approval before implementation;
- **request/reply and subscription adaptor flows**, integration of adaptor
  external events with the scheduler/real-time executor, lifecycle ownership
  for external resources, and concrete adaptor families (kafka/sql/…);
- ``@component`` and the recordable-id/traits ecosystem it depends on;
- a Python-drivable registration shape for services (the wiring surface above
  is compile-time; the runtime dispatch contract for operators —
  ``OperatorRegistry`` — is the model to mirror when Python lands).

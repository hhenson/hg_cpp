Services, shared outputs and contexts
=====================================

This page is the **authoritative design record** for the graph-boundary layer:
shared outputs, the context runtime primitive, and the three service flavours
(**reference**, **subscription**, and **request/reply**). It records the model
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

A service is declared as a plain struct naming its schemas; the three flavours
are distinguished by which type aliases they provide:

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

Registration binds ``Impl`` to the service at a path; the client expression
returns the service output for that path. Registration is **separate** from
client use — clients resolve whatever implementation was registered:

.. code-block:: cpp

   using namespace hgraph::service;

   // register (usually once, at an outer graph):
   register_reference_service<ReferencePricesService, ReferencePricesImpl>(w);
   register_subscription_service<PricesService, PricesImpl>(w);
   register_request_reply_service<AddOneService, AddOneImplNode>(w, path("premium"));

   // client side:
   auto prices = reference_service<ReferencePricesService>(w);
   auto quote  = subscription_service<PricesService>(w);           // clients then subscribe keys
   auto reply  = request_reply_service<AddOneService>(w, request,  // Port<TS<Int>> request
                                                      path("premium"));

**Paths.** Services are addressed by ``ServicePath`` (``service::path("…")``;
``default_service_path()`` when omitted). Path resolution derives the concrete
storage keys (``reference_output_path`` / ``subscriptions_path`` /
``request_input_path`` / ``request_reply_output_path`` …) so distinct paths
keep independent shared outputs — the same service can be registered with
different implementations under different paths and each client binds to the
implementation at its own path.

**Semantics proven by tests** (``test_service_wiring.cpp``): a reference client
reads the implementation output by reference (no copy); paths keep shared
outputs separate; a subscription client's keys reach the implementation on the
next cycle and the response flows back keyed; request/reply replies are keyed
by the client's request id; two clients' requests reach the implementation as
one cumulative delta.


How a client expression lowers
------------------------------

``register_*_service`` compiles the implementation (node or sub-graph) and the
flavour's source node; the client expression wires the capture sink for its
input side (subscription key / request value) and binds the client's output to
the source-owned output (reference / response) — all with the ordinary
``wire<>`` machinery. Interning follows the normal rules: the service struct,
path, and implementation identity fold into the node intern keys, so a service
registered once and consumed many times shares one source/impl instance per
path. Service markers (``…_source_marker`` / ``…_capture_marker`` in
``service_wiring.h``) provide the interning identities for the synthesized
nodes.


Status / deferred
-----------------

Landed and green: all three service flavours end-to-end (with path injection
and explicit paths), shared outputs, the context source/capture primitive, and
wall-clock alarms.

Deferred (see :doc:`roadmap` Priority 1):

- the user-facing **context wiring API** (graph-level capture/lookup, nested
  graph context import/export) — the runtime primitive exists; the C++ surface
  needs approval before implementation;
- **adaptor foundations** (source/sink/request-reply/subscription flows to the
  outside world) and concrete adaptor families;
- ``@component`` and the recordable-id/traits ecosystem it depends on;
- a Python-drivable registration shape for services (the wiring surface above
  is compile-time; the runtime dispatch contract for operators —
  ``OperatorRegistry`` — is the model to mirror when Python lands).

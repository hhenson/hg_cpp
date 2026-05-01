Data Structures
===============

This page starts with the logical data structures used by the runtime. The goal is to name the layers and the elements that move through them before fixing the exact memory layout.

The first implementation should keep these structures simple and explicit. Once the relationships are stable, each element can be refined into arena storage, packed arrays, intrusive handles, or type-specialized storage where that provides measurable value.

The chapters below split the description by responsibility:

- *Core Concepts* introduces the Plan / Schema / Ops / Builder / Value /
  View vocabulary that every layer reuses.
- *Overview* names the structural layers (Execution, Graph, Node,
  scheduling, TimeSeries) and how they fit together.
- *Schemas* describes type identity at four layers — scalar values,
  time-series, nodes, and graphs (including nested graphs) — and the
  registries that intern them.
- *Allocation, Plans and Ops* describes how those types are laid out in
  memory and what behaviour their ops vtables expose, separately for
  scalars and time-series.
- *Linking Strategies* covers the three flavours of binding,
  unbinding, and rebinding that connect time-series instances at
  runtime.
- *Refinement Topics* collects open questions for the next design pass.

.. toctree::
   :maxdepth: 2

   data_structures/core_concepts
   data_structures/overview/index
   data_structures/schemas/index
   data_structures/plans_and_ops/index
   data_structures/linking_strategies
   data_structures/refinements

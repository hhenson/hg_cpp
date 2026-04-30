What Is HGraph?
===============

HGraph is a framework for writing functional reactive programs. Programs are modeled as forward propagation graphs over time-series values.

In the current Python implementation, users describe graphs through a Python DSL. In this C++ first implementation, the runtime and system nodes are native C++, while Python remains the primary compatibility and wiring surface for the existing ecosystem.

The core model is:

- nodes perform computation, manage data sources, or produce side effects,
- edges connect node outputs to inputs,
- time-series values carry both data and time-oriented state,
- graph execution proceeds in ordered evaluation steps,
- changes propagate forward through dependent nodes.

This model is useful for real-time processing, simulation, backtesting, and other domains where time ordering and graph dependencies must be explicit.

Implementation Direction
------------------------

The objective of this repository is not to embed a C++ runtime behind a Python-first system. The objective is to make the C++ runtime authoritative, with Python wiring and Python user nodes supported as integration layers.

That means:

- system nodes are C++ only,
- C++ graphs and C++ nodes are first-class,
- Python graph wiring remains supported,
- Python user nodes can run inside the C++ runtime where needed,
- the same runtime semantics should apply across C++ and Python-authored graphs.

Python compatibility
====================

The ``hg_cpp`` distribution provides the public ``hgraph`` Python package on
top of the C++ runtime. Install it in an isolated environment: the upstream
``hgraph`` distribution exports the same import package and the two
distributions must not be installed together.

API boundary
------------

The documented ``hgraph`` package, its public adaptors, and Python node/graph
decorators are the supported Python API. ``_hgraph`` and modules or attributes
whose names start with an underscore are implementation details. The Python
API is the compatibility commitment for the ``0.4`` release line. The native
C++ authoring API remains source- and binary-provisional while it is refined
by production use.

Python-authored compute, sink, generator, graph, service, adaptor, component,
and push-source code executes in the C++ runtime. Python adapts values and
callables; it does not implement a second graph engine.

Runtime callables
-----------------

``apply(fn, *args, **kwargs)`` invokes a callable value at evaluation time and
publishes its result. ``call(fn, *args, **kwargs)`` invokes it for side effects.
Both operators use the native ``ValueCallable`` scalar and packed C++ runtime
nodes. A plain Python callable is adapted to the same value type at the bridge.

All positional inputs must be valid before invocation. Invalid keyword inputs
are omitted, allowing the callable's Python default to apply. A plain callable
with a return annotation lets ``apply`` infer its output type; otherwise use
``apply[TS[Result]](...)``. C++ authors use ``value_fn<F>()`` for a native
runtime callable.

Scalar boundaries
-----------------

``TS[object]`` is the native type-erased ``Any`` schema. Values with a native
hgraph representation retain their concrete type record, so native conversion
and ``type_`` dispatch remain available. An arbitrary Python object is retained
as an opaque bridge value only when no native representation exists.

Native ``DateTime`` is a timezone-naive UTC value. Python timezone-aware
``datetime`` values are normalized to UTC and then made naive on ingress;
naive values are already interpreted as UTC. Timezone-aware standalone
``time`` values are rejected because they cannot be normalized without a date.

Data frames
-----------

``Frame`` values use Arrow storage. The typed ``from_data_frame[OUT]`` operator
honours ``OUT`` exactly and rejects Arrow columns whose type does not match the
requested scalar schema. It never rewrites an explicit output type from the
data. The data-source convenience functions infer unresolved key/value types
from the Arrow schema before wiring, then call the same native operator with a
concrete output type.

Intentional differences from upstream hgraph are maintained in
:doc:`../developer_guide/roadmap` under *Accepted Deviations*. They are part of
the compatibility contract rather than temporary Python-side workarounds.

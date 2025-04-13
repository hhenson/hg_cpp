import _hgraph
import hgraph

hgraph._runtime._node.NodeSignature = _hgraph.NodeSignature
hgraph._runtime._evaluation_engine.EvaluationMode = _hgraph.EvaluationMode
hgraph._runtime._evaluation_engine.EvaluationLifeCycleObserver = _hgraph.EvaluationLifeCycleObserver

hgraph.NodeSignature = _hgraph.NodeSignature
hgraph.EvaluationMode = _hgraph.EvaluationMode
hgraph.EvaluationLifeCycleObserver = _hgraph.EvaluationLifeCycleObserver

from hg_cpp._builder_factories import HgCppFactory

hgraph.TimeSeriesReference._BUILDER = _hgraph.TimeSeriesReference.make
hgraph.TimeSeriesReference._INSTANCE_OF = lambda obj: isinstance(obj, _hgraph.TimeSeriesReference)

# The graph engine type
hgraph.GraphEngineFactory.declare(_hgraph.GraphExecutorImpl)

#The time-series builder factory.
hgraph.TimeSeriesBuilderFactory.declare(HgCppFactory())

hgraph._wiring._wiring_node_class.PythonWiringNodeClass.BUILDER_CLASS = _hgraph.PythonNodeBuilder

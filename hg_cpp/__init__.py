import _hgraph
import hgraph

hgraph._wiring._wiring_node_class._wiring_node_class.WiringNodeInstance.NODE_SIGNATURE = _hgraph.NodeSignature
hgraph._wiring._wiring_node_class._wiring_node_class.WiringNodeInstance.NODE_TYPE_ENUM = _hgraph.NodeTypeEnum
hgraph._wiring._wiring_node_class._wiring_node_class.WiringNodeInstance.INJECTABLE_TYPES_ENUM = _hgraph.InjectableTypesEnum

hgraph._runtime._evaluation_engine.EvaluationMode = _hgraph.EvaluationMode
hgraph._runtime._evaluation_engine.EvaluationLifeCycleObserver = _hgraph.EvaluationLifeCycleObserver

from hg_cpp._builder_factories import HgCppFactory

hgraph.TimeSeriesReference._BUILDER = _hgraph.TimeSeriesReference.make
hgraph.TimeSeriesReference._INSTANCE_OF = lambda obj: isinstance(obj, _hgraph.TimeSeriesReference)

hgraph._builder._graph_builder.EDGE_TYPE = _hgraph.Edge
hgraph.GraphBuilderFactory.declare(_hgraph.GraphBuilder)

# The graph engine type
hgraph.GraphEngineFactory.declare(lambda graph, run_mode, observers: _hgraph.GraphExecutorImpl(
    graph, {hgraph.EvaluationMode.SIMULATION: _hgraph.EvaluationMode.SIMULATION,
            hgraph.EvaluationMode.REAL_TIME: _hgraph.EvaluationMode.REAL_TIME}[run_mode], observers))

#The time-series builder factory.
hgraph.TimeSeriesBuilderFactory.declare(HgCppFactory())

hgraph._wiring._wiring_node_class.PythonWiringNodeClass.BUILDER_CLASS = _hgraph.PythonNodeBuilder
hgraph._wiring._wiring_node_class.PythonGeneratorWiringNodeClass.BUILDER_CLASS = _hgraph.PythonGeneratorNodeBuilder

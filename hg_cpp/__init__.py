from hgraph import GraphEngineFactory
from hg_cpp._builder_factories import HgCppFactory
from _hgraph import GraphEngine

GraphEngineFactory.declare()
HgCppFactory.declare(HgCppFactory())

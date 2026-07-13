"""Batch LLDB validation for the hgraph type-erasure printers."""

import lldb


def _global(target, name):
    value = target.FindFirstGlobalVariable(name)
    if not value.IsValid():
        raise RuntimeError("missing debugger fixture global: {}".format(name))
    return value


def _summary(value):
    summary = value.GetSummary()
    if summary is None:
        raise RuntimeError("missing summary for {}".format(value.GetName()))
    if "<printer error:" in summary:
        raise RuntimeError(summary)
    return summary


def _require(text, expected):
    if expected not in text:
        raise RuntimeError("expected {!r} in {!r}".format(expected, text))


def _children(value):
    return {
        value.GetChildAtIndex(index).GetName(): value.GetChildAtIndex(index)
        for index in range(value.GetNumChildren())
    }


def run(debugger, _command, _exe_ctx, result, _internal_dict):
    try:
        target = debugger.GetSelectedTarget()

        _require(_summary(_global(target, "fixture_atomic_pointer")), "value=42")
        _require(_summary(_global(target, "fixture_bundle_pointer")), 'semantic="DebuggerFixture"')
        _require(_summary(_global(target, "fixture_map_pointer")), 'semantic="MutableMap[int32,int32]"')
        _require(_summary(_global(target, "fixture_node_pointer")), 'semantic="debugger_fixture_node"')

        graph = _global(target, "fixture_graph_pointer")
        graph_summary = _summary(graph)
        _require(graph_summary, 'semantic="debugger_fixture_graph"')
        _require(graph_summary, 'implementation="hgraph.graph.root"')

        _require(_summary(_global(target, "fixture_typed_null_pointer")), "typed-null Value/Instance")
        _require(_summary(_global(target, "fixture_malformed_pointer")), "malformed record=null")
        _require(_summary(_global(target, "fixture_invalid_record").Dereference()), "TypeRecord{invalid Value/Instance")
        _require(_summary(_global(target, "fixture_invalid_descriptor")), "DebugDescriptor{invalid layout=Atomic")

        bundle_children = _children(_global(target, "fixture_bundle_pointer"))
        if not {"record", "data", "number", "enabled"}.issubset(bundle_children):
            raise RuntimeError("bundle navigation is incomplete: {}".format(sorted(bundle_children)))
        _require(_summary(bundle_children["number"]), "value=42")

        map_children = _children(_global(target, "fixture_map_pointer"))
        if not {"key[0]", "value[0]"}.issubset(map_children):
            raise RuntimeError("map slot navigation is incomplete: {}".format(sorted(map_children)))

        graph_children = _children(graph)
        if "[0]" not in graph_children:
            raise RuntimeError("graph node navigation is incomplete: {}".format(sorted(graph_children)))
        _require(_summary(graph_children["[0]"]), 'semantic="debugger_fixture_graph_node"')
    except Exception as error:
        result.SetError(str(error))
        return

    result.AppendMessage("hgraph LLDB type-erasure smoke test passed")


def __lldb_init_module(debugger, _internal_dict):
    debugger.HandleCommand("command script add -f hgraph_lldb_smoke.run hgraph-smoke")

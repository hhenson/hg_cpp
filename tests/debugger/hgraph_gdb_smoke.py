"""Batch GDB validation for the hgraph type-erasure printers."""

import gdb


def _printer(expression):
    value = gdb.parse_and_eval(expression)
    printer = gdb.default_visualizer(value)
    if printer is None:
        raise gdb.GdbError("missing printer for {}".format(expression))
    return value, printer


def _summary(expression):
    _, printer = _printer(expression)
    summary = str(printer.to_string())
    if "<printer error:" in summary:
        raise gdb.GdbError(summary)
    return summary


def _require(text, expected):
    if expected not in text:
        raise gdb.GdbError("expected {!r} in {!r}".format(expected, text))


def _children(expression):
    _, printer = _printer(expression)
    return {name: value for name, value in printer.children()}


class HGraphSmoke(gdb.Command):
    def __init__(self):
        super().__init__("hgraph-smoke", gdb.COMMAND_USER)

    def invoke(self, _argument, _from_tty):
        _require(_summary("fixture_atomic_pointer"), "value=42")
        _require(_summary("fixture_bundle_pointer"), 'semantic="DebuggerFixture"')
        _require(_summary("fixture_map_pointer"), 'semantic="MutableMap[int32,int32]"')
        _require(_summary("fixture_node_pointer"), 'semantic="debugger_fixture_node"')

        graph_summary = _summary("fixture_graph_pointer")
        _require(graph_summary, 'semantic="debugger_fixture_graph"')
        _require(graph_summary, 'implementation="hgraph.graph.root"')

        _require(_summary("fixture_typed_null_pointer"), "typed-null Value/Instance")
        _require(_summary("fixture_malformed_pointer"), "malformed record=null")
        _require(_summary("*fixture_invalid_record"), "TypeRecord{invalid Value/Instance")
        _require(_summary("fixture_invalid_descriptor"), "DebugDescriptor{invalid layout=Atomic")

        bundle_children = _children("fixture_bundle_pointer")
        if not {"record", "data", "number", "enabled"}.issubset(bundle_children):
            raise gdb.GdbError("bundle navigation is incomplete: {}".format(sorted(bundle_children)))
        number_printer = gdb.default_visualizer(bundle_children["number"])
        _require(str(number_printer.to_string()), "value=42")

        map_children = _children("fixture_map_pointer")
        if not {"key[0]", "value[0]"}.issubset(map_children):
            raise gdb.GdbError("map slot navigation is incomplete: {}".format(sorted(map_children)))

        graph_children = _children("fixture_graph_pointer")
        if "[0]" not in graph_children:
            raise gdb.GdbError("graph node navigation is incomplete: {}".format(sorted(graph_children)))
        node_printer = gdb.default_visualizer(graph_children["[0]"])
        _require(str(node_printer.to_string()), 'semantic="debugger_fixture_graph_node"')

        gdb.write("hgraph GDB type-erasure smoke test passed\n")


HGraphSmoke()

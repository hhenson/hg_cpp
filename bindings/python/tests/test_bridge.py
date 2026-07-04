"""The Python bridge, slice 1: wire and run a graph purely by operator name.

Run directly (ctest registers it when HGRAPH_BUILD_PYTHON_BINDINGS=ON).
"""
import _hgraph as hg


def ts_int():
    # Looked up per test: type metadata pointers do not survive a registry
    # reset, so never cache them across resets.
    return hg.ts_type("TS[int]")


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def test_const_add_record():
    w = hg.Wiring()
    lhs = w.wire("const", (42,), {}, output_type=ts_int())
    rhs = w.wire("const", (8,), {}, output_type=ts_int())
    total = w.wire("add_", (lhs, rhs), {})
    w.wire("record", (total, "out"), {})
    run = w.run()
    check(run.recorded("out") == [50], f"unexpected: {run.recorded('out')}")


def test_replay_through_graph():
    w = hg.Wiring()
    src = w.wire("replay", ("in",), {}, output_type=ts_int())
    doubled = w.wire("add_", (src, src), {})
    w.wire("record", (doubled, "out"), {})
    w.set_replay("in", [1, None, 3])
    run = w.run()
    check(run.recorded("out") == [2, None, 6], f"unexpected: {run.recorded('out')}")


def test_keyword_arguments():
    w = hg.Wiring()
    src = w.wire("const", (), {"value": 7}, output_type=ts_int())
    total = w.wire("add_", (), {"rhs": src, "lhs": src})
    w.wire("record", (total, "kw"), {})
    run = w.run()
    check(run.recorded("kw") == [14], f"unexpected: {run.recorded('kw')}")


def test_json_round_trip():
    w = hg.Wiring()
    src = w.wire("replay", ("in",), {}, output_type=ts_int())
    text = w.wire("to_json", (src,), {})
    back = w.wire("from_json", (text,), {}, output_type=ts_int())
    w.wire("record", (back, "out"), {})
    w.set_replay("in", [5, -9])
    run = w.run()
    check(run.recorded("out") == [5, -9], f"unexpected: {run.recorded('out')}")


def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"{len(tests)} bridge tests passed")


if __name__ == "__main__":
    main()

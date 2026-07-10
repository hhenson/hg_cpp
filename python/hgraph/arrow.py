"""hgraph.arrow compat - the minimal combinator surface the ported tests
use: ``eval_(inputs...) | arrow(fn) >> assert_(*expected)``. The chain
wires the inputs as replay ports, applies ``fn`` (a pair port supports
``p[0]`` / ``p[1]``), records the output's per-tick VALUES through a
python probe node, runs, and asserts the sequence."""

import _hgraph

from ._runtime import (WiringPort, _hgraph as _m, _infer_ts_type, _simplify_delta, _unwrap,
                       _wiring_stack, compute_node)
from ._types import TS

__all__ = ("eval_", "arrow", "assert_")


class _Assert:
    def __init__(self, expected, message=None):
        self.expected = expected
        self.message = message


class _Chain:
    def __init__(self, fns, check=None, cmp=None):
        self.fns = fns
        self.check = check
        self.cmp = cmp

    def __call__(self, *, cmp=None, **kwargs):
        if kwargs:
            names = ", ".join(sorted(kwargs))
            raise TypeError(f"arrow: unsupported option(s): {names}")
        return _Chain(self.fns, self.check, cmp if cmp is not None else self.cmp)

    def __rshift__(self, other):
        if isinstance(other, _Assert):
            return _Chain(self.fns, other, self.cmp)
        if isinstance(other, _Chain):
            return _Chain(self.fns + other.fns, other.check, other.cmp or self.cmp)
        return NotImplemented

    def __or__(self, other):
        return self.__rshift__(other)


def arrow(fn=None, *_, cmp=None, **kwargs):
    return _Chain([fn], cmp=cmp)(**kwargs)


def assert_(*expected, message=None):
    return _Assert(expected, message)


class _Pair:
    """The pair port: p[0] / p[1]."""

    def __init__(self, first, second):
        self._items = (first, second)

    def __getitem__(self, index):
        return self._items[index]


@compute_node
def _probe(ts: TS[object]) -> TS[object]:
    return ts.value


def eval_(first, second=None, type_map=None, **_ignored):
    return _EvalArrowInput(first, second, type_map)


class _EvalArrowInput:
    def __init__(self, first, second, type_map):
        self.series = [first] if second is None else [first, second]
        self.type_map = type_map

    def __or__(self, chain):
        if not isinstance(chain, _Chain):
            return NotImplemented
        w = _m.Wiring()
        _wiring_stack.append(w)
        try:
            ports = []
            for index, series in enumerate(self.series):
                if self.type_map is not None:
                    annotation = self.type_map[index]
                    handle = annotation.handle
                else:
                    annotation = _infer_ts_type(series)
                    if annotation is None:
                        raise TypeError("arrow eval_: cannot infer the input type; pass type_map")
                    handle = annotation.handle
                key = f"arrow::{index}"
                src = w.wire("__harness_replay", (key,), {}, output_type=handle)
                w.set_replay(key, list(series), ts_type=handle)
                ports.append(WiringPort(src))

            value = ports[0] if len(ports) == 1 else _Pair(*ports)
            from ._runtime import _OperatorFunction

            for fn in chain.fns:
                if isinstance(value, _Pair) and isinstance(fn, _OperatorFunction):
                    # A bare binary OPERATOR in the chain consumes the pair as
                    # its two arguments; user lambdas take the pair itself.
                    value = fn(value[0], value[1])
                else:
                    value = fn(value)
            probed = _probe(value)
            w.wire("__harness_record", (_unwrap(probed), "arrow::out"), {"sparse": True})
            run = w.run()
        finally:
            _wiring_stack.pop()

        actual = [_simplify_delta(v) for _, v in run.recorded("arrow::out", sparse=True)]
        expected = list(chain.check.expected) if chain.check is not None else None
        if expected is not None:
            suffix = f" ({chain.check.message})" if chain.check.message else ""
            matches = actual == expected
            if chain.cmp is not None:
                matches = len(actual) == len(expected) and all(
                    chain.cmp(value, wanted) for value, wanted in zip(actual, expected)
                )
            assert matches, f"arrow: expected {expected} but got {actual}{suffix}"
        return actual

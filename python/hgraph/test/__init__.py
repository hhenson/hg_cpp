"""hgraph.test - the test utilities (hgraph-compatible import path)."""
from contextlib import contextmanager

from .._wiring import eval_node


@contextmanager
def use_wiring(wiring):
    """Install ``wiring`` as the active wiring context for the duration.

    Test support: lets a test intercept ``hgraph.wire`` calls with a stub
    (the sanctioned route — test code must not reach into ``hgraph._wiring``).
    """
    from .._wiring import _wiring_stack

    _wiring_stack.append(wiring)
    try:
        yield wiring
    finally:
        _wiring_stack.pop()


class EvaluationTrace:
    """hgraph's evaluation-trace life-cycle observer. The C++ runtime has no
    python trace observer; the configuration surface is accepted and ignored
    so ported tests that toggle tracing run unchanged."""

    @staticmethod
    def set_use_logger(value: bool) -> None: ...

    @staticmethod
    def set_print_all_values(value: bool) -> None: ...


__all__ = ["eval_node", "EvaluationTrace", "use_wiring"]

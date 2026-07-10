"""hgraph.test - the test utilities (hgraph-compatible import path)."""
from hgraph._runtime import eval_node


class EvaluationTrace:
    """hgraph's evaluation-trace life-cycle observer. The C++ runtime has no
    python trace observer; the configuration surface is accepted and ignored
    so ported tests that toggle tracing run unchanged."""

    @staticmethod
    def set_use_logger(value: bool) -> None: ...

    @staticmethod
    def set_print_all_values(value: bool) -> None: ...


__all__ = ["eval_node", "EvaluationTrace"]

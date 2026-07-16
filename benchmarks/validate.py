"""Run benchmark workload guards without requiring pytest in benchmark venvs."""

import importlib.util
from pathlib import Path


TEST_PATH = Path(__file__).parents[1] / "python" / "tests" / "test_benchmark_scenarios.py"


def main() -> None:
    spec = importlib.util.spec_from_file_location("hgraph_benchmark_checks", TEST_PATH)
    checks = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checks)
    for name in sorted(vars(checks)):
        if name.startswith("test_"):
            getattr(checks, name)()


if __name__ == "__main__":
    main()

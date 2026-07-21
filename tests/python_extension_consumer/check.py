"""Build and import a nanobind extension against the installed wheel SDK."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import tempfile


SOURCE_DIR = Path(__file__).resolve().parent


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="hgraph-python-extension-consumer-") as directory:
        build_dir = Path(directory)
        configure = [
            "cmake",
            "-S",
            str(SOURCE_DIR),
            "-B",
            str(build_dir),
            "-DCMAKE_BUILD_TYPE=Release",
            f"-DPython_EXECUTABLE={sys.executable}",
        ]
        if sys.platform == "win32":
            # The Windows wheel and its import libraries are built with MSVC;
            # a MinGW consumer would require incompatible .dll.a files.
            configure.extend(["-G", "Visual Studio 18 2026", "-A", "x64"])
        else:
            configure.extend(["-G", "Ninja"])
        if prefix := os.environ.get("HGRAPH_CMAKE_PREFIX"):
            configure.append(f"-DCMAKE_PREFIX_PATH={prefix}")
        subprocess.run(
            configure,
            check=True,
        )
        build = ["cmake", "--build", str(build_dir), "--parallel", "2"]
        if sys.platform == "win32":
            build.extend(["--config", "Release"])
        subprocess.run(
            build,
            check=True,
        )

        module_dir = build_dir / "Release" if sys.platform == "win32" else build_dir
        check = subprocess.run(
            [
                sys.executable,
                "-c",
                """
import importlib
import sys

import _hgraph  # Load the wheel's shared runtime first.

sys.path.insert(0, sys.argv[1])
consumer = importlib.import_module("_hgraph_consumer")
address = consumer.registry_address()
if not isinstance(address, int) or address == 0:
    raise RuntimeError("downstream extension returned an invalid registry address")
print(f"installed Python extension consumer passed: registry={address:#x}")
""",
                str(module_dir),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        print(check.stdout, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

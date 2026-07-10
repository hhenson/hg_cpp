"""Packaging contract checks for the optional Python bridge."""

from pathlib import Path
import tomllib


ROOT = Path(__file__).resolve().parents[2]
PYARROW_REQUIREMENT = "pyarrow>=24,<25"
SUPPORTED_PYTHON_MINIMUM = ">=3.12"
STABLE_ABI_TAG = "cp312"


def load_project():
    return tomllib.loads((ROOT / "pyproject.toml").read_text())


def test_pyarrow_build_and_runtime_requirements_share_the_supported_abi():
    project = load_project()

    build_requires = project["build-system"]["requires"]
    runtime_requires = project["project"]["dependencies"]

    assert PYARROW_REQUIREMENT in build_requires
    assert PYARROW_REQUIREMENT in runtime_requires


def test_supported_python_versions_are_declared():
    project = load_project()["project"]

    assert project["requires-python"] == SUPPORTED_PYTHON_MINIMUM
    assert "Programming Language :: Python :: 3.12" in project["classifiers"]
    assert "Programming Language :: Python :: 3.13" in project["classifiers"]
    assert "Programming Language :: Python :: 3.14" in project["classifiers"]


def test_wheel_targets_the_python_312_stable_abi():
    scikit_build = load_project()["tool"]["scikit-build"]

    assert scikit_build["wheel"]["py-api"] == STABLE_ABI_TAG


def main():
    test_pyarrow_build_and_runtime_requirements_share_the_supported_abi()
    test_supported_python_versions_are_declared()
    test_wheel_targets_the_python_312_stable_abi()
    print("PASS test_pyarrow_build_and_runtime_requirements_share_the_supported_abi")
    print("PASS test_supported_python_versions_are_declared")
    print("PASS test_wheel_targets_the_python_312_stable_abi")


if __name__ == "__main__":
    main()

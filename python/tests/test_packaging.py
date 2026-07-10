"""Packaging contract checks for the optional Python bridge."""

from pathlib import Path
import re
import tomllib


ROOT = Path(__file__).resolve().parents[2]
PYARROW_REQUIREMENT = "pyarrow>=24,<25"
SUPPORTED_PYTHON_MINIMUM = ">=3.12"
STABLE_ABI_TAG = "cp312"
DISTRIBUTION_NAME = "hg_cpp"


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


def test_release_metadata_is_consistent():
    project = load_project()["project"]
    version = project["version"]

    assert project["name"] == DISTRIBUTION_NAME
    assert re.fullmatch(r"\d+\.\d+\.\d+", version)
    assert tuple(map(int, version.split("."))) >= (0, 3, 0)

    cmake = (ROOT / "CMakeLists.txt").read_text()
    cmake_version = re.search(r"project\(\s*hgraph\s+VERSION\s+(\d+\.\d+\.\d+)", cmake)
    assert cmake_version is not None
    assert cmake_version.group(1) == version

    sphinx = (ROOT / "docs/source/conf.py").read_text()
    sphinx_version = re.search(r'^release = "(\d+\.\d+\.\d+)"$', sphinx, re.MULTILINE)
    assert sphinx_version is not None
    assert sphinx_version.group(1) == version


def test_wheel_targets_the_python_312_stable_abi():
    scikit_build = load_project()["tool"]["scikit-build"]

    assert scikit_build["wheel"]["py-api"] == STABLE_ABI_TAG


def test_release_workflow_targets_supported_platforms():
    workflow = (ROOT / ".github/workflows/build.yml").read_text()

    assert "macos-15-intel" not in workflow
    assert workflow.count("macos-26") == 2
    assert "CMAKE_OSX_DEPLOYMENT_TARGET=15.0" in workflow
    assert 'python-version: "3.12"' in workflow
    assert '- "3.13"' in workflow
    assert '- "3.14"' in workflow


def main():
    test_pyarrow_build_and_runtime_requirements_share_the_supported_abi()
    test_supported_python_versions_are_declared()
    test_release_metadata_is_consistent()
    test_wheel_targets_the_python_312_stable_abi()
    test_release_workflow_targets_supported_platforms()
    print("PASS test_pyarrow_build_and_runtime_requirements_share_the_supported_abi")
    print("PASS test_supported_python_versions_are_declared")
    print("PASS test_release_metadata_is_consistent")
    print("PASS test_wheel_targets_the_python_312_stable_abi")
    print("PASS test_release_workflow_targets_supported_platforms")


if __name__ == "__main__":
    main()

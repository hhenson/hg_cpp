Build System
============

Goals
-----

- CMake is the primary build system.
- The default configure path builds C++ without requiring Python.
- ``pyproject.toml`` drives Python packaging and the optional Python bridge.
- Public C++ consumers should depend on ``hgraph::core``.

Current Targets
---------------

``hgraph_options``
    Interface target for C++ standard, warnings, include paths, sanitizer flags, and common compile definitions.

``hgraph_core``
    Core runtime target. This is exported publicly as ``hgraph::core``.

Version Header
--------------

``include/hgraph/version.h`` is generated from ``include/hgraph/version.h.in`` into the CMake build tree. The generated include directory appears before the source include directory so normal includes resolve to the configured header.

Python Releases
---------------

The preview Python distribution is published to the existing ``hg_cpp`` PyPI
project while it is validated independently of the main ``hgraph``
distribution.  It installs the ``hgraph`` Python package and ``_hgraph`` native
extension, so the two distributions should be tested in separate environments.

``.github/workflows/build.yml`` builds one ``cp312-abi3`` wheel for Linux x86_64,
Windows x86_64, and Apple Silicon macOS, then installs each platform wheel under
CPython 3.12, 3.13, and 3.14.  A tag matching ``v_x.x.x`` publishes the tested
wheels and source distribution through PyPI trusted publishing.  The tag version
must exactly match the versions in ``pyproject.toml``, ``CMakeLists.txt``, and
``docs/source/conf.py``.  The PyPI trusted publisher is bound to the ``build.yml``
workflow and the GitHub ``release`` environment.

The macOS build uses the current system Clang from the latest Apple Silicon
runner image while retaining a macOS 15 deployment target.

Open Design Items
-----------------

- Decide when to split runtime, system nodes, schema, and Python bridge into separate targets.
- Decide whether tests should use a bundled test framework or depend on system packages.
- Define packaging expectations for static and shared library builds.

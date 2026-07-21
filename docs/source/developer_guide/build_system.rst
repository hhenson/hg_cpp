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

Third-Party Dependencies
------------------------

Arrow is the native table, series, and numerical-reduction substrate. The
runtime links Arrow, Arrow Compute, and Arrow Acero; wheel builds resolve those
libraries from PyArrow, while standalone C++ builds use their CMake packages.

Boost.Math supplies the correlation kernel used by ``hgraph.numpy_``. CMake
uses an installed ``boost_math`` package at version 1.90 or newer when
available, otherwise it fetches the pinned standalone Boost.Math release. The
header-only target is private to the stdlib build and does not add a Boost
dependency to installed hgraph consumers.

``simdjson`` **requires version 4.5 or newer** — ``json_impl.cpp`` uses
``simdjson::dom::element_type::BIGINT``, which first appeared in 4.5. Wheel
builds (``HGRAPH_BUILD_PYTHON_BINDINGS=ON``) fetch a pinned release (currently
v4.6.4) and link it statically; the default C++ build resolves a system package
via ``find_package(simdjson CONFIG REQUIRED)`` followed by an explicit
``simdjson_VERSION`` check, which rejects older distro packages (Ubuntu 24.04
ships 3.x) at configure time instead of failing mid compile. The check is
explicit rather than a ``find_package`` version argument because simdjson's
package version file uses same-minor compatibility (requesting 4.5 would
reject 4.6.x). The installed ``hgraphConfig.cmake`` carries the same floor.

``HGRAPH_WARNINGS_AS_ERRORS`` applies to this project's targets only;
third-party dependencies such as simdjson build with their own flags and are
not expected to be warning-clean under ours.

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
wheels and source distribution through PyPI trusted publishing.  The tag is the
release version authority: the publish job restamps the metadata of artifacts
already tested for that exact commit, rather than rebuilding them.  The version
in ``pyproject.toml`` and ``docs/source/conf.py`` is the untagged artifact
baseline.  CMake's ``project(VERSION)`` field is numeric and matches that
baseline's base version (for example ``0.4.0rc1`` maps to ``0.4.0``).  Packaging
tests enforce the baseline relationships and the release workflow validates
the tag syntax and rejects versions already present on PyPI.  The PyPI trusted
publisher is bound to the ``build.yml`` workflow and the GitHub ``release``
environment.

The macOS build uses the current system Clang from the latest Apple Silicon
runner image while retaining a macOS 15 deployment target.

Open Design Items
-----------------

- Decide when to split runtime, system nodes, schema, and Python bridge into separate targets.
- Decide whether tests should use a bundled test framework or depend on system packages.
- Define packaging expectations for static and shared library builds.

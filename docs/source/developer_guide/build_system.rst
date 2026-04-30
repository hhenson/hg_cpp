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

Open Design Items
-----------------

- Decide when to split runtime, system nodes, schema, and Python bridge into separate targets.
- Decide whether tests should use a bundled test framework or depend on system packages.
- Define packaging expectations for static and shared library builds.

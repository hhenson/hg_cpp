Release readiness
=================

This is the operational release contract for ``hg_cpp 0.4.0rc1``. The release
candidate validates the C++ runtime and Python compatibility package in the
wild; it is not yet the cut-over release for the upstream ``hgraph`` package.

Compatibility policy
--------------------

- The documented Python ``hgraph`` surface is stable for the ``0.4`` line.
  Private modules and ``_hgraph`` are not public APIs.
- The C++ runtime is the source of truth, but its public source and binary ABI
  remain provisional for this release candidate.
- Every Python-visible runtime behavior must have a native C++ wiring route
  and comparable behavioral coverage. Python-only syntax and opaque Python
  object adaptation are boundary exceptions, not alternate runtime semantics.
- Accepted differences from upstream are listed in :doc:`roadmap`. A new
  difference requires a test and documentation; it must not be hidden by an
  unexplained skip, xfail, or permissive compatibility shim.

Supported platforms
-------------------

.. list-table:: ``0.4`` release targets
   :header-rows: 1
   :widths: 22 28 20 30

   * - Platform
     - Build toolchain
     - Package contract
     - Validation role
   * - Linux x86_64
     - Official ``manylinux_2_28`` image, GCC 14
     - ``cp312-abi3`` wheel, glibc 2.28+
     - Official wheel; Ubuntu 24.04/GCC 13 is the native and performance host
   * - macOS arm64
     - macOS 26 runner, current AppleClang, deployment target 15.0
     - ``cp312-abi3`` wheel, macOS 15+
     - Primary local and CI correctness gate; Intel macOS is not built
   * - Windows x86_64
     - Latest Visual Studio generator
     - ``cp312-abi3`` wheel
     - Python compatibility suite gates the wheel; standalone native C++ is
       best effort

CPython 3.12, 3.13, and 3.14 install and test the same stable-ABI wheel on all
three wheel platforms. Linux wheel construction is intentionally separate from
the Ubuntu native build so a newer host glibc cannot silently raise the wheel's
compatibility floor.

Migration canaries
------------------

The repository contains three levels of migration evidence:

1. ``python/tests/test_release_readiness.py`` rejects unexplained static skips,
   xfails, and module-wide WIP markers.
2. ``python/tests/ported`` exercises public upstream-compatible behavior,
   including mixed Python/C++ graphs. Behavior tests use ``eval_node``.
3. The native suite proves the equivalent C++ wiring paths and the lower-level
   ownership, type-erasure, nested-graph, and lifecycle contracts.

Before the final ``0.4.0`` release, run representative real applications in an
environment containing ``hg_cpp`` but not upstream ``hgraph``. Cover startup
and shutdown, services/adaptors, record/replay, nested map/mesh/switch graphs,
Python user nodes, exceptions, real-time execution, and long-running memory
behavior. Record any incompatibility as a minimal repository test before
changing the implementation.

Release gates
-------------

The definition-of-done commands in ``AGENTS.md`` are authoritative. A release
candidate requires all of the following from a clean checkout:

- fresh Release native configure/build and the complete C++ suite on macOS;
- a Python 3.12 ``cp312-abi3`` wheel, installed into a fresh Python 3.14
  environment, and the complete non-WIP Python suite;
- the same Release gates on Ubuntu 24.04/GCC 13;
- Linux Debug/AddressSanitizer native and Python suites for type-erasure,
  ownership, nested-graph, or bridge-lifetime changes;
- strict ``abi3audit`` for every wheel, install/consumer validation for public
  CMake changes, and a warning-clean build on the gating native toolchains;
- Sphinx ``-W`` documentation, packaging metadata tests, and a source
  distribution inspection; and
- the benchmark correctness guards plus a recorded Linux benchmark run.

GitHub CI is post-push platform evidence, not a substitute for the local gates.
Tagged releases reuse the successful distribution artifacts for the exact
commit SHA. Prerelease tags use the same ``v_`` convention, for example
``v_0.4.0rc1``.

Performance evidence
--------------------

``benchmarks/orchestrate.py`` is the cross-implementation benchmark driver and
``python/tests/test_benchmark_scenarios.py`` proves that timed workloads perform
meaningful work. Record comparative results on the Ubuntu 24.04 VM, including
compiler, build type, commit, CPU, sample count, and scaling parameters.
Dynamic TSL scenarios are ``hg_cpp``-only and must not claim an upstream ratio.
Native microbenchmarks in ``tests/cpp/type_erasure_perf.cpp`` and
``tests/cpp/json_perf.cpp`` protect lower-level dispatch and serialization
costs.

Release review
--------------

The release reviewer should verify:

- package, documentation, wheel, and tag versions agree under PEP 440, and
  CMake's numeric version matches their base version;
- no public Python name relies on a private bridge-only runtime implementation;
- the parity matrix and roadmap contain no stale implemented-as-missing rows;
- every remaining skip has a ``deviation:`` or ``gap:`` reason and an owner or
  accepted policy;
- no compiler warning was suppressed broadly to make a platform green;
- wheel contents exclude build trees, tests, caches, and private development
  artifacts; and
- the validation evidence below names the exact tested commit and is updated
  only after the commands complete.

Validation evidence
-------------------

The following pre-tag evidence was recorded on 19 July 2026. The source was the
working tree based on commit ``4aeeddb05f586e427da9b1e85f228916a70cc0e9``;
it intentionally contained the uncommitted release-candidate changes, so this
is implementation-review evidence rather than evidence for a final tag.

**macOS 26.5.2, arm64, Apple Clang 21.0.0:**

- a fresh Release configure and warning-as-error build passed all 1,131 native
  tests;
- ``hg_cpp-0.4.0rc1-cp312-abi3-macosx_15_0_arm64.whl`` passed strict
  ``abi3audit`` and loaded from a fresh Python 3.14 environment; and
- that installed wheel passed 1,356 Python tests with 16 documented skips.

**Ubuntu 24.04 VM, x86_64, GCC 13.3.0:**

- a fresh Release configure and warning-as-error build passed all 1,131 native
  tests;
- the local ``cp312-abi3`` wheel passed strict ``abi3audit``, loaded from a
  fresh Python 3.14 environment, and passed 1,356 Python tests with 16
  documented skips; and
- Debug AddressSanitizer builds passed both complete suites (1,131 native and
  1,356 Python tests with 16 skips) without sanitizer diagnostics. Leak
  detection was disabled for the Python process because ownership of the
  interpreter and Arrow shutdown allocations is outside the tested runtime.

The controlled Linux core benchmark is recorded in
``benchmarks/results/matrix-20260719-124516.md`` with the corresponding raw
samples in ``raw-20260719-124516.json``. It used three fresh-process samples,
Release/GCC 13, Python 3.12.3, and source fingerprint
``ab42c7aa882b03be53e33de437e4099cf915e6ed558fd7ecad308942dd72c587``.
All workload guards passed. The result preserves both improvements and
regressions relative to upstream instead of treating timings as a release
pass/fail gate.

The cross-platform CMake install/consumer check passed for the new public
headers. Sphinx 9.1 completed with ``-W``; all 44 packaging, readiness, and
benchmark-harness canaries passed; and ``uv lock --check`` plus workflow YAML
validation succeeded. The final source distribution contained 696 files
(approximately 1.9 MiB), retained the public headers and implementation
sources, and excluded the reference submodule, benchmark environments/results,
release-review reports, caches, and build trees.

The official ``manylinux_2_28``/GCC 14 wheel and Windows Visual Studio wheel
remain post-push GitHub CI evidence. They must not be inferred from these local
results, and the final tag review must replace the working-tree revision above
with the exact tagged commit.

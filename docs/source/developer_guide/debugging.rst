Debugging
=========

The repository includes opt-in LLDB and GDB summaries plus expandable child
navigation for the type-erased runtime structures in ``tools/debugger``.

Load LLDB support with:

.. code-block:: text

   (lldb) command script import /path/to/hg_cpp/tools/debugger/hgraph_lldb.py

Load GDB support with:

.. code-block:: text

   (gdb) source /path/to/hg_cpp/tools/debugger/hgraph_gdb.py

When installed through CMake, the scripts are copied to
``share/hgraph/debugger`` under the install prefix.

The printers cover the common ``SchemaHeader`` and ``TypeRecord`` structures,
``AnyPtr``, and every ``TypedPtr<Family, Role>`` specialization (including the
public value, time-series, node, graph, executor, and clock pointer aliases).
A pointer summary shows its live/typed-null/unbound state, access mode,
family/role/kind, semantic label, implementation label, record address, and
data address. Expanding it exposes the canonical record and then its schema,
plan, ops, and optional debug descriptor pointers.

They only inspect debug-info fields and memory; they do not call methods in the
stopped process. Records carrying a stable data-only debug descriptor expose
bool, signed/unsigned integer, and 32/64-bit floating-point payloads directly.
Fixed tuples and bundles expand into child ``AnyPtr`` values using descriptor
offsets; unset fields appear typed-null through the published validity bitmap.
Supported sequences expand in logical order, including fixed arrays, dense
compact lists/sets, ring buffers, queues, and mutable slot-backed lists/sets.
Mutable maps expose live key/value pairs. Graphs expose their in-place nodes;
nodes expose state and scalar owners; single, switch, map, and mesh nested
nodes expose retained child graph owners. Slot navigation reads the stable
pointer table and ``SlotBitmap`` state, so erased slots are omitted while
constructed stopped entries remain inspectable.

Opaque atomic storage, nullable dynamic-list/compact-map validity, and node
endpoint owners remain explicitly opaque. The printers do not infer a payload
layout from semantic labels, C++ template names, or private container fields.

Build with debug information enabled for reliable output. Optimized builds may
hide or fold the private fields that the summaries read.

Interactive printer validation
------------------------------

The deterministic fixture and batch validators exercise real debugger child
navigation.  Configure with ``HGRAPH_ENABLE_DEBUGGER_SMOKE_TESTS=ON`` to add
the platform debugger to CTest, or run the same check directly from the
repository root.

On macOS:

.. code-block:: console

   lldb --batch \
       -o 'command script import tools/debugger/hgraph_lldb.py' \
       -o 'command script import tests/debugger/hgraph_lldb_smoke.py' \
       -o 'breakpoint set --name hgraph_debugger_fixture_stop' \
       -o run -o hgraph-smoke \
       build/tests/cpp/hgraph_debugger_fixture

On Linux:

.. code-block:: console

   gdb --nx --batch \
       -ex 'set pagination off' \
       -ex 'source tools/debugger/hgraph_gdb.py' \
       -ex 'source tests/debugger/hgraph_gdb_smoke.py' \
       -ex 'break hgraph_debugger_fixture_stop' \
       -ex run -ex hgraph-smoke \
       build/tests/cpp/hgraph_debugger_fixture

A successful run ends with ``hgraph LLDB type-erasure smoke test passed`` or
``hgraph GDB type-erasure smoke test passed``.  The validator checks summaries
such as ``semantic="debugger_fixture_graph"`` with
``implementation="hgraph.graph.root"``, then expands the graph's ``[0]`` child
and verifies ``semantic="debugger_fixture_graph_node"``.  It also checks
bundle fields, mutable-map key/value pairs, typed-null pointers, malformed
pointers, and unsupported ABI versions.  The fixture also runs real switch,
map, and mesh nodes to the point where both switch banks are populated and
keyed slots contain one live and one stopped/pending child.  The validators
expand those children into their nested graph nodes, require keys 22 and 33,
and verify that the physically erased key 11 is no longer visible.

Some VM security configurations deny ``ptrace``.  In that environment GDB may
load both scripts successfully but fail with ``Couldn't get registers`` before
the fixture breakpoint.  Use a native Linux host or the required Linux CI gate
for the interactive smoke test; this failure does not exercise the printers.

Linux Python/ASan debugging from macOS
--------------------------------------

Some lifetime failures only reproduce when the Python bridge is loaded into a
Linux process. A small Linux VM is useful on an Apple Silicon Mac because it
exercises the Linux/GCC build without modifying the macOS toolchain. The
workflow below uses an Ubuntu 24.04 OrbStack machine named ``ubuntu``. Other
Linux VMs work as long as the repository is shared into the guest and the
commands are run inside the guest.

Keep the build directory and virtual environment on the Linux filesystem, not
in the shared source tree. The examples use ``/tmp`` for both. Replace
``/Users/<mac-user>/src/hg_cpp`` with the macOS path to the checkout; OrbStack
mounts that path at the same location in the guest.

Prepare the guest
~~~~~~~~~~~~~~~~~

Open a shell in the VM and install a current GCC plus Python development
support:

.. code-block:: bash

   orb -m ubuntu bash
   sudo apt update
   sudo apt install -y build-essential git python3-dev python3-venv

Inside that Linux shell, create a disposable Python environment containing the
binding and test dependencies:

.. code-block:: bash

   export REPO=/Users/<mac-user>/src/hg_cpp
   export VENV=/tmp/hg_cpp-asan-venv
   export BUILD=/tmp/hg_cpp-linux-asan

   python3 -m venv "$VENV"
   source "$VENV/bin/activate"
   python -m pip install --upgrade pip cmake ninja nanobind \
       "pyarrow>=24,<25" "numpy>=2" "pytest>=8" "frozendict>=2.4"

Confirm the compiler before configuring. The investigation that established
this workflow used GCC 13.3:

.. code-block:: bash

   c++ --version
   python --version

Configure and build the Python extension
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Configure a Debug build with Python user nodes and the stable ABI enabled.
Warnings remain errors in this configuration because GCC diagnostics have
identified real emitted-code problems in the past.

.. code-block:: bash

   cmake -S "$REPO" -B "$BUILD" -GNinja \
       -DCMAKE_BUILD_TYPE=Debug \
       -DCMAKE_CXX_COMPILER=/usr/bin/c++ \
       -DPython_EXECUTABLE="$VENV/bin/python" \
       -DHGRAPH_BUILD_PYTHON_BINDINGS=ON \
       -DHGRAPH_ENABLE_PYTHON_USER_NODES=ON \
       -DHGRAPH_PYTHON_STABLE_ABI=ON \
       -DHGRAPH_ENABLE_ASAN=ON \
       -DHGRAPH_WARNINGS_AS_ERRORS=ON \
       -DBUILD_TESTING=OFF
   cmake --build "$BUILD" --target _hgraph --parallel 4

Do not start pytest from another shell until the build command has exited
successfully. A header change can rebuild most of the extension; running tests
while that build is active can silently exercise the previous ``_hgraph``
binary.

Run under ASan
~~~~~~~~~~~~~~

The Python executable is not itself linked with ASan, so the sanitizer runtime
must be loaded before the instrumented extension. Resolve both runtime paths
through the same compiler used for the extension:

.. code-block:: bash

   export LD_PRELOAD="$(c++ -print-file-name=libasan.so):$(c++ -print-file-name=libstdc++.so)"
   export ASAN_OPTIONS=detect_leaks=0:halt_on_error=1:abort_on_error=1
   export PYTHONPATH="$BUILD/python:$REPO/python"

Preloading ``libstdc++`` as well as ``libasan`` prevents Python or another
extension from selecting an older C++ runtime first. Leak detection is disabled
for this workflow because process-wide Python and third-party shutdown state is
too noisy for the ownership errors being investigated; use a dedicated native
leak run when leaks are the target.

First confirm that Python imports the extension from the Linux build tree:

.. code-block:: bash

   python -c "import _hgraph; print(_hgraph.__file__)"

Then minimize the failure to one test before running the complete compatibility
suite:

.. code-block:: bash

   python -m pytest -q \
       "$REPO/python/tests/ported/_operators/test_control_operators.py::test_race_tsd_of_bundles_switch_bundle_types"
   python -m pytest -q "$REPO/python/tests" -m "not wip"

Change the focused node id to the failing test. ``-s -vv`` and
``PYTHONFAULTHANDLER=1`` are useful when a native abort occurs before pytest
flushes its captured output.

Capture and interpret reports
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For failures with a large amount of pytest output, direct ASan reports to a
separate file:

.. code-block:: bash

   rm -f /tmp/hgraph-asan.*
   export ASAN_OPTIONS=detect_leaks=0:halt_on_error=1:abort_on_error=1:log_path=/tmp/hgraph-asan
   python -m pytest -s -vv "$REPO/python/tests/path_to_test.py::test_name"
   grep -n -A100 -B10 -E \
       "ERROR: AddressSanitizer|freed by thread|previously allocated|SUMMARY:" \
       /tmp/hgraph-asan.*

Read the report in ownership order: the first stack is the invalid access, the
``freed by`` stack identifies the premature teardown, and the allocation stack
identifies the owner that should have controlled the lifetime. Debug builds and
``-fno-omit-frame-pointer`` (added by ``HGRAPH_ENABLE_ASAN``) keep those native
stacks usable.

An ``assert`` abort is not an ASan violation and may not create an ASan report.
Prefer GDB inside the VM for those failures. Some VM security configurations
deny ``ptrace`` and GDB then fails while reading registers. In that case,
temporarily adding a Linux ``backtrace``/``backtrace_symbols_fd`` call at the
failing invariant can identify the native caller. Remove that diagnostic as
soon as the call path is known.

Common problems
~~~~~~~~~~~~~~~

``ASan runtime does not come first``
   ``LD_PRELOAD`` was omitted or points at a different compiler's runtime. Use
   the ``c++ -print-file-name`` form above in the same shell that runs Python.

Python imports the wrong extension
   Check ``_hgraph.__file__`` and put ``$BUILD/python`` before ``$REPO/python``
   in ``PYTHONPATH``. Also make sure the extension build has completed.

The stack does not match the latest source
   Rebuild ``_hgraph`` and wait for a successful exit. For a difficult case,
   use ``nm -C "$BUILD/python/_hgraph.abi3.so"`` to confirm that an expected
   symbol is present in the binary under test.

GDB reports ``Couldn't get registers``
   The VM is denying ``ptrace`` across its process boundary. Run GDB entirely
   inside a suitably configured VM, or use the ASan log and temporary native
   backtrace approach above.

``rg`` is unavailable in the guest
   Install ripgrep or use ``grep`` for the ASan log. This does not affect the
   build or the report itself.

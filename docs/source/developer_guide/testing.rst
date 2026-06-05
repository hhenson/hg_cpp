Testing
=======

Testing should scale with the runtime surface being introduced.

C++ Tests
---------

C++ tests live under ``tests/cpp``. They should cover:

- public headers and exported CMake targets,
- memory ownership and teardown,
- schema resolution,
- time-series state transitions,
- scheduler ordering,
- node lifecycle behavior,
- graph execution behavior.

The Catch2 unit-test executable links ``registry_test_listener.cpp``. The
listener resets all process-wide registries/factories before and after each
test case. Because ``reset()`` clears the singleton's normal auto-seeded state,
the listener then re-seeds the standard scalar/time-series vocabulary before the
test body runs. Tests should normally use the default registry state instead of
calling ``stdlib::register_standard_types()`` themselves; use a private test-only
scalar type when a test needs to exercise unregistered-type behaviour.

Python Compatibility Tests
--------------------------

Python tests live under ``tests/python`` and should be used where Python wiring or Python user nodes cross into the C++ runtime.

Initial Commands
----------------

.. code-block:: bash

   cmake -S . -B /tmp/hg_cpp-cmake-check
   cmake --build /tmp/hg_cpp-cmake-check
   ctest --test-dir /tmp/hg_cpp-cmake-check --output-on-failure

Open Design Items
-----------------

- Select the long-term C++ test framework.
- Decide how to run Python compatibility tests against locally built bindings.
- Add sanitizer and leak-checking CI profiles.

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

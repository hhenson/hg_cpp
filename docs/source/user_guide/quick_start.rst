Quick Start
===========

The C++ first implementation is still at its initial scaffolding stage. The current quick start is therefore focused on building and validating the native runtime skeleton.

Build the C++ Project
---------------------

From the repository root:

.. code-block:: bash

   cmake -S . -B /tmp/hg_cpp-cmake-check
   cmake --build /tmp/hg_cpp-cmake-check
   ctest --test-dir /tmp/hg_cpp-cmake-check --output-on-failure

The initial smoke test verifies that the public C++ header and generated version header are usable through the ``hgraph::core`` target.

Install Locally
---------------

.. code-block:: bash

   cmake --install /tmp/hg_cpp-cmake-check --prefix /tmp/hg_cpp-install

This installs the public headers, the ``hgraph_core`` library, and the CMake package files needed by downstream C++ projects.

Python Integration
------------------

Python bindings and Python user-node support are opt-in. Normal CMake configure and build should remain usable without Python or nanobind.

The relevant options are:

.. code-block:: bash

   -DHGRAPH_BUILD_PYTHON_BINDINGS=ON
   -DHGRAPH_ENABLE_PYTHON_USER_NODES=ON

The Python-facing examples from the existing HGraph ecosystem will be reintroduced here once the C++ runtime bridge is ready.

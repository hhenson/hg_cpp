Python Integration
==================

Python integration exists to preserve the current ecosystem and to support Python user-authored nodes. It should not define the core runtime architecture.

Supported Roles
---------------

- Python graph wiring,
- compatibility with existing Python HGraph user code,
- Python user nodes executed by the C++ runtime,
- packaging of optional bindings.

Boundaries
----------

Normal CMake builds should not require Python. Python-specific code should live behind optional CMake targets and be enabled only through:

.. code-block:: bash

   -DHGRAPH_BUILD_PYTHON_BINDINGS=ON
   -DHGRAPH_ENABLE_PYTHON_USER_NODES=ON

Topics To Specify
-----------------

- GIL ownership during node evaluation,
- Python object lifetime inside C++ runtime state,
- exception translation,
- Python callback scheduling,
- conversion between Python type metadata and C++ schemas,
- packaging and ABI policy.

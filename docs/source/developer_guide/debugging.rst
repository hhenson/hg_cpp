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

The printers cover ``Value``, ``ValueView``, value and time-series metadata,
``TypeBinding``, ``StoragePlan``, ``TSDataView``, ``TSInputView``, and
``TSOutputView``. Expanding those objects exposes bindings, schemas, storage
plans, ops pointers, payload/data pointers, and metadata field arrays where
available.

They only inspect debug-info fields and decode simple scalar payloads from
memory; they do not call methods in the stopped process.

Build with debug information enabled for reliable output. Optimized builds may
hide or fold the private fields that the summaries read.

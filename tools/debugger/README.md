# hgraph Debugger Printers

This directory contains opt-in debugger summaries for hgraph's type-erased
runtime data structures.

The printers currently cover:

- `hgraph::Value`
- `hgraph::ValueView`
- `hgraph::ValueTypeMetaData`
- `hgraph::TSValueTypeMetaData`
- `hgraph::TypeMetaData`
- `hgraph::TypeBinding<...>`
- `hgraph::MemoryUtils::StoragePlan`
- `hgraph::TSDataView`
- `hgraph::TSInputView`
- `hgraph::TSOutputView`

They are intentionally read-only. They inspect fields from debug info and
decode common scalar payloads directly from memory, but do not call methods in
the debugged process.

## LLDB

From the repository:

```lldb
(lldb) command script import /path/to/hg_cpp/tools/debugger/hgraph_lldb.py
```

From an installed CMake package:

```lldb
(lldb) command script import /install/prefix/share/hgraph/debugger/hgraph_lldb.py
```

For a per-checkout setup, add the import line to `.lldbinit` and enable local
LLDB init loading if your LLDB configuration requires it.

## GDB

From the repository:

```gdb
(gdb) source /path/to/hg_cpp/tools/debugger/hgraph_gdb.py
```

From an installed CMake package:

```gdb
(gdb) source /install/prefix/share/hgraph/debugger/hgraph_gdb.py
```

## Notes

- Build with debug information enabled. Optimized builds may remove or fold
  fields that the printers inspect.
- The scalar payload decoder is best-effort for common registered scalar names
  such as `bool`, `int`, `float`, `int32`, `datetime`, `timedelta`, and `time`.
- Composite values are summarized by schema and storage state. Traversing
  composite contents safely through ops tables can be added later as an
  explicit debugger command if needed.

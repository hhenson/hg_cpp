# hgraph Debugger Printers

This directory contains opt-in debugger summaries and expandable navigation for
hgraph's type-erased runtime data structures.

The printers currently cover the common type-erasure ABI:

- `hgraph::SchemaHeader`
- `hgraph::TypeRecord`
- `hgraph::AnyPtr`
- every `hgraph::TypedPtr<Family, Role>` specialization, including the public
  `ValuePtr`, time-series pointer, `NodePtr`, `GraphPtr`, `ExecutorPtr`, and
  `ClockPtr` aliases

They are intentionally read-only. They inspect fields from debug info and
inferior memory but never call methods in the debugged process. A pointer
summary includes its state and access mode, family/role/kind, semantic and
implementation labels, record address, and data address. Expanding it exposes
the canonical `TypeRecord`, then its `SchemaHeader`, plan, ops, and optional
debug descriptor pointers.

Deep payload traversal is deliberately not inferred from C++ template names or
private container layouts. It will be enabled only for records carrying the
stable data-only debug descriptors introduced by the deeper printer milestones.

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
- Family-specific kind values are decoded only after reading the family from
  the common schema header, so overlapping numeric kind values are unambiguous.
- Invalid magic, ABI, family, role, access, and required pointers are reported
  as invalid or malformed instead of being cast to a guessed representation.

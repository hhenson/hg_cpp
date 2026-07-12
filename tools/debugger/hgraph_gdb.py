"""Read-only GDB printers for hgraph's common type-erasure ABI.

Load with::

    (gdb) source /path/to/hg_cpp/tools/debugger/hgraph_gdb.py

The printers read debug-info fields and inferior memory only. They never call
functions in the stopped process.
"""

import os
import sys

import gdb
import gdb.printing

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

import hgraph_debug_common as common


def field(value, name):
    try:
        return value[name]
    except Exception:
        value_type = value.type.strip_typedefs()
        for member in value_type.fields():
            if member.is_base_class:
                try:
                    return field(value.cast(member.type), name)
                except Exception:
                    pass
        raise


def integer(value):
    try:
        return int(value)
    except Exception:
        return 0


def pointer(value):
    return integer(value)


def c_string(value):
    if pointer(value) == 0:
        return None
    try:
        return value.string()
    except Exception:
        return None


def dereference(value):
    if pointer(value) == 0:
        return None
    try:
        return value.dereference()
    except Exception:
        return None


def schema_snapshot(value):
    return {
        "magic": integer(field(value, "magic")),
        "abi_version": integer(field(value, "abi_version")),
        "family": integer(field(value, "family")),
        "kind": integer(field(value, "kind")),
        "label": c_string(field(value, "label")),
        "introspection": pointer(field(value, "introspection")),
    }


def record_snapshot(value):
    schema_value = dereference(field(value, "schema"))
    return {
        "magic": integer(field(value, "magic")),
        "abi_version": integer(field(value, "abi_version")),
        "role": integer(field(value, "role")),
        "reserved0": integer(field(value, "reserved0")),
        "ops_abi_version": integer(field(value, "ops_abi_version")),
        "reserved1": integer(field(value, "reserved1")),
        "capabilities": integer(field(value, "capabilities")),
        "implementation_label": c_string(field(value, "implementation_label")),
        "schema": schema_snapshot(schema_value) if schema_value is not None else None,
        "plan": pointer(field(value, "plan")),
        "ops": pointer(field(value, "ops")),
        "debug": pointer(field(value, "debug")),
    }


def record_at(address):
    if address == 0:
        return None
    try:
        record_type = gdb.lookup_type("hgraph::TypeRecord")
        return gdb.Value(address).cast(record_type.pointer()).dereference()
    except Exception:
        return None


def any_pointer_value(value):
    type_name = str(value.type.strip_typedefs())
    if "TypedPtr<" in type_name:
        return field(value, "value_")
    return value


def pointer_state(value):
    erased = any_pointer_value(value)
    tagged = field(erased, "type_")
    bits = integer(field(tagged, "m_bits"))
    return bits & ~0x3, pointer(field(erased, "data_")), bits & 0x3


def display_type_name(value):
    type_name = str(value.type.strip_typedefs())
    if type_name == "hgraph::AnyPtr":
        return "AnyPtr"
    return "TypedPtr"


class SchemaHeaderPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            return common.schema_summary(schema_snapshot(self.value))
        except Exception as exc:
            return "SchemaHeader{<printer error: %s>}" % exc

    def children(self):
        for name in ("magic", "abi_version", "family", "kind", "label", "introspection"):
            try:
                yield name, field(self.value, name)
            except Exception:
                return


class TypeRecordPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            return common.record_summary(record_snapshot(self.value))
        except Exception as exc:
            return "TypeRecord{<printer error: %s>}" % exc

    def children(self):
        try:
            schema_pointer = field(self.value, "schema")
            schema_value = dereference(schema_pointer)
            if schema_value is not None:
                yield "schema", schema_value
            yield "schema_ptr", schema_pointer
            for name in (
                "role",
                "capabilities",
                "implementation_label",
                "plan",
                "ops",
                "debug",
                "magic",
                "abi_version",
                "ops_abi_version",
            ):
                yield name, field(self.value, name)
        except Exception:
            return


class TypePointerPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            record_address, data_address, access = pointer_state(self.value)
            record_value = record_at(record_address)
            snapshot = record_snapshot(record_value) if record_value is not None else None
            return common.pointer_summary(
                display_type_name(self.value), record_address, data_address, access, snapshot
            )
        except Exception as exc:
            return "TypePointer{<printer error: %s>}" % exc

    def children(self):
        try:
            record_address, _, access = pointer_state(self.value)
            record_value = record_at(record_address)
            if record_value is not None:
                yield "record", record_value
            erased = any_pointer_value(self.value)
            yield "data", field(erased, "data_")
            yield "access", gdb.Value(access)
        except Exception:
            return


def build_pretty_printer():
    printer = gdb.printing.RegexpCollectionPrettyPrinter("hgraph")
    printer.add_printer("hgraph::SchemaHeader", r"^hgraph::SchemaHeader$", SchemaHeaderPrinter)
    printer.add_printer("hgraph::TypeRecord", r"^hgraph::TypeRecord$", TypeRecordPrinter)
    printer.add_printer("hgraph::AnyPtr", r"^hgraph::AnyPtr$", TypePointerPrinter)
    printer.add_printer("hgraph::TypedPtr", r"^hgraph::TypedPtr<.*>$", TypePointerPrinter)
    return printer


def register_hgraph_printers(obj=None):
    if obj is None:
        obj = gdb.current_objfile()
    gdb.printing.register_pretty_printer(obj, build_pretty_printer(), replace=True)


register_hgraph_printers()

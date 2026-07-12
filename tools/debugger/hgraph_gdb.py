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


def descriptor_at(address):
    if address == 0:
        return None
    try:
        descriptor_type = gdb.lookup_type("hgraph::DebugDescriptor")
        return gdb.Value(address).cast(descriptor_type.pointer()).dereference()
    except Exception:
        return None


def descriptor_snapshot(value):
    return {
        "magic": integer(field(value, "magic")),
        "abi_version": integer(field(value, "abi_version")),
        "layout": integer(field(value, "layout")),
        "atomic_kind": integer(field(value, "atomic_kind")),
        "flags": integer(field(value, "flags")),
        "field_count": integer(field(value, "field_count")),
        "fields": pointer(field(value, "fields")),
        "validity_offset": integer(field(value, "validity_offset")),
        "validity_word_size": integer(field(value, "validity_word_size")),
        "reserved0": integer(field(value, "reserved0")),
        "key_type": pointer(field(value, "key_type")),
        "element_type": pointer(field(value, "element_type")),
        "dynamic_layout": pointer(field(value, "dynamic_layout")),
    }


def debug_field_snapshot(value):
    return {
        "name": c_string(field(value, "name")),
        "offset": integer(field(value, "offset")),
        "type": pointer(field(value, "type")),
        "validity_bit": integer(field(value, "validity_bit")),
        "flags": integer(field(value, "flags")),
    }


def descriptor_fields(value, snapshot):
    fields = field(value, "fields")
    for index in range(snapshot["field_count"]):
        try:
            yield index, fields[index], debug_field_snapshot(fields[index])
        except Exception:
            return


def target_byte_order():
    try:
        output = gdb.execute("show endian", to_string=True).lower()
        return "big" if "big endian" in output else "little"
    except Exception:
        return "little"


def target_pointer_size():
    return int(gdb.lookup_type("void").pointer().sizeof)


def read_memory(address, size):
    if address == 0 or size <= 0:
        return None
    try:
        return bytes(gdb.selected_inferior().read_memory(address, size))
    except Exception:
        return None


def record_plan_size(record_value):
    try:
        plan = dereference(field(record_value, "plan"))
        return integer(field(field(plan, "layout"), "size")) if plan is not None else 0
    except Exception:
        return 0


def atomic_value(record_value, data_address):
    try:
        descriptor_value = descriptor_at(pointer(field(record_value, "debug")))
        if descriptor_value is None:
            return common._MISSING
        snapshot = descriptor_snapshot(descriptor_value)
        if not common.debug_descriptor_valid(snapshot) or snapshot["layout"] != 1:
            return common._MISSING
        payload = read_memory(data_address, record_plan_size(record_value))
        return common.decode_atomic(snapshot["atomic_kind"], payload, target_byte_order())
    except Exception:
        return common._MISSING


def make_any_pointer(record_address, data_address, access):
    try:
        size = target_pointer_size()
        order = target_byte_order()
        payload = (record_address | access).to_bytes(size, order) + data_address.to_bytes(size, order)
        return gdb.Value(payload, gdb.lookup_type("hgraph::AnyPtr"))
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
            debug_pointer = field(self.value, "debug")
            debug_value = descriptor_at(pointer(debug_pointer))
            if debug_value is not None:
                yield "debug_descriptor", debug_value
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


class DebugDescriptorPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            return common.debug_descriptor_summary(descriptor_snapshot(self.value))
        except Exception as exc:
            return "DebugDescriptor{<printer error: %s>}" % exc

    def children(self):
        try:
            snapshot = descriptor_snapshot(self.value)
            for index, field_value, field_snapshot in descriptor_fields(self.value, snapshot):
                name = field_snapshot["name"] or "[{}]".format(index)
                yield name, field_value
            for name in (
                "layout",
                "atomic_kind",
                "flags",
                "validity_offset",
                "validity_word_size",
                "key_type",
                "element_type",
                "dynamic_layout",
                "magic",
                "abi_version",
            ):
                yield name, field(self.value, name)
        except Exception:
            return


class DebugFieldPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            return common.debug_field_summary(debug_field_snapshot(self.value))
        except Exception as exc:
            return "DebugField{<printer error: %s>}" % exc

    def children(self):
        try:
            type_pointer = field(self.value, "type")
            type_value = record_at(pointer(type_pointer))
            if type_value is not None:
                yield "type_record", type_value
            for name in ("name", "offset", "type", "validity_bit", "flags"):
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
            payload = (
                atomic_value(record_value, data_address)
                if record_value is not None and data_address
                else common._MISSING
            )
            return common.pointer_summary(
                display_type_name(self.value), record_address, data_address, access, snapshot, payload
            )
        except Exception as exc:
            return "TypePointer{<printer error: %s>}" % exc

    def children(self):
        try:
            record_address, data_address, access = pointer_state(self.value)
            record_value = record_at(record_address)
            if record_value is not None:
                yield "record", record_value
            erased = any_pointer_value(self.value)
            yield "data", field(erased, "data_")
            yield "access", gdb.Value(access)

            if record_value is None or record_address == 0 or data_address == 0 or access not in common.ACCESS_NAMES:
                return
            descriptor_value = descriptor_at(pointer(field(record_value, "debug")))
            if descriptor_value is None:
                return
            snapshot = descriptor_snapshot(descriptor_value)
            if not common.debug_descriptor_valid(snapshot) or snapshot["layout"] != 2:
                return

            validity = None
            if snapshot["flags"] & common.DEBUG_DESCRIPTOR_HAS_VALIDITY:
                bits_per_word = snapshot["validity_word_size"] * 8
                word_count = (snapshot["field_count"] + bits_per_word - 1) // bits_per_word
                validity = read_memory(
                    data_address + snapshot["validity_offset"],
                    word_count * snapshot["validity_word_size"],
                )
            for index, _, field_snapshot in descriptor_fields(descriptor_value, snapshot):
                name = field_snapshot["name"] or "[{}]".format(index)
                is_set = common.field_is_set(
                    validity,
                    field_snapshot["validity_bit"],
                    snapshot["validity_word_size"]
                    if snapshot["flags"] & common.DEBUG_DESCRIPTOR_HAS_VALIDITY
                    else 0,
                    target_byte_order(),
                )
                child = make_any_pointer(
                    field_snapshot["type"],
                    data_address + field_snapshot["offset"] if is_set else 0,
                    access if is_set else 0,
                )
                if child is not None:
                    yield name, child
        except Exception:
            return


def build_pretty_printer():
    printer = gdb.printing.RegexpCollectionPrettyPrinter("hgraph")
    printer.add_printer("hgraph::SchemaHeader", r"^hgraph::SchemaHeader$", SchemaHeaderPrinter)
    printer.add_printer("hgraph::TypeRecord", r"^hgraph::TypeRecord$", TypeRecordPrinter)
    printer.add_printer("hgraph::DebugDescriptor", r"^hgraph::DebugDescriptor$", DebugDescriptorPrinter)
    printer.add_printer("hgraph::DebugField", r"^hgraph::DebugField$", DebugFieldPrinter)
    printer.add_printer("hgraph::AnyPtr", r"^hgraph::AnyPtr$", TypePointerPrinter)
    printer.add_printer("hgraph::TypedPtr", r"^hgraph::TypedPtr<.*>$", TypePointerPrinter)
    return printer


def register_hgraph_printers(obj=None):
    if obj is None:
        obj = gdb.current_objfile()
    gdb.printing.register_pretty_printer(obj, build_pretty_printer(), replace=True)


register_hgraph_printers()

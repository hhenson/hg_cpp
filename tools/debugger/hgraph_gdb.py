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


def dynamic_layout_at(address):
    if address == 0:
        return None
    try:
        layout_type = gdb.lookup_type("hgraph::DebugDynamicLayout")
        return gdb.Value(address).cast(layout_type.pointer()).dereference()
    except Exception:
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


def dynamic_layout_snapshot(value):
    return {
        "magic": integer(field(value, "magic")),
        "abi_version": integer(field(value, "abi_version")),
        "kind": integer(field(value, "kind")),
        "reserved0": integer(field(value, "reserved0")),
        "flags": integer(field(value, "flags")),
        "reserved1": integer(field(value, "reserved1")),
        "size_offset": integer(field(value, "size_offset")),
        "size_constant": integer(field(value, "size_constant")),
        "data_offset": integer(field(value, "data_offset")),
        "stride": integer(field(value, "stride")),
        "key_data_offset": integer(field(value, "key_data_offset")),
        "key_stride": integer(field(value, "key_stride")),
        "state_offset": integer(field(value, "state_offset")),
        "auxiliary_offset": integer(field(value, "auxiliary_offset")),
        "entry_offset": integer(field(value, "entry_offset")),
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


def read_unsigned(address, size=None):
    size = target_pointer_size() if size is None else size
    payload = read_memory(address, size)
    return int.from_bytes(payload, target_byte_order()) if payload is not None else None
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


def make_owner_pointer(owner_address, fallback_record, access):
    size = target_pointer_size()
    record_address = read_unsigned(owner_address)
    state_word = read_unsigned(owner_address + size)
    if record_address is None or state_word is None:
        return None
    if record_address == 0:
        record_address = fallback_record
    state = state_word & common.DEBUG_OWNER_STATE_MASK
    if state == 0:
        data_address = 0
    elif state == common.DEBUG_OWNER_INLINE_STATE:
        data_address = owner_address + 2 * size
    elif state in (common.DEBUG_OWNER_HEAP_STATE, common.DEBUG_OWNER_BORROWED_STATE):
        data_address = read_unsigned(owner_address + 2 * size)
        if data_address is None:
            return None
    else:
        return None
    return make_any_pointer(record_address, data_address, access if data_address else 0)


def dynamic_child_addresses(data_address, layout):
    flags = layout["flags"]
    size = (
        layout["size_constant"]
        if flags & common.DEBUG_DYNAMIC_SIZE_CONSTANT
        else read_unsigned(data_address + layout["size_offset"])
    )
    if size is None or size > (1 << 30):
        return
    visible_size = min(size, 4096)
    head = (
        read_unsigned(data_address + layout["auxiliary_offset"])
        if flags & common.DEBUG_DYNAMIC_HAS_HEAD
        else 0
    )
    if head is None:
        return
    data_base = data_address + layout["data_offset"]
    if flags & common.DEBUG_DYNAMIC_DATA_INDIRECT:
        data_base = read_unsigned(data_base)
    if data_base is None:
        return
    key_base = 0
    if layout["key_stride"]:
        key_base = data_address + layout["key_data_offset"]
        if flags & common.DEBUG_DYNAMIC_KEY_DATA_INDIRECT:
            key_base = read_unsigned(key_base)
        if key_base is None:
            return
    state_words = 0
    state_bits = 0
    if flags & common.DEBUG_DYNAMIC_HAS_SLOT_STATE:
        state_words = read_unsigned(data_address + layout["state_offset"])
        state_bits = read_unsigned(data_address + layout["state_offset"] + target_pointer_size())
        if state_words is None or state_bits is None:
            return
    for logical_index in range(visible_size):
        physical_index = (head + logical_index) % size if size and flags & common.DEBUG_DYNAMIC_HAS_HEAD else logical_index
        if flags & common.DEBUG_DYNAMIC_HAS_SLOT_STATE:
            if physical_index >= state_bits:
                continue
            word = read_unsigned(state_words + (physical_index // 64) * 8, 8)
            if word is None or not (word & (1 << (physical_index % 64))):
                continue
        if flags & common.DEBUG_DYNAMIC_DATA_POINTER_TABLE:
            element = read_unsigned(data_base + physical_index * target_pointer_size())
        else:
            element = data_base + physical_index * layout["stride"]
        if key_base:
            if flags & common.DEBUG_DYNAMIC_KEY_DATA_POINTER_TABLE:
                key = read_unsigned(key_base + physical_index * target_pointer_size())
            else:
                key = key_base + physical_index * layout["key_stride"]
        else:
            key = 0
        if element:
            element += layout["entry_offset"]
        yield logical_index, key, element


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
            dynamic_value = dynamic_layout_at(snapshot["dynamic_layout"])
            if dynamic_value is not None:
                yield "dynamic_layout_value", dynamic_value
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


class DebugDynamicLayoutPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            return common.debug_dynamic_layout_summary(dynamic_layout_snapshot(self.value))
        except Exception as exc:
            return "DebugDynamicLayout{<printer error: %s>}" % exc

    def children(self):
        for name in (
            "kind",
            "flags",
            "size_offset",
            "size_constant",
            "data_offset",
            "stride",
            "key_data_offset",
            "key_stride",
            "state_offset",
            "auxiliary_offset",
            "entry_offset",
            "magic",
            "abi_version",
        ):
            try:
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
            if not common.debug_descriptor_valid(snapshot):
                return
            if snapshot["layout"] in (2, 5, 6):
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
                    if field_snapshot["flags"] & common.DEBUG_FIELD_EMBEDDED_OWNER:
                        child = make_owner_pointer(
                            data_address + field_snapshot["offset"], field_snapshot["type"], access
                        )
                    else:
                        child = make_any_pointer(
                            field_snapshot["type"],
                            data_address + field_snapshot["offset"] if is_set else 0,
                            access if is_set else 0,
                        )
                    if child is not None:
                        yield name, child
                if not snapshot["dynamic_layout"]:
                    return
            if snapshot["layout"] not in (3, 4, 5, 6):
                return
            dynamic_value = dynamic_layout_at(snapshot["dynamic_layout"])
            if dynamic_value is None:
                return
            layout = dynamic_layout_snapshot(dynamic_value)
            if not common.debug_dynamic_layout_valid(layout):
                return
            for slot, key_address, element_address in dynamic_child_addresses(data_address, layout):
                if key_address and snapshot["key_type"]:
                    if layout["flags"] & common.DEBUG_DYNAMIC_KEYS_ARE_OWNERS:
                        key_child = make_owner_pointer(key_address, snapshot["key_type"], access)
                    else:
                        key_child = make_any_pointer(snapshot["key_type"], key_address, access)
                    if key_child is not None:
                        yield "key[{}]".format(slot), key_child
                if element_address:
                    if layout["flags"] & common.DEBUG_DYNAMIC_ELEMENTS_ARE_OWNERS:
                        element_child = make_owner_pointer(element_address, snapshot["element_type"], access)
                    else:
                        element_child = make_any_pointer(snapshot["element_type"], element_address, access)
                    if element_child is not None:
                        yield "value[{}]".format(slot) if snapshot["key_type"] else "[{}]".format(slot), element_child
        except Exception:
            return


def build_pretty_printer():
    printer = gdb.printing.RegexpCollectionPrettyPrinter("hgraph")
    printer.add_printer("hgraph::SchemaHeader", r"^hgraph::SchemaHeader$", SchemaHeaderPrinter)
    printer.add_printer("hgraph::TypeRecord", r"^hgraph::TypeRecord$", TypeRecordPrinter)
    printer.add_printer("hgraph::DebugDescriptor", r"^hgraph::DebugDescriptor$", DebugDescriptorPrinter)
    printer.add_printer("hgraph::DebugDynamicLayout", r"^hgraph::DebugDynamicLayout$", DebugDynamicLayoutPrinter)
    printer.add_printer("hgraph::DebugField", r"^hgraph::DebugField$", DebugFieldPrinter)
    printer.add_printer("hgraph::AnyPtr", r"^hgraph::AnyPtr$", TypePointerPrinter)
    printer.add_printer("hgraph::TypedPtr", r"^hgraph::TypedPtr<.*>$", TypePointerPrinter)
    return printer


def register_hgraph_printers(obj=None):
    if obj is None:
        obj = gdb.current_objfile()
    gdb.printing.register_pretty_printer(obj, build_pretty_printer(), replace=True)


register_hgraph_printers()

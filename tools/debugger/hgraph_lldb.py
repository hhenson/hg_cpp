"""Read-only LLDB printers for hgraph's common type-erasure ABI.

Load with::

    (lldb) command script import /path/to/hg_cpp/tools/debugger/hgraph_lldb.py

The printers read debug-info fields and inferior memory only. They never call
functions in the stopped process.
"""

import os
import sys

import lldb

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

import hgraph_debug_common as common


def valid(value):
    return value is not None and value.IsValid()


def raw(value):
    if not valid(value):
        return lldb.SBValue()
    result = value.GetNonSyntheticValue()
    return result if valid(result) else value


def child(value, name):
    if not valid(value):
        return lldb.SBValue()
    value = raw(value)
    result = value.GetChildMemberWithName(name)
    if valid(result):
        return result
    value_type = value.GetType()
    for index in range(value_type.GetNumberOfDirectBaseClasses()):
        base = value.GetChildAtIndex(index)
        result = child(base, name)
        if valid(result):
            return result
    return lldb.SBValue()


def integer(value):
    return value.GetValueAsUnsigned(0) if valid(value) else 0


def pointer(value):
    return integer(value)


def c_string(value):
    address = pointer(value)
    if address == 0 or not valid(value):
        return None
    error = lldb.SBError()
    text = value.GetProcess().ReadCStringFromMemory(address, 256, error)
    return text if error.Success() else None


def dereference(value):
    if pointer(value) == 0:
        return lldb.SBValue()
    result = value.Dereference()
    return result if valid(result) else lldb.SBValue()


def schema_snapshot(value):
    return {
        "magic": integer(child(value, "magic")),
        "abi_version": integer(child(value, "abi_version")),
        "family": integer(child(value, "family")),
        "kind": integer(child(value, "kind")),
        "label": c_string(child(value, "label")),
        "introspection": pointer(child(value, "introspection")),
    }


def record_snapshot(value):
    schema_value = dereference(child(value, "schema"))
    return {
        "magic": integer(child(value, "magic")),
        "abi_version": integer(child(value, "abi_version")),
        "role": integer(child(value, "role")),
        "reserved0": integer(child(value, "reserved0")),
        "ops_abi_version": integer(child(value, "ops_abi_version")),
        "reserved1": integer(child(value, "reserved1")),
        "capabilities": integer(child(value, "capabilities")),
        "implementation_label": c_string(child(value, "implementation_label")),
        "schema": schema_snapshot(schema_value) if valid(schema_value) else None,
        "plan": pointer(child(value, "plan")),
        "ops": pointer(child(value, "ops")),
        "debug": pointer(child(value, "debug")),
    }


def record_at(value, address):
    if address == 0 or not valid(value):
        return lldb.SBValue()
    target = value.GetTarget()
    record_type = target.FindFirstType("hgraph::TypeRecord")
    if not record_type.IsValid():
        return lldb.SBValue()
    return target.CreateValueFromAddress("record", target.ResolveLoadAddress(address), record_type)


def descriptor_at(value, address):
    if address == 0 or not valid(value):
        return lldb.SBValue()
    target = value.GetTarget()
    descriptor_type = target.FindFirstType("hgraph::DebugDescriptor")
    if not descriptor_type.IsValid():
        return lldb.SBValue()
    return target.CreateValueFromAddress(
        "debug_descriptor", target.ResolveLoadAddress(address), descriptor_type
    )


def dynamic_layout_at(value, address):
    if address == 0 or not valid(value):
        return lldb.SBValue()
    target = value.GetTarget()
    layout_type = target.FindFirstType("hgraph::DebugDynamicLayout")
    if not layout_type.IsValid():
        return lldb.SBValue()
    return target.CreateValueFromAddress(
        "dynamic_layout", target.ResolveLoadAddress(address), layout_type
    )


def descriptor_snapshot(value):
    return {
        "magic": integer(child(value, "magic")),
        "abi_version": integer(child(value, "abi_version")),
        "layout": integer(child(value, "layout")),
        "atomic_kind": integer(child(value, "atomic_kind")),
        "flags": integer(child(value, "flags")),
        "field_count": integer(child(value, "field_count")),
        "fields": pointer(child(value, "fields")),
        "validity_offset": integer(child(value, "validity_offset")),
        "validity_word_size": integer(child(value, "validity_word_size")),
        "reserved0": integer(child(value, "reserved0")),
        "key_type": pointer(child(value, "key_type")),
        "element_type": pointer(child(value, "element_type")),
        "dynamic_layout": pointer(child(value, "dynamic_layout")),
    }


def debug_field_snapshot(value):
    return {
        "name": c_string(child(value, "name")),
        "offset": integer(child(value, "offset")),
        "type": pointer(child(value, "type")),
        "validity_bit": integer(child(value, "validity_bit")),
        "flags": integer(child(value, "flags")),
    }


def dynamic_layout_snapshot(value):
    return {
        "magic": integer(child(value, "magic")),
        "abi_version": integer(child(value, "abi_version")),
        "kind": integer(child(value, "kind")),
        "reserved0": integer(child(value, "reserved0")),
        "flags": integer(child(value, "flags")),
        "reserved1": integer(child(value, "reserved1")),
        "size_offset": integer(child(value, "size_offset")),
        "size_constant": integer(child(value, "size_constant")),
        "data_offset": integer(child(value, "data_offset")),
        "stride": integer(child(value, "stride")),
        "key_data_offset": integer(child(value, "key_data_offset")),
        "key_stride": integer(child(value, "key_stride")),
        "state_offset": integer(child(value, "state_offset")),
        "auxiliary_offset": integer(child(value, "auxiliary_offset")),
        "entry_offset": integer(child(value, "entry_offset")),
    }


def descriptor_fields(value, snapshot):
    target = value.GetTarget()
    field_type = target.FindFirstType("hgraph::DebugField")
    if not field_type.IsValid():
        return
    base = snapshot["fields"]
    for index in range(snapshot["field_count"]):
        field_value = target.CreateValueFromAddress(
            "field",
            target.ResolveLoadAddress(base + index * field_type.GetByteSize()),
            field_type,
        )
        if not valid(field_value):
            return
        yield index, field_value, debug_field_snapshot(field_value)


def target_byte_order(value):
    return "big" if value.GetTarget().GetByteOrder() == lldb.eByteOrderBig else "little"


def read_memory(value, address, size):
    if not valid(value) or address == 0 or size <= 0:
        return None
    error = lldb.SBError()
    payload = value.GetProcess().ReadMemory(address, size, error)
    return payload if error.Success() else None


def read_unsigned(value, address, size=None):
    if not valid(value) or address == 0:
        return None
    size = value.GetTarget().GetAddressByteSize() if size is None else size
    error = lldb.SBError()
    result = value.GetProcess().ReadUnsignedFromMemory(address, size, error)
    return result if error.Success() else None


def record_plan_size(record_value):
    plan = dereference(child(record_value, "plan"))
    return integer(child(child(plan, "layout"), "size")) if valid(plan) else 0


def atomic_value(record_value, data_address):
    try:
        descriptor_value = descriptor_at(record_value, pointer(child(record_value, "debug")))
        if not valid(descriptor_value):
            return common._MISSING
        snapshot = descriptor_snapshot(descriptor_value)
        if not common.debug_descriptor_valid(snapshot) or snapshot["layout"] != 1:
            return common._MISSING
        payload = read_memory(record_value, data_address, record_plan_size(record_value))
        return common.decode_atomic(snapshot["atomic_kind"], payload, target_byte_order(record_value))
    except Exception:
        return common._MISSING


def make_any_pointer(value, name, record_address, data_address, access):
    target = value.GetTarget()
    pointer_size = target.GetAddressByteSize()
    byte_order = target.GetByteOrder()
    words = [record_address | access, data_address]
    if pointer_size == 8:
        data = lldb.SBData.CreateDataFromUInt64Array(byte_order, pointer_size, words)
    elif pointer_size == 4:
        data = lldb.SBData.CreateDataFromUInt32Array(byte_order, pointer_size, words)
    else:
        return lldb.SBValue()
    pointer_type = target.FindFirstType("hgraph::AnyPtr")
    return target.CreateValueFromData(name, data, pointer_type) if pointer_type.IsValid() else lldb.SBValue()


def make_owner_pointer(value, name, owner_address, fallback_record, access):
    size = value.GetTarget().GetAddressByteSize()
    record_address = read_unsigned(value, owner_address)
    state_word = read_unsigned(value, owner_address + size)
    if record_address is None or state_word is None:
        return lldb.SBValue()
    if record_address == 0:
        record_address = fallback_record
    state = state_word & common.DEBUG_OWNER_STATE_MASK
    if state == 0:
        data_address = 0
    elif state == common.DEBUG_OWNER_INLINE_STATE:
        data_address = owner_address + 2 * size
    elif state in (common.DEBUG_OWNER_HEAP_STATE, common.DEBUG_OWNER_BORROWED_STATE):
        data_address = read_unsigned(value, owner_address + 2 * size)
        if data_address is None:
            return lldb.SBValue()
    else:
        return lldb.SBValue()
    return make_any_pointer(value, name, record_address, data_address, access if data_address else 0)


def dynamic_child_addresses(value, data_address, layout):
    flags = layout["flags"]
    size = (
        layout["size_constant"]
        if flags & common.DEBUG_DYNAMIC_SIZE_CONSTANT
        else read_unsigned(value, data_address + layout["size_offset"])
    )
    if size is None or size > (1 << 30):
        return
    visible_size = min(size, 4096)
    head = (
        read_unsigned(value, data_address + layout["auxiliary_offset"])
        if flags & common.DEBUG_DYNAMIC_HAS_HEAD
        else 0
    )
    if head is None:
        return
    data_base = data_address + layout["data_offset"]
    if flags & common.DEBUG_DYNAMIC_DATA_INDIRECT:
        data_base = read_unsigned(value, data_base)
    if data_base is None:
        return
    key_base = 0
    if layout["key_stride"]:
        key_base = data_address + layout["key_data_offset"]
        if flags & common.DEBUG_DYNAMIC_KEY_DATA_INDIRECT:
            key_base = read_unsigned(value, key_base)
        if key_base is None:
            return
    state_words = 0
    state_bits = 0
    if flags & common.DEBUG_DYNAMIC_HAS_SLOT_STATE:
        state_words = read_unsigned(value, data_address + layout["state_offset"])
        state_bits = read_unsigned(
            value,
            data_address + layout["state_offset"] + value.GetTarget().GetAddressByteSize(),
        )
        if state_words is None or state_bits is None:
            return
    pointer_size = value.GetTarget().GetAddressByteSize()
    for logical_index in range(visible_size):
        physical_index = (
            (head + logical_index) % size
            if size and flags & common.DEBUG_DYNAMIC_HAS_HEAD
            else logical_index
        )
        if flags & common.DEBUG_DYNAMIC_HAS_SLOT_STATE:
            if physical_index >= state_bits:
                continue
            word = read_unsigned(value, state_words + (physical_index // 64) * 8, 8)
            if word is None or not (word & (1 << (physical_index % 64))):
                continue
        if flags & common.DEBUG_DYNAMIC_DATA_POINTER_TABLE:
            element = read_unsigned(value, data_base + physical_index * pointer_size)
        else:
            element = data_base + physical_index * layout["stride"]
        if key_base:
            if flags & common.DEBUG_DYNAMIC_KEY_DATA_POINTER_TABLE:
                key = read_unsigned(value, key_base + physical_index * pointer_size)
            else:
                key = key_base + physical_index * layout["key_stride"]
        else:
            key = 0
        if element:
            element += layout["entry_offset"]
        yield logical_index, key, element


def any_pointer_value(value):
    type_name = raw(value).GetType().GetCanonicalType().GetName() or ""
    if "TypedPtr<" in type_name:
        return child(value, "value_")
    return raw(value)


def pointer_state(value):
    erased = any_pointer_value(value)
    bits = integer(child(child(erased, "type_"), "m_bits"))
    return bits & ~0x3, pointer(child(erased, "data_")), bits & 0x3


def display_type_name(value):
    return "AnyPtr" if (value.GetTypeName() or "") == "hgraph::AnyPtr" else "TypedPtr"


def schema_header_summary(value, _internal_dict, _options):
    try:
        return common.schema_summary(schema_snapshot(value))
    except Exception as exc:
        return "SchemaHeader{<printer error: %s>}" % exc


def type_record_summary(value, _internal_dict, _options):
    try:
        return common.record_summary(record_snapshot(value))
    except Exception as exc:
        return "TypeRecord{<printer error: %s>}" % exc


def debug_descriptor_summary(value, _internal_dict, _options):
    try:
        return common.debug_descriptor_summary(descriptor_snapshot(value))
    except Exception as exc:
        return "DebugDescriptor{<printer error: %s>}" % exc


def debug_field_summary(value, _internal_dict, _options):
    try:
        return common.debug_field_summary(debug_field_snapshot(value))
    except Exception as exc:
        return "DebugField{<printer error: %s>}" % exc


def debug_dynamic_layout_summary(value, _internal_dict, _options):
    try:
        return common.debug_dynamic_layout_summary(dynamic_layout_snapshot(value))
    except Exception as exc:
        return "DebugDynamicLayout{<printer error: %s>}" % exc


def type_pointer_summary(value, _internal_dict, _options):
    try:
        record_address, data_address, access = pointer_state(value)
        record_value = record_at(value, record_address)
        snapshot = record_snapshot(record_value) if valid(record_value) else None
        payload = (
            atomic_value(record_value, data_address)
            if valid(record_value) and data_address
            else common._MISSING
        )
        return common.pointer_summary(
            display_type_name(value), record_address, data_address, access, snapshot, payload
        )
    except Exception as exc:
        return "TypePointer{<printer error: %s>}" % exc


class SyntheticProvider:
    def __init__(self, value, _internal_dict):
        self.value = value
        self.children = []
        self.update()

    def update(self):
        self.children = self.build_children()
        return False

    def num_children(self):
        return len(self.children)

    def get_child_at_index(self, index):
        if index < 0 or index >= len(self.children):
            return lldb.SBValue()
        return self.children[index][1]

    def get_child_index(self, name):
        for index, (child_name, _) in enumerate(self.children):
            if child_name == name:
                return index
        return -1

    def has_children(self):
        return bool(self.children)

    def build_children(self):
        return []

    @staticmethod
    def add(values, name, value):
        if valid(value):
            values.append((name, value))


class TypeRecordSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        schema_pointer = child(self.value, "schema")
        self.add(values, "schema", dereference(schema_pointer))
        self.add(values, "schema_ptr", schema_pointer)
        self.add(
            values,
            "debug_descriptor",
            descriptor_at(self.value, pointer(child(self.value, "debug"))),
        )
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
            self.add(values, name, child(self.value, name))
        return values


class DebugDescriptorSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        snapshot = descriptor_snapshot(self.value)
        for index, field_value, field_snapshot in descriptor_fields(self.value, snapshot):
            self.add(values, field_snapshot["name"] or "[{}]".format(index), field_value)
        self.add(
            values,
            "dynamic_layout_value",
            dynamic_layout_at(self.value, snapshot["dynamic_layout"]),
        )
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
            self.add(values, name, child(self.value, name))
        return values


class DebugDynamicLayoutSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
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
            self.add(values, name, child(self.value, name))
        return values


class DebugFieldSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        self.add(values, "type_record", record_at(self.value, pointer(child(self.value, "type"))))
        for name in ("name", "offset", "type", "validity_bit", "flags"):
            self.add(values, name, child(self.value, name))
        return values


class TypePointerSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        try:
            record_address, data_address, access = pointer_state(self.value)
            record_value = record_at(self.value, record_address)
            self.add(values, "record", record_value)
            self.add(values, "data", child(any_pointer_value(self.value), "data_"))
            if (
                not valid(record_value)
                or record_address == 0
                or data_address == 0
                or access not in common.ACCESS_NAMES
            ):
                return values

            descriptor_value = descriptor_at(record_value, pointer(child(record_value, "debug")))
            if not valid(descriptor_value):
                return values
            snapshot = descriptor_snapshot(descriptor_value)
            if not common.debug_descriptor_valid(snapshot):
                return values
            if snapshot["layout"] in (2, 5, 6):
                validity = None
                if snapshot["flags"] & common.DEBUG_DESCRIPTOR_HAS_VALIDITY:
                    bits_per_word = snapshot["validity_word_size"] * 8
                    word_count = (snapshot["field_count"] + bits_per_word - 1) // bits_per_word
                    validity = read_memory(
                        self.value,
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
                        target_byte_order(self.value),
                    )
                    if field_snapshot["flags"] & common.DEBUG_FIELD_EMBEDDED_OWNER:
                        pointer_value = make_owner_pointer(
                            self.value,
                            name,
                            data_address + field_snapshot["offset"],
                            field_snapshot["type"],
                            access,
                        )
                    else:
                        pointer_value = make_any_pointer(
                            self.value,
                            name,
                            field_snapshot["type"],
                            data_address + field_snapshot["offset"] if is_set else 0,
                            access if is_set else 0,
                        )
                    self.add(values, name, pointer_value)
                if not snapshot["dynamic_layout"]:
                    return values
            if snapshot["layout"] not in (3, 4, 5, 6):
                return values
            dynamic_value = dynamic_layout_at(self.value, snapshot["dynamic_layout"])
            if not valid(dynamic_value):
                return values
            layout = dynamic_layout_snapshot(dynamic_value)
            if not common.debug_dynamic_layout_valid(layout):
                return values
            for slot, key_address, element_address in dynamic_child_addresses(self.value, data_address, layout):
                if key_address and snapshot["key_type"]:
                    if layout["flags"] & common.DEBUG_DYNAMIC_KEYS_ARE_OWNERS:
                        key_value = make_owner_pointer(
                            self.value, "key", key_address, snapshot["key_type"], access
                        )
                    else:
                        key_value = make_any_pointer(
                            self.value, "key", snapshot["key_type"], key_address, access
                        )
                    self.add(
                        values,
                        "key[{}]".format(slot),
                        key_value,
                    )
                if element_address:
                    name = "value[{}]".format(slot) if snapshot["key_type"] else "[{}]".format(slot)
                    if layout["flags"] & common.DEBUG_DYNAMIC_ELEMENTS_ARE_OWNERS:
                        pointer_value = make_owner_pointer(
                            self.value, name, element_address, snapshot["element_type"], access
                        )
                    else:
                        pointer_value = make_any_pointer(
                            self.value, name, snapshot["element_type"], element_address, access
                        )
                    self.add(values, name, pointer_value)
        except Exception:
            pass
        return values


def __lldb_init_module(debugger, _internal_dict):
    summaries = (
        (r"^hgraph::SchemaHeader$", "hgraph_lldb.schema_header_summary"),
        (r"^hgraph::TypeRecord$", "hgraph_lldb.type_record_summary"),
        (r"^hgraph::DebugDescriptor$", "hgraph_lldb.debug_descriptor_summary"),
        (r"^hgraph::DebugField$", "hgraph_lldb.debug_field_summary"),
        (r"^hgraph::DebugDynamicLayout$", "hgraph_lldb.debug_dynamic_layout_summary"),
        (r"^hgraph::AnyPtr$", "hgraph_lldb.type_pointer_summary"),
        (r"^hgraph::TypedPtr<.*>$", "hgraph_lldb.type_pointer_summary"),
    )
    synthetics = (
        (r"^hgraph::TypeRecord$", "hgraph_lldb.TypeRecordSyntheticProvider"),
        (r"^hgraph::DebugDescriptor$", "hgraph_lldb.DebugDescriptorSyntheticProvider"),
        (r"^hgraph::DebugField$", "hgraph_lldb.DebugFieldSyntheticProvider"),
        (r"^hgraph::DebugDynamicLayout$", "hgraph_lldb.DebugDynamicLayoutSyntheticProvider"),
        (r"^hgraph::AnyPtr$", "hgraph_lldb.TypePointerSyntheticProvider"),
        (r"^hgraph::TypedPtr<.*>$", "hgraph_lldb.TypePointerSyntheticProvider"),
    )
    for pattern, function in summaries:
        debugger.HandleCommand(
            'type summary add --category hgraph --regex "{}" --python-function {}'.format(pattern, function)
        )
    for pattern, provider in synthetics:
        debugger.HandleCommand(
            'type synthetic add --category hgraph --regex "{}" --python-class {}'.format(pattern, provider)
        )
    debugger.HandleCommand("type category enable hgraph")
    print("hgraph common type-erasure printers loaded")

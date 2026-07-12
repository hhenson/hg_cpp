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


def type_pointer_summary(value, _internal_dict, _options):
    try:
        record_address, data_address, access = pointer_state(value)
        record_value = record_at(value, record_address)
        snapshot = record_snapshot(record_value) if valid(record_value) else None
        return common.pointer_summary(
            display_type_name(value), record_address, data_address, access, snapshot
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


class TypePointerSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        try:
            record_address, _, _ = pointer_state(self.value)
            self.add(values, "record", record_at(self.value, record_address))
            self.add(values, "data", child(any_pointer_value(self.value), "data_"))
        except Exception:
            pass
        return values


def __lldb_init_module(debugger, _internal_dict):
    summaries = (
        (r"^hgraph::SchemaHeader$", "hgraph_lldb.schema_header_summary"),
        (r"^hgraph::TypeRecord$", "hgraph_lldb.type_record_summary"),
        (r"^hgraph::AnyPtr$", "hgraph_lldb.type_pointer_summary"),
        (r"^hgraph::TypedPtr<.*>$", "hgraph_lldb.type_pointer_summary"),
    )
    synthetics = (
        (r"^hgraph::TypeRecord$", "hgraph_lldb.TypeRecordSyntheticProvider"),
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

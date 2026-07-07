"""GDB pretty printers and child navigation for hgraph's type-erased runtime data structures.

Load with:

    (gdb) source /path/to/hg_cpp/tools/debugger/hgraph_gdb.py

The printers are deliberately read-only: they inspect debug-info fields,
provide shallow navigation through the erased handles, and infer common scalar
payloads from memory, but they do not call methods in the inferior process.
"""

import struct

import gdb
import gdb.printing


VALUE_KIND_NAMES = {
    0: "Atomic",
    1: "Tuple",
    2: "Bundle",
    3: "List",
    4: "Set",
    5: "Map",
    6: "CyclicBuffer",
    7: "Queue",
    8: "Any",
}

TS_KIND_NAMES = {
    0: "TS",
    1: "TSS",
    2: "TSD",
    3: "TSL",
    4: "TSW",
    5: "TSB",
    6: "REF",
    7: "SIGNAL",
}

META_CATEGORY_NAMES = {
    0: "Value",
    1: "TimeSeries",
}

STORAGE_STATE_NAMES = {
    0: "empty",
    1: "inline",
    2: "heap",
    3: "borrowed",
}

BINDING_TAG_NAMES = {
    0: "read-only",
    1: "writable",
    2: "mutable",
}

COMPOSITE_KIND_NAMES = {
    0: "None",
    1: "Tuple",
    2: "NamedTuple",
    3: "Array",
}

SCALAR_FORMATS = {
    "bool": ("?", 1),
    "int": ("q", 8),
    "int64": ("q", 8),
    "int32": ("i", 4),
    "int16": ("h", 2),
    "int8": ("b", 1),
    "uint64": ("Q", 8),
    "uint32": ("I", 4),
    "uint16": ("H", 2),
    "uint8": ("B", 1),
    "float": ("d", 8),
    "float64": ("d", 8),
    "double": ("d", 8),
    "float32": ("f", 4),
    "datetime": ("q", 8),
    "timedelta": ("q", 8),
    "time": ("q", 8),
}


class ValuePrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            storage = field(self.value, "storage_")
            binding_addr = ptr_int(field(storage, "m_identity"))
            state = storage_state(storage)
            data_addr = value_storage_data_addr(storage, state)
            if binding_addr == 0:
                return "Value{unbound}"

            schema = schema_from_binding_ptr(binding_addr, "value")
            schema_name = schema_label(schema)
            if state == 0:
                return "Value{{schema={} typed-null}}".format(schema_name)

            payload = scalar_payload_summary(schema, data_addr)
            payload_text = " value={}".format(payload) if payload is not None else ""
            return "Value{{schema={} state={} data={}{} }}".format(
                schema_name, STORAGE_STATE_NAMES.get(state, "state={}".format(state)), format_ptr(data_addr), payload_text
            )
        except Exception as exc:  # pragma: no cover - debugger safety net
            return "Value{<printer error: {}>}".format(exc)

    def children(self):
        try:
            storage = field(self.value, "storage_")
            yield "storage", storage
            binding = binding_from_address(ptr_int(field(storage, "m_identity")), "value")
            if binding is not None:
                yield "binding", binding
                yield from binding_children(binding)
            yield "storage_state", gdb.Value(storage_state(storage))
            yield "data", void_ptr(value_storage_data_addr(storage, storage_state(storage)))
        except Exception:
            return


class ValueViewPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            bits = tagged_bits(field(self.value, "binding_"))
            binding_addr = bits & ~0x3
            tag = bits & 0x3
            data_addr = ptr_int(field(self.value, "data_"))
            if binding_addr == 0:
                return "ValueView{unbound}"

            schema = schema_from_binding_ptr(binding_addr, "value")
            schema_name = schema_label(schema)
            access = BINDING_TAG_NAMES.get(tag, "tag={}".format(tag))
            if data_addr == 0:
                return "ValueView{{schema={} typed-null {}}}".format(schema_name, access)

            payload = scalar_payload_summary(schema, data_addr)
            payload_text = " value={}".format(payload) if payload is not None else ""
            return "ValueView{{schema={} live {} data={}{} }}".format(
                schema_name, access, format_ptr(data_addr), payload_text
            )
        except Exception as exc:  # pragma: no cover
            return "ValueView{<printer error: {}>}".format(exc)

    def children(self):
        try:
            bits = tagged_bits(field(self.value, "binding_"))
            binding = binding_from_address(bits & ~0x3, "value")
            if binding is not None:
                yield "binding", binding
                yield from binding_children(binding)
            yield "binding_tag", gdb.Value(bits & 0x3)
            yield "data", field(self.value, "data_")
        except Exception:
            return


class ValueTypeMetaDataPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            return value_meta_summary(self.value)
        except Exception as exc:  # pragma: no cover
            return "ValueTypeMetaData{<printer error: {}>}".format(exc)

    def children(self):
        try:
            yield from value_meta_children(self.value)
        except Exception:
            return


class TSValueTypeMetaDataPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            return ts_meta_summary(self.value)
        except Exception as exc:  # pragma: no cover
            return "TSValueTypeMetaData{<printer error: {}>}".format(exc)

    def children(self):
        try:
            yield from ts_meta_children(self.value)
        except Exception:
            return


class TypeMetaDataPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            category = enum_name(field(self.value, "category"), META_CATEGORY_NAMES)
            name = c_string(field(self.value, "display_name"))
            suffix = " name={}".format(name) if name else ""
            return "TypeMetaData{{category={}{} }}".format(category, suffix)
        except Exception as exc:  # pragma: no cover
            return "TypeMetaData{<printer error: {}>}".format(exc)

    def children(self):
        try:
            yield "category", field(self.value, "category")
            yield "display_name", field(self.value, "display_name")
        except Exception:
            return


class TypeBindingPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            type_meta = deref(field(self.value, "type_meta"))
            plan = field(self.value, "storage_plan")
            ops = field(self.value, "ops")
            return "TypeBinding{{schema={} plan={} ops={} }}".format(
                schema_label(type_meta), format_ptr(ptr_int(plan)), format_ptr(ptr_int(ops))
            )
        except Exception as exc:  # pragma: no cover
            return "TypeBinding{<printer error: {}>}".format(exc)

    def children(self):
        try:
            yield from binding_children(self.value)
        except Exception:
            return


class StoragePlanPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            layout = field(self.value, "layout")
            size = int_value(field(layout, "size"))
            alignment = int_value(field(layout, "alignment"))
            composite = enum_name(field(self.value, "composite_kind_tag"), COMPOSITE_KIND_NAMES)
            flags = []
            if bool(field(self.value, "trivially_destructible")):
                flags.append("trivial-dtor")
            if bool(field(self.value, "trivially_copyable")):
                flags.append("trivial-copy")
            if bool(field(self.value, "trivially_move_constructible")):
                flags.append("trivial-move")
            flag_text = " flags=[{}]".format(", ".join(flags)) if flags else ""
            return "StoragePlan{{size={} align={} kind={}{} }}".format(size, alignment, composite, flag_text)
        except Exception as exc:  # pragma: no cover
            return "StoragePlan{<printer error: {}>}".format(exc)

    def children(self):
        try:
            yield "layout", field(self.value, "layout")
            yield "lifecycle", field(self.value, "lifecycle")
            yield "lifecycle_context", field(self.value, "lifecycle_context")
            yield "composite_kind_tag", field(self.value, "composite_kind_tag")
            yield "trivially_destructible", field(self.value, "trivially_destructible")
            yield "trivially_copyable", field(self.value, "trivially_copyable")
            yield "trivially_move_constructible", field(self.value, "trivially_move_constructible")
            yield from storage_plan_children(self.value)
        except Exception:
            return


class TSDataViewPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            return ts_data_view_text(self.value)
        except Exception as exc:  # pragma: no cover
            return "TSDataView{<printer error: {}>}".format(exc)

    def children(self):
        try:
            storage = field(self.value, "storage_")
            yield "storage", storage
            yield from ts_data_storage_children(storage)
        except Exception:
            return


class TSInputViewPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            input_ptr = ptr_int(field(self.value, "input_"))
            data_cursor = field(self.value, "data_")
            value_data = field(data_cursor, "value_data")
            raw_data = field(data_cursor, "raw_data")
            target_node = ptr_int(field(data_cursor, "target_node"))
            evaluation_time = chrono_us(field(self.value, "evaluation_time_"))
            return "TSInputView{{input={} value={} raw={} target_node={} evaluation_us={} }}".format(
                format_ptr(input_ptr),
                ts_data_view_text(value_data),
                ts_data_view_text(raw_data),
                format_ptr(target_node),
                evaluation_time,
            )
        except Exception as exc:  # pragma: no cover
            return "TSInputView{<printer error: {}>}".format(exc)

    def children(self):
        try:
            data_cursor = field(self.value, "data_")
            yield "input", field(self.value, "input_")
            yield "data_cursor", data_cursor
            yield "value_data", field(data_cursor, "value_data")
            yield "raw_data", field(data_cursor, "raw_data")
            yield "target_node", field(data_cursor, "target_node")
            yield "scheduling_notifier", field(self.value, "scheduling_notifier_")
            yield "evaluation_time", field(self.value, "evaluation_time_")
        except Exception:
            return


class TSOutputViewPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            output_ptr = ptr_int(field(self.value, "output_"))
            data = field(self.value, "data_")
            evaluation_time = chrono_us(field(self.value, "evaluation_time_"))
            return "TSOutputView{{output={} data={} evaluation_us={} }}".format(
                format_ptr(output_ptr), ts_data_view_text(data), evaluation_time
            )
        except Exception as exc:  # pragma: no cover
            return "TSOutputView{<printer error: {}>}".format(exc)

    def children(self):
        try:
            yield "output", field(self.value, "output_")
            yield "data", field(self.value, "data_")
            yield "evaluation_time", field(self.value, "evaluation_time_")
        except Exception:
            return


def build_pretty_printer():
    printer = gdb.printing.RegexpCollectionPrettyPrinter("hgraph")
    printer.add_printer("hgraph::Value", r"^hgraph::Value$", ValuePrinter)
    printer.add_printer("hgraph::ValueView", r"^hgraph::ValueView$", ValueViewPrinter)
    printer.add_printer("hgraph::ValueTypeMetaData", r"^hgraph::ValueTypeMetaData$", ValueTypeMetaDataPrinter)
    printer.add_printer("hgraph::TSValueTypeMetaData", r"^hgraph::TSValueTypeMetaData$", TSValueTypeMetaDataPrinter)
    printer.add_printer("hgraph::TypeMetaData", r"^hgraph::TypeMetaData$", TypeMetaDataPrinter)
    printer.add_printer("hgraph::TypeBinding", r"^hgraph::TypeBinding<.*>$", TypeBindingPrinter)
    printer.add_printer("hgraph::StoragePlan", r"^hgraph::MemoryUtils::StoragePlan$", StoragePlanPrinter)
    printer.add_printer("hgraph::TSDataView", r"^hgraph::TSDataView$", TSDataViewPrinter)
    printer.add_printer("hgraph::TSInputView", r"^hgraph::TSInputView$", TSInputViewPrinter)
    printer.add_printer("hgraph::TSOutputView", r"^hgraph::TSOutputView$", TSOutputViewPrinter)
    return printer


def register_hgraph_printers(obj=None):
    if obj is None:
        obj = gdb.current_objfile()
    gdb.printing.register_pretty_printer(obj, build_pretty_printer(), replace=True)


def binding_children(binding):
    type_meta = field(binding, "type_meta")
    storage_plan = field(binding, "storage_plan")
    yield "schema", deref(type_meta)
    yield "type_meta", type_meta
    yield "storage_plan", deref(storage_plan)
    yield "storage_plan_ptr", storage_plan
    yield "ops", field(binding, "ops")


def value_meta_children(meta):
    yield "category", field(meta, "category")
    yield "display_name", field(meta, "display_name")
    yield "kind", field(meta, "kind")
    yield "flags", field(meta, "flags")
    element_type = field(meta, "element_type")
    key_type = field(meta, "key_type")
    yield "element_type_ptr", element_type
    yield "key_type_ptr", key_type
    element = deref(element_type)
    key = deref(key_type)
    if element is not None:
        yield "element_type", element
    if key is not None:
        yield "key_type", key
    fields = field(meta, "fields")
    yield "fields", fields
    yield "field_count", field(meta, "field_count")
    yield "fixed_size", field(meta, "fixed_size")
    wrapped_un_named = field(meta, "wrapped_un_named")
    yield "wrapped_un_named_ptr", wrapped_un_named
    wrapped = deref(wrapped_un_named)
    if wrapped is not None:
        yield "wrapped_un_named", wrapped
    yield from pointer_array_children(fields, int_value(field(meta, "field_count")), "field", max_items=32)


def ts_meta_children(meta):
    yield "category", field(meta, "category")
    yield "display_name", field(meta, "display_name")
    yield "kind", field(meta, "kind")
    for name in ("value_type", "value_schema", "delta_value_schema"):
        pointer = field(meta, name)
        yield "{}_ptr".format(name), pointer
        target = deref(pointer)
        if target is not None:
            yield name, target
    data = field(meta, "data")
    yield "data", data
    tsb = field(data, "tsb")
    yield from pointer_array_children(field(tsb, "fields"), int_value(field(tsb, "field_count")), "tsb_field", max_items=32)


def storage_plan_children(plan):
    context = ptr_int(field(plan, "lifecycle_context"))
    if context == 0:
        return

    composite_kind = enum_int(field(plan, "composite_kind_tag"))
    if composite_kind in (1, 2):
        state_type = gdb.lookup_type("hgraph::MemoryUtils::CompositeState")
        state = value_at(context, state_type)
        yield "composite_state", state
        component_type = gdb.lookup_type("hgraph::MemoryUtils::CompositeComponent")
        component_base = context + state_type.sizeof
        component_count = int_value(field(state, "component_count"))
        yield from address_array_children(component_base, component_type, component_count, "component", max_items=32)
    elif composite_kind == 3:
        state_type = gdb.lookup_type("hgraph::MemoryUtils::ArrayState")
        yield "array_state", value_at(context, state_type)


def ts_data_storage_children(storage):
    storage_ref = field(storage, "storage_")
    yield "storage_ref", storage_ref
    binding = binding_from_address(ptr_int(field(storage_ref, "m_binding")), "ts")
    if binding is not None:
        yield "binding", binding
        yield from binding_children(binding)
    yield "data", field(storage_ref, "m_data")
    yield "ops", field(storage, "ops_")


def binding_from_address(address, binding_kind):
    if address == 0:
        return None
    binding_type = value_binding_type() if binding_kind == "value" else ts_binding_type()
    return value_at(address, binding_type)


def value_at(address, value_type):
    if address == 0:
        return None
    return gdb.Value(address).cast(value_type.pointer()).dereference()


def pointer_array_children(pointer, count, prefix, max_items):
    if ptr_int(pointer) == 0 or count <= 0:
        return
    limit = min(count, max_items)
    for index in range(limit):
        yield "{}[{}]".format(prefix, index), (pointer + index).dereference()


def address_array_children(base, element_type, count, prefix, max_items):
    if base == 0 or count <= 0:
        return
    pointer = gdb.Value(base).cast(element_type.pointer())
    yield from pointer_array_children(pointer, count, prefix, max_items)


def void_ptr(address):
    return gdb.Value(address).cast(gdb.lookup_type("void").pointer())


def ts_data_view_text(value):
    storage = field(value, "storage_")
    storage_ref = field(storage, "storage_")
    binding_addr = ptr_int(field(storage_ref, "m_binding"))
    data_addr = ptr_int(field(storage_ref, "m_data"))
    if binding_addr == 0:
        return "TSDataView{unbound}"
    schema = schema_from_binding_ptr(binding_addr, "ts")
    state = "live" if data_addr else "typed-null"
    return "TSDataView{{schema={} {} data={} }}".format(schema_label(schema), state, format_ptr(data_addr))


def value_storage_data_addr(storage, state):
    if state == 1:
        return int(field(field(storage, "m_storage"), "inline_bytes").address)
    if state == 2 or state == 3:
        return ptr_int(field(field(storage, "m_storage"), "ptr"))
    return 0


def storage_state(storage):
    return tagged_bits(field(storage, "m_allocator_state")) & 0x3


def schema_from_binding_ptr(address, binding_kind):
    binding_type = value_binding_type() if binding_kind == "value" else ts_binding_type()
    binding = gdb.Value(address).cast(binding_type.pointer()).dereference()
    return deref(field(binding, "type_meta"))


def value_binding_type():
    return lookup_first_type(
        (
            "hgraph::ValueTypeBinding",
            "hgraph::TypeBinding<hgraph::ValueTypeMetaData, hgraph::ValueOps>",
        )
    )


def ts_binding_type():
    return lookup_first_type(
        (
            "hgraph::TSDataBinding",
            "hgraph::TypeBinding<hgraph::TSValueTypeMetaData, hgraph::TSDataOps>",
        )
    )


def lookup_first_type(type_names):
    errors = []
    for type_name in type_names:
        try:
            return gdb.lookup_type(type_name)
        except gdb.error as exc:
            errors.append(str(exc))
    raise gdb.error("none of these hgraph debug types were available: {}".format(", ".join(type_names)))


def schema_label(meta):
    if meta is None:
        return "<schema?>"
    name = c_string(field(meta, "display_name"))
    if name:
        return name
    category = enum_int(field(meta, "category"))
    if category == 1:
        return ts_meta_short(meta)
    return value_meta_short(meta)


def value_meta_summary(meta):
    name = c_string(field(meta, "display_name"))
    kind = enum_name(field(meta, "kind"), VALUE_KIND_NAMES)
    flags = int_value(field(meta, "flags"))
    parts = ["kind={}".format(kind)]
    if name:
        parts.insert(0, "name={}".format(name))
    element = deref(field(meta, "element_type"))
    key = deref(field(meta, "key_type"))
    if key is not None:
        parts.append("key={}".format(schema_label(key)))
    if element is not None:
        parts.append("element={}".format(schema_label(element)))
    field_count = int_value(field(meta, "field_count"))
    if field_count:
        parts.append("fields={}".format(field_count))
    fixed_size = int_value(field(meta, "fixed_size"))
    if fixed_size:
        parts.append("fixed_size={}".format(fixed_size))
    if flags:
        parts.append("flags=0x{:x}".format(flags))
    return "ValueTypeMetaData{{{} }}".format(" ".join(parts))


def value_meta_short(meta):
    kind = enum_name(field(meta, "kind"), VALUE_KIND_NAMES)
    if kind == "List":
        fixed_size = int_value(field(meta, "fixed_size"))
        suffix = ", {}".format(fixed_size) if fixed_size else ""
        return "List[{}{}]".format(schema_label(deref(field(meta, "element_type"))), suffix)
    if kind == "Set":
        return "Set[{}]".format(schema_label(deref(field(meta, "element_type"))))
    if kind == "Map":
        return "Map[{}, {}]".format(
            schema_label(deref(field(meta, "key_type"))),
            schema_label(deref(field(meta, "element_type"))),
        )
    if kind == "Tuple":
        return "Tuple[{}]".format(int_value(field(meta, "field_count")))
    if kind == "Bundle":
        return "Bundle[{}]".format(int_value(field(meta, "field_count")))
    return kind


def ts_meta_summary(meta):
    name = c_string(field(meta, "display_name"))
    kind = enum_name(field(meta, "kind"), TS_KIND_NAMES)
    parts = ["kind={}".format(kind)]
    if name:
        parts.insert(0, "name={}".format(name))
    value_type = deref(field(meta, "value_type"))
    value_schema = deref(field(meta, "value_schema"))
    delta_schema = deref(field(meta, "delta_value_schema"))
    if value_type is not None:
        parts.append("value_type={}".format(schema_label(value_type)))
    if value_schema is not None:
        parts.append("value={}".format(schema_label(value_schema)))
    if delta_schema is not None:
        parts.append("delta={}".format(schema_label(delta_schema)))
    return "TSValueTypeMetaData{{{} }}".format(" ".join(parts))


def ts_meta_short(meta):
    kind = enum_name(field(meta, "kind"), TS_KIND_NAMES)
    name = c_string(field(meta, "display_name"))
    if name:
        return name
    value_type = deref(field(meta, "value_type"))
    if kind == "TS" and value_type is not None:
        return "TS[{}]".format(schema_label(value_type))
    if kind == "TSS" and value_type is not None:
        return "TSS[{}]".format(schema_label(value_type))
    if kind == "SIGNAL":
        return "SIGNAL"
    return kind


def scalar_payload_summary(schema, data_addr):
    if data_addr == 0 or schema is None:
        return None
    if enum_name(field(schema, "kind"), VALUE_KIND_NAMES) != "Atomic":
        return None
    name = c_string(field(schema, "display_name"))
    if not name:
        return None
    if name in ("str", "string"):
        return std_string_payload(data_addr)
    if name == "bytes":
        return None
    spec = SCALAR_FORMATS.get(name)
    if spec is None:
        return None
    fmt, size = spec
    raw = read_memory(data_addr, size)
    if raw is None or len(raw) != size:
        return None
    value = struct.unpack(endian_prefix() + fmt, raw)[0]
    if name in ("datetime", "timedelta", "time"):
        return "{}us".format(value)
    return repr(value)


def std_string_payload(address):
    try:
        string_type = lookup_first_type(("std::string", "std::__1::string"))
        value = gdb.Value(address).cast(string_type.pointer()).dereference()
        return str(value)
    except Exception:
        return None


def chrono_us(value):
    try:
        raw = read_memory(int(value.address), 8)
        if raw is None or len(raw) != 8:
            return "?"
        return struct.unpack(endian_prefix() + "q", raw)[0]
    except Exception:
        return "?"


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


def deref(value):
    if ptr_int(value) == 0:
        return None
    return value.dereference()


def ptr_int(value):
    try:
        return int(value)
    except Exception:
        return 0


def int_value(value):
    try:
        return int(value)
    except Exception:
        return 0


def tagged_bits(tagged_ptr):
    return int_value(field(tagged_ptr, "m_bits"))


def enum_int(value):
    return int_value(value)


def enum_name(value, names):
    try:
        text = str(value)
        if "::" in text:
            return text.split("::")[-1]
    except Exception:
        pass
    return names.get(enum_int(value), str(enum_int(value)))


def c_string(ptr):
    if ptr_int(ptr) == 0:
        return None
    try:
        return ptr.string()
    except Exception:
        return None


def read_memory(address, size):
    if address == 0:
        return None
    try:
        return bytes(gdb.selected_inferior().read_memory(address, size))
    except Exception:
        return None


def endian_prefix():
    try:
        output = gdb.execute("show endian", to_string=True).lower()
        return ">" if "big endian" in output else "<"
    except Exception:
        return "<"


def format_ptr(address):
    return "0x{:x}".format(address) if address else "null"


register_hgraph_printers()

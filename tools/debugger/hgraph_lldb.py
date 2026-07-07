"""LLDB summaries and synthetic children for hgraph's type-erased runtime data structures.

Load with:

    (lldb) command script import /path/to/hg_cpp/tools/debugger/hgraph_lldb.py

The printers are deliberately read-only: they inspect debug-info fields,
provide shallow navigation through the erased handles, and infer common scalar
payloads from memory, but they do not call methods in the inferior process.
"""

import struct

import lldb


MAX_STRING_BYTES = 256

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


def __lldb_init_module(debugger, _internal_dict):
    summary_registrations = [
        (r"^hgraph::Value$", "hgraph_lldb.value_summary"),
        (r"^hgraph::ValueView$", "hgraph_lldb.value_view_summary"),
        (r"^hgraph::ValueTypeMetaData$", "hgraph_lldb.value_type_meta_data_summary"),
        (r"^hgraph::TSValueTypeMetaData$", "hgraph_lldb.ts_value_type_meta_data_summary"),
        (r"^hgraph::TypeMetaData$", "hgraph_lldb.type_meta_data_summary"),
        (r"^hgraph::TypeBinding<.*>$", "hgraph_lldb.type_binding_summary"),
        (r"^hgraph::MemoryUtils::StoragePlan$", "hgraph_lldb.storage_plan_summary"),
        (r"^hgraph::TSDataView$", "hgraph_lldb.ts_data_view_summary"),
        (r"^hgraph::TSInputView$", "hgraph_lldb.ts_input_view_summary"),
        (r"^hgraph::TSOutputView$", "hgraph_lldb.ts_output_view_summary"),
    ]
    synthetic_registrations = [
        (r"^hgraph::Value$", "hgraph_lldb.ValueSyntheticProvider"),
        (r"^hgraph::ValueView$", "hgraph_lldb.ValueViewSyntheticProvider"),
        (r"^hgraph::ValueTypeMetaData$", "hgraph_lldb.ValueTypeMetaDataSyntheticProvider"),
        (r"^hgraph::TSValueTypeMetaData$", "hgraph_lldb.TSValueTypeMetaDataSyntheticProvider"),
        (r"^hgraph::TypeMetaData$", "hgraph_lldb.TypeMetaDataSyntheticProvider"),
        (r"^hgraph::TypeBinding<.*>$", "hgraph_lldb.TypeBindingSyntheticProvider"),
        (r"^hgraph::MemoryUtils::StoragePlan$", "hgraph_lldb.StoragePlanSyntheticProvider"),
        (r"^hgraph::TSDataView$", "hgraph_lldb.TSDataViewSyntheticProvider"),
        (r"^hgraph::TSInputView$", "hgraph_lldb.TSInputViewSyntheticProvider"),
        (r"^hgraph::TSOutputView$", "hgraph_lldb.TSOutputViewSyntheticProvider"),
    ]
    for pattern, function in summary_registrations:
        debugger.HandleCommand(
            'type summary add --category hgraph --regex "{pattern}" --python-function {function}'.format(
                pattern=pattern, function=function
            )
        )
    for pattern, provider in synthetic_registrations:
        debugger.HandleCommand(
            'type synthetic add --category hgraph --regex "{pattern}" --python-class {provider}'.format(
                pattern=pattern, provider=provider
            )
        )
    debugger.HandleCommand("type category enable hgraph")
    print("hgraph LLDB summaries and synthetic children loaded")


class SyntheticProvider:
    def __init__(self, valobj, _internal_dict):
        self.valobj = valobj
        self.children = []
        self.update()

    def update(self):
        self.children = self.build_children()

    def num_children(self):
        return len(self.children)

    def get_child_at_index(self, index):
        if index < 0 or index >= len(self.children):
            return lldb.SBValue()
        return self.children[index][1]

    def get_child_index(self, name):
        for index, (child_name, value) in enumerate(self.children):
            if name == child_name or name == value.GetName():
                return index
        return -1

    def has_children(self):
        return bool(self.children)

    def build_children(self):
        return []

    def add(self, values, name, value):
        if valid(value):
            values.append((name, value))


class ValueSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        storage = child(self.valobj, "storage_")
        self.add(values, "storage", storage)
        if not valid(storage):
            return values

        binding_addr = ptr_value(child(storage, "m_identity"))
        binding = object_at(self.valobj.GetTarget(), binding_addr, value_binding_type_names(), "binding")
        self.add(values, "binding", binding)
        add_binding_children(values, binding)

        state = storage_state(storage)
        payload = value_storage_data_value(storage, state)
        self.add(values, "payload", payload)
        return values


class ValueViewSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        bits = tagged_bits(child(self.valobj, "binding_"))
        binding_addr = bits & ~0x3
        binding = object_at(self.valobj.GetTarget(), binding_addr, value_binding_type_names(), "binding")
        self.add(values, "binding", binding)
        add_binding_children(values, binding)
        self.add(values, "data", child(self.valobj, "data_"))
        return values


class TypeMetaDataSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        self.add(values, "category", child(self.valobj, "category"))
        self.add(values, "display_name", child(self.valobj, "display_name"))
        return values


class ValueTypeMetaDataSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        add_value_meta_children(values, self.valobj)
        return values


class TSValueTypeMetaDataSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        add_ts_meta_children(values, self.valobj)
        return values


class TypeBindingSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        add_binding_children(values, self.valobj)
        return values


class StoragePlanSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        self.add(values, "layout", child(self.valobj, "layout"))
        self.add(values, "lifecycle", child(self.valobj, "lifecycle"))
        self.add(values, "lifecycle_context", child(self.valobj, "lifecycle_context"))
        self.add(values, "composite_kind_tag", child(self.valobj, "composite_kind_tag"))
        self.add(values, "trivially_destructible", child(self.valobj, "trivially_destructible"))
        self.add(values, "trivially_copyable", child(self.valobj, "trivially_copyable"))
        self.add(values, "trivially_move_constructible", child(self.valobj, "trivially_move_constructible"))
        add_storage_plan_components(values, self.valobj)
        return values


class TSDataViewSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        storage = child(self.valobj, "storage_")
        self.add(values, "storage", storage)
        add_ts_data_storage_children(values, storage)
        return values


class TSInputViewSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        self.add(values, "input", child(self.valobj, "input_"))
        data_cursor = child(self.valobj, "data_")
        self.add(values, "data_cursor", data_cursor)
        self.add(values, "value_data", child(data_cursor, "value_data"))
        self.add(values, "raw_data", child(data_cursor, "raw_data"))
        self.add(values, "target_node", child(data_cursor, "target_node"))
        self.add(values, "scheduling_notifier", child(self.valobj, "scheduling_notifier_"))
        self.add(values, "evaluation_time", child(self.valobj, "evaluation_time_"))
        return values


class TSOutputViewSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        self.add(values, "output", child(self.valobj, "output_"))
        self.add(values, "data", child(self.valobj, "data_"))
        self.add(values, "evaluation_time", child(self.valobj, "evaluation_time_"))
        return values


def value_summary(valobj, _internal_dict, _options):
    try:
        storage = child(valobj, "storage_")
        if not valid(storage):
            return "Value{storage=<unavailable>}"

        identity = child(storage, "m_identity")
        binding_addr = ptr_value(identity)
        state = storage_state(storage)
        data_addr = value_storage_data_addr(storage, state)

        if binding_addr == 0:
            return "Value{unbound}"

        schema = schema_from_binding_ptr(valobj.GetTarget(), binding_addr, "value")
        schema_name = schema_label(schema)
        state_name = STORAGE_STATE_NAMES.get(state, "state={}".format(state))
        if state == 0:
            return "Value{{schema={} typed-null}}".format(schema_name)

        payload = scalar_payload_summary(valobj, schema, data_addr)
        payload_text = " value={}".format(payload) if payload is not None else ""
        return "Value{{schema={} state={} data={}{} }}".format(
            schema_name, state_name, format_ptr(data_addr), payload_text
        )
    except Exception as exc:  # pragma: no cover - debugger safety net
        return "Value{<printer error: {}>}".format(exc)


def value_view_summary(valobj, _internal_dict, _options):
    try:
        binding_tagged = child(valobj, "binding_")
        bits = tagged_bits(binding_tagged)
        binding_addr = bits & ~0x3
        tag = bits & 0x3
        data_addr = ptr_value(child(valobj, "data_"))

        if binding_addr == 0:
            return "ValueView{unbound}"

        schema = schema_from_binding_ptr(valobj.GetTarget(), binding_addr, "value")
        schema_name = schema_label(schema)
        access = BINDING_TAG_NAMES.get(tag, "tag={}".format(tag))
        if data_addr == 0:
            return "ValueView{{schema={} typed-null {}}}".format(schema_name, access)

        payload = scalar_payload_summary(valobj, schema, data_addr)
        payload_text = " value={}".format(payload) if payload is not None else ""
        return "ValueView{{schema={} live {} data={}{} }}".format(
            schema_name, access, format_ptr(data_addr), payload_text
        )
    except Exception as exc:  # pragma: no cover - debugger safety net
        return "ValueView{<printer error: {}>}".format(exc)


def value_type_meta_data_summary(valobj, _internal_dict, _options):
    try:
        return value_meta_summary(valobj)
    except Exception as exc:  # pragma: no cover
        return "ValueTypeMetaData{<printer error: {}>}".format(exc)


def ts_value_type_meta_data_summary(valobj, _internal_dict, _options):
    try:
        return ts_meta_summary(valobj)
    except Exception as exc:  # pragma: no cover
        return "TSValueTypeMetaData{<printer error: {}>}".format(exc)


def type_meta_data_summary(valobj, _internal_dict, _options):
    try:
        category = enum_name(child(valobj, "category"), META_CATEGORY_NAMES)
        name = c_string(child(valobj, "display_name"))
        suffix = " name={}".format(name) if name else ""
        return "TypeMetaData{{category={}{} }}".format(category, suffix)
    except Exception as exc:  # pragma: no cover
        return "TypeMetaData{<printer error: {}>}".format(exc)


def type_binding_summary(valobj, _internal_dict, _options):
    try:
        type_meta = deref(child(valobj, "type_meta"))
        plan = child(valobj, "storage_plan")
        ops = child(valobj, "ops")
        return "TypeBinding{{schema={} plan={} ops={} }}".format(
            schema_label(type_meta), format_ptr(ptr_value(plan)), format_ptr(ptr_value(ops))
        )
    except Exception as exc:  # pragma: no cover
        return "TypeBinding{<printer error: {}>}".format(exc)


def storage_plan_summary(valobj, _internal_dict, _options):
    try:
        layout = child(valobj, "layout")
        size = unsigned(child(layout, "size"))
        alignment = unsigned(child(layout, "alignment"))
        composite = enum_name(child(valobj, "composite_kind_tag"), COMPOSITE_KIND_NAMES)
        flags = []
        if bool_value(child(valobj, "trivially_destructible")):
            flags.append("trivial-dtor")
        if bool_value(child(valobj, "trivially_copyable")):
            flags.append("trivial-copy")
        if bool_value(child(valobj, "trivially_move_constructible")):
            flags.append("trivial-move")
        flag_text = " flags=[{}]".format(", ".join(flags)) if flags else ""
        return "StoragePlan{{size={} align={} kind={}{} }}".format(size, alignment, composite, flag_text)
    except Exception as exc:  # pragma: no cover
        return "StoragePlan{<printer error: {}>}".format(exc)


def ts_data_view_summary(valobj, _internal_dict, _options):
    try:
        return ts_data_view_text(valobj)
    except Exception as exc:  # pragma: no cover
        return "TSDataView{<printer error: {}>}".format(exc)


def ts_input_view_summary(valobj, _internal_dict, _options):
    try:
        input_ptr = ptr_value(child(valobj, "input_"))
        data_cursor = child(valobj, "data_")
        value_data = child(data_cursor, "value_data")
        raw_data = child(data_cursor, "raw_data")
        target_node = ptr_value(child(data_cursor, "target_node"))
        evaluation_time = chrono_us(child(valobj, "evaluation_time_"))
        return (
            "TSInputView{{input={} value={} raw={} target_node={} evaluation_us={} }}".format(
                format_ptr(input_ptr),
                ts_data_view_text(value_data),
                ts_data_view_text(raw_data),
                format_ptr(target_node),
                evaluation_time,
            )
        )
    except Exception as exc:  # pragma: no cover
        return "TSInputView{<printer error: {}>}".format(exc)


def ts_output_view_summary(valobj, _internal_dict, _options):
    try:
        output_ptr = ptr_value(child(valobj, "output_"))
        data = child(valobj, "data_")
        evaluation_time = chrono_us(child(valobj, "evaluation_time_"))
        return "TSOutputView{{output={} data={} evaluation_us={} }}".format(
            format_ptr(output_ptr), ts_data_view_text(data), evaluation_time
        )
    except Exception as exc:  # pragma: no cover
        return "TSOutputView{<printer error: {}>}".format(exc)


def add_child(values, name, value):
    if valid(value):
        values.append((name, value))


def add_binding_children(values, binding):
    if not valid(binding):
        return
    type_meta = child(binding, "type_meta")
    storage_plan = child(binding, "storage_plan")
    add_child(values, "schema", deref_named(type_meta, "schema"))
    add_child(values, "type_meta", type_meta)
    add_child(values, "storage_plan", deref_named(storage_plan, "storage_plan"))
    add_child(values, "storage_plan_ptr", storage_plan)
    add_child(values, "ops", child(binding, "ops"))


def add_value_meta_children(values, meta):
    add_child(values, "category", child(meta, "category"))
    add_child(values, "display_name", child(meta, "display_name"))
    add_child(values, "kind", child(meta, "kind"))
    add_child(values, "flags", child(meta, "flags"))
    add_child(values, "element_type", deref_named(child(meta, "element_type"), "element_type"))
    add_child(values, "key_type", deref_named(child(meta, "key_type"), "key_type"))
    add_child(values, "fields", child(meta, "fields"))
    add_child(values, "field_count", child(meta, "field_count"))
    add_child(values, "fixed_size", child(meta, "fixed_size"))
    add_child(values, "wrapped_un_named", deref_named(child(meta, "wrapped_un_named"), "wrapped_un_named"))
    add_pointer_array_children(
        values,
        child(meta, "fields"),
        unsigned(child(meta, "field_count")),
        "field",
        max_items=32,
    )


def add_ts_meta_children(values, meta):
    add_child(values, "category", child(meta, "category"))
    add_child(values, "display_name", child(meta, "display_name"))
    add_child(values, "kind", child(meta, "kind"))
    add_child(values, "value_type", deref_named(child(meta, "value_type"), "value_type"))
    add_child(values, "value_schema", deref_named(child(meta, "value_schema"), "value_schema"))
    add_child(values, "delta_value_schema", deref_named(child(meta, "delta_value_schema"), "delta_value_schema"))
    data = child(meta, "data")
    add_child(values, "data", data)
    tsb = child(data, "tsb")
    add_pointer_array_children(
        values,
        child(tsb, "fields"),
        unsigned(child(tsb, "field_count")),
        "tsb_field",
        max_items=32,
    )


def add_storage_plan_components(values, plan):
    context = ptr_value(child(plan, "lifecycle_context"))
    if context == 0:
        return

    target = plan.GetTarget()
    composite_kind = enum_value(child(plan, "composite_kind_tag"))
    if composite_kind in (1, 2):
        state = object_at(target, context, ["hgraph::MemoryUtils::CompositeState"], "composite_state")
        add_child(values, "composite_state", state)
        component_count = unsigned(child(state, "component_count"))
        component_type = target.FindFirstType("hgraph::MemoryUtils::CompositeComponent")
        if component_type.IsValid():
            add_address_array_children(
                values,
                target,
                context + state.GetType().GetByteSize(),
                component_type,
                component_count,
                "component",
                max_items=32,
            )
    elif composite_kind == 3:
        state = object_at(target, context, ["hgraph::MemoryUtils::ArrayState"], "array_state")
        add_child(values, "array_state", state)


def add_ts_data_storage_children(values, storage):
    storage_ref = child(storage, "storage_")
    add_child(values, "storage_ref", storage_ref)
    binding_addr = ptr_value(child(storage_ref, "m_binding"))
    binding = object_at(storage.GetTarget(), binding_addr, ts_binding_type_names(), "binding")
    add_child(values, "binding", binding)
    add_binding_children(values, binding)
    add_child(values, "data", child(storage_ref, "m_data"))
    add_child(values, "ops", child(storage, "ops_"))


def add_pointer_array_children(values, pointer, count, prefix, max_items):
    base = ptr_value(pointer)
    if base == 0 or count == 0:
        return
    element_type = pointer.GetType().GetPointeeType() if valid(pointer) else lldb.SBType()
    if not element_type.IsValid():
        return
    add_address_array_children(values, pointer.GetTarget(), base, element_type, count, prefix, max_items)


def add_address_array_children(values, target, base, element_type, count, prefix, max_items):
    if base == 0 or count == 0 or not element_type.IsValid():
        return
    stride = element_type.GetByteSize()
    limit = min(count, max_items)
    for index in range(limit):
        value = target.CreateValueFromAddress(
            "{}[{}]".format(prefix, index),
            lldb.SBAddress(base + index * stride, target),
            element_type,
        )
        add_child(values, "{}[{}]".format(prefix, index), value)


def value_binding_type_names():
    return [
        "hgraph::ValueTypeBinding",
        "hgraph::TypeBinding<hgraph::ValueTypeMetaData, hgraph::ValueOps>",
    ]


def ts_binding_type_names():
    return [
        "hgraph::TSDataBinding",
        "hgraph::TypeBinding<hgraph::TSValueTypeMetaData, hgraph::TSDataOps>",
    ]


def value_storage_data_value(storage, state):
    storage_union = child(storage, "m_storage")
    if state == 1:
        return child(storage_union, "inline_bytes")
    if state == 2 or state == 3:
        return child(storage_union, "ptr")
    return lldb.SBValue()


def ts_data_view_text(valobj):
    storage = child(valobj, "storage_")
    storage_ref = child(storage, "storage_")
    binding_addr = ptr_value(child(storage_ref, "m_binding"))
    data_addr = ptr_value(child(storage_ref, "m_data"))
    if binding_addr == 0:
        return "TSDataView{unbound}"
    schema = schema_from_binding_ptr(valobj.GetTarget(), binding_addr, "ts")
    state = "live" if data_addr else "typed-null"
    return "TSDataView{{schema={} {} data={} }}".format(schema_label(schema), state, format_ptr(data_addr))


def value_storage_data_addr(storage, state):
    if state == 1:
        inline_bytes = child(child(storage, "m_storage"), "inline_bytes")
        return address_of(inline_bytes)
    if state == 2 or state == 3:
        return ptr_value(child(child(storage, "m_storage"), "ptr"))
    return 0


def storage_state(storage):
    allocator_state = child(storage, "m_allocator_state")
    return tagged_bits(allocator_state) & 0x3


def schema_from_binding_ptr(target, address, binding_kind):
    binding = object_at(
        target,
        address,
        value_binding_type_names() if binding_kind == "value" else ts_binding_type_names(),
    )
    if not valid(binding):
        return lldb.SBValue()
    return deref(child(binding, "type_meta"))


def schema_label(meta):
    if not valid(meta):
        return "<schema?>"
    name = c_string(child(meta, "display_name"))
    if name:
        return name
    category = enum_value(child(meta, "category"))
    if category == 1:
        return ts_meta_short(meta)
    return value_meta_short(meta)


def value_meta_summary(meta):
    name = c_string(child(meta, "display_name"))
    kind = enum_name(child(meta, "kind"), VALUE_KIND_NAMES)
    flags = unsigned(child(meta, "flags"))
    parts = ["kind={}".format(kind)]
    if name:
        parts.insert(0, "name={}".format(name))
    element = deref(child(meta, "element_type"))
    key = deref(child(meta, "key_type"))
    if valid(key):
        parts.append("key={}".format(schema_label(key)))
    if valid(element):
        parts.append("element={}".format(schema_label(element)))
    field_count = unsigned(child(meta, "field_count"))
    if field_count:
        parts.append("fields={}".format(field_count))
    fixed_size = unsigned(child(meta, "fixed_size"))
    if fixed_size:
        parts.append("fixed_size={}".format(fixed_size))
    if flags:
        parts.append("flags=0x{:x}".format(flags))
    return "ValueTypeMetaData{{{} }}".format(" ".join(parts))


def value_meta_short(meta):
    kind = enum_name(child(meta, "kind"), VALUE_KIND_NAMES)
    if kind == "List":
        element = deref(child(meta, "element_type"))
        fixed_size = unsigned(child(meta, "fixed_size"))
        suffix = ", {}".format(fixed_size) if fixed_size else ""
        return "List[{}{}]".format(schema_label(element), suffix)
    if kind == "Set":
        return "Set[{}]".format(schema_label(deref(child(meta, "element_type"))))
    if kind == "Map":
        return "Map[{}, {}]".format(
            schema_label(deref(child(meta, "key_type"))),
            schema_label(deref(child(meta, "element_type"))),
        )
    if kind == "Tuple":
        return "Tuple[{}]".format(unsigned(child(meta, "field_count")))
    if kind == "Bundle":
        return "Bundle[{}]".format(unsigned(child(meta, "field_count")))
    return kind


def ts_meta_summary(meta):
    name = c_string(child(meta, "display_name"))
    kind = enum_name(child(meta, "kind"), TS_KIND_NAMES)
    parts = ["kind={}".format(kind)]
    if name:
        parts.insert(0, "name={}".format(name))
    value_type = deref(child(meta, "value_type"))
    value_schema = deref(child(meta, "value_schema"))
    delta_schema = deref(child(meta, "delta_value_schema"))
    if valid(value_type):
        parts.append("value_type={}".format(schema_label(value_type)))
    if valid(value_schema):
        parts.append("value={}".format(schema_label(value_schema)))
    if valid(delta_schema):
        parts.append("delta={}".format(schema_label(delta_schema)))
    return "TSValueTypeMetaData{{{} }}".format(" ".join(parts))


def ts_meta_short(meta):
    kind = enum_name(child(meta, "kind"), TS_KIND_NAMES)
    name = c_string(child(meta, "display_name"))
    if name:
        return name
    value_type = deref(child(meta, "value_type"))
    if kind == "TS" and valid(value_type):
        return "TS[{}]".format(schema_label(value_type))
    if kind == "TSS" and valid(value_type):
        return "TSS[{}]".format(schema_label(value_type))
    if kind == "SIGNAL":
        return "SIGNAL"
    return kind


def scalar_payload_summary(anchor, schema, data_addr):
    if data_addr == 0 or not valid(schema):
        return None
    if enum_name(child(schema, "kind"), VALUE_KIND_NAMES) != "Atomic":
        return None
    name = c_string(child(schema, "display_name"))
    if not name:
        return None
    if name in ("str", "string"):
        return std_string_payload(anchor, data_addr)
    if name == "bytes":
        return None
    spec = SCALAR_FORMATS.get(name)
    if spec is None:
        return None
    fmt, size = spec
    raw = read_memory(anchor, data_addr, size)
    if raw is None or len(raw) != size:
        return None
    endian = "<" if little_endian(anchor) else ">"
    value = struct.unpack(endian + fmt, raw)[0]
    if name in ("datetime", "timedelta", "time"):
        return "{}us".format(value)
    return repr(value)


def std_string_payload(anchor, address):
    target = anchor.GetTarget()
    for type_name in ("std::string", "std::__1::string"):
        string_type = target.FindFirstType(type_name)
        if string_type.IsValid():
            value = target.CreateValueFromAddress(
                "hgraph_debug_string", lldb.SBAddress(address, target), string_type
            )
            summary = value.GetSummary()
            if summary:
                return summary
    return None


def chrono_us(valobj):
    address = address_of(valobj)
    if address == 0:
        return "?"
    raw = read_memory(valobj, address, 8)
    if raw is None or len(raw) != 8:
        return "?"
    return struct.unpack(("<" if little_endian(valobj) else ">") + "q", raw)[0]


def child(valobj, name):
    if not valid(valobj):
        return lldb.SBValue()
    result = valobj.GetChildMemberWithName(name)
    if result.IsValid():
        return result
    for index in range(valobj.GetNumChildren()):
        candidate = valobj.GetChildAtIndex(index)
        if candidate.GetName() == name:
            return candidate
        nested = candidate.GetChildMemberWithName(name)
        if nested.IsValid():
            return nested
    return lldb.SBValue()


def valid(valobj):
    return valobj is not None and valobj.IsValid()


def deref(ptr):
    if not valid(ptr) or ptr_value(ptr) == 0:
        return lldb.SBValue()
    result = ptr.Dereference()
    return result if result.IsValid() else lldb.SBValue()


def unsigned(valobj, default=0):
    if not valid(valobj):
        return default
    return valobj.GetValueAsUnsigned(default)


def bool_value(valobj):
    return unsigned(valobj, 0) != 0


def ptr_value(valobj):
    return unsigned(valobj, 0)


def tagged_bits(tagged_ptr):
    return unsigned(child(tagged_ptr, "m_bits"), 0)


def enum_value(valobj):
    return unsigned(valobj, 0)


def enum_name(valobj, names):
    raw = valobj.GetValue() if valid(valobj) else None
    if raw:
        return raw.split("::")[-1]
    return names.get(enum_value(valobj), str(enum_value(valobj)))


def c_string(ptr):
    address = ptr_value(ptr)
    if address == 0:
        return None
    process = ptr.GetProcess()
    error = lldb.SBError()
    result = process.ReadCStringFromMemory(address, MAX_STRING_BYTES, error)
    return result if error.Success() and result else None


def deref_named(ptr, name):
    if not valid(ptr):
        return lldb.SBValue()
    address = ptr_value(ptr)
    if address == 0:
        return lldb.SBValue()
    pointee_type = ptr.GetType().GetPointeeType()
    if not pointee_type.IsValid():
        return lldb.SBValue()
    target = ptr.GetTarget()
    return target.CreateValueFromAddress(name, lldb.SBAddress(address, target), pointee_type)


def object_at(target, address, type_names, name="hgraph_debug_object"):
    if address == 0:
        return lldb.SBValue()
    for type_name in type_names:
        target_type = target.FindFirstType(type_name)
        if target_type.IsValid():
            return target.CreateValueFromAddress(
                name, lldb.SBAddress(address, target), target_type
            )
    return lldb.SBValue()


def address_of(valobj):
    if not valid(valobj):
        return 0
    address = valobj.GetLoadAddress()
    if address != lldb.LLDB_INVALID_ADDRESS:
        return address
    pointer = valobj.AddressOf()
    return pointer.GetValueAsUnsigned(0) if pointer.IsValid() else 0


def read_memory(anchor, address, size):
    process = anchor.GetProcess()
    if process is None or address == 0:
        return None
    error = lldb.SBError()
    data = process.ReadMemory(address, size, error)
    return data if error.Success() else None


def little_endian(anchor):
    try:
        return anchor.GetTarget().GetByteOrder() == lldb.eByteOrderLittle
    except Exception:
        return True


def format_ptr(address):
    return "0x{:x}".format(address) if address else "null"

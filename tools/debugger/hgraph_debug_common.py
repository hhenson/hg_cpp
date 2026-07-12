"""Debugger-independent formatting for hgraph's common type-erasure ABI."""

SCHEMA_HEADER_MAGIC = 0x48475348
TYPE_RECORD_MAGIC = 0x48475452
SCHEMA_HEADER_ABI_VERSION = 1
TYPE_RECORD_ABI_VERSION = 1
TYPE_KIND_NONE = 0xFF

FAMILY_NAMES = {
    0: "Invalid",
    1: "Value",
    2: "TimeSeries",
    3: "Node",
    4: "Graph",
    5: "Executor",
    6: "Clock",
}

ROLE_NAMES = {
    0: "Invalid",
    1: "Instance",
    2: "Data",
    3: "Runtime",
    4: "Input",
    5: "Output",
}

ACCESS_NAMES = {
    0: "read-only",
    1: "writable",
    2: "mutation",
}

CAPABILITY_NAMES = (
    (1 << 0, "constructible"),
    (1 << 1, "destructible"),
    (1 << 2, "copyable"),
    (1 << 3, "movable"),
    (1 << 4, "mutable"),
    (1 << 5, "equatable"),
    (1 << 6, "comparable"),
    (1 << 7, "hashable"),
    (1 << 8, "children"),
    (1 << 9, "viewable"),
)
KNOWN_CAPABILITIES = sum(bit for bit, _ in CAPABILITY_NAMES)

KIND_NAMES = {
    1: {
        0: "Atomic",
        1: "Tuple",
        2: "Bundle",
        3: "List",
        4: "Set",
        5: "Map",
        6: "CyclicBuffer",
        7: "Queue",
        8: "Any",
    },
    2: {
        0: "TS",
        1: "TSS",
        2: "TSD",
        3: "TSL",
        4: "TSW",
        5: "TSB",
        6: "REF",
        7: "SIGNAL",
    },
    3: {
        0: "Compute",
        1: "PushSource",
        2: "PullSource",
        3: "Sink",
        4: "Nested",
    },
    5: {
        0: "Simulation",
        1: "RealTime",
    },
}

ALLOWED_ROLES = {
    1: (1,),
    2: (2, 4, 5),
    3: (3,),
    4: (3,),
    5: (3,),
    6: (3,),
}


def pointer_text(address):
    return "0x{:x}".format(address) if address else "null"


def family_text(family):
    return FAMILY_NAMES.get(family, "unknown({})".format(family))


def role_text(role):
    return ROLE_NAMES.get(role, "unknown({})".format(role))


def kind_text(family, kind):
    if kind == TYPE_KIND_NONE:
        return "none"
    name = KIND_NAMES.get(family, {}).get(kind)
    return "{}({})".format(name, kind) if name is not None else str(kind)


def access_text(access):
    return ACCESS_NAMES.get(access, "invalid({})".format(access))


def capabilities_text(capabilities):
    names = [name for bit, name in CAPABILITY_NAMES if capabilities & bit]
    unknown = capabilities & ~KNOWN_CAPABILITIES
    if unknown:
        names.append("unknown=0x{:x}".format(unknown))
    return "|".join(names) if names else "none"


def schema_valid(snapshot):
    return (
        snapshot.get("magic") == SCHEMA_HEADER_MAGIC
        and snapshot.get("abi_version") == SCHEMA_HEADER_ABI_VERSION
        and snapshot.get("family") in ALLOWED_ROLES
        and bool(snapshot.get("label"))
    )


def record_valid(snapshot):
    schema = snapshot.get("schema")
    family = schema.get("family") if schema else 0
    role = snapshot.get("role", 0)
    implementation = snapshot.get("implementation_label")
    return (
        snapshot.get("magic") == TYPE_RECORD_MAGIC
        and snapshot.get("abi_version") == TYPE_RECORD_ABI_VERSION
        and snapshot.get("reserved0") == 0
        and snapshot.get("reserved1") == 0
        and schema is not None
        and schema_valid(schema)
        and role in ALLOWED_ROLES.get(family, ())
        and snapshot.get("ops_abi_version", 0) != 0
        and (snapshot.get("capabilities", 0) & ~KNOWN_CAPABILITIES) == 0
        and snapshot.get("plan", 0) != 0
        and snapshot.get("ops", 0) != 0
        and implementation != ""
    )


def schema_summary(snapshot):
    state = "valid" if schema_valid(snapshot) else "invalid"
    return 'SchemaHeader{{{} family={} kind={} abi={} label="{}"}}'.format(
        state,
        family_text(snapshot.get("family", 0)),
        kind_text(snapshot.get("family", 0), snapshot.get("kind", TYPE_KIND_NONE)),
        snapshot.get("abi_version", 0),
        snapshot.get("label") or "",
    )


def record_summary(snapshot):
    schema = snapshot.get("schema") or {}
    state = "valid" if record_valid(snapshot) else "invalid"
    implementation = snapshot.get("implementation_label") or "<semantic>"
    return (
        'TypeRecord{{{} {}/{} kind={} semantic="{}" implementation="{}" '
        "abi={}/{} capabilities={} plan={} ops={} debug={}}}"
    ).format(
        state,
        family_text(schema.get("family", 0)),
        role_text(snapshot.get("role", 0)),
        kind_text(schema.get("family", 0), schema.get("kind", TYPE_KIND_NONE)),
        schema.get("label") or "",
        implementation,
        snapshot.get("abi_version", 0),
        snapshot.get("ops_abi_version", 0),
        capabilities_text(snapshot.get("capabilities", 0)),
        pointer_text(snapshot.get("plan", 0)),
        pointer_text(snapshot.get("ops", 0)),
        pointer_text(snapshot.get("debug", 0)),
    )


def pointer_summary(type_name, record_address, data_address, access, record=None):
    if record_address == 0 and data_address == 0:
        return "{}{{unbound}}".format(type_name)
    if record_address == 0 or access not in ACCESS_NAMES:
        return "{}{{malformed record={} data={} access={}}}".format(
            type_name, pointer_text(record_address), pointer_text(data_address), access_text(access)
        )

    state = "typed-null" if data_address == 0 else "live"
    if record is None:
        classification = "unknown"
        semantic = ""
        implementation = ""
    else:
        schema = record.get("schema") or {}
        classification = "{}{}/{} kind={}".format(
            "invalid-record " if not record_valid(record) else "",
            family_text(schema.get("family", 0)),
            role_text(record.get("role", 0)),
            kind_text(schema.get("family", 0), schema.get("kind", TYPE_KIND_NONE)),
        )
        semantic = schema.get("label") or ""
        implementation = record.get("implementation_label") or "<semantic>"
    return (
        '{}{{{} {} access={} semantic="{}" implementation="{}" record={} data={}}}'
    ).format(
        type_name,
        state,
        classification,
        access_text(access),
        semantic,
        implementation,
        pointer_text(record_address),
        pointer_text(data_address),
    )

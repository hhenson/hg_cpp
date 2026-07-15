"""hgraph.adaptors.data_frame parity surface. Frame operators resolve
through the C++ registry; names not implemented yet raise at USE with a
"gap:" message (the hgraph.stream precedent)."""


def group_by(ts, keys):
    """Partition a Frame-valued TS into TSD[key, TS[Frame]] by column(s)."""
    from .._wiring import operator_function

    return operator_function("group_by")(ts, keys)

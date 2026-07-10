"""hgraph.stream.stream - the stream-status library surface. The upstream
module builds on Base[COMPOUND_SCALAR] generic compound scalars, which the
bridge does not support yet; the names are importable (the ported suite
imports them at module load) and raise at USE with a "gap:" message."""


def combine_status_messages(*args, **kwargs):
    raise NotImplementedError(
        "gap: hgraph.stream (Base[COMPOUND_SCALAR] generics) is not bridged yet")


def register_status_message_pattern(*args, **kwargs):
    raise NotImplementedError(
        "gap: hgraph.stream (Base[COMPOUND_SCALAR] generics) is not bridged yet")

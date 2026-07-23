"""Field-wise hgraph metadata encoded in an Arrow table schema."""

from __future__ import annotations

from . import _hgraph
from ._types import _value_type


def with_frame_metadata(table, metadata):
    """Return an Arrow table carrying ``metadata`` in its schema metadata."""
    return _hgraph._with_frame_metadata(table, _value_type(type(metadata)), metadata)


def frame_metadata(table, metadata_type=None):
    """Decode frame metadata, using its optional type marker when no type is supplied."""
    if metadata_type is None:
        return _hgraph._frame_metadata_reflective(table)
    return _hgraph._frame_metadata(table, _value_type(metadata_type))


def has_frame_metadata(table):
    """Return whether ``table`` carries reserved hgraph metadata entries."""
    return _hgraph._has_frame_metadata(table)


def without_frame_metadata(table):
    """Return an Arrow table with only the reserved hgraph metadata removed."""
    return _hgraph._without_frame_metadata(table)


__all__ = [
    "with_frame_metadata",
    "frame_metadata",
    "has_frame_metadata",
    "without_frame_metadata",
]

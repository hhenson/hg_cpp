from hgraph import TS, WiringPort, const

from ._perspective import PerspectiveTablesManager, perspective_web as _perspective_web_node
from ._perspective_publish import _publish_table, _receive_table_edits

__all__ = (
    "publish_table",
    "publish_table_editable",
    "publish_multitable",
    "publish_table_impl",
    "publish_table_editable_impl",
    "publish_multitable_impl",
    "register_perspective_adaptors",
    "perspective_web",
)


def publish_table(path, ts, index_col_name, history=None):
    manager = PerspectiveTablesManager.current()
    _publish_table(
        ts,
        name=path,
        index_col_name=index_col_name,
        editable=False,
        edit_role=None,
        history=history,
        manager=manager,
    )


def publish_table_editable(
    path,
    ts,
    index_col_name,
    history=None,
    edit_role=None,
    empty_row=False,
):
    if empty_row:
        raise NotImplementedError("Perspective empty-row creation is not supported")
    manager = PerspectiveTablesManager.current()
    _publish_table(
        ts,
        name=path,
        index_col_name=index_col_name,
        editable=True,
        edit_role=edit_role,
        history=history,
        manager=manager,
    )
    return _receive_table_edits(path, ts, index_col_name, manager)


def publish_multitable(path, key, ts, unique, index_col_name, history=None):
    raise NotImplementedError(
        "Perspective multi-client reduction is not exposed; publish a native TSD with publish_table"
    )


def perspective_web(host="localhost", port=8080, _sig=True, **_):
    manager = PerspectiveTablesManager.current()
    signal = _sig if isinstance(_sig, WiringPort) else const(_sig, tp=TS[bool])
    return _perspective_web_node(host=host, port=port, _sig=signal, manager=manager)


def register_perspective_adaptors():
    """Compatibility no-op; publication wires directly to the native graph."""


publish_table_impl = publish_table
publish_table_editable_impl = publish_table_editable
publish_multitable_impl = publish_multitable

import threading

import pyarrow as pa

from hgraph import GlobalState, TS, sink_node

__all__ = (
    "PerspectiveTablesManager",
    "perspective_web",
)


class PerspectiveTablesManager:
    _STATE_KEY = ":adaptors:perspective:manager"

    def __init__(self, client=None, *, host_server_tables=True, **options):
        self._client = client
        self._server = None
        self._host_server_tables = host_server_tables
        self.options = options
        self._tables = {}
        self._table_options = {}
        self._subscribers = {}
        self._views = {}
        self._web = None
        self._lock = threading.RLock()

    @classmethod
    def current(cls, global_state=None):
        state = global_state or GlobalState.instance()
        manager = state.get(cls._STATE_KEY)
        if manager is None:
            manager = cls()
            state[cls._STATE_KEY] = manager
        return manager

    @classmethod
    def set_current(cls, manager, global_state=None):
        state = global_state or GlobalState.instance()
        if cls._STATE_KEY in state:
            raise ValueError("a PerspectiveTablesManager is already configured")
        state[cls._STATE_KEY] = manager

    @property
    def server_tables(self):
        return self._host_server_tables

    def _ensure_client(self):
        if self._client is not None:
            return self._client
        try:
            import perspective
        except ModuleNotFoundError as error:
            raise RuntimeError("Perspective adaptors require the 'perspective' extra") from error
        self._server = perspective.Server()
        new_local_client = getattr(self._server, "new_local_client", None)
        if new_local_client is not None:
            self._client = new_local_client()
        else:
            self._client = perspective.Client.from_server(self._server)
        return self._client

    def create_table(
        self,
        data,
        *,
        name,
        index=None,
        editable=False,
        edit_role=None,
        temporary=False,
        **options,
    ):
        with self._lock:
            if name in self._tables:
                raise ValueError(f"Perspective table {name!r} already exists")
            client = self._ensure_client()
            kwargs = dict(options)
            if index is not None:
                kwargs["index"] = index
            try:
                table = client.table(data, name=name, **kwargs)
            except TypeError:
                table = client.table(data, **kwargs)
            self._tables[name] = table
            self._table_options[name] = {
                "editable": editable,
                "edit_role": edit_role,
                "temporary": temporary,
                "index": index,
            }
            self._attach_subscribers(name)
            return table

    def add_table(self, name, table, *, editable=False, edit_role=None, **options):
        with self._lock:
            if name in self._tables:
                raise ValueError(f"Perspective table {name!r} already exists")
            self._tables[name] = table
            self._table_options[name] = {
                "editable": editable,
                "edit_role": edit_role,
                **options,
            }
            self._attach_subscribers(name)

    def update_table(
        self,
        name,
        data,
        removals=None,
        *,
        index=None,
        editable=False,
        edit_role=None,
    ):
        rows = list(data or ())
        with self._lock:
            table = self._tables.get(name)
            if table is None:
                if not rows:
                    return
                table = self.create_table(
                    rows,
                    name=name,
                    index=index,
                    editable=editable,
                    edit_role=edit_role,
                )
                rows = []
            if rows:
                table.update(rows)
            if removals:
                remove = getattr(table, "remove", None)
                if remove is not None:
                    remove(list(removals))

    def replace_table(self, name, data):
        with self._lock:
            table = self._tables[name]
            replace = getattr(table, "replace", None)
            if replace is None:
                raise TypeError("the configured Perspective table does not support replace")
            replace(data)

    def get_table_names(self):
        with self._lock:
            return list(self._tables)

    def get_table(self, name):
        with self._lock:
            return self._tables.get(name)

    def is_table_editable(self, name):
        return bool(self._table_options.get(name, {}).get("editable"))

    def subscribe_table_updates(self, name, callback, self_updates=False):
        token = object()
        with self._lock:
            self._subscribers.setdefault(name, {})[token] = callback
            self._attach_subscribers(name)
        return token

    def unsubscribe_table_updates(self, name, token):
        with self._lock:
            subscribers = self._subscribers.get(name)
            if subscribers is not None:
                subscribers.pop(token, None)

    def publish_edits(self, name, updates=(), removals=()):
        """Feed edits into graph subscribers; also useful for injected clients."""
        with self._lock:
            callbacks = tuple(self._subscribers.get(name, {}).values())
        for callback in callbacks:
            callback(list(updates), list(removals))

    def _attach_subscribers(self, name):
        if name in self._views or name not in self._tables or name not in self._subscribers:
            return
        table = self._tables[name]
        view_factory = getattr(table, "view", None)
        if view_factory is None:
            return
        view = view_factory()
        on_update = getattr(view, "on_update", None)
        if on_update is None:
            return

        def updated(*args):
            delta = args[-1] if args else None
            rows = _decode_update(delta)
            self.publish_edits(name, rows, ())

        on_update(updated, mode="row")
        self._views[name] = view

    def start_web(self, host, port):
        self._ensure_client()
        if self._server is None:
            raise RuntimeError("an injected Perspective client must provide its own web transport")
        try:
            from perspective.handlers.tornado import PerspectiveTornadoHandler
        except (ImportError, ModuleNotFoundError) as error:
            raise RuntimeError("Perspective web hosting requires the 'perspective' and 'web' extras") from error
        from hgraph.adaptors.tornado._tornado_web import TornadoWeb

        web = TornadoWeb.instance(port)
        web.add_handler(
            r"/perspective",
            PerspectiveTornadoHandler,
            {"perspective_server": self._server},
        )
        web.start()
        self._web = web

    def stop_web(self):
        if self._web is not None:
            self._web.stop()
            self._web = None

    def close(self):
        self.stop_web()
        with self._lock:
            views = tuple(self._views.values())
            self._views.clear()
        for view in views:
            delete = getattr(view, "delete", None)
            if delete is not None:
                delete()


def _decode_update(delta):
    if delta is None:
        return []
    if isinstance(delta, list):
        return delta
    if isinstance(delta, dict):
        return [delta]
    if isinstance(delta, (bytes, bytearray, memoryview, pa.Buffer)):
        return pa.ipc.open_stream(delta).read_all().to_pylist()
    to_pylist = getattr(delta, "to_pylist", None)
    if to_pylist is not None:
        return to_pylist()
    raise TypeError(f"unsupported Perspective update payload {type(delta)!r}")


@sink_node
def perspective_web(
    host: str,
    port: int,
    _sig: TS[bool],
    manager: object,
):
    if _sig.value:
        manager.start_web(host, port)


@perspective_web.stop
def _stop_perspective_web(manager: object):
    manager.stop_web()

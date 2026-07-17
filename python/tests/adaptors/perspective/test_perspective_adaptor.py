from dataclasses import dataclass

import hgraph as hg
from hgraph.adaptors.perspective import PerspectiveTablesManager, publish_table


@dataclass(frozen=True)
class _Row(hg.CompoundScalar):
    name: str
    value: int


class _Table:
    def __init__(self, rows):
        self.updates = [list(rows)]
        self.removals = []

    def update(self, rows):
        self.updates.append(list(rows))

    def remove(self, keys):
        self.removals.append(list(keys))


class _Client:
    def __init__(self):
        self.tables = {}

    def table(self, rows, *, name, **kwargs):
        table = _Table(rows)
        self.tables[name] = (table, kwargs)
        return table


def test_publish_table_applies_tsd_add_modify_and_remove_deltas_with_eval_node():
    client = _Client()
    manager = PerspectiveTablesManager(client)

    @hg.graph
    def app(rows: hg.TSD[int, hg.TS[_Row]]):
        publish_table("rows", rows, index_col_name="id")

    with hg.GlobalContext(hg.GlobalState()):
        PerspectiveTablesManager.set_current(manager)
        assert hg.eval_node(
            app,
            [
                {1: _Row("a", 1)},
                {1: _Row("b", 2), 2: _Row("c", 3)},
                {1: hg.REMOVE},
            ],
        ) is None

    table, options = client.tables["rows"]
    assert options["index"] == "id"
    assert table.updates == [
        [{"id": 1, "name": "a", "value": 1}],
        [
            {"id": 1, "name": "b", "value": 2},
            {"id": 2, "name": "c", "value": 3},
        ],
    ]
    assert table.removals == [[1]]


def test_manager_edit_callbacks_do_not_require_perspective_to_be_installed():
    manager = PerspectiveTablesManager(_Client())
    updates = []
    token = manager.subscribe_table_updates(
        "rows", lambda rows, removals: updates.append((rows, removals))
    )
    manager.publish_edits("rows", [{"id": 1, "value": 2}], [3])
    manager.unsubscribe_table_updates("rows", token)
    manager.publish_edits("rows", [{"id": 2}], ())

    assert updates == [([{"id": 1, "value": 2}], [3])]

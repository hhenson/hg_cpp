"""Where does a client-supplied scalar option reach the implementation?

Client scalar options are recorded per
``(wiring, flavour, interface, specialization, path)`` and read back in
``_bind_registered_impl``. Which stub is matched there differs by registration
shape, so this pins the whole matrix rather than one cell - a gap in any of
them substitutes the implementation's own default *silently*, which is how the
multi-interface case escaped notice.

One known gap is recorded at the end.
"""

import pytest

import hgraph as hg
from hgraph import TS, TSD, graph
from hgraph.test import eval_node


def test_single_interface_service_exact_path():
    seen = []

    @hg.reference_service
    def r(path: str = "p", m: int = 1) -> TS[int]: ...

    @hg.service_impl(interfaces=r)
    def impl(path: str = "p", m: int = 1) -> TS[int]:
        seen.append(m)
        return hg.const(m)

    @graph
    def app() -> TS[int]:
        hg.register_service("p", impl)
        return r(path="p", m=7)

    assert eval_node(app) == [7]
    assert seen[0] == 7


def test_single_interface_service_default_fallback():
    seen = []

    @hg.reference_service
    def r(path: str = "p", m: int = 1) -> TS[int]: ...

    @hg.service_impl(interfaces=r)
    def impl(path: str = "p", m: int = 1) -> TS[int]:
        seen.append(m)
        return hg.const(m)

    @graph
    def app() -> TS[int]:
        hg.register_service(None, impl)
        return r(path="custom", m=7)

    assert eval_node(app) == [7]
    assert seen[0] == 7


def test_multi_interface_service_default_fallback():
    seen = []

    @hg.reference_service
    def r(path: str = "p", m: int = 1) -> TS[int]: ...

    @hg.request_reply_service
    def d(value: TS[int], path: str = "p") -> TS[int]: ...

    @hg.service_impl(interfaces=(r, d))
    def impl(path: str, m: int = 1):
        seen.append(m)
        hg.to_graph(r, hg.const(m), path)
        hg.to_graph(d, hg.map_(lambda v: v, hg.from_graph(d, path)), path)

    @graph
    def app(v: TS[int]) -> TS[int]:
        hg.register_service(None, impl)
        return r(path="custom", m=7) + d(v, path="custom")

    eval_node(app, [1], __end_time__=hg.MIN_ST + 6 * hg.MIN_TD)
    assert seen[0] == 7, seen


def test_single_interface_adaptor_exact_and_fallback():
    for path_arg, client_path in (("p", "p"), (None, "custom")):
        seen = []

        @hg.adaptor
        def a(value: TS[int], path: str = "p", m: int = 1) -> TS[int]: ...

        @hg.adaptor_impl(interfaces=a)
        def impl(value: TS[int], path: str = "p", m: int = 1) -> TS[int]:
            seen.append(m)
            return value * m

        @graph
        def app(v: TS[int]) -> TS[int]:
            hg.register_adaptor(path_arg, impl)
            return a(v, path=client_path, m=7)

        assert eval_node(app, [2]) == [14], (path_arg, seen)
        assert seen[0] == 7, (path_arg, seen)


def test_multi_interface_adaptor_exact_path():
    seen = []

    @hg.adaptor
    def a1(value: TS[int], path: str = "p", m: int = 1) -> TS[int]: ...

    @hg.adaptor
    def a2(value: TS[int], path: str = "p") -> TS[int]: ...

    @hg.adaptor_impl(interfaces=(a1, a2))
    def impl(path: str, m: int = 1):
        seen.append(m)
        hg.to_graph(a1, hg.from_graph(a1, path) * m, path)
        hg.to_graph(a2, hg.from_graph(a2, path), path)

    @graph
    def app(v: TS[int]) -> TS[int]:
        hg.register_adaptor("p", impl)
        return a1(v, path="p", m=7) + a2(v, path="p")

    eval_node(app, [2])
    assert seen[0] == 7, seen


def test_service_adaptor_exact_path():
    seen = []

    @hg.service_adaptor
    def sa(value: TS[int], path: str = "p", m: int = 1) -> TS[int]: ...

    @hg.service_adaptor_impl(interfaces=sa)
    def impl(value: TSD[int, TS[int]], path: str = "p", m: int = 1) -> TSD[int, TS[int]]:
        seen.append(m)
        return hg.map_(lambda x: x * m, value)

    @graph
    def app(v: TS[int]) -> TS[int]:
        hg.register_adaptor("p", impl)
        return sa(v, path="p", m=7)

    eval_node(app, [2], __end_time__=hg.MIN_ST + 6 * hg.MIN_TD)
    assert seen[0] == 7, seen


def test_specialized_service_exact_path():
    seen = []

    @hg.reference_service
    def r(path: str = "p", m: int = 1) -> TS[hg.SCALAR]: ...

    ri = r[hg.SCALAR:int]

    @hg.service_impl(interfaces=ri)
    def impl(path: str = "p", m: int = 1) -> TS[int]:
        seen.append(m)
        return hg.const(m)

    @graph
    def app() -> TS[int]:
        hg.register_service("p", impl)
        return ri(path="p", m=7)

    # The config key strips the [spec] suffix on both the recording and the
    # lookup side, so a specialized interface resolves like any other.
    assert eval_node(app) == [7]
    assert seen[0] == 7


@pytest.mark.xfail(
    reason="KNOWN GAP (pre-existing, both families): a catch-all declares no "
           "interfaces, so there is no stub to key client configuration by, and "
           "which endpoints it claims is only known once its body has run. "
           "Registration-supplied configuration reaches it; client-supplied "
           "configuration is silently replaced by the implementation default.",
    strict=True,
)
def test_catch_all_receives_client_options():
    seen = []

    @hg.adaptor
    def a(value: TS[int], path: str = "p", m: int = 1) -> TS[int]: ...

    @hg.adaptor_impl(interfaces=())
    def impl(m: int = 1):
        seen.append(m)
        hg.to_graph(a, hg.from_graph(a, "custom") * m, "custom")

    @graph
    def app(v: TS[int]) -> TS[int]:
        hg.register_adaptor(None, impl)
        return a(v, path="custom", m=7)

    eval_node(app, [2])
    assert seen[0] == 7, seen


def test_catch_all_receives_registration_options():
    """The supported channel for configuring a catch-all."""
    seen = []

    @hg.adaptor
    def a(value: TS[int], path: str = "p") -> TS[int]: ...

    @hg.adaptor_impl(interfaces=())
    def impl(m: int = 1):
        seen.append(m)
        hg.to_graph(a, hg.from_graph(a, "custom") * m, "custom")

    @graph
    def app(v: TS[int]) -> TS[int]:
        hg.register_adaptor(None, impl, m=7)
        return a(v, path="custom")

    assert eval_node(app, [2]) == [14]
    assert seen[0] == 7, seen

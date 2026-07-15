import pytest

import hgraph as hg
from hgraph import NUMBER, NUMBER_2, TS, TSD, graph
from hgraph.test import eval_node


def test_numeric_vocabulary_is_a_constrained_native_generic():
    @hg.compute_node
    def numeric_rhs(lhs: TS[NUMBER], rhs: TS[NUMBER_2]) -> TS[NUMBER_2]:
        return rhs.value

    @graph
    def int_float(lhs: TS[int], rhs: TS[float]) -> TS[float]:
        return numeric_rhs(lhs, rhs)

    @graph
    def invalid(lhs: TS[str], rhs: TS[float]) -> TS[float]:
        return numeric_rhs(lhs, rhs)

    assert eval_node(int_float, [1, 2], [1.5, 2.5]) == [1.5, 2.5]
    with pytest.raises(hg.IncorrectTypeBinding, match="numeric_rhs.*expects"):
        eval_node(invalid, ["not numeric"], [1.5])


def test_specialized_numeric_reference_services_share_one_interface_name():
    @hg.reference_service
    def numeric_reference() -> TS[NUMBER]: ...

    integer_reference = numeric_reference[NUMBER:int]
    float_reference = numeric_reference[NUMBER:float]

    @hg.service_impl(interfaces=integer_reference)
    def integer_reference_impl() -> TS[int]:
        return hg.const(1, tp=TS[int])

    @hg.service_impl(interfaces=float_reference)
    def float_reference_impl() -> TS[float]:
        return hg.const(0.5, tp=TS[float])

    @graph
    def integer_client(value: TS[int]) -> TS[int]:
        hg.register_service("numbers", integer_reference_impl)
        return value + hg.passive(integer_reference(path="numbers"))

    @graph
    def float_client(value: TS[float]) -> TS[float]:
        hg.register_service("numbers", float_reference_impl)
        return value + hg.passive(float_reference(path="numbers"))

    assert eval_node(integer_client, [2]) == [3]
    assert eval_node(float_client, [2.0]) == [2.5]
    with pytest.raises(TypeError, match="NUMBER must be one of int, float"):
        numeric_reference[NUMBER:str]


def test_specialized_numeric_request_reply_services_use_native_exchange():
    @hg.request_reply_service
    def numeric_adjust(request: TS[NUMBER]) -> TS[NUMBER]: ...

    integer_adjust = numeric_adjust[NUMBER:int]
    float_adjust = numeric_adjust[NUMBER:float]

    @hg.service_impl(interfaces=integer_adjust)
    def integer_adjust_impl(requests: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return hg.map_(lambda value: value + 1, requests)

    @hg.service_impl(interfaces=float_adjust)
    def float_adjust_impl(requests: TSD[int, TS[float]]) -> TSD[int, TS[float]]:
        return hg.map_(lambda value: value + 0.5, requests)

    @graph
    def integer_client(value: TS[int]) -> TS[int]:
        hg.register_service("adjust", integer_adjust_impl)
        return integer_adjust(value, path="adjust")

    @graph
    def float_client(value: TS[float]) -> TS[float]:
        hg.register_service("adjust", float_adjust_impl)
        return float_adjust(value, path="adjust")

    assert eval_node(integer_client, [1], __end_time__=hg.MIN_ST + 3 * hg.MIN_TD) == [None, 2]
    assert eval_node(float_client, [1.5], __end_time__=hg.MIN_ST + 3 * hg.MIN_TD) == [None, 2.0]

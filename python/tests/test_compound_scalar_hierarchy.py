from dataclasses import dataclass
from typing import Generic, Optional, TypeVar

from hgraph import CompoundScalar, TS, compute_node, from_json_builder, graph, to_json_builder
# White-box: these tests assert on the interned C++ value-type metadata
# (qualified names, generic specialisation identity), which has no public
# introspection surface — the module under test is imported directly.
from hgraph._types import _value_type
from hgraph.test import eval_node


def test_compound_scalar_qualified_names_and_hierarchy():
    @dataclass
    class Base(CompoundScalar, namespace="tests.orders", abstract=True):
        order_id: int

    @dataclass
    class Limit(Base):
        price: float

    base = _value_type(Base)
    limit = _value_type(Limit)

    assert base.name == "tests.orders::Base"
    assert base.namespace == "tests.orders"
    assert base.local_name == "Base"
    assert limit.name.endswith("::Limit")
    assert limit != base


def test_compound_scalar_default_namespace_includes_enclosing_scope():
    @dataclass
    class Local(CompoundScalar):
        value: int

    meta = _value_type(Local)
    assert meta.namespace == f"{__name__}.test_compound_scalar_default_namespace_includes_enclosing_scope.<locals>"
    assert meta.local_name == "Local"


def test_compound_scalar_generic_specializations_are_invariant():
    value_type = TypeVar("value_type")

    @dataclass
    class Box(CompoundScalar, Generic[value_type], namespace="tests.generics"):
        value: value_type

    integer_box = _value_type(Box[int])
    string_box = _value_type(Box[str])

    assert integer_box != string_box
    assert integer_box.name == "tests.generics::Box[int]"
    assert string_box.name == "tests.generics::Box[str]"
    assert _value_type(Box[int]) == integer_box


def test_concrete_generic_child_reuses_specialized_parent_fields():
    value_type = TypeVar("value_type")

    @dataclass
    class Base(CompoundScalar, Generic[value_type], namespace="tests.generic_hierarchy", abstract=True):
        value: value_type

    @dataclass
    class IntegerValue(Base[int]):
        label: str

    integer_base = _value_type(Base[int])
    integer_value = _value_type(IntegerValue)
    string_base = _value_type(Base[str])

    assert integer_value != integer_base
    assert integer_value != string_base

    @compute_node
    def is_integer_value(value: TS[Base[int]]) -> TS[bool]:
        return isinstance(value.value, IntegerValue)

    assert eval_node(is_integer_value, [IntegerValue(value=1, label="one")]) == [True]


def test_multiple_inheritance_is_visible_through_each_parent():
    @dataclass
    class Identified(CompoundScalar, namespace="tests.multiple", abstract=True):
        identifier: int

    @dataclass
    class Priced(CompoundScalar, namespace="tests.multiple", abstract=True):
        price: float

    @dataclass
    class Order(Identified, Priced):
        venue: str

    @compute_node
    def identified_is_order(value: TS[Identified]) -> TS[bool]:
        return isinstance(value.value, Order)

    @compute_node
    def priced_is_order(value: TS[Priced]) -> TS[bool]:
        return isinstance(value.value, Order)

    order = Order(identifier=1, price=2.5, venue="X")
    assert eval_node(identified_is_order, [order]) == [True]
    assert eval_node(priced_is_order, [order]) == [True]


def test_python_node_round_trips_concrete_value_through_polymorphic_base_output():
    @dataclass
    class Base(CompoundScalar, namespace="tests.runtime", abstract=True):
        value: int

    @dataclass
    class Derived(Base):
        label: str

    @compute_node
    def upcast(value: TS[Derived]) -> TS[Base]:
        return value.value

    @compute_node
    def is_derived(value: TS[Base]) -> TS[bool]:
        return isinstance(value.value, Derived)

    @graph
    def app(value: TS[Derived]) -> TS[bool]:
        return is_derived(upcast(value))

    assert eval_node(app, [Derived(value=1, label="one")]) == [True]


def test_base_annotation_closes_over_defined_python_subclasses():
    @dataclass
    class Base(CompoundScalar, namespace="tests.closed", abstract=True):
        value: int

    @dataclass
    class Derived(Base):
        label: str

    @compute_node
    def is_derived(value: TS[Base]) -> TS[bool]:
        return isinstance(value.value, Derived)

    assert eval_node(is_derived, [Derived(value=1, label="one")]) == [True]


def test_polymorphic_bundle_field_uses_the_graph_realization():
    @dataclass
    class Base(CompoundScalar, namespace="tests.nested", abstract=True):
        value: int

    @dataclass
    class Derived(Base):
        label: str

    @dataclass
    class Envelope(CompoundScalar, namespace="tests.nested"):
        item: Base

    @compute_node
    def contains_derived(value: TS[Envelope]) -> TS[bool]:
        return isinstance(value.value.item, Derived)

    assert eval_node(
        contains_derived,
        [Envelope(item=Derived(value=1, label="one"))],
    ) == [True]


def test_compact_container_elements_use_the_graph_realization():
    @dataclass
    class Base(CompoundScalar, namespace="tests.container", abstract=True):
        value: int

    @dataclass
    class Derived(Base):
        label: str

    @dataclass
    class Batch(CompoundScalar, namespace="tests.container"):
        items: tuple[Base, ...]

    @compute_node
    def contains_derived(value: TS[Batch]) -> TS[bool]:
        return isinstance(value.value.items[0], Derived)

    assert eval_node(
        contains_derived,
        [Batch(items=(Derived(value=1, label="one"),))],
    ) == [True]


def test_configured_json_discriminator_selects_the_concrete_class():
    @dataclass
    class Base(
        CompoundScalar,
        namespace="tests.json",
        abstract=True,
        discriminator="kind",
    ):
        value: int

    @dataclass
    class Derived(Base, namespace="tests.json"):
        label: str

    decode = from_json_builder(Base)
    encode = to_json_builder(Base)
    value = decode(
        '{"kind": "Derived", "value": 1, "label": "one"}'
    )

    assert isinstance(value, Derived)
    assert '"kind": "tests.json::Derived"' in encode(value)


def test_self_recursive_compound_scalar_allocates_children_on_demand():
    @dataclass
    class RecursiveValue(CompoundScalar, namespace="tests.recursion"):
        value: int
        next: Optional["RecursiveValue"] = None

    @compute_node
    def child_value(value: TS[RecursiveValue]) -> TS[int]:
        child = value.value.next
        return -1 if child is None else child.value

    result = eval_node(
        child_value,
        [RecursiveValue(1, RecursiveValue(2)), RecursiveValue(3)],
    )

    assert result == [2, -1]

"""The dispatch composition (key utility + enumerated switch_) over
python-class scalars - the object value kind keeps the dynamic type, so
runtime dispatch works today. (CompoundScalar hierarchies await the bundle
lineage design; the ported test_dispatch file records that gap.)"""
from hgraph import TS, compute_node, dispatch, dispatch_, graph, operator
from hgraph.test import eval_node


class Pet: ...


class Dog(Pet): ...


class Cat(Pet): ...


def test_dispatch_decorator():
    @compute_node
    def sound_default(pet: TS[Pet], count: TS[int]) -> TS[str]:
        return f"unknown {count.value}"

    @dispatch
    def pet_sound(pet: TS[Pet], count: TS[int]) -> TS[str]:
        return sound_default(pet, count)

    @graph(overloads=pet_sound)
    def pet_sound_dog(pet: TS[Dog], count: TS[int]) -> TS[str]:
        return "woof"

    @graph(overloads=pet_sound)
    def pet_sound_cat(pet: TS[Cat], count: TS[int]) -> TS[str]:
        return "meow"

    @graph
    def make_sound(pet: TS[Pet], count: TS[int]) -> TS[str]:
        return pet_sound(pet, count)

    assert eval_node(
        make_sound, [None, Dog(), None, Cat(), Pet(), None], [None, 1, None, None, 2, 3]
    ) == [None, "woof", None, "meow", "unknown 2", "unknown 3"]


def test_dispatch_fn_multi():
    class Food: ...

    class Plant(Food): ...

    class Meat(Food): ...

    @operator
    def eats(animal: TS[Pet], food: TS[Food]) -> TS[bool]: ...

    @graph(overloads=eats)
    def eats_default(animal: TS[Pet], food: TS[Food]) -> TS[bool]:
        return False

    @graph(overloads=eats)
    def cat_eats_meat(animal: TS[Cat], food: TS[Meat]) -> TS[bool]:
        return True

    @graph(overloads=eats)
    def dog_eats_everything(animal: TS[Dog], food: TS[Food]) -> TS[bool]:
        return True

    @graph
    def eat(animal: TS[Pet], food: TS[Food]) -> TS[bool]:
        return dispatch_(eats, animal, food)

    assert eval_node(
        eat,
        [None, Cat(), None, Dog(), Cat()],
        [None, Plant(), Meat(), Plant(), Meat()],
    ) == [None, False, True, True, True]

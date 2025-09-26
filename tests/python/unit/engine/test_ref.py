from hgraph import compute_node, TS, REF, graph
from hgraph.test import eval_node


@compute_node
def process_reference(ts: REF[TS[int]]) -> REF[TS[int]]:
    #print(f"As reference: {ts.value}")
    return ts.value


@compute_node
def convert_reference(ts: TS[int]) -> TS[int]:
    #print(f"As value: {ts.value}")
    return ts.value

def test_reference_conversion():

    @graph
    def g(ts: TS[int]) -> TS[int]:
        return convert_reference(process_reference(ts))
    
    assert eval_node(g, [1, 2, 3]) == [1, 2, 3]

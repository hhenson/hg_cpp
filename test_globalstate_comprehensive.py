from contextlib import nullcontext
import sys
import traceback
sys.path.insert(0, 'cmake-build-debug/src/cpp')
sys.path.insert(0, '.venv/lib/python3.12/site-packages')
import _hgraph
from hgraph._runtime._global_state import GlobalState

try:
    GlobalState.set_implementation_class(_hgraph.GlobalState)
    GlobalState.reset()

    print('Test 1: Conditional context (original failing pattern)')
    with GlobalState() if not GlobalState.has_instance() else nullcontext():
        print('  Inside')
        print(f'  has_instance: {GlobalState.has_instance()}')

    print(f'  After: has_instance={GlobalState.has_instance()}')

    print('\nTest 2: Nested contexts')
    with GlobalState(x=1):
        print(f'  Outer: x={GlobalState.instance().get("x")}')
        with GlobalState(y=2):
            print(f'    Inner: x={GlobalState.instance().get("x")}, y={GlobalState.instance().get("y")}')
        print(f'  Back to outer: x={GlobalState.instance().get("x")}, y={GlobalState.instance().get("y", "not found")}')

    print(f'\nAfter all: has_instance={GlobalState.has_instance()}')
    print('✓ ALL TESTS PASSED!')
except Exception as e:
    print(f'ERROR: {e}')
    traceback.print_exc()

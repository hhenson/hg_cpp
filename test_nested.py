import sys
sys.path.insert(0, 'cmake-build-debug/src/cpp')
sys.path.insert(0, '.venv/lib/python3.12/site-packages')
import _hgraph
from hgraph._runtime._global_state import GlobalState

GlobalState.set_implementation_class(_hgraph.GlobalState)
GlobalState.reset()

print('Step 1: Create and enter outer context')
gs1 = GlobalState(x=1)
gs1.__enter__()
GlobalState.set_instance(gs1)
print(f'  Outer active, x={GlobalState.instance().get("x")}')

print('\nStep 2: Create and enter inner context')
gs2 = GlobalState(y=2)
gs2.set_previous(gs1)
gs2.__enter__()
GlobalState.set_instance(gs2)
print(f'  Inner active, y={GlobalState.instance().get("y")}')

print('\nStep 3: Exit inner context')
prev_from_inner = gs2.get_previous()
print(f'  Previous from inner: {prev_from_inner}')
gs2.__exit__(None, None, None)
print('  C++ __exit__ succeeded')
print(f'  Restoring previous: {prev_from_inner}')
GlobalState.set_instance(prev_from_inner)
print(f'  Back to outer, x={GlobalState.instance().get("x")}')

print('\nStep 4: Exit outer context')
gs1.__exit__(None, None, None)
GlobalState.set_instance(None)
print('  Exited outer')

print('\n✓ Manual test passed')

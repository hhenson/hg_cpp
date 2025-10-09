import sys
sys.path.insert(0, 'cmake-build-debug/src/cpp')
sys.path.insert(0, '.venv/lib/python3.12/site-packages')
import _hgraph
from hgraph._runtime._global_state import GlobalState

GlobalState.set_implementation_class(_hgraph.GlobalState)
GlobalState.reset()

print('Step 1: Create outer and call __enter__')
outer = GlobalState()
print(f'  outer: {outer}')
outer.__enter__()
print('  outer entered')

print('\nStep 2: Create inner')
inner = GlobalState()
print(f'  inner: {inner}')

print('\nStep 3: Call __enter__ on inner')
inner.__enter__()
print('  inner entered')

print('\nSUCCESS')

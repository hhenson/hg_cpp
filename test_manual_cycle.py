import sys
sys.path.insert(0, 'cmake-build-debug/src/cpp')
sys.path.insert(0, '.venv/lib/python3.12/site-packages')
import _hgraph
from hgraph._runtime._global_state import GlobalState

GlobalState.set_implementation_class(_hgraph.GlobalState)
GlobalState.reset()

print('Testing manual context cycle')
outer = GlobalState()
outer.__enter__()
print('Outer entered')

inner = GlobalState()
inner.__enter__()
print('Inner entered')

print('\nCalling inner.__exit__')
inner.__exit__(None, None, None)
print('Inner exited')

print('\nCalling outer.__exit__')
outer.__exit__(None, None, None)
print('Outer exited')

print('\nSUCCESS')

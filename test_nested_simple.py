import sys
sys.path.insert(0, 'cmake-build-debug/src/cpp')
sys.path.insert(0, '.venv/lib/python3.12/site-packages')
import _hgraph
from hgraph._runtime._global_state import GlobalState

GlobalState.set_implementation_class(_hgraph.GlobalState)
GlobalState.reset()

print('Testing nested contexts (no kwargs)')
try:
    with GlobalState():
        print('  Outer context')
        with GlobalState():
            print('    Inner context')
        print('  Back to outer')
    print('SUCCESS')
except Exception as e:
    print(f'ERROR: {e}')
    import traceback
    traceback.print_exc()

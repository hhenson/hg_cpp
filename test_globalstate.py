from contextlib import nullcontext
import sys
import traceback
sys.path.insert(0, 'hg_cpp')
sys.path.insert(0, 'cmake-build-debug/src/cpp')
sys.path.insert(0, '.venv/lib/python3.12/site-packages')

try:
    print("Importing _hgraph...")
    import _hgraph
    print("Importing GlobalState...")
    from hgraph._runtime._global_state import GlobalState

    print("Setting implementation class...")
    GlobalState.set_implementation_class(_hgraph.GlobalState)
    print("Resetting...")
    GlobalState.reset()

    print('Test: Conditional context (original failing pattern)')
    print(f'Before: has_instance={GlobalState.has_instance()}')

    print("Creating GlobalState instance...")
    gs = GlobalState()
    print(f"Created: {gs}")

    print("Entering context manager...")
    with gs:
        print('  Inside')
        print(f'  has_instance: {GlobalState.has_instance()}')

    print(f'  After: has_instance={GlobalState.has_instance()}')
    print('✓ SUCCESS!')
except Exception as e:
    print(f'ERROR: {e}')
    traceback.print_exc()

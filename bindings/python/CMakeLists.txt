# The Python bridge module (roadmap P4). Opt-in only: the default build never
# needs Python (HGRAPH_BUILD_PYTHON_BINDINGS=ON enables this subtree).
nanobind_add_module(_hgraph NB_STATIC NOSTRIP module.cpp)
target_link_libraries(_hgraph PRIVATE hgraph_core hgraph::options)

add_test(
    NAME hgraph_python_bridge
    COMMAND "${Python_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_bridge.py"
)
set_tests_properties(hgraph_python_bridge PROPERTIES
    ENVIRONMENT "PYTHONPATH=$<TARGET_FILE_DIR:_hgraph>"
)

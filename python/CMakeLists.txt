# The Python bridge module (roadmap P4). Opt-in only: the default build never
# needs Python (HGRAPH_BUILD_PYTHON_BINDINGS=ON enables this subtree).
nanobind_add_module(_hgraph NB_STATIC NOSTRIP
    module.cpp
    value_conversion.cpp
)
target_link_libraries(_hgraph PRIVATE hgraph_core hgraph::options)
hgraph_enable_private_pch(_hgraph)

foreach(_hgraph_py_test IN ITEMS test_bridge test_hgraph_api)
    add_test(
        NAME hgraph_python_${_hgraph_py_test}
        COMMAND "${Python_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/tests/${_hgraph_py_test}.py"
    )
    set_tests_properties(hgraph_python_${_hgraph_py_test} PROPERTIES
        ENVIRONMENT "PYTHONPATH=$<TARGET_FILE_DIR:_hgraph>:${CMAKE_CURRENT_SOURCE_DIR}"
    )
endforeach()

# The PORTED hgraph test suite (python/tests/ported): the parity yardstick.
# Near-verbatim copies of ext/main's tests; GREEN and gating (skips carry
# precise "gap:"/"deviation:" reasons).
add_test(
    NAME hgraph_python_ported_suite
    COMMAND "${Python_EXECUTABLE}" -m pytest "${CMAKE_CURRENT_SOURCE_DIR}/tests/ported" -q -m "not wip"
)
set_tests_properties(hgraph_python_ported_suite PROPERTIES
    ENVIRONMENT "PYTHONPATH=$<TARGET_FILE_DIR:_hgraph>:${CMAKE_CURRENT_SOURCE_DIR}"
    LABELS "parity"
)

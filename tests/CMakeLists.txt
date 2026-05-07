add_subdirectory(cpp)

if(HGRAPH_BUILD_PYTHON_BINDINGS AND TARGET _hgraph AND Python_EXECUTABLE)
    if(WIN32)
        set(_hgraph_pythonpath_sep ";")
    else()
        set(_hgraph_pythonpath_sep ":")
    endif()

    add_test(
        NAME hgraph.python
        COMMAND "${Python_EXECUTABLE}" -m pytest "${PROJECT_SOURCE_DIR}/tests/python"
        WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    )
    set_tests_properties(hgraph.python PROPERTIES
        ENVIRONMENT "PYTHONPATH=${PROJECT_BINARY_DIR}/python${_hgraph_pythonpath_sep}${PROJECT_SOURCE_DIR}/python"
    )
endif()

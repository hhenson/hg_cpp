set(_hgraph_python_package_dir "${PROJECT_BINARY_DIR}/python/hgraph")
file(MAKE_DIRECTORY "${_hgraph_python_package_dir}")

configure_file(
    "${PROJECT_SOURCE_DIR}/python/hgraph/__init__.py"
    "${_hgraph_python_package_dir}/__init__.py"
    COPYONLY
)

get_target_property(_hgraph_core_type hgraph_core TYPE)
if(NOT _hgraph_core_type STREQUAL "INTERFACE_LIBRARY")
    set_target_properties(hgraph_core PROPERTIES POSITION_INDEPENDENT_CODE ON)
endif()

nanobind_add_module(_hgraph NB_STATIC hgraph_module.cpp)

target_link_libraries(_hgraph
    PRIVATE
        hgraph::core
)

set_target_properties(_hgraph PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${_hgraph_python_package_dir}"
    RUNTIME_OUTPUT_DIRECTORY "${_hgraph_python_package_dir}"
)

install(TARGETS _hgraph
    LIBRARY DESTINATION hgraph
    RUNTIME DESTINATION hgraph
)

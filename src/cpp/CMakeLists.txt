project(_hgraph)

set(HGRAPH_SOURCES
        python/_hgraph_graph.cpp
        python/_hgraph_module.cpp
        python/_hgraph_types.cpp
        python/py_hgraph.cpp

        runtime/evaluation_context.cpp
        runtime/evaluation_engine.cpp

        types/time_series_type.cpp

        util/lifecycle.cpp
        )

add_definitions(-Dhgraph_EXPORTS)

nanobind_add_module(${PROJECT_NAME} ${HGRAPH_SOURCES} ${HGRAPH_INCLUDES})

target_link_libraries(${PROJECT_NAME} PRIVATE Threads::Threads spdlog::spdlog Python::Python)

# Install directive for scikit-build-core
install(TARGETS ${PROJECT_NAME} LIBRARY DESTINATION ${PROJECT_NAME})

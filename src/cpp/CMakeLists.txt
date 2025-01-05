project(_hgraph)

set(HGRAPH_SOURCES

        builders/node_builder.cpp

        python/_hgraph_runtime.cpp
        python/_hgraph_module.cpp
        python/_hgraph_types.cpp
        python/_hgraph_utils.cpp

        runtime/evaluation_context.cpp
        runtime/evaluation_engine.cpp
        runtime/graph_executor.cpp

        types/error_type.cpp
        types/graph.cpp
        types/node.cpp
        types/ref.cpp
        types/time_series_type.cpp
        types/traits.cpp
        types/ts.cpp
        types/tsb.cpp

        util/lifecycle.cpp
        util/sender_receiver_state.cpp
        )

add_definitions(-Dhgraph_EXPORTS)

nanobind_add_module(${PROJECT_NAME} ${HGRAPH_SOURCES} ${HGRAPH_INCLUDES})

target_link_libraries(${PROJECT_NAME} PRIVATE Threads::Threads spdlog::spdlog Python::Python)

# Install directive for scikit-build-core
install(TARGETS ${PROJECT_NAME} LIBRARY DESTINATION .)

#nanobind_add_stub(
#        "${PROJECT_NAME}_stub"
#        MODULE ${PROJECT_NAME}
#        OUTPUT "${PROJECT_NAME}.pyi"
#        PYTHON_PATH $<TARGET_FILE_DIR:${PROJECT_NAME}>
#        DEPENDS ${PROJECT_NAME}
#)
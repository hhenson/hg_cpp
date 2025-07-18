project(_hgraph)

set(HGRAPH_SOURCES
        builders/builder.cpp
        builders/graph_builder.cpp
        builders/input_builder.cpp
        builders/node_builder.cpp
        builders/output_builder.cpp

        nodes/map_node.cpp
        nodes/nested_node.cpp

        python/_hgraph_builder.cpp
        python/_hgraph_runtime.cpp
        python/_hgraph_module.cpp
        python/_hgraph_types.cpp
        python/_hgraph_utils.cpp

        runtime/evaluation_context.cpp
        runtime/evaluation_engine.cpp
        runtime/graph_executor.cpp
        runtime/record_replay.cpp

        types/constants.cpp
        types/error_type.cpp
        types/feature_extension.cpp
        types/graph.cpp
        types/node.cpp
        types/ref.cpp
        types/time_series_type.cpp
        types/traits.cpp
        types/ts.cpp
        types/ts_signal.cpp
        types/tsd.cpp
        types/tss.cpp
        types/ts_indexed.cpp
        types/tsb.cpp
        types/tsl.cpp

        util/lifecycle.cpp
        util/sender_receiver_state.cpp
        )

add_definitions(-Dhgraph_EXPORTS)

nanobind_add_module(${PROJECT_NAME} ${HGRAPH_SOURCES} ${HGRAPH_INCLUDES})

target_link_libraries(${PROJECT_NAME} PRIVATE Threads::Threads fmt::fmt Python::Python)

# Install directive for scikit-build-core
install(TARGETS ${PROJECT_NAME} LIBRARY DESTINATION .)

#nanobind_add_stub(
#        "${PROJECT_NAME}_stub"
#        MODULE ${PROJECT_NAME}
#        OUTPUT "${PROJECT_NAME}.pyi"
#        PYTHON_PATH $<TARGET_FILE_DIR:${PROJECT_NAME}>
#        DEPENDS ${PROJECT_NAME}
#)
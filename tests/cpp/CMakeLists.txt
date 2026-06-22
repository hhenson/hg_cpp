add_executable(hgraph_smoke_test
    smoke_test.cpp
)

target_link_libraries(hgraph_smoke_test
    PRIVATE
        hgraph::core
)

add_test(NAME hgraph.smoke COMMAND hgraph_smoke_test)

add_executable(hgraph_header_compile_check
    header_compile_check.cpp
)

target_link_libraries(hgraph_header_compile_check
    PRIVATE
        hgraph::core
)

add_test(NAME hgraph.header_compile_check COMMAND hgraph_header_compile_check)

# Catch2 for the utility test suites.
find_package(Catch2 3 CONFIG QUIET)
if(NOT Catch2_FOUND)
    include(FetchContent)
    FetchContent_Declare(
        Catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG        v3.6.0
        GIT_SHALLOW    TRUE
    )
    FetchContent_MakeAvailable(Catch2)
endif()

add_executable(hgraph_unit_tests
    registry_test_listener.cpp
    test_intern_table.cpp
    test_memory_utils.cpp
    test_plan_factories.cpp
    test_schema_examples.cpp
    test_slot_utils.cpp
    test_scope.cpp
    test_runtime_value_view.cpp
    test_simulation_execution.cpp
    test_realtime_execution.cpp
    test_node_scheduler.cpp
    test_endpoint_owner.cpp
    test_feedback.cpp
    test_record_replay.cpp
    test_eval_node.cpp
    test_std_nodes.cpp
    test_std_type_registration.cpp
    test_check_output.cpp
    test_value_types.cpp
    test_collection_nodes.cpp
    test_tss_nodes.cpp
    test_tsl_nodes.cpp
    test_static_node.cpp
    test_graph_wiring.cpp
    test_nested_wiring.cpp
    lift/test_lift.cpp
    test_reduce.cpp
    test_switch.cpp
    test_map.cpp
    test_mesh.cpp
    test_error_handling.cpp
    test_operators.cpp
    test_std_operators.cpp
    test_compact_storage.cpp
    test_bundle_builder.cpp
    test_ts_delta.cpp
    test_type_resolution.cpp
    test_specialized_views.cpp
    test_static_schema.cpp
    test_time_series_reference.cpp
    test_ts_input.cpp
    test_ts_schemas.cpp
    test_tsd_proxy.cpp
    test_ts_output.cpp
    test_type_registry.cpp
    test_value.cpp
    test_any_value.cpp
    test_mutable_list.cpp
    test_mutable_map.cpp
    test_mutable_set.cpp
    test_global_state.cpp
)

target_link_libraries(hgraph_unit_tests
    PRIVATE
        hgraph::core
        Catch2::Catch2WithMain
)

include(Catch)
catch_discover_tests(hgraph_unit_tests)

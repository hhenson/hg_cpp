add_executable(hgraph_smoke_test
    smoke_test.cpp
)

target_link_libraries(hgraph_smoke_test
    PRIVATE
        hgraph::core
)

hgraph_enable_private_pch(hgraph_smoke_test)
add_test(NAME hgraph.smoke COMMAND hgraph_smoke_test)

add_executable(hgraph_header_compile_check
    header_compile_check.cpp
)

target_link_libraries(hgraph_header_compile_check
    PRIVATE
        hgraph::core
)

hgraph_enable_private_pch(hgraph_header_compile_check)
add_test(NAME hgraph.header_compile_check COMMAND hgraph_header_compile_check)

add_executable(hgraph_debugger_fixture
    ../debugger/debugger_fixture.cpp
)

target_link_libraries(hgraph_debugger_fixture
    PRIVATE
        hgraph::core
)

hgraph_enable_private_pch(hgraph_debugger_fixture)
if(MSVC)
    target_compile_options(hgraph_debugger_fixture PRIVATE /Od /Zi)
else()
    target_compile_options(hgraph_debugger_fixture PRIVATE -O0 -g)
endif()
add_test(NAME hgraph.debugger_fixture COMMAND hgraph_debugger_fixture)

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

add_library(hgraph_test_listener_objects OBJECT
    registry_test_listener.cpp
)
target_link_libraries(hgraph_test_listener_objects
    PRIVATE
        hgraph::core
        Catch2::Catch2
)
hgraph_enable_private_pch(hgraph_test_listener_objects)

function(hgraph_add_test_objects target_name)
    add_library(${target_name} OBJECT ${ARGN})
    target_link_libraries(${target_name}
        PRIVATE
            hgraph::core
            Catch2::Catch2
    )
    hgraph_enable_private_pch(${target_name})
endfunction()

hgraph_add_test_objects(hgraph_runtime_test_objects
    test_check_output.cpp
    test_lifecycle_observers.cpp
    test_context_node.cpp
    test_endpoint_owner.cpp
    test_feedback.cpp
    test_global_state.cpp
    test_graph_introspection.cpp
    test_logger.cpp
    test_node_scheduler.cpp
    test_realtime_execution.cpp
    test_ref_executor.cpp
    test_service_node.cpp
    test_shared_output_node.cpp
    test_simulation_execution.cpp
)

hgraph_add_test_objects(hgraph_wiring_test_objects
    test_adaptor_wiring.cpp
    test_component.cpp
    test_context_wiring.cpp
    test_erased_wiring.cpp
    test_error_handling.cpp
    test_eval_node.cpp
    test_graph_wiring.cpp
    test_nested_wiring.cpp
    test_operators.cpp
    test_race_semantics.cpp
    test_service_runtime.cpp
    test_service_wiring.cpp
    test_static_node.cpp
    test_type_resolution.cpp
)

hgraph_add_test_objects(hgraph_stdlib_test_objects
    lift/test_lift.cpp
    test_collection_nodes.cpp
    test_const_eval.cpp
    test_map.cpp
    test_mesh.cpp
    test_reduce.cpp
    test_std_nodes.cpp
    test_std_operators.cpp
    test_std_type_registration.cpp
    test_switch.cpp
    test_tsl_nodes.cpp
    test_tss_nodes.cpp
)

hgraph_add_test_objects(hgraph_value_test_objects
    test_any_value.cpp
    test_bundle_builder.cpp
    test_compact_storage.cpp
    test_intern_table.cpp
    test_memory_utils.cpp
    test_mutable_list.cpp
    test_mutable_map.cpp
    test_mutable_set.cpp
    test_plan_factories.cpp
    test_runtime_value_view.cpp
    test_schema_examples.cpp
    test_scope.cpp
    test_slot_utils.cpp
    test_specialized_views.cpp
    test_static_schema.cpp
    test_time_series_reference.cpp
    test_ts_delta.cpp
    test_ts_input.cpp
    test_ts_output.cpp
    test_ts_type_ref.cpp
    test_ts_schemas.cpp
    test_type_erasure_layout.cpp
    test_type_record.cpp
    test_type_pointer.cpp
    test_tsd_proxy.cpp
    test_type_registry.cpp
    test_value.cpp
    test_value_types.cpp
)

hgraph_add_test_objects(hgraph_record_test_objects
    test_json.cpp
    test_record_replay.cpp
    test_record_replay_config.cpp
    test_record_replay_frame.cpp
    test_table.cpp
)

set(HGRAPH_TEST_OBJECT_LIBS
    hgraph_runtime_test_objects
    hgraph_wiring_test_objects
    hgraph_stdlib_test_objects
    hgraph_value_test_objects
    hgraph_record_test_objects
)

function(hgraph_add_test_executable target_name)
    add_executable(${target_name}
        $<TARGET_OBJECTS:hgraph_test_listener_objects>
        ${ARGN}
    )
    target_link_libraries(${target_name}
        PRIVATE
            hgraph::core
            Catch2::Catch2WithMain
    )
    hgraph_enable_private_pch(${target_name})
endfunction()

hgraph_add_test_executable(hgraph_unit_tests
    $<TARGET_OBJECTS:hgraph_runtime_test_objects>
    $<TARGET_OBJECTS:hgraph_wiring_test_objects>
    $<TARGET_OBJECTS:hgraph_stdlib_test_objects>
    $<TARGET_OBJECTS:hgraph_value_test_objects>
    $<TARGET_OBJECTS:hgraph_record_test_objects>
)

foreach(_hgraph_test_domain IN LISTS HGRAPH_TEST_OBJECT_LIBS)
    string(REPLACE "hgraph_" "" _hgraph_domain_name "${_hgraph_test_domain}")
    string(REPLACE "_test_objects" "" _hgraph_domain_name "${_hgraph_domain_name}")
    hgraph_add_test_executable("hgraph_unit_tests_${_hgraph_domain_name}"
        $<TARGET_OBJECTS:${_hgraph_test_domain}>
    )
endforeach()

add_executable(hgraph_json_perf
    json_perf.cpp
)

target_link_libraries(hgraph_json_perf
    PRIVATE
        hgraph::core
)

hgraph_enable_private_pch(hgraph_json_perf)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # json_perf.cpp replaces the global operator new/delete with malloc/free
    # to count allocations; GCC pairs an inlined replaced new with the free
    # inside the replaced delete and reports a spurious mismatched-new-delete.
    target_compile_options(hgraph_json_perf PRIVATE -Wno-mismatched-new-delete)
endif()

add_executable(hgraph_type_erasure_perf
    type_erasure_perf.cpp
)

target_link_libraries(hgraph_type_erasure_perf
    PRIVATE
        hgraph::core
)

hgraph_enable_private_pch(hgraph_type_erasure_perf)

include(Catch)
catch_discover_tests(hgraph_unit_tests)

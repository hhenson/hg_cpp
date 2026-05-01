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

# Catch2 for the ported utility test suites.
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
    test_intern_table.cpp
    test_memory_utils.cpp
    test_plan_factories.cpp
    test_schema_examples.cpp
    test_slot_utils.cpp
    test_ts_schemas.cpp
    test_type_registry.cpp
)

target_link_libraries(hgraph_unit_tests
    PRIVATE
        hgraph::core
        Catch2::Catch2WithMain
)

include(Catch)
catch_discover_tests(hgraph_unit_tests)

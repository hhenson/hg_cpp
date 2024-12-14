project(hgraph_test)

find_package(Catch2 CONFIG REQUIRED)

enable_testing()


set(HGRAPH_UNIT_TEST_SOURCES
        ./test_hgraph.cpp
        python/test_hgraph.cpp
)

add_executable(${PROJECT_NAME} ${HGRAPH_UNIT_TEST_SOURCES} ${HGRAPH_INCLUDES})


target_link_libraries(${PROJECT_NAME} PRIVATE _hgraph Threads::Threads Catch2::Catch2 Catch2::Catch2WithMain
        pybind11::embed fmt::fmt spdlog::spdlog)


include(CTest)
include(Catch)

catch_discover_tests(${PROJECT_NAME})

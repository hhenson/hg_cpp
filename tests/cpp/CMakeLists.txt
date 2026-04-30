add_executable(hgraph_smoke_test
    smoke_test.cpp
)

target_link_libraries(hgraph_smoke_test
    PRIVATE
        hgraph::core
)

add_test(NAME hgraph.smoke COMMAND hgraph_smoke_test)

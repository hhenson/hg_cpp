set(HGRAPH_CORE_SOURCES
    hgraph/version.cpp
    hgraph/types/metadata/type_registry.cpp
    hgraph/types/metadata/ts_value_plan_factory.cpp
    hgraph/types/metadata/value_plan_factory.cpp
    hgraph/types/time_series_reference.cpp
)

add_library(hgraph_core ${HGRAPH_CORE_TARGET_TYPE} ${HGRAPH_CORE_SOURCES})

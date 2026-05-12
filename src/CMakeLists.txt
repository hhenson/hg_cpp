set(HGRAPH_CORE_SOURCES
    hgraph/version.cpp
    hgraph/types/metadata/type_registry.cpp
    hgraph/types/metadata/ts_data_atomic_ops.cpp
    hgraph/types/metadata/ts_data_fixed_structured_ops.cpp
    hgraph/types/metadata/ts_data_fixed_structured_plan.cpp
    hgraph/types/metadata/ts_data_slot_ops.cpp
    hgraph/types/metadata/ts_data_window_ops.cpp
    hgraph/types/metadata/ts_data_plan_factory.cpp
    hgraph/types/metadata/value_plan_factory.cpp
    hgraph/types/time_series/ts_data/base_view.cpp
    hgraph/types/time_series/ts_data/dict_view.cpp
    hgraph/types/time_series/ts_data/indexed_view.cpp
    hgraph/types/time_series/ts_data/ops.cpp
    hgraph/types/time_series/ts_data/set_view.cpp
    hgraph/types/time_series/ts_data/storage.cpp
    hgraph/types/time_series/ts_data/types.cpp
    hgraph/types/time_series/ts_data/window_view.cpp
    hgraph/types/time_series/ts_output.cpp
    hgraph/types/time_series_reference.cpp
    hgraph/types/value/compact_container_ops.cpp
    hgraph/types/value/value_view.cpp
)

# Public headers — listed so CLion (and other IDEs) pick them up under the
# library target. They are not compiled directly; the build relies on
# include directories. ``CMAKE_CURRENT_SOURCE_DIR/../include`` is already
# on the include path via the parent CMakeLists.
set(HGRAPH_CORE_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/hgraph.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/hgraph_export.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/notifiable.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/static_schema.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/time_series_reference.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/metadata/ts_data_plan_factory.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/metadata/ts_value_type_meta_data.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/metadata/type_binding.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/metadata/type_meta_data.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/metadata/type_registry.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/metadata/value_plan_factory.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/metadata/value_type_meta_data.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/utils/intern_table.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/utils/key_slot_store.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/utils/memory_utils.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/utils/slot_observer.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/utils/stable_slot_storage.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/utils/value_slot_store.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/time_series/ts_data.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/time_series/ts_data/base_view.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/time_series/ts_data/dict_view.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/time_series/ts_data/indexed_view.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/time_series/ts_data/ops.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/time_series/ts_data/set_view.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/time_series/ts_data/storage.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/time_series/ts_data/types.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/time_series/ts_data/window_view.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/time_series/ts_output.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/value/compact_container_ops.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/value/compact_storage.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/value/container_ops.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/value/specialized_views.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/value/value.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/value/value_builder.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/value/value_ops.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/value/value_range.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/types/value/value_view.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/util/date_time.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/util/scope.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/hgraph/util/tagged_ptr.h
)

add_library(hgraph_core ${HGRAPH_CORE_TARGET_TYPE} ${HGRAPH_CORE_SOURCES} ${HGRAPH_CORE_HEADERS})

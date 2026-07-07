set(HGRAPH_RUNTIME_SOURCES
    hgraph/version.cpp
    hgraph/runtime/context_node.cpp
    hgraph/runtime/executor.cpp
    hgraph/runtime/feedback_node.cpp
    hgraph/runtime/global_state.cpp
    hgraph/runtime/graph.cpp
    hgraph/runtime/logger.cpp
    hgraph/runtime/map_node.cpp
    hgraph/runtime/mesh_node.cpp
    hgraph/runtime/nested_graph_node.cpp
    hgraph/runtime/node.cpp
    hgraph/runtime/node_error.cpp
    hgraph/runtime/push_source_node.cpp
    hgraph/runtime/race_tsd_node.cpp
    hgraph/runtime/reduce_node.cpp
    hgraph/runtime/service_node.cpp
    hgraph/runtime/shared_output_node.cpp
    hgraph/runtime/switch_node.cpp
    hgraph/runtime/try_except_node.cpp
    hgraph/types/frame.cpp
    hgraph/types/metadata/ts_data_atomic_ops.cpp
    hgraph/types/metadata/ts_data_dynamic_list_ops.cpp
    hgraph/types/metadata/ts_data_fixed_structured_ops.cpp
    hgraph/types/metadata/ts_data_fixed_structured_plan.cpp
    hgraph/types/metadata/ts_data_plan_factory.cpp
    hgraph/types/metadata/ts_data_slot_ops.cpp
    hgraph/types/metadata/ts_data_window_ops.cpp
    hgraph/types/metadata/type_registry.cpp
    hgraph/types/metadata/value_plan_factory.cpp
    hgraph/types/primitive_types.cpp
    hgraph/types/time_series/endpoint_owner.cpp
    hgraph/types/time_series/endpoint_schema.cpp
    hgraph/types/time_series/ts_data/base_view.cpp
    hgraph/types/time_series/ts_data/dict_view.cpp
    hgraph/types/time_series/ts_data/indexed_view.cpp
    hgraph/types/time_series/ts_data/ops.cpp
    hgraph/types/time_series/ts_data/proxy.cpp
    hgraph/types/time_series/ts_data/set_view.cpp
    hgraph/types/time_series/ts_data/storage.cpp
    hgraph/types/time_series/ts_data/types.cpp
    hgraph/types/time_series/ts_data/window_view.cpp
    hgraph/types/time_series/ts_delta.cpp
    hgraph/types/time_series/ts_input.cpp
    hgraph/types/time_series/ts_input/base_view.cpp
    hgraph/types/time_series/ts_input/bundle_view.cpp
    hgraph/types/time_series/ts_input/dict_view.cpp
    hgraph/types/time_series/ts_input/list_view.cpp
    hgraph/types/time_series/ts_input/set_view.cpp
    hgraph/types/time_series/ts_input/target_link.cpp
    hgraph/types/time_series/ts_input/target_link_ops.cpp
    hgraph/types/time_series/ts_input/window_view.cpp
    hgraph/types/time_series/ts_output.cpp
    hgraph/types/time_series/ts_output/alternative.cpp
    hgraph/types/time_series/ts_output/base_view.cpp
    hgraph/types/time_series/ts_output/bundle_view.cpp
    hgraph/types/time_series/ts_output/dict_view.cpp
    hgraph/types/time_series/ts_output/list_view.cpp
    hgraph/types/time_series/ts_output/mutation_view.cpp
    hgraph/types/time_series/ts_output/set_view.cpp
    hgraph/types/time_series/ts_output/window_view.cpp
    hgraph/types/time_series_reference.cpp
    hgraph/types/value/any_ops.cpp
    hgraph/types/value/compact_container_ops.cpp
    hgraph/types/value/json_codec.cpp
    hgraph/types/value/table_codec.cpp
    hgraph/types/value/value_view.cpp
    hgraph/util/date_time.cpp
)

set(HGRAPH_WIRING_SOURCES
    hgraph/types/graph_wiring.cpp
    hgraph/types/operator_dispatch.cpp
    hgraph/types/record_replay.cpp
    hgraph/types/registry_reset.cpp
    hgraph/types/service_runtime.cpp
    hgraph/types/type_pattern.cpp
)

set(HGRAPH_STDLIB_SOURCES
    hgraph/lib/std/operators/arithmetic_impl.cpp
    hgraph/lib/std/operators/collection_impl.cpp
    hgraph/lib/std/operators/comparison_impl.cpp
    hgraph/lib/std/operators/container_impl.cpp
    hgraph/lib/std/operators/control_impl.cpp
    hgraph/lib/std/operators/conversion_impl.cpp
    hgraph/lib/std/operators/higher_order_impl.cpp
    hgraph/lib/std/operators/io_impl.cpp
    hgraph/lib/std/operators/json_impl.cpp
    hgraph/lib/std/operators/logical_impl.cpp
    hgraph/lib/std/operators/record_replay_frame_impl.cpp
    hgraph/lib/std/operators/registration.cpp
    hgraph/lib/std/operators/stream_impl.cpp
    hgraph/lib/std/operators/string_impl.cpp
    hgraph/lib/std/operators/table_impl.cpp
    hgraph/lib/std/operators/temporal_impl.cpp
)

add_library(hgraph_runtime ${HGRAPH_CORE_TARGET_TYPE} ${HGRAPH_RUNTIME_SOURCES})
add_library(hgraph_wiring ${HGRAPH_CORE_TARGET_TYPE} ${HGRAPH_WIRING_SOURCES})
add_library(hgraph_stdlib ${HGRAPH_CORE_TARGET_TYPE} ${HGRAPH_STDLIB_SOURCES})
add_library(hgraph_core INTERFACE)

add_library(hgraph::runtime ALIAS hgraph_runtime)
add_library(hgraph::wiring ALIAS hgraph_wiring)
add_library(hgraph::stdlib ALIAS hgraph_stdlib)

target_link_libraries(hgraph_runtime PUBLIC hgraph::options)
target_link_libraries(hgraph_wiring PUBLIC hgraph_runtime hgraph::options)
target_link_libraries(hgraph_stdlib PUBLIC hgraph_wiring hgraph::options)
target_link_libraries(hgraph_core INTERFACE hgraph_stdlib hgraph::options)

hgraph_enable_private_pch(hgraph_runtime)
hgraph_enable_private_pch(hgraph_wiring)
hgraph_enable_private_pch(hgraph_stdlib)

set_target_properties(hgraph_runtime PROPERTIES EXPORT_NAME runtime)
set_target_properties(hgraph_wiring PROPERTIES EXPORT_NAME wiring)
set_target_properties(hgraph_stdlib PROPERTIES EXPORT_NAME stdlib)

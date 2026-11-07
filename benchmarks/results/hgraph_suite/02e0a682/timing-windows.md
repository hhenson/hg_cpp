# HG versus hgraph: timing

- HG: hg-cpp `02e0a682+hgraph_suite`
- hgraph: `7e45748c9`
- Platform: Windows 11 AMD64, AMD64 Family 26 Model 112 Stepping 0, AuthenticAMD, Python 3.14.7
- Samples: 5 per cell, cycle scale 1.0, size scale 1.0

Cells show the median complete `run_graph` time ± median absolute deviation.
`hgraph/HG` above 1 means HG is faster.

| Group | Scenario | Fidelity | hgraph | HG | hgraph/HG |
| --- | --- | --- | ---: | ---: | ---: |
| Graph construction | Wide/deep graph - native operators (`construct_std`) | same | 0.0623 s ±0.0012 | 0.1521 s ±0.0010 | 0.41x |
| Graph construction | Wide/deep graph - Python nodes (`construct_py`) | same | 0.1271 s ±0.0017 | 0.1730 s ±0.0016 | 0.73x |
| Graph construction | Layered map_ of Python children with nested switch_ (`construct_higher_order_py`) | adapted | 0.0463 s ±0.0021 | 0.0281 s ±0.0009 | 1.65x |
| Graph construction | dispatch with many registered cases (`construct_dispatch_cases`) | adapted | 0.2943 s ±0.0214 | 0.2318 s ±0.0021 | 1.27x |
| Graph construction | Operator with many overloads at many call sites (`construct_overloads`) | same | 0.0278 s ±0.0010 | 0.0237 s ±0.0002 | 1.17x |
| Scheduler | Feedback hot loop - native add (`tick_std`) | adapted | 0.0299 s ±0.0004 | 0.1407 s ±0.0009 | 0.21x |
| Scheduler | Five-node Python compute chain (`tick_py`) | same | 0.0264 s ±0.0003 | 0.1028 s ±0.0014 | 0.26x |
| Scheduler | One source fanning out to many sinks (`scheduler_fan_out_std`) | same | 0.0748 s ±0.0022 | 0.0403 s ±0.0004 | 1.85x |
| Scheduler | Many branches joining one output (`scheduler_fan_in_std`) | same | 0.1020 s ±0.0014 | 0.0640 s ±0.0001 | 1.59x |
| Scheduler | Eight notifications conflated into one reducer (`scheduler_conflated_fixed_tsl_std`) | adapted | 0.0495 s ±0.0002 | 0.0349 s ±0.0001 | 1.42x |
| Python boundary | Python scalar generator to native sink (`python_generator_boundary`) | same | 0.0065 s ±0.0001 | 0.0162 s ±0.0001 | 0.40x |
| Python boundary | Python scalar generator to Python sink (`python_sink_boundary`) | same | 0.0156 s ±0.0001 | 0.0234 s ±0.0001 | 0.67x |
| Python boundary | Python compute with injected GlobalState (`python_global_state_boundary`) | unsupported | 0.0259 s ±0.0004 | N/A |  |
| Value types | Integer arithmetic (`type_int_std`) | same | 0.0117 s ±0.0003 | 0.0209 s ±0.0004 | 0.56x |
| Value types | Floating-point arithmetic (`type_float_std`) | same | 0.0119 s ±0.0002 | 0.0211 s ±0.0003 | 0.56x |
| Value types | String concatenation (`type_str_std`) | same | 0.0146 s ±0.0007 | 0.0323 s ±0.0003 | 0.45x |
| Value types | CompoundScalar field access - native operators (`type_cs_std`) | adapted | 0.0164 s ±0.0002 | 0.0536 s ±0.0002 | 0.31x |
| Value types | CompoundScalar crossing Python nodes (`type_cs_py`) | same | 0.0208 s ±0.0001 | 0.1078 s ±0.0007 | 0.19x |
| Python-owned structured scalars | Whole-object pass-through - native layout (`python_owned_pass_through_native`) | same | 0.0091 s ±0.0001 | 0.0466 s ±0.0002 | 0.19x |
| Python-owned structured scalars | Whole-object pass-through - Python-owned (`python_owned_pass_through_python`) | same | 0.0082 s ±0.0001 | 0.0236 s ±0.0002 | 0.35x |
| Python-owned structured scalars | One field projection - native layout (`python_owned_project_one_native`) | same | 0.0120 s ±0.0002 | 0.0489 s ±0.0001 | 0.25x |
| Python-owned structured scalars | One field projection - Python-owned (`python_owned_project_one_python`) | same | 0.0129 s ±0.0000 | 0.0375 s ±0.0004 | 0.34x |
| Python-owned structured scalars | Three field projections - native layout (`python_owned_project_several_native`) | same | 0.0183 s ±0.0003 | 0.0533 s ±0.0005 | 0.34x |
| Python-owned structured scalars | Three field projections - Python-owned (`python_owned_project_several_python`) | same | 0.0236 s ±0.0001 | 0.0713 s ±0.0007 | 0.33x |
| Python-owned structured scalars | Construction from fields - native layout (`python_owned_construct_native`) | adapted | 0.0215 s ±0.0004 | 0.2098 s ±0.0015 | 0.10x |
| Python-owned structured scalars | Construction from fields - Python-owned (`python_owned_construct_python`) | unsupported | 0.0374 s ±0.0001 | N/A |  |
| Python-owned structured scalars | Equality and deduplication - native layout (`python_owned_dedup_native`) | same | 0.0129 s ±0.0001 | 0.0533 s ±0.0007 | 0.24x |
| Python-owned structured scalars | Equality and deduplication - Python-owned (`python_owned_dedup_python`) | same | 0.0175 s ±0.0004 | 0.0318 s ±0.0002 | 0.55x |
| Value types | Bundle with partial field updates (`type_tsb_partial_fields_std`) | same | 0.0287 s ±0.0002 | 0.0338 s ±0.0003 | 0.85x |
| Value types | Tick window append and eviction (`type_tsw_append_evict_std`) | unsupported | 0.0104 s ±0.0002 | N/A |  |
| Value types | Set add/remove deltas (`tss_add_remove_std`) | same | 0.0139 s ±0.0001 | 0.0136 s ±0.0003 | 1.02x |
| TSD - dense | Map and reduce - native child graph (`tsd_dense_std`) | same | 0.0915 s ±0.0002 | 0.2322 s ±0.0019 | 0.39x |
| TSD - dense | Map and reduce - Python map child (`tsd_dense_py`) | same | 0.1007 s ±0.0001 | 0.4410 s ±0.0120 | 0.23x |
| TSD - dense | Source only (`tsd_dense_source_std`) | same | 0.0162 s ±0.0001 | 0.0860 s ±0.0000 | 0.19x |
| TSD - dense | Map only (`tsd_dense_map_std`) | same | 0.0828 s ±0.0001 | 0.1352 s ±0.0013 | 0.61x |
| TSD - dense | Reduce only (`tsd_dense_reduce_std`) | same | 0.0256 s ±0.0005 | 0.1293 s ±0.0023 | 0.20x |
| TSD - dense | String-key map and reduce (`tsd_dense_strkeys_std`) | same | 0.0980 s ±0.0008 | 0.3004 s ±0.0254 | 0.33x |
| TSD - sparse | Map and reduce with five updates per cycle (`tsd_sparse_std`) | same | 0.0394 s ±0.0008 | 1.9961 s ±0.0270 | 0.02x |
| TSD - sparse | Source only (`tsd_sparse_source_std`) | same | 0.0041 s ±0.0000 | 0.0083 s ±0.0001 | 0.50x |
| TSD - sparse | Map only (`tsd_sparse_map_std`) | same | 0.0304 s ±0.0011 | 0.0174 s ±0.0002 | 1.75x |
| TSD - sparse | Reduce only (`tsd_sparse_reduce_std`) | same | 0.0116 s ±0.0002 | 0.0161 s ±0.0005 | 0.72x |
| TSD - sparse | Large retained capacity with two updates per cycle (`tsd_sparse_large_capacity_std`) |  | 0.5430 s ±0.0121 | FAIL |  |
| TSD - key lifecycle | Map and reduce with key replacement (`tsd_churn_std`) | same | 0.0696 s ±0.0007 | 0.7043 s ±0.0079 | 0.10x |
| TSD - key lifecycle | Python map with key replacement (`tsd_churn_py`) | same | 0.0592 s ±0.0014 | 0.7018 s ±0.0138 | 0.08x |
| TSD - key lifecycle | Key replacement - source only (`tsd_churn_source_std`) | same | 0.0053 s ±0.0000 | 0.0096 s ±0.0001 | 0.55x |
| TSD - key lifecycle | Key replacement - map only (`tsd_churn_map_std`) | same | 0.0613 s ±0.0010 | 0.0343 s ±0.0004 | 1.79x |
| TSD - key lifecycle | Key replacement - reduce only (`tsd_churn_reduce_std`) | same | 0.0118 s ±0.0001 | 0.0174 s ±0.0002 | 0.68x |
| TSD - key lifecycle | Monotonic key growth across capacity boundaries (`tsd_capacity_growth_std`) | same | 0.0413 s ±0.0009 | 12.5739 s ±0.0937 | 0.00x |
| TSD - key lifecycle | Full clear followed by repopulation (`tsd_clear_repopulate_std`) | same | 0.7207 s ±0.0188 | 1.1061 s ±0.0186 | 0.65x |
| TSD - key lifecycle | Remove and later recreate the same keys (`tsd_key_reactivation_std`) | same | 0.0372 s ±0.0004 | 0.6446 s ±0.0100 | 0.06x |
| TSD - key lifecycle | Two-input map with union membership (`tsd_two_input_union_std`) | same | 0.0562 s ±0.0009 | 1.4253 s ±0.0035 | 0.04x |
| TSD - key lifecycle | Two-input map with intersection membership (`tsd_two_input_intersection_std`) | adapted | 0.0300 s ±0.0008 | 0.4705 s ±0.0016 | 0.06x |
| TSD - key lifecycle | Map driven by an explicit key set (`tsd_explicit_key_set_std`) | adapted | 0.0248 s ±0.0002 | 0.6999 s ±0.0027 | 0.04x |
| Reduce | Dense TSD with nested graph combiner (`reduce_tsd_nested_graph_std`) | same | 0.0645 s ±0.0011 | 0.1257 s ±0.0002 | 0.51x |
| Reduce | Dense TSD with Python node combiner (`reduce_tsd_python_combiner`) | same | 0.0958 s ±0.0008 | 0.4324 s ±0.0100 | 0.22x |
| Reduce | Ordered non-associative fixed-list reduction (`reduce_fixed_tsl_ordered_std`) | unsupported | 0.0225 s ±0.0000 | N/A |  |
| Reduce | Empty/singleton/two-value TSD without zero (`reduce_tsd_without_zero_std`) | same | 0.0398 s ±0.0005 | 0.0371 s ±0.0003 | 1.07x |
| C++-first - dynamic TSL | Sparse map and reduce over an unbounded list (`reduce_dynamic_tsl_std`) | unsupported | 0.0219 s ±0.0004 | N/A |  |
| Nested graphs | Switch alternating small and large branches (`switch_alternating_branch_sizes_std`) | adapted | 0.2191 s ±0.0029 | 0.0584 s ±0.0004 | 3.75x |
| Nested graphs | Switch returning a churning keyed collection (`switch_keyed_collection_std`) | adapted | 1.1003 s ±0.0029 | 1.1804 s ±0.0060 | 0.93x |
| Nested graphs | Mesh with predecessor dependencies and key churn (`mesh_std`) | adapted | 0.0312 s ±0.0004 | 0.0468 s ±0.0002 | 0.67x |
| Services | Reference service - native implementation (`service_reference_std`) | same | 0.0109 s ±0.0001 | 0.0165 s ±0.0002 | 0.66x |
| Services | Reference service - Python implementation (`service_reference_py`) | same | 0.0161 s ±0.0002 | 0.0338 s ±0.0004 | 0.48x |
| Services | Request/reply service - native implementation (`service_request_reply_std`) | same | 0.0495 s ±0.0019 | 0.0404 s ±0.0002 | 1.23x |
| Services | Request/reply service - Python implementation (`service_request_reply_py`) | same | 0.0503 s ±0.0007 | 0.0755 s ±0.0022 | 0.67x |
| Services | Request/reply service across multiple paths (`service_request_reply_multiple_paths_std`) | same | 0.0343 s ±0.0004 | 0.0255 s ±0.0002 | 1.35x |
| Services | Subscription service - native implementation (`service_subscription_std`) | unsupported | 0.0641 s ±0.0007 | N/A |  |
| Services | Subscription service - Python implementation (`service_subscription_py`) | unsupported | 0.0851 s ±0.0008 | N/A |  |
| Adaptors | Duplex adaptor - native implementation (`adaptor_std`) | unsupported | 0.0107 s ±0.0000 | N/A |  |
| Adaptors | Duplex adaptor - Python implementation (`adaptor_py`) | unsupported | 0.0136 s ±0.0001 | N/A |  |
| Adaptors | Multiplexed service adaptor - native implementation (`service_adaptor_std`) | unsupported | 0.0492 s ±0.0012 | N/A |  |
| Adaptors | Multiplexed service adaptor - Python implementation (`service_adaptor_py`) | unsupported | 0.0519 s ±0.0003 | N/A |  |
| Audited operators | Scalar-collection convert and collect (`audit_convert_collect_std`) | matched | 0.0317 s ±0.0002 | 0.2771 s ±0.0126 | 0.11x |
| Audited operators | Scalar-map flip/keys/values (`audit_map_transform_std`) | matched | 0.0826 s ±0.0007 | 0.1142 s ±0.0003 | 0.72x |
| Audited operators | TSD getitem with a ticking key (`audit_tsd_getitem_std`) | same | 0.0436 s ±0.0002 | 0.1225 s ±0.0003 | 0.36x |
| Audited operators | Tuple-scalar eq_/cmp_ fallback (`audit_eq_tuple_std`) | matched | 0.0146 s ±0.0002 | 0.0681 s ±0.0009 | 0.21x |
| Audited operators | Regex match_ and replace (`audit_string_match_std`) | same | 0.0161 s ±0.0006 | 0.0208 s ±0.0001 | 0.78x |
| Audited operators | CompoundScalar to_json/from_json round trip (`audit_json_roundtrip_std`) | unsupported | 0.0213 s ±0.0006 | N/A |  |
| Audited operators | race over if_-routed references (`audit_ref_race_std`) | same | 0.0287 s ±0.0002 | 0.0379 s ±0.0016 | 0.76x |
| Audited operators | Buffered stream lag/gate (`audit_stream_buffered_std`) | same | 0.0065 s ±0.0001 | 0.0201 s ±0.0003 | 0.33x |
| Audited operators | Stream take/drop/step counters (`audit_take_drop_std`) | same | 0.0170 s ±0.0002 | 0.0251 s ±0.0000 | 0.68x |
| Audited operators | Running mean accumulator (`audit_running_mean_std`) | adapted | 0.0221 s ±0.0000 | 0.0451 s ±0.0002 | 0.49x |

## Adaptations

- `construct_higher_order_py` (adapted): switch_ cases are @graph definitions instead of lambdas
- `construct_dispatch_cases` (adapted): base-typed source_node replaces const(..., TS[base])
- `tick_std` (adapted): default_(feedback, 0) supplies the initial value; one extra node
- `scheduler_conflated_fixed_tsl_std` (adapted): sum_ over a fixed TSL; HG reduce accepts only TSD
- `type_cs_std` (adapted): HG has no mixed int/float add; ident is converted to float
- `python_owned_construct_native` (adapted): combine builds a TSB that convert turns into the Bundle; price uses convert[TS[float]](ident) * 1.5; label uses str_
- `tsd_two_input_intersection_std` (adapted): membership TSS is an unmatched keyword argument; HG map does not check its name
- `tsd_explicit_key_set_std` (adapted): membership TSS is an unmatched keyword argument; HG map does not check its name
- `switch_alternating_branch_sizes_std` (adapted): adds an empty DEFAULT case for the invalid key at graph start
- `switch_keyed_collection_std` (adapted): adds an empty DEFAULT case for the invalid key at graph start
- `mesh_std` (adapted): switch_ cases are @graph definitions instead of lambdas
- `audit_convert_collect_std` (matched): collect only; HG convert has no scalar-to-Set form
- `audit_map_transform_std` (matched): HG flip accepts TSD; the Dict is converted to TSD before flip
- `audit_eq_tuple_std` (matched): eq only; HG has no cmp_ operator
- `audit_running_mean_std` (adapted): HG spells the running mean as means

## Unsupported and failed

- `python_global_state_boundary` HG: not supported: HG Python does not expose GlobalState
- `python_owned_construct_python` HG: not supported: HG combine builds only TSL and TSB; it cannot construct a Python object scalar
- `type_tsw_append_evict_std` HG: not supported: HG has no tick-window (TSW) time series or to_window
- `tsd_sparse_large_capacity_std` HG: failed: exit 3221225477:
- `reduce_fixed_tsl_ordered_std` HG: not supported: HG reduce requires a TSD and an associative, commutative reducer
- `reduce_dynamic_tsl_std` HG: not supported: HG has no unbounded TSL
- `service_subscription_std` HG: not supported: HG subscription keys require the final guarantee; the scenario retargets the key
- `service_subscription_py` HG: not supported: HG subscription keys require the final guarantee; the scenario retargets the key
- `adaptor_std` HG: not supported: HG has no adaptor API
- `adaptor_py` HG: not supported: HG has no adaptor API
- `service_adaptor_std` HG: not supported: HG has no service adaptor API
- `service_adaptor_py` HG: not supported: HG has no service adaptor API
- `audit_json_roundtrip_std` HG: not supported: HG has no to_json/from_json operators

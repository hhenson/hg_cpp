# HG versus hgraph: timing

- HG: hg-cpp `02e0a682+hgraph_suite`
- hgraph: `7e45748c9`
- Platform: Linux 7.0.0-38-generic x86_64, AMD Ryzen Threadripper 9980X 64-Cores, Python 3.14.4
- Samples: 5 per cell, cycle scale 1.0, size scale 1.0

Cells show the median complete `run_graph` time ± median absolute deviation.
`hgraph/HG` above 1 means HG is faster.

| Group | Scenario | Fidelity | hgraph | HG | hgraph/HG |
| --- | --- | --- | ---: | ---: | ---: |
| Graph construction | Wide/deep graph - native operators (`construct_std`) | same | 0.0318 s ±0.0013 | 0.0616 s ±0.0030 | 0.52x |
| Graph construction | Wide/deep graph - Python nodes (`construct_py`) | same | 0.0681 s ±0.0017 | 0.0714 s ±0.0004 | 0.95x |
| Graph construction | Layered map_ of Python children with nested switch_ (`construct_higher_order_py`) | adapted | 0.0253 s ±0.0005 | 0.0113 s ±0.0001 | 2.24x |
| Graph construction | dispatch with many registered cases (`construct_dispatch_cases`) | adapted | 0.1417 s ±0.0039 | 0.0631 s ±0.0002 | 2.25x |
| Graph construction | Operator with many overloads at many call sites (`construct_overloads`) | same | 0.0155 s ±0.0006 | 0.0090 s ±0.0002 | 1.72x |
| Scheduler | Feedback hot loop - native add (`tick_std`) | adapted | 0.0198 s ±0.0006 | 0.0319 s ±0.0001 | 0.62x |
| Scheduler | Five-node Python compute chain (`tick_py`) | same | 0.0169 s ±0.0000 | 0.0405 s ±0.0007 | 0.42x |
| Scheduler | One source fanning out to many sinks (`scheduler_fan_out_std`) | same | 0.0548 s ±0.0008 | 0.0310 s ±0.0014 | 1.77x |
| Scheduler | Many branches joining one output (`scheduler_fan_in_std`) | same | 0.0720 s ±0.0001 | 0.0510 s ±0.0006 | 1.41x |
| Scheduler | Eight notifications conflated into one reducer (`scheduler_conflated_fixed_tsl_std`) | adapted | 0.0351 s ±0.0005 | 0.0201 s ±0.0002 | 1.75x |
| Python boundary | Python scalar generator to native sink (`python_generator_boundary`) | same | 0.0043 s ±0.0001 | 0.0093 s ±0.0001 | 0.47x |
| Python boundary | Python scalar generator to Python sink (`python_sink_boundary`) | same | 0.0112 s ±0.0000 | 0.0129 s ±0.0006 | 0.86x |
| Python boundary | Python compute with injected GlobalState (`python_global_state_boundary`) | unsupported | 0.0184 s ±0.0004 | N/A |  |
| Value types | Integer arithmetic (`type_int_std`) | same | 0.0079 s ±0.0000 | 0.0122 s ±0.0002 | 0.64x |
| Value types | Floating-point arithmetic (`type_float_std`) | same | 0.0087 s ±0.0002 | 0.0127 s ±0.0001 | 0.69x |
| Value types | String concatenation (`type_str_std`) | same | 0.0100 s ±0.0001 | 0.0234 s ±0.0002 | 0.43x |
| Value types | CompoundScalar field access - native operators (`type_cs_std`) | adapted | 0.0136 s ±0.0004 | 0.0330 s ±0.0008 | 0.41x |
| Value types | CompoundScalar crossing Python nodes (`type_cs_py`) | same | 0.0155 s ±0.0002 | 0.0605 s ±0.0005 | 0.26x |
| Python-owned structured scalars | Whole-object pass-through - native layout (`python_owned_pass_through_native`) | same | 0.0067 s ±0.0002 | 0.0287 s ±0.0002 | 0.23x |
| Python-owned structured scalars | Whole-object pass-through - Python-owned (`python_owned_pass_through_python`) | same | 0.0055 s ±0.0000 | 0.0106 s ±0.0000 | 0.52x |
| Python-owned structured scalars | One field projection - native layout (`python_owned_project_one_native`) | same | 0.0092 s ±0.0001 | 0.0287 s ±0.0005 | 0.32x |
| Python-owned structured scalars | One field projection - Python-owned (`python_owned_project_one_python`) | same | 0.0105 s ±0.0004 | 0.0211 s ±0.0003 | 0.50x |
| Python-owned structured scalars | Three field projections - native layout (`python_owned_project_several_native`) | same | 0.0161 s ±0.0008 | 0.0330 s ±0.0008 | 0.49x |
| Python-owned structured scalars | Three field projections - Python-owned (`python_owned_project_several_python`) | same | 0.0181 s ±0.0000 | 0.0445 s ±0.0004 | 0.41x |
| Python-owned structured scalars | Construction from fields - native layout (`python_owned_construct_native`) | adapted | 0.0160 s ±0.0001 | 0.0472 s ±0.0016 | 0.34x |
| Python-owned structured scalars | Construction from fields - Python-owned (`python_owned_construct_python`) | unsupported | 0.0298 s ±0.0008 | N/A |  |
| Python-owned structured scalars | Equality and deduplication - native layout (`python_owned_dedup_native`) | same | 0.0106 s ±0.0000 | 0.0312 s ±0.0010 | 0.34x |
| Python-owned structured scalars | Equality and deduplication - Python-owned (`python_owned_dedup_python`) | same | 0.0119 s ±0.0001 | 0.0152 s ±0.0007 | 0.79x |
| Value types | Bundle with partial field updates (`type_tsb_partial_fields_std`) | same | 0.0219 s ±0.0006 | 0.0225 s ±0.0006 | 0.97x |
| Value types | Tick window append and eviction (`type_tsw_append_evict_std`) | unsupported | 0.0080 s ±0.0001 | N/A |  |
| Value types | Set add/remove deltas (`tss_add_remove_std`) | same | 0.0098 s ±0.0005 | 0.0086 s ±0.0001 | 1.14x |
| TSD - dense | Map and reduce - native child graph (`tsd_dense_std`) | same | 0.0736 s ±0.0014 | 0.0977 s ±0.0023 | 0.75x |
| TSD - dense | Map and reduce - Python map child (`tsd_dense_py`) | same | 0.0799 s ±0.0005 | 0.1547 s ±0.0007 | 0.52x |
| TSD - dense | Source only (`tsd_dense_source_std`) | same | 0.0157 s ±0.0003 | 0.0296 s ±0.0010 | 0.53x |
| TSD - dense | Map only (`tsd_dense_map_std`) | same | 0.0665 s ±0.0007 | 0.0623 s ±0.0015 | 1.07x |
| TSD - dense | Reduce only (`tsd_dense_reduce_std`) | same | 0.0239 s ±0.0002 | 0.0547 s ±0.0013 | 0.44x |
| TSD - dense | String-key map and reduce (`tsd_dense_strkeys_std`) | same | 0.0757 s ±0.0004 | 0.1198 s ±0.0020 | 0.63x |
| TSD - sparse | Map and reduce with five updates per cycle (`tsd_sparse_std`) | same | 0.0327 s ±0.0004 | 0.5896 s ±0.0013 | 0.06x |
| TSD - sparse | Source only (`tsd_sparse_source_std`) | same | 0.0029 s ±0.0001 | 0.0035 s ±0.0001 | 0.84x |
| TSD - sparse | Map only (`tsd_sparse_map_std`) | same | 0.0244 s ±0.0004 | 0.0086 s ±0.0003 | 2.82x |
| TSD - sparse | Reduce only (`tsd_sparse_reduce_std`) | same | 0.0125 s ±0.0002 | 0.0081 s ±0.0000 | 1.53x |
| TSD - sparse | Large retained capacity with two updates per cycle (`tsd_sparse_large_capacity_std`) | same | 0.6025 s ±0.0048 | 14.8949 s ±0.0674 | 0.04x |
| TSD - key lifecycle | Map and reduce with key replacement (`tsd_churn_std`) | same | 0.0486 s ±0.0013 | 0.4020 s ±0.0055 | 0.12x |
| TSD - key lifecycle | Python map with key replacement (`tsd_churn_py`) | same | 0.0377 s ±0.0001 | 0.3985 s ±0.0064 | 0.09x |
| TSD - key lifecycle | Key replacement - source only (`tsd_churn_source_std`) | same | 0.0038 s ±0.0000 | 0.0047 s ±0.0000 | 0.82x |
| TSD - key lifecycle | Key replacement - map only (`tsd_churn_map_std`) | same | 0.0453 s ±0.0001 | 0.0172 s ±0.0002 | 2.64x |
| TSD - key lifecycle | Key replacement - reduce only (`tsd_churn_reduce_std`) | same | 0.0082 s ±0.0004 | 0.0094 s ±0.0003 | 0.87x |
| TSD - key lifecycle | Monotonic key growth across capacity boundaries (`tsd_capacity_growth_std`) | same | 0.0332 s ±0.0005 | 7.0249 s ±0.2530 | 0.00x |
| TSD - key lifecycle | Full clear followed by repopulation (`tsd_clear_repopulate_std`) | same | 0.4599 s ±0.0010 | 0.6944 s ±0.0116 | 0.66x |
| TSD - key lifecycle | Remove and later recreate the same keys (`tsd_key_reactivation_std`) | same | 0.0266 s ±0.0006 | 0.3745 s ±0.0016 | 0.07x |
| TSD - key lifecycle | Two-input map with union membership (`tsd_two_input_union_std`) | same | 0.0461 s ±0.0020 | 0.4506 s ±0.0076 | 0.10x |
| TSD - key lifecycle | Two-input map with intersection membership (`tsd_two_input_intersection_std`) | adapted | 0.0250 s ±0.0006 | 0.1625 s ±0.0026 | 0.15x |
| TSD - key lifecycle | Map driven by an explicit key set (`tsd_explicit_key_set_std`) | adapted | 0.0178 s ±0.0005 | 0.3969 s ±0.0091 | 0.04x |
| Reduce | Dense TSD with nested graph combiner (`reduce_tsd_nested_graph_std`) | same | 0.0577 s ±0.0004 | 0.0560 s ±0.0006 | 1.03x |
| Reduce | Dense TSD with Python node combiner (`reduce_tsd_python_combiner`) | same | 0.0802 s ±0.0004 | 0.1437 s ±0.0012 | 0.56x |
| Reduce | Ordered non-associative fixed-list reduction (`reduce_fixed_tsl_ordered_std`) | unsupported | 0.0148 s ±0.0001 | N/A |  |
| Reduce | Empty/singleton/two-value TSD without zero (`reduce_tsd_without_zero_std`) | same | 0.0290 s ±0.0002 | 0.0225 s ±0.0008 | 1.29x |
| C++-first - dynamic TSL | Sparse map and reduce over an unbounded list (`reduce_dynamic_tsl_std`) | unsupported | 0.0210 s ±0.0007 | N/A |  |
| Nested graphs | Switch alternating small and large branches (`switch_alternating_branch_sizes_std`) | adapted | 0.1481 s ±0.0013 | 0.0365 s ±0.0015 | 4.06x |
| Nested graphs | Switch returning a churning keyed collection (`switch_keyed_collection_std`) | adapted | 0.7491 s ±0.0048 | 0.6704 s ±0.0091 | 1.12x |
| Nested graphs | Mesh with predecessor dependencies and key churn (`mesh_std`) | adapted | 0.0208 s ±0.0002 | 0.0268 s ±0.0009 | 0.78x |
| Services | Reference service - native implementation (`service_reference_std`) | same | 0.0076 s ±0.0000 | 0.0095 s ±0.0002 | 0.81x |
| Services | Reference service - Python implementation (`service_reference_py`) | same | 0.0122 s ±0.0001 | 0.0161 s ±0.0002 | 0.76x |
| Services | Request/reply service - native implementation (`service_request_reply_std`) | same | 0.0337 s ±0.0008 | 0.0216 s ±0.0001 | 1.56x |
| Services | Request/reply service - Python implementation (`service_request_reply_py`) | same | 0.0376 s ±0.0009 | 0.0337 s ±0.0016 | 1.12x |
| Services | Request/reply service across multiple paths (`service_request_reply_multiple_paths_std`) | same | 0.0257 s ±0.0008 | 0.0133 s ±0.0003 | 1.94x |
| Services | Subscription service - native implementation (`service_subscription_std`) | unsupported | 0.0444 s ±0.0006 | N/A |  |
| Services | Subscription service - Python implementation (`service_subscription_py`) | unsupported | 0.0604 s ±0.0006 | N/A |  |
| Adaptors | Duplex adaptor - native implementation (`adaptor_std`) | unsupported | 0.0084 s ±0.0003 | N/A |  |
| Adaptors | Duplex adaptor - Python implementation (`adaptor_py`) | unsupported | 0.0097 s ±0.0002 | N/A |  |
| Adaptors | Multiplexed service adaptor - native implementation (`service_adaptor_std`) | unsupported | 0.0350 s ±0.0004 | N/A |  |
| Adaptors | Multiplexed service adaptor - Python implementation (`service_adaptor_py`) | unsupported | 0.0370 s ±0.0007 | N/A |  |
| Audited operators | Scalar-collection convert and collect (`audit_convert_collect_std`) | matched | 0.0291 s ±0.0000 | 0.0468 s ±0.0003 | 0.62x |
| Audited operators | Scalar-map flip/keys/values (`audit_map_transform_std`) | matched | 0.0626 s ±0.0007 | 0.0766 s ±0.0006 | 0.82x |
| Audited operators | TSD getitem with a ticking key (`audit_tsd_getitem_std`) | same | 0.0378 s ±0.0003 | 0.0575 s ±0.0008 | 0.66x |
| Audited operators | Tuple-scalar eq_/cmp_ fallback (`audit_eq_tuple_std`) | matched | 0.0122 s ±0.0001 | 0.0315 s ±0.0006 | 0.39x |
| Audited operators | Regex match_ and replace (`audit_string_match_std`) | same | 0.0113 s ±0.0002 | 0.0148 s ±0.0003 | 0.76x |
| Audited operators | CompoundScalar to_json/from_json round trip (`audit_json_roundtrip_std`) | unsupported | 0.0156 s ±0.0004 | N/A |  |
| Audited operators | race over if_-routed references (`audit_ref_race_std`) | same | 0.0224 s ±0.0006 | 0.0244 s ±0.0005 | 0.92x |
| Audited operators | Buffered stream lag/gate (`audit_stream_buffered_std`) | same | 0.0047 s ±0.0001 | 0.0074 s ±0.0004 | 0.64x |
| Audited operators | Stream take/drop/step counters (`audit_take_drop_std`) | same | 0.0144 s ±0.0003 | 0.0119 s ±0.0001 | 1.21x |
| Audited operators | Running mean accumulator (`audit_running_mean_std`) | adapted | 0.0181 s ±0.0004 | 0.0247 s ±0.0006 | 0.73x |

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
- `reduce_fixed_tsl_ordered_std` HG: not supported: HG reduce requires a TSD and an associative, commutative reducer
- `reduce_dynamic_tsl_std` HG: not supported: HG has no unbounded TSL
- `service_subscription_std` HG: not supported: HG subscription keys require the final guarantee; the scenario retargets the key
- `service_subscription_py` HG: not supported: HG subscription keys require the final guarantee; the scenario retargets the key
- `adaptor_std` HG: not supported: HG has no adaptor API
- `adaptor_py` HG: not supported: HG has no adaptor API
- `service_adaptor_std` HG: not supported: HG has no service adaptor API
- `service_adaptor_py` HG: not supported: HG has no service adaptor API
- `audit_json_roundtrip_std` HG: not supported: HG has no to_json/from_json operators

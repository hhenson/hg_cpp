# HG versus hgraph: timing

- HG: hg-cpp `02e0a682+hgraph_suite`
- hgraph: `7e45748c9`
- Platform: Darwin 27.0.0 arm64, Apple M2 Ultra, Python 3.14.7
- Samples: 5 per cell, cycle scale 1.0, size scale 1.0

Cells show the median complete `run_graph` time ± median absolute deviation.
`hgraph/HG` above 1 means HG is faster.

| Group | Scenario | Fidelity | hgraph | HG | hgraph/HG |
| --- | --- | --- | ---: | ---: | ---: |
| Graph construction | Wide/deep graph - native operators (`construct_std`) | same | 0.0372 s ±0.0011 | 0.1404 s ±0.0017 | 0.27x |
| Graph construction | Wide/deep graph - Python nodes (`construct_py`) | same | 0.0839 s ±0.0012 | 0.1492 s ±0.0017 | 0.56x |
| Graph construction | Layered map_ of Python children with nested switch_ (`construct_higher_order_py`) | adapted | 0.0222 s ±0.0004 | 0.0131 s ±0.0001 | 1.70x |
| Graph construction | dispatch with many registered cases (`construct_dispatch_cases`) | adapted | 0.1429 s ±0.0014 | 0.0788 s ±0.0014 | 1.81x |
| Graph construction | Operator with many overloads at many call sites (`construct_overloads`) | same | 0.0166 s ±0.0001 | 0.0313 s ±0.0001 | 0.53x |
| Scheduler | Feedback hot loop - native add (`tick_std`) | adapted | 0.0169 s ±0.0002 | 0.0295 s ±0.0005 | 0.57x |
| Scheduler | Five-node Python compute chain (`tick_py`) | same | 0.0164 s ±0.0002 | 0.0364 s ±0.0004 | 0.45x |
| Scheduler | One source fanning out to many sinks (`scheduler_fan_out_std`) | same | 0.0462 s ±0.0004 | 0.0282 s ±0.0002 | 1.64x |
| Scheduler | Many branches joining one output (`scheduler_fan_in_std`) | same | 0.0645 s ±0.0007 | 0.0504 s ±0.0011 | 1.28x |
| Scheduler | Eight notifications conflated into one reducer (`scheduler_conflated_fixed_tsl_std`) | adapted | 0.0273 s ±0.0002 | 0.0198 s ±0.0004 | 1.38x |
| Python boundary | Python scalar generator to native sink (`python_generator_boundary`) | same | 0.0043 s ±0.0000 | 0.0079 s ±0.0001 | 0.54x |
| Python boundary | Python scalar generator to Python sink (`python_sink_boundary`) | same | 0.0093 s ±0.0001 | 0.0111 s ±0.0002 | 0.84x |
| Python boundary | Python compute with injected GlobalState (`python_global_state_boundary`) | unsupported | 0.0140 s ±0.0003 | N/A |  |
| Value types | Integer arithmetic (`type_int_std`) | same | 0.0071 s ±0.0001 | 0.0119 s ±0.0004 | 0.60x |
| Value types | Floating-point arithmetic (`type_float_std`) | same | 0.0073 s ±0.0001 | 0.0116 s ±0.0001 | 0.63x |
| Value types | String concatenation (`type_str_std`) | same | 0.0082 s ±0.0000 | 0.0208 s ±0.0005 | 0.40x |
| Value types | CompoundScalar field access - native operators (`type_cs_std`) | adapted | 0.0104 s ±0.0001 | 0.0314 s ±0.0003 | 0.33x |
| Value types | CompoundScalar crossing Python nodes (`type_cs_py`) | same | 0.0137 s ±0.0002 | 0.0658 s ±0.0003 | 0.21x |
| Python-owned structured scalars | Whole-object pass-through - native layout (`python_owned_pass_through_native`) | same | 0.0058 s ±0.0001 | 0.0270 s ±0.0005 | 0.22x |
| Python-owned structured scalars | Whole-object pass-through - Python-owned (`python_owned_pass_through_python`) | same | 0.0055 s ±0.0001 | 0.0102 s ±0.0001 | 0.54x |
| Python-owned structured scalars | One field projection - native layout (`python_owned_project_one_native`) | same | 0.0075 s ±0.0001 | 0.0278 s ±0.0005 | 0.27x |
| Python-owned structured scalars | One field projection - Python-owned (`python_owned_project_one_python`) | same | 0.0082 s ±0.0001 | 0.0204 s ±0.0003 | 0.40x |
| Python-owned structured scalars | Three field projections - native layout (`python_owned_project_several_native`) | same | 0.0123 s ±0.0002 | 0.0309 s ±0.0003 | 0.40x |
| Python-owned structured scalars | Three field projections - Python-owned (`python_owned_project_several_python`) | same | 0.0156 s ±0.0002 | 0.0447 s ±0.0001 | 0.35x |
| Python-owned structured scalars | Construction from fields - native layout (`python_owned_construct_native`) | adapted | 0.0125 s ±0.0000 | 0.0488 s ±0.0015 | 0.26x |
| Python-owned structured scalars | Construction from fields - Python-owned (`python_owned_construct_python`) | unsupported | 0.0259 s ±0.0005 | N/A |  |
| Python-owned structured scalars | Equality and deduplication - native layout (`python_owned_dedup_native`) | same | 0.0078 s ±0.0000 | 0.0292 s ±0.0006 | 0.27x |
| Python-owned structured scalars | Equality and deduplication - Python-owned (`python_owned_dedup_python`) | same | 0.0115 s ±0.0001 | 0.0145 s ±0.0003 | 0.79x |
| Value types | Bundle with partial field updates (`type_tsb_partial_fields_std`) | same | 0.0162 s ±0.0001 | 0.0198 s ±0.0002 | 0.82x |
| Value types | Tick window append and eviction (`type_tsw_append_evict_std`) | unsupported | 0.0068 s ±0.0001 | N/A |  |
| Value types | Set add/remove deltas (`tss_add_remove_std`) | same | 0.0096 s ±0.0001 | 0.0090 s ±0.0001 | 1.07x |
| TSD - dense | Map and reduce - native child graph (`tsd_dense_std`) | same | 0.0651 s ±0.0004 | 0.1040 s ±0.0008 | 0.63x |
| TSD - dense | Map and reduce - Python map child (`tsd_dense_py`) | same | 0.0694 s ±0.0006 | 0.1527 s ±0.0011 | 0.45x |
| TSD - dense | Source only (`tsd_dense_source_std`) | same | 0.0139 s ±0.0002 | 0.0348 s ±0.0007 | 0.40x |
| TSD - dense | Map only (`tsd_dense_map_std`) | same | 0.0591 s ±0.0002 | 0.0668 s ±0.0001 | 0.88x |
| TSD - dense | Reduce only (`tsd_dense_reduce_std`) | same | 0.0200 s ±0.0003 | 0.0649 s ±0.0001 | 0.31x |
| TSD - dense | String-key map and reduce (`tsd_dense_strkeys_std`) | same | 0.0685 s ±0.0001 | 0.1318 s ±0.0022 | 0.52x |
| TSD - sparse | Map and reduce with five updates per cycle (`tsd_sparse_std`) | same | 0.0225 s ±0.0001 | 0.5989 s ±0.0034 | 0.04x |
| TSD - sparse | Source only (`tsd_sparse_source_std`) | same | 0.0025 s ±0.0000 | 0.0037 s ±0.0000 | 0.68x |
| TSD - sparse | Map only (`tsd_sparse_map_std`) | same | 0.0164 s ±0.0000 | 0.0089 s ±0.0004 | 1.83x |
| TSD - sparse | Reduce only (`tsd_sparse_reduce_std`) | same | 0.0082 s ±0.0000 | 0.0090 s ±0.0000 | 0.91x |
| TSD - sparse | Large retained capacity with two updates per cycle (`tsd_sparse_large_capacity_std`) | same | 0.3551 s ±0.0042 | 14.8455 s ±0.0644 | 0.02x |
| TSD - key lifecycle | Map and reduce with key replacement (`tsd_churn_std`) | same | 0.0429 s ±0.0004 | 0.4168 s ±0.0009 | 0.10x |
| TSD - key lifecycle | Python map with key replacement (`tsd_churn_py`) | same | 0.0353 s ±0.0001 | 0.4281 s ±0.0042 | 0.08x |
| TSD - key lifecycle | Key replacement - source only (`tsd_churn_source_std`) | same | 0.0031 s ±0.0000 | 0.0047 s ±0.0000 | 0.67x |
| TSD - key lifecycle | Key replacement - map only (`tsd_churn_map_std`) | same | 0.0382 s ±0.0002 | 0.0172 s ±0.0000 | 2.22x |
| TSD - key lifecycle | Key replacement - reduce only (`tsd_churn_reduce_std`) | same | 0.0071 s ±0.0001 | 0.0107 s ±0.0001 | 0.66x |
| TSD - key lifecycle | Monotonic key growth across capacity boundaries (`tsd_capacity_growth_std`) | same | 0.0235 s ±0.0001 | 8.8936 s ±0.0031 | 0.00x |
| TSD - key lifecycle | Full clear followed by repopulation (`tsd_clear_repopulate_std`) | same | 0.3705 s ±0.0008 | 0.8605 s ±0.0015 | 0.43x |
| TSD - key lifecycle | Remove and later recreate the same keys (`tsd_key_reactivation_std`) | same | 0.0232 s ±0.0002 | 0.3871 s ±0.0002 | 0.06x |
| TSD - key lifecycle | Two-input map with union membership (`tsd_two_input_union_std`) | same | 0.0337 s ±0.0005 | 0.4560 s ±0.0026 | 0.07x |
| TSD - key lifecycle | Two-input map with intersection membership (`tsd_two_input_intersection_std`) | adapted | 0.0180 s ±0.0001 | 0.1657 s ±0.0022 | 0.11x |
| TSD - key lifecycle | Map driven by an explicit key set (`tsd_explicit_key_set_std`) | adapted | 0.0142 s ±0.0003 | 0.4096 s ±0.0018 | 0.03x |
| Reduce | Dense TSD with nested graph combiner (`reduce_tsd_nested_graph_std`) | same | 0.0456 s ±0.0008 | 0.0671 s ±0.0006 | 0.68x |
| Reduce | Dense TSD with Python node combiner (`reduce_tsd_python_combiner`) | same | 0.0649 s ±0.0014 | 0.1527 s ±0.0014 | 0.43x |
| Reduce | Ordered non-associative fixed-list reduction (`reduce_fixed_tsl_ordered_std`) | unsupported | 0.0134 s ±0.0002 | N/A |  |
| Reduce | Empty/singleton/two-value TSD without zero (`reduce_tsd_without_zero_std`) | same | 0.0222 s ±0.0000 | 0.0195 s ±0.0002 | 1.14x |
| C++-first - dynamic TSL | Sparse map and reduce over an unbounded list (`reduce_dynamic_tsl_std`) | unsupported | 0.0120 s ±0.0001 | N/A |  |
| Nested graphs | Switch alternating small and large branches (`switch_alternating_branch_sizes_std`) | adapted | 0.1364 s ±0.0013 | 0.0319 s ±0.0002 | 4.28x |
| Nested graphs | Switch returning a churning keyed collection (`switch_keyed_collection_std`) | adapted | 0.6315 s ±0.0024 | 0.6671 s ±0.0042 | 0.95x |
| Nested graphs | Mesh with predecessor dependencies and key churn (`mesh_std`) | adapted | 0.0195 s ±0.0000 | 0.0261 s ±0.0001 | 0.75x |
| Services | Reference service - native implementation (`service_reference_std`) | same | 0.0069 s ±0.0001 | 0.0081 s ±0.0001 | 0.85x |
| Services | Reference service - Python implementation (`service_reference_py`) | same | 0.0097 s ±0.0000 | 0.0140 s ±0.0002 | 0.69x |
| Services | Request/reply service - native implementation (`service_request_reply_std`) | same | 0.0268 s ±0.0003 | 0.0196 s ±0.0004 | 1.36x |
| Services | Request/reply service - Python implementation (`service_request_reply_py`) | same | 0.0285 s ±0.0004 | 0.0289 s ±0.0008 | 0.99x |
| Services | Request/reply service across multiple paths (`service_request_reply_multiple_paths_std`) | same | 0.0192 s ±0.0002 | 0.0117 s ±0.0002 | 1.64x |
| Services | Subscription service - native implementation (`service_subscription_std`) | unsupported | 0.0364 s ±0.0003 | N/A |  |
| Services | Subscription service - Python implementation (`service_subscription_py`) | unsupported | 0.0544 s ±0.0010 | N/A |  |
| Adaptors | Duplex adaptor - native implementation (`adaptor_std`) | unsupported | 0.0064 s ±0.0001 | N/A |  |
| Adaptors | Duplex adaptor - Python implementation (`adaptor_py`) | unsupported | 0.0079 s ±0.0000 | N/A |  |
| Adaptors | Multiplexed service adaptor - native implementation (`service_adaptor_std`) | unsupported | 0.0283 s ±0.0003 | N/A |  |
| Adaptors | Multiplexed service adaptor - Python implementation (`service_adaptor_py`) | unsupported | 0.0299 s ±0.0005 | N/A |  |
| Audited operators | Scalar-collection convert and collect (`audit_convert_collect_std`) | matched | 0.0262 s ±0.0000 | 0.0372 s ±0.0002 | 0.70x |
| Audited operators | Scalar-map flip/keys/values (`audit_map_transform_std`) | matched | 0.0588 s ±0.0002 | 0.0738 s ±0.0007 | 0.80x |
| Audited operators | TSD getitem with a ticking key (`audit_tsd_getitem_std`) | same | 0.0287 s ±0.0001 | 0.0563 s ±0.0005 | 0.51x |
| Audited operators | Tuple-scalar eq_/cmp_ fallback (`audit_eq_tuple_std`) | matched | 0.0105 s ±0.0003 | 0.0302 s ±0.0006 | 0.35x |
| Audited operators | Regex match_ and replace (`audit_string_match_std`) | same | 0.0195 s ±0.0002 | 0.0140 s ±0.0002 | 1.39x |
| Audited operators | CompoundScalar to_json/from_json round trip (`audit_json_roundtrip_std`) | unsupported | 0.0162 s ±0.0002 | N/A |  |
| Audited operators | race over if_-routed references (`audit_ref_race_std`) | same | 0.0157 s ±0.0001 | 0.0226 s ±0.0002 | 0.70x |
| Audited operators | Buffered stream lag/gate (`audit_stream_buffered_std`) | same | 0.0037 s ±0.0000 | 0.0065 s ±0.0001 | 0.58x |
| Audited operators | Stream take/drop/step counters (`audit_take_drop_std`) | same | 0.0108 s ±0.0002 | 0.0110 s ±0.0001 | 0.98x |
| Audited operators | Running mean accumulator (`audit_running_mean_std`) | adapted | 0.0143 s ±0.0001 | 0.0226 s ±0.0001 | 0.63x |

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

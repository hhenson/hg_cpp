# HG versus hgraph: memory

- HG: hg-cpp `02e0a682+hgraph_suite`
- hgraph: `7e45748c9`
- Platform: Windows 11 AMD64, AMD64 Family 26 Model 112 Stepping 0, AuthenticAMD, Python 3.14.7
- Samples: 3 per cell, cycle scale 1.0, size scale 1.0

Growth is peak RSS during the run minus RSS after import and graph-definition construction.
Retained is RSS after teardown and two GC passes minus the same baseline.
`hgraph/HG` above 1 means HG uses less memory.

| Group | Profile | Ready RSS hgraph / HG | Growth hgraph | Growth HG | hgraph/HG | Retained hgraph | Retained HG |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Static graph | Wide/deep native graph - small (`construct_std__small`) | 47 / 34 MB | 2.1 MB ±0.0 | 1.3 MB ±0.0 | 1.66x | 2.1 MB ±0.0 | 1.3 MB ±0.0 |
| Static graph | Wide/deep native graph - medium (`construct_std__medium`) | 47 / 34 MB | 4.2 MB ±0.1 | 2.9 MB ±0.1 | 1.43x | 2.6 MB ±0.0 | 2.1 MB ±0.0 |
| Static graph | Wide/deep native graph - large (`construct_std__large`) | 47 / 34 MB | 5.5 MB ±1.2 | 11.2 MB ±0.2 | 0.49x | 4.3 MB ±0.1 | 2.6 MB ±0.1 |
| Bounded execution | Native scalar hot loop - short (`tick_std__short`) | 47 / 34 MB | 1.4 MB ±0.0 | 1.3 MB ±0.0 | 1.10x | 1.4 MB ±0.0 | 1.3 MB ±0.0 |
| Bounded execution | Native scalar hot loop - medium (`tick_std__medium`) | 47 / 34 MB | 1.4 MB ±0.0 | 1.3 MB ±0.0 | 1.08x | 1.4 MB ±0.0 | 1.3 MB ±0.0 |
| Bounded execution | Native scalar hot loop - long (`tick_std__long`) | 47 / 34 MB | 1.4 MB ±0.0 | 1.3 MB ±0.0 | 1.11x | 1.4 MB ±0.0 | 1.3 MB ±0.0 |
| Bounded execution | Python compute chain - short (`tick_py__short`) | 47 / 34 MB | 1.5 MB ±0.0 | 0.8 MB ±0.0 | 1.79x | 1.5 MB ±0.0 | 0.8 MB ±0.0 |
| Bounded execution | Python compute chain - medium (`tick_py__medium`) | 47 / 34 MB | 1.6 MB ±0.1 | 0.9 MB ±0.0 | 1.80x | 1.6 MB ±0.1 | 0.9 MB ±0.0 |
| Bounded execution | Python compute chain - long (`tick_py__long`) | 47 / 34 MB | 1.5 MB ±0.0 | 0.8 MB ±0.0 | 1.78x | 1.5 MB ±0.0 | 0.8 MB ±0.0 |
| Process lifetime | Repeated small graph - once (`construct_std__repeat_once`) | 47 / 34 MB | 1.8 MB ±0.0 | 1.1 MB ±0.0 | 1.54x | 1.8 MB ±0.0 | 1.1 MB ±0.0 |
| Process lifetime | Repeated small graph - ten (`construct_std__repeat_ten`) | 47 / 34 MB | 1.7 MB ±0.0 | 1.3 MB ±0.1 | 1.37x | 1.7 MB ±0.0 | 1.2 MB ±0.1 |
| Process lifetime | Repeated small graph - hundred (`construct_std__repeat_hundred`) | 47 / 34 MB | 1.8 MB ±0.0 | 1.4 MB ±0.1 | 1.31x | 1.8 MB ±0.0 | 1.4 MB ±0.1 |
| Process lifetime | Repeated novel graph programs - ten (`construct_std__novel_ten`) | 47 / 34 MB | 3.0 MB ±0.0 | 1.8 MB ±0.0 | 1.67x | 3.0 MB ±0.0 | 1.7 MB ±0.1 |
| Process lifetime | Repeated service/adaptor graph - once (`service_adaptor_py__repeat_once`) | 47 / N/A MB | 2.5 MB ±0.1 | N/A |  | 2.5 MB ±0.1 | N/A |
| Process lifetime | Repeated service/adaptor graph - ten (`service_adaptor_py__repeat_ten`) | 47 / N/A MB | 2.6 MB ±0.0 | N/A |  | 2.6 MB ±0.0 | N/A |
| Process lifetime | Repeated service/adaptor graph - fifty (`service_adaptor_py__repeat_fifty`) | 47 / N/A MB | 2.5 MB ±0.0 | N/A |  | 2.5 MB ±0.0 | N/A |
| Value storage | String arithmetic - short (`type_str_std__short`) | 47 / 34 MB | 1.6 MB ±0.0 | 1.1 MB ±0.0 | 1.38x | 1.6 MB ±0.0 | 1.1 MB ±0.0 |
| Value storage | String arithmetic - long (`type_str_std__long`) | 47 / 34 MB | 1.6 MB ±0.0 | 1.1 MB ±0.0 | 1.38x | 1.6 MB ±0.0 | 1.1 MB ±0.0 |
| Value storage | CompoundScalar through Python - short (`type_cs_py__short`) | 47 / 34 MB | 1.7 MB ±0.0 | 1.2 MB ±0.0 | 1.40x | 1.7 MB ±0.0 | 1.2 MB ±0.0 |
| Value storage | CompoundScalar through Python - long (`type_cs_py__long`) | 47 / 34 MB | 1.7 MB ±0.0 | 1.2 MB ±0.0 | 1.39x | 1.7 MB ±0.0 | 1.2 MB ±0.0 |
| Value storage | Fixed tick window - short (`type_tsw_append_evict_std__short`) | 47 / N/A MB | 1.6 MB ±0.0 | N/A |  | 1.6 MB ±0.0 | N/A |
| Value storage | Fixed tick window - medium (`type_tsw_append_evict_std__medium`) | 47 / N/A MB | 1.6 MB ±0.0 | N/A |  | 1.6 MB ±0.0 | N/A |
| Value storage | Fixed tick window - long (`type_tsw_append_evict_std__long`) | 47 / N/A MB | 1.6 MB ±0.0 | N/A |  | 1.6 MB ±0.0 | N/A |
| Value storage | Set add/remove - small (`tss_add_remove_std__small`) | 47 / 34 MB | 1.6 MB ±0.0 | 1.1 MB ±0.0 | 1.47x | 1.6 MB ±0.0 | 1.1 MB ±0.0 |
| Value storage | Set add/remove - medium (`tss_add_remove_std__medium`) | 47 / 34 MB | 1.7 MB ±0.0 | 1.1 MB ±0.0 | 1.54x | 1.7 MB ±0.0 | 1.1 MB ±0.0 |
| Value storage | Set add/remove - large (`tss_add_remove_std__large`) | 47 / 34 MB | 2.0 MB ±0.0 | 1.3 MB ±0.0 | 1.56x | 2.0 MB ±0.0 | 1.3 MB ±0.0 |
| Keyed collections | Dense TSD map/reduce - small (`tsd_dense_std__small`) | 47 / 34 MB | 2.3 MB ±0.0 | 1.7 MB ±0.0 | 1.34x | 2.3 MB ±0.0 | 1.7 MB ±0.0 |
| Keyed collections | Dense TSD map/reduce - medium (`tsd_dense_std__medium`) | 47 / 34 MB | 2.7 MB ±0.0 | 1.9 MB ±0.0 | 1.43x | 2.7 MB ±0.0 | 1.9 MB ±0.0 |
| Keyed collections | Dense TSD map/reduce - large (`tsd_dense_std__large`) | 47 / 34 MB | 3.1 MB ±0.1 | 2.0 MB ±0.0 | 1.51x | 3.1 MB ±0.1 | 2.0 MB ±0.1 |
| Keyed collections | Sparse retained capacity - small (`tsd_sparse_large_capacity_std__small`) | 47 / 34 MB | 2.6 MB ±0.0 | 3.4 MB ±0.0 | 0.77x | 2.6 MB ±0.0 | 1.9 MB ±0.0 |
| Keyed collections | Sparse retained capacity - medium (`tsd_sparse_large_capacity_std__medium`) | 47 / 34 MB | 22.3 MB ±0.0 | 10.5 MB ±0.0 | 2.11x | 2.6 MB ±0.1 | 2.3 MB ±0.1 |
| Keyed collections | Sparse retained capacity - large (`tsd_sparse_large_capacity_std__large`) | 47 / FAIL MB | 81.3 MB ±0.1 | FAIL |  | 3.2 MB ±0.2 | FAIL |
| Keyed collections | Bounded key churn - short (`tsd_churn_std__short`) | 47 / 34 MB | 3.0 MB ±0.1 | 2.5 MB ±0.0 | 1.18x | 2.4 MB ±0.1 | 1.7 MB ±0.0 |
| Keyed collections | Bounded key churn - medium (`tsd_churn_std__medium`) | 47 / 34 MB | 3.0 MB ±0.0 | 3.6 MB ±0.0 | 0.82x | 2.3 MB ±0.0 | 1.8 MB ±0.0 |
| Keyed collections | Bounded key churn - long (`tsd_churn_std__long`) | 47 / 34 MB | 3.0 MB ±0.0 | 5.0 MB ±0.1 | 0.60x | 2.4 MB ±0.1 | 1.8 MB ±0.1 |
| Keyed collections | Monotonic key growth - short (`tsd_capacity_growth_std__short`) | 47 / 34 MB | 2.8 MB ±0.0 | 2.5 MB ±0.0 | 1.10x | 2.8 MB ±0.0 | 2.2 MB ±0.1 |
| Keyed collections | Monotonic key growth - medium (`tsd_capacity_growth_std__medium`) | 47 / 34 MB | 3.0 MB ±0.0 | 6.1 MB ±0.1 | 0.48x | 3.0 MB ±0.0 | 1.8 MB ±0.1 |
| Keyed collections | Monotonic key growth - long (`tsd_capacity_growth_std__long`) | 47 / 34 MB | 11.3 MB ±6.5 | 10.6 MB ±0.1 | 1.06x | 3.6 MB ±0.2 | 2.0 MB ±0.1 |
| Keyed collections | Clear and repopulate - short (`tsd_clear_repopulate_std__short`) | 47 / 34 MB | 6.5 MB ±0.3 | 5.1 MB ±0.4 | 1.28x | 3.0 MB ±0.0 | 2.4 MB ±0.1 |
| Keyed collections | Clear and repopulate - medium (`tsd_clear_repopulate_std__medium`) | 47 / 34 MB | 6.6 MB ±0.0 | 10.8 MB ±0.2 | 0.61x | 2.9 MB ±0.0 | 1.9 MB ±0.1 |
| Keyed collections | Clear and repopulate - long (`tsd_clear_repopulate_std__long`) | 47 / 34 MB | 6.7 MB ±0.1 | 31.4 MB ±0.3 | 0.21x | 2.8 MB ±0.0 | 2.0 MB ±0.0 |
| Keyed collections | Key reactivation - short (`tsd_key_reactivation_std__short`) | 47 / 34 MB | 2.9 MB ±0.0 | 2.1 MB ±0.1 | 1.37x | 2.9 MB ±0.0 | 1.8 MB ±0.1 |
| Keyed collections | Key reactivation - medium (`tsd_key_reactivation_std__medium`) | 47 / 34 MB | 3.0 MB ±0.0 | 2.1 MB ±0.1 | 1.45x | 3.0 MB ±0.0 | 1.7 MB ±0.1 |
| Keyed collections | Key reactivation - long (`tsd_key_reactivation_std__long`) | 47 / 34 MB | 3.0 MB ±0.0 | 2.2 MB ±0.0 | 1.36x | 3.0 MB ±0.0 | 1.8 MB ±0.0 |
| Nested graphs | TSD nested-graph reduce - small (`reduce_tsd_nested_graph_std__small`) | 47 / 34 MB | 2.1 MB ±0.0 | 1.5 MB ±0.0 | 1.42x | 2.1 MB ±0.0 | 1.5 MB ±0.0 |
| Nested graphs | TSD nested-graph reduce - medium (`reduce_tsd_nested_graph_std__medium`) | 47 / 34 MB | 2.2 MB ±0.0 | 1.5 MB ±0.0 | 1.47x | 2.2 MB ±0.0 | 1.5 MB ±0.0 |
| Nested graphs | TSD nested-graph reduce - large (`reduce_tsd_nested_graph_std__large`) | 47 / 34 MB | 2.4 MB ±0.1 | 1.5 MB ±0.0 | 1.55x | 2.4 MB ±0.1 | 1.5 MB ±0.0 |
| Nested graphs | Keyed collection switch - small (`switch_keyed_collection_std__small`) | 47 / 34 MB | 2.7 MB ±0.0 | 2.2 MB ±0.0 | 1.22x | 2.7 MB ±0.0 | 2.0 MB ±0.0 |
| Nested graphs | Keyed collection switch - medium (`switch_keyed_collection_std__medium`) | 47 / 34 MB | 3.4 MB ±0.0 | 2.8 MB ±0.0 | 1.24x | 3.4 MB ±0.0 | 2.4 MB ±0.1 |
| Nested graphs | Keyed collection switch - large (`switch_keyed_collection_std__large`) | 47 / 34 MB | 4.2 MB ±0.0 | 3.2 MB ±0.0 | 1.32x | 3.6 MB ±0.0 | 2.2 MB ±0.0 |
| Nested graphs | Dependency mesh - small (`mesh_std__small`) | 47 / 34 MB | 2.9 MB ±0.0 | 2.0 MB ±0.1 | 1.44x | 2.9 MB ±0.0 | 2.0 MB ±0.1 |
| Nested graphs | Dependency mesh - medium (`mesh_std__medium`) | 47 / 34 MB | 3.4 MB ±0.1 | 2.2 MB ±0.0 | 1.55x | 3.4 MB ±0.1 | 2.2 MB ±0.0 |
| Nested graphs | Dependency mesh - large (`mesh_std__large`) | 47 / 34 MB | 3.2 MB ±0.0 | 1.8 MB ±0.0 | 1.77x | 3.2 MB ±0.0 | 1.8 MB ±0.0 |
| Services | Multiplexed Python service adaptor - small (`service_adaptor_py__small`) | 47 / N/A MB | 2.4 MB ±0.0 | N/A |  | 2.4 MB ±0.0 | N/A |
| Services | Multiplexed Python service adaptor - medium (`service_adaptor_py__medium`) | 47 / N/A MB | 2.5 MB ±0.0 | N/A |  | 2.5 MB ±0.0 | N/A |
| Services | Multiplexed Python service adaptor - large (`service_adaptor_py__large`) | 47 / N/A MB | 2.8 MB ±0.0 | N/A |  | 2.8 MB ±0.0 | N/A |
| C++-first dynamic storage | Dynamic TSL map/reduce - small (`reduce_dynamic_tsl_std__small`) | 47 / N/A MB | 2.4 MB ±0.0 | N/A |  | 2.4 MB ±0.0 | N/A |
| C++-first dynamic storage | Dynamic TSL map/reduce - medium (`reduce_dynamic_tsl_std__medium`) | 47 / N/A MB | 2.8 MB ±0.0 | N/A |  | 2.8 MB ±0.0 | N/A |
| C++-first dynamic storage | Dynamic TSL map/reduce - large (`reduce_dynamic_tsl_std__large`) | 47 / N/A MB | 2.7 MB ±0.0 | N/A |  | 2.7 MB ±0.0 | N/A |

## Unsupported and failed

- `service_adaptor_py__repeat_once` hg: not supported: HG has no service adaptor API
- `service_adaptor_py__repeat_ten` hg: not supported: HG has no service adaptor API
- `service_adaptor_py__repeat_fifty` hg: not supported: HG has no service adaptor API
- `type_tsw_append_evict_std__short` hg: not supported: HG has no tick-window (TSW) time series or to_window
- `type_tsw_append_evict_std__medium` hg: not supported: HG has no tick-window (TSW) time series or to_window
- `type_tsw_append_evict_std__long` hg: not supported: HG has no tick-window (TSW) time series or to_window
- `tsd_sparse_large_capacity_std__large` hg: failed: exit 3221225477:
- `service_adaptor_py__small` hg: not supported: HG has no service adaptor API
- `service_adaptor_py__medium` hg: not supported: HG has no service adaptor API
- `service_adaptor_py__large` hg: not supported: HG has no service adaptor API
- `reduce_dynamic_tsl_std__small` hg: not supported: HG has no unbounded TSL
- `reduce_dynamic_tsl_std__medium` hg: not supported: HG has no unbounded TSL
- `reduce_dynamic_tsl_std__large` hg: not supported: HG has no unbounded TSL

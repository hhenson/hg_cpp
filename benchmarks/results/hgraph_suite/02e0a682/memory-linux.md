# HG versus hgraph: memory

- HG: hg-cpp `02e0a682+hgraph_suite`
- hgraph: `7e45748c9`
- Platform: Linux 7.0.0-38-generic x86_64, AMD Ryzen Threadripper 9980X 64-Cores, Python 3.14.4
- Samples: 3 per cell, cycle scale 1.0, size scale 1.0

Growth is peak RSS during the run minus RSS after import and graph-definition construction.
Retained is RSS after teardown and two GC passes minus the same baseline.
`hgraph/HG` above 1 means HG uses less memory.

| Group | Profile | Ready RSS hgraph / HG | Growth hgraph | Growth HG | hgraph/HG | Retained hgraph | Retained HG |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Static graph | Wide/deep native graph - small (`construct_std__small`) | 99 / 58 MB | 1.5 MB ±0.0 | 1.6 MB ±0.0 | 0.94x | 1.5 MB ±0.0 | 1.6 MB ±0.0 |
| Static graph | Wide/deep native graph - medium (`construct_std__medium`) | 99 / 57 MB | 3.3 MB ±0.0 | 3.1 MB ±0.0 | 1.06x | 3.3 MB ±0.0 | 3.1 MB ±0.0 |
| Static graph | Wide/deep native graph - large (`construct_std__large`) | 99 / 58 MB | 11.0 MB ±0.0 | 9.9 MB ±0.4 | 1.11x | 9.6 MB ±0.0 | 9.5 MB ±0.0 |
| Bounded execution | Native scalar hot loop - short (`tick_std__short`) | 99 / 58 MB | 1.0 MB ±0.0 | 1.1 MB ±0.0 | 0.96x | 1.0 MB ±0.0 | 1.1 MB ±0.0 |
| Bounded execution | Native scalar hot loop - medium (`tick_std__medium`) | 99 / 58 MB | 1.0 MB ±0.0 | 1.1 MB ±0.0 | 0.94x | 1.0 MB ±0.0 | 1.1 MB ±0.0 |
| Bounded execution | Native scalar hot loop - long (`tick_std__long`) | 99 / 58 MB | 1.0 MB ±0.0 | 1.0 MB ±0.0 | 0.97x | 1.0 MB ±0.0 | 1.0 MB ±0.0 |
| Bounded execution | Python compute chain - short (`tick_py__short`) | 99 / 58 MB | 1.0 MB ±0.0 | 0.6 MB ±0.0 | 1.71x | 1.0 MB ±0.0 | 0.6 MB ±0.0 |
| Bounded execution | Python compute chain - medium (`tick_py__medium`) | 99 / 58 MB | 1.0 MB ±0.0 | 0.6 MB ±0.0 | 1.71x | 1.0 MB ±0.0 | 0.6 MB ±0.0 |
| Bounded execution | Python compute chain - long (`tick_py__long`) | 99 / 58 MB | 1.0 MB ±0.0 | 0.6 MB ±0.0 | 1.71x | 1.0 MB ±0.0 | 0.6 MB ±0.0 |
| Process lifetime | Repeated small graph - once (`construct_std__repeat_once`) | 99 / 58 MB | 1.0 MB ±0.0 | 1.0 MB ±0.0 | 0.97x | 1.0 MB ±0.0 | 1.0 MB ±0.0 |
| Process lifetime | Repeated small graph - ten (`construct_std__repeat_ten`) | 99 / 57 MB | 1.0 MB ±0.0 | 1.1 MB ±0.0 | 0.94x | 1.0 MB ±0.0 | 1.1 MB ±0.0 |
| Process lifetime | Repeated small graph - hundred (`construct_std__repeat_hundred`) | 99 / 58 MB | 1.1 MB ±0.0 | 1.1 MB ±0.0 | 0.93x | 1.1 MB ±0.0 | 1.1 MB ±0.0 |
| Process lifetime | Repeated novel graph programs - ten (`construct_std__novel_ten`) | 99 / 58 MB | 2.2 MB ±0.0 | 1.8 MB ±0.0 | 1.19x | 2.2 MB ±0.0 | 1.8 MB ±0.0 |
| Process lifetime | Repeated service/adaptor graph - once (`service_adaptor_py__repeat_once`) | 99 / N/A MB | 1.1 MB ±0.0 | N/A |  | 1.1 MB ±0.0 | N/A |
| Process lifetime | Repeated service/adaptor graph - ten (`service_adaptor_py__repeat_ten`) | 99 / N/A MB | 1.2 MB ±0.0 | N/A |  | 1.2 MB ±0.0 | N/A |
| Process lifetime | Repeated service/adaptor graph - fifty (`service_adaptor_py__repeat_fifty`) | 99 / N/A MB | 1.2 MB ±0.0 | N/A |  | 1.2 MB ±0.0 | N/A |
| Value storage | String arithmetic - short (`type_str_std__short`) | 99 / 58 MB | 1.0 MB ±0.0 | 1.0 MB ±0.0 | 1.01x | 1.0 MB ±0.0 | 1.0 MB ±0.0 |
| Value storage | String arithmetic - long (`type_str_std__long`) | 99 / 57 MB | 1.0 MB ±0.0 | 1.0 MB ±0.0 | 1.02x | 1.0 MB ±0.0 | 1.0 MB ±0.0 |
| Value storage | CompoundScalar through Python - short (`type_cs_py__short`) | 99 / 58 MB | 1.1 MB ±0.0 | 1.1 MB ±0.0 | 1.00x | 1.1 MB ±0.0 | 1.1 MB ±0.0 |
| Value storage | CompoundScalar through Python - long (`type_cs_py__long`) | 99 / 57 MB | 1.1 MB ±0.0 | 1.1 MB ±0.0 | 1.00x | 1.1 MB ±0.0 | 1.1 MB ±0.0 |
| Value storage | Fixed tick window - short (`type_tsw_append_evict_std__short`) | 99 / N/A MB | 0.9 MB ±0.0 | N/A |  | 0.9 MB ±0.0 | N/A |
| Value storage | Fixed tick window - medium (`type_tsw_append_evict_std__medium`) | 99 / N/A MB | 0.9 MB ±0.0 | N/A |  | 0.9 MB ±0.0 | N/A |
| Value storage | Fixed tick window - long (`type_tsw_append_evict_std__long`) | 99 / N/A MB | 0.9 MB ±0.0 | N/A |  | 0.9 MB ±0.0 | N/A |
| Value storage | Set add/remove - small (`tss_add_remove_std__small`) | 99 / 58 MB | 1.1 MB ±0.0 | 0.9 MB ±0.0 | 1.17x | 1.1 MB ±0.0 | 0.9 MB ±0.0 |
| Value storage | Set add/remove - medium (`tss_add_remove_std__medium`) | 99 / 58 MB | 1.1 MB ±0.0 | 0.9 MB ±0.0 | 1.21x | 1.1 MB ±0.0 | 0.9 MB ±0.0 |
| Value storage | Set add/remove - large (`tss_add_remove_std__large`) | 99 / 57 MB | 1.4 MB ±0.0 | 1.1 MB ±0.0 | 1.26x | 1.4 MB ±0.0 | 1.1 MB ±0.0 |
| Keyed collections | Dense TSD map/reduce - small (`tsd_dense_std__small`) | 99 / 58 MB | 1.2 MB ±0.0 | 1.1 MB ±0.0 | 1.12x | 1.2 MB ±0.0 | 1.1 MB ±0.0 |
| Keyed collections | Dense TSD map/reduce - medium (`tsd_dense_std__medium`) | 99 / 58 MB | 1.5 MB ±0.0 | 1.2 MB ±0.0 | 1.24x | 1.5 MB ±0.0 | 1.2 MB ±0.0 |
| Keyed collections | Dense TSD map/reduce - large (`tsd_dense_std__large`) | 99 / 58 MB | 1.6 MB ±0.0 | 1.4 MB ±0.0 | 1.15x | 1.6 MB ±0.0 | 1.4 MB ±0.0 |
| Keyed collections | Sparse retained capacity - small (`tsd_sparse_large_capacity_std__small`) | 99 / 58 MB | 3.3 MB ±0.0 | 2.7 MB ±0.0 | 1.24x | 3.3 MB ±0.0 | 2.7 MB ±0.0 |
| Keyed collections | Sparse retained capacity - medium (`tsd_sparse_large_capacity_std__medium`) | 99 / 57 MB | 20.0 MB ±0.0 | 9.4 MB ±0.0 | 2.11x | 12.2 MB ±0.0 | 9.2 MB ±0.0 |
| Keyed collections | Sparse retained capacity - large (`tsd_sparse_large_capacity_std__large`) | 99 / 58 MB | 76.8 MB ±0.0 | 34.5 MB ±0.0 | 2.22x | 43.4 MB ±0.0 | 33.3 MB ±0.0 |
| Keyed collections | Bounded key churn - short (`tsd_churn_std__short`) | 99 / 58 MB | 1.6 MB ±0.0 | 1.8 MB ±0.0 | 0.88x | 1.6 MB ±0.0 | 1.8 MB ±0.0 |
| Keyed collections | Bounded key churn - medium (`tsd_churn_std__medium`) | 99 / 58 MB | 1.6 MB ±0.0 | 2.9 MB ±0.0 | 0.55x | 1.6 MB ±0.0 | 2.9 MB ±0.0 |
| Keyed collections | Bounded key churn - long (`tsd_churn_std__long`) | 99 / 58 MB | 1.6 MB ±0.0 | 4.2 MB ±0.0 | 0.38x | 1.6 MB ±0.0 | 4.0 MB ±0.0 |
| Keyed collections | Monotonic key growth - short (`tsd_capacity_growth_std__short`) | 99 / 58 MB | 2.4 MB ±0.0 | 1.9 MB ±0.0 | 1.26x | 2.4 MB ±0.0 | 1.9 MB ±0.0 |
| Keyed collections | Monotonic key growth - medium (`tsd_capacity_growth_std__medium`) | 99 / 58 MB | 5.8 MB ±0.1 | 5.2 MB ±0.0 | 1.13x | 5.8 MB ±0.0 | 5.2 MB ±0.0 |
| Keyed collections | Monotonic key growth - long (`tsd_capacity_growth_std__long`) | 99 / 58 MB | 10.1 MB ±0.0 | 9.2 MB ±0.0 | 1.09x | 10.1 MB ±0.0 | 9.2 MB ±0.0 |
| Keyed collections | Clear and repopulate - short (`tsd_clear_repopulate_std__short`) | 99 / 57 MB | 3.8 MB ±0.0 | 3.9 MB ±0.0 | 0.97x | 3.8 MB ±0.0 | 3.8 MB ±0.0 |
| Keyed collections | Clear and repopulate - medium (`tsd_clear_repopulate_std__medium`) | 99 / 57 MB | 5.1 MB ±0.1 | 9.1 MB ±0.0 | 0.56x | 3.8 MB ±0.0 | 8.7 MB ±0.0 |
| Keyed collections | Clear and repopulate - long (`tsd_clear_repopulate_std__long`) | 99 / 57 MB | 5.2 MB ±0.0 | 28.3 MB ±0.0 | 0.18x | 3.8 MB ±0.0 | 26.1 MB ±0.0 |
| Keyed collections | Key reactivation - short (`tsd_key_reactivation_std__short`) | 99 / 58 MB | 1.6 MB ±0.0 | 1.5 MB ±0.0 | 1.06x | 1.6 MB ±0.0 | 1.5 MB ±0.0 |
| Keyed collections | Key reactivation - medium (`tsd_key_reactivation_std__medium`) | 99 / 58 MB | 1.6 MB ±0.0 | 1.5 MB ±0.0 | 1.03x | 1.6 MB ±0.0 | 1.5 MB ±0.0 |
| Keyed collections | Key reactivation - long (`tsd_key_reactivation_std__long`) | 99 / 58 MB | 1.6 MB ±0.0 | 1.6 MB ±0.0 | 1.00x | 1.6 MB ±0.0 | 1.6 MB ±0.0 |
| Nested graphs | TSD nested-graph reduce - small (`reduce_tsd_nested_graph_std__small`) | 99 / 57 MB | 1.1 MB ±0.0 | 1.1 MB ±0.0 | 0.99x | 1.1 MB ±0.0 | 1.1 MB ±0.0 |
| Nested graphs | TSD nested-graph reduce - medium (`reduce_tsd_nested_graph_std__medium`) | 99 / 57 MB | 1.2 MB ±0.0 | 1.1 MB ±0.0 | 1.13x | 1.2 MB ±0.0 | 1.1 MB ±0.0 |
| Nested graphs | TSD nested-graph reduce - large (`reduce_tsd_nested_graph_std__large`) | 99 / 57 MB | 1.4 MB ±0.0 | 1.1 MB ±0.0 | 1.29x | 1.4 MB ±0.0 | 1.1 MB ±0.0 |
| Nested graphs | Keyed collection switch - small (`switch_keyed_collection_std__small`) | 99 / 58 MB | 1.4 MB ±0.0 | 1.6 MB ±0.0 | 0.90x | 1.4 MB ±0.0 | 1.6 MB ±0.0 |
| Nested graphs | Keyed collection switch - medium (`switch_keyed_collection_std__medium`) | 99 / 58 MB | 2.1 MB ±0.0 | 1.9 MB ±0.0 | 1.13x | 2.1 MB ±0.0 | 1.9 MB ±0.0 |
| Nested graphs | Keyed collection switch - large (`switch_keyed_collection_std__large`) | 99 / 58 MB | 2.8 MB ±0.0 | 2.3 MB ±0.0 | 1.18x | 2.8 MB ±0.0 | 2.3 MB ±0.0 |
| Nested graphs | Dependency mesh - small (`mesh_std__small`) | 99 / 58 MB | 1.5 MB ±0.0 | 1.2 MB ±0.0 | 1.28x | 1.5 MB ±0.0 | 1.2 MB ±0.0 |
| Nested graphs | Dependency mesh - medium (`mesh_std__medium`) | 99 / 58 MB | 1.7 MB ±0.0 | 1.3 MB ±0.0 | 1.27x | 1.7 MB ±0.0 | 1.3 MB ±0.0 |
| Nested graphs | Dependency mesh - large (`mesh_std__large`) | 99 / 57 MB | 2.1 MB ±0.0 | 1.5 MB ±0.0 | 1.44x | 2.1 MB ±0.0 | 1.5 MB ±0.0 |
| Services | Multiplexed Python service adaptor - small (`service_adaptor_py__small`) | 99 / N/A MB | 1.1 MB ±0.0 | N/A |  | 1.1 MB ±0.0 | N/A |
| Services | Multiplexed Python service adaptor - medium (`service_adaptor_py__medium`) | 99 / N/A MB | 1.1 MB ±0.0 | N/A |  | 1.1 MB ±0.0 | N/A |
| Services | Multiplexed Python service adaptor - large (`service_adaptor_py__large`) | 99 / N/A MB | 1.4 MB ±0.0 | N/A |  | 1.4 MB ±0.0 | N/A |
| C++-first dynamic storage | Dynamic TSL map/reduce - small (`reduce_dynamic_tsl_std__small`) | 99 / N/A MB | 2.5 MB ±0.0 | N/A |  | 2.5 MB ±0.0 | N/A |
| C++-first dynamic storage | Dynamic TSL map/reduce - medium (`reduce_dynamic_tsl_std__medium`) | 99 / N/A MB | 2.8 MB ±0.0 | N/A |  | 2.8 MB ±0.0 | N/A |
| C++-first dynamic storage | Dynamic TSL map/reduce - large (`reduce_dynamic_tsl_std__large`) | 99 / N/A MB | 3.5 MB ±0.0 | N/A |  | 3.5 MB ±0.0 | N/A |

## Unsupported and failed

- `service_adaptor_py__repeat_once` hg: not supported: HG has no service adaptor API
- `service_adaptor_py__repeat_ten` hg: not supported: HG has no service adaptor API
- `service_adaptor_py__repeat_fifty` hg: not supported: HG has no service adaptor API
- `type_tsw_append_evict_std__short` hg: not supported: HG has no tick-window (TSW) time series or to_window
- `type_tsw_append_evict_std__medium` hg: not supported: HG has no tick-window (TSW) time series or to_window
- `type_tsw_append_evict_std__long` hg: not supported: HG has no tick-window (TSW) time series or to_window
- `service_adaptor_py__small` hg: not supported: HG has no service adaptor API
- `service_adaptor_py__medium` hg: not supported: HG has no service adaptor API
- `service_adaptor_py__large` hg: not supported: HG has no service adaptor API
- `reduce_dynamic_tsl_std__small` hg: not supported: HG has no unbounded TSL
- `reduce_dynamic_tsl_std__medium` hg: not supported: HG has no unbounded TSL
- `reduce_dynamic_tsl_std__large` hg: not supported: HG has no unbounded TSL

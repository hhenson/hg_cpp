#include <hgraph/lib/std/operators/impl/collection_impl.h>
#include <hgraph/lib/std/operators/impl/conversion_impl.h>

namespace hgraph::stdlib
{
    void register_conversion_operators()
    {
        register_overload<const_, const_source>();    // const(value)         -> tick at start
        register_overload<const_, const_delayed>();   // const(value, delay)  -> tick at start + delay
        register_overload<nothing, nothing_source>(); // nothing              -> never ticks

        register_graph_overload<zero_, zero_int>();
        register_graph_overload<zero_, zero_float>();
        register_graph_overload<zero_, zero_str>();

        register_overload<default_, default_impl>();
        register_overload<str_, str_impl>();
        register_overload<convert, convert_identity_impl>();
        register_overload<convert, convert_numeric_impl<Int, Float>>();
        register_overload<convert, convert_numeric_impl<Float, Int>>();
        register_overload<convert, convert_numeric_impl<Int, Bool>>();
        register_overload<convert, convert_numeric_impl<Bool, Int>>();
        register_overload<convert, convert_numeric_impl<Float, Bool>>();
        register_overload<convert, convert_numeric_impl<Bool, Float>>();
        register_overload<convert, convert_to_str_impl<Int>>();
        register_overload<convert, convert_to_str_impl<Float>>();
        register_overload<convert, convert_to_str_impl<Bool>>();
        register_overload<convert, convert_list_to_str_impl>();
        register_overload<convert, convert_list_to_bool_impl>();
        register_overload<convert, convert_date_to_datetime_impl>();
        register_overload<convert, convert_datetime_to_date_impl>();
        register_overload<convert, convert_ts_to_tss_impl>();
        register_overload<convert, convert_ts_to_collection_impl>();
        register_overload<convert, convert_collection_to_collection_impl>();
        register_overload<convert, convert_tss_to_collection_impl>();
        register_overload<convert, convert_collection_to_tss_impl>();
        register_overload<convert, convert_tsd_to_map_impl>();
        register_overload<convert, convert_map_to_tsd_impl>();
        register_overload<convert, convert_kv_to_map_impl>();
        register_overload<convert, convert_kv_to_tsd_impl>();
        register_overload<convert, collection_impl_detail::convert_tsb_to_cs_impl>();
        register_overload<convert, collection_impl_detail::convert_tsb_to_cs_lenient_impl>();
        register_overload<convert, collection_impl_detail::convert_cs_to_tsb_impl>();
        register_overload<combine, combine_date_impl>();
        register_overload<combine, combine_timedelta_impl<true>>();
        register_overload<combine, combine_timedelta_impl<false>>();
        register_overload<combine, combine_datetime_impl>();
        register_overload<combine, combine_tsb_strict_impl>();
        register_overload<collect, collect_collection_impl>();
        register_overload<collect, collect_map_impl>();
        register_overload<collect, collect_map_zip_impl>();
        register_overload<convert, convert_tsl_to_tuple_impl<true>>();
        register_overload<convert, convert_tsl_to_tuple_impl<false>>();
        register_graph_overload<convert, convert_tsl_to_tsd_impl>();
        register_overload<convert, convert_zip_to_tsd_impl>();
        register_graph_overload<combine, combine_tss_scalars_impl>();
        register_overload<combine_tss_from_tsl_marker, combine_tss_from_tsl_impl>();
        register_overload<convert, convert_zip_to_map_impl>();
        register_overload<convert, convert_tsl_to_map_impl>();
        register_overload<convert, convert_tsb_to_map_impl>();
        register_overload<combine, combine_tuple_impl<true>>();
        register_overload<combine, combine_tuple_impl<false>>();
        register_overload<convert, convert_list_to_tsl_impl>();
        register_overload<convert, convert_tsb_to_bool_impl>();
        register_overload<convert, convert_tsb_to_tsd_impl<false>>();
        register_overload<convert, convert_tsb_to_tsd_impl<true>>();
        register_overload<collect, collect_tsd_impl>();
        register_overload<collect, collect_tss_impl>();
        register_overload<convert, convert_list_to_enumerated_tsd_impl>();
        register_overload<collect, collect_tsd_zip_impl>();
        register_overload<collect, collect_tsd_from_map_impl>();
        register_overload<collect, collect_tsd_from_tsd_impl>();
        register_overload<emit, emit_collection_impl>();
        register_overload<emit, emit_tsl_impl>();
        register_overload<emit, emit_map_impl>();
        register_overload<str_, str_tsl_impl>();
    }
}  // namespace hgraph::stdlib

#ifndef HGRAPH_TYPES_VALUE_TABLE_CODEC_H
#define HGRAPH_TYPES_VALUE_TABLE_CODEC_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace arrow
{
    class Array;
    class ArrayBuilder;
    class DataType;
    class Schema;
}  // namespace arrow

namespace hgraph
{
    /**
     * Interned per-schema table converter — the Arrow arm of the serializer-
     * ops pattern (design record: *Record/replay, tables and const_fn*, P4).
     * One converter per value schema: the flattened column layout (Python's
     * ``extract_table_schema``) plus per-column append/read fn-ptrs that
     * write DIRECTLY into Arrow array builders and read from Arrow arrays —
     * no per-tick row tuples.
     *
     * The row shape is bitemporal: ``[date_key, as_of_key, *value columns]``
     * with the key names taken from ``record_replay::config()`` at synthesis
     * (the config is fixed before wiring, per the P2 ruling). Bundles flatten
     * to dotted column names. v1 covers atomic leaves and depth-1 bundles;
     * TSD partition keys land with the record/replay backend (step 4).
     */
    class HGRAPH_EXPORT TableConverter
    {
      public:
        struct Column
        {
            using AppendFn = void (*)(const Column &, const ValueView &leaf, arrow::ArrayBuilder &);
            using ReadFn   = Value (*)(const Column &, const arrow::Array &, std::int64_t row);

            std::string                      name{};
            const ValueTypeMetaData         *leaf_meta{nullptr};
            std::vector<std::size_t>         path{};   ///< field-index chain into the value
            std::shared_ptr<arrow::DataType> type{};
            AppendFn                         append{nullptr};
            ReadFn                           read{nullptr};
        };

        const ValueTypeMetaData        *meta{nullptr};
        std::shared_ptr<arrow::Schema>  arrow_schema{};   ///< [date, as_of, *columns]
        std::string                     date_key{};
        std::string                     as_of_key{};
        std::vector<Column>             columns{};        ///< the value columns (date/as_of excluded)
    };

    /** The interned converter for ``meta`` (synthesized on first use). */
    [[nodiscard]] HGRAPH_EXPORT const TableConverter &table_converter(const ValueTypeMetaData *meta);

    /** Clear the interned converters (registry reset). */
    HGRAPH_EXPORT void clear_table_converters() noexcept;

    /**
     * Node-State payload carrying the converter resolved in ``start`` (the
     * lifecycle form of the builder pattern: compose once, read per tick).
     */
    struct TableCodecState
    {
        const TableConverter *converter{nullptr};
    };

    /** Build a one-row bitemporal frame for one tick's value. */
    [[nodiscard]] HGRAPH_EXPORT Frame single_row_frame(const TableConverter &converter, DateTime value_time,
                                                       DateTime as_of, const ValueView &value);

    /**
     * Reconstruct the value from ``row`` of ``frame``. Column resolution is
     * BY NAME (the input-minimum rule: extra columns in the frame are
     * ignored; a missing required column throws).
     */
    [[nodiscard]] HGRAPH_EXPORT Value read_row(const TableConverter &converter, const Frame &frame,
                                               std::int64_t row);
}  // namespace hgraph

#endif  // HGRAPH_TYPES_VALUE_TABLE_CODEC_H

#include <hgraph/types/value/table_codec.h>

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/value_builder.h>

#include <arrow/api.h>

#include <fmt/core.h>

#include <chrono>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace hgraph
{
    namespace
    {
        using Column = TableConverter::Column;

        [[noreturn]] void fail_status(const arrow::Status &status, const char *what)
        {
            throw std::runtime_error(fmt::format("table codec: {} failed: {}", what, status.ToString()));
        }

        void check(const arrow::Status &status, const char *what)
        {
            if (!status.ok()) { fail_status(status, what); }
        }

        // ---------------------------------------------------------------
        // Per-leaf append / read thunks (fn-ptrs; first param the column)
        // ---------------------------------------------------------------

        template <typename Builder, typename Get>
        void append_with(arrow::ArrayBuilder &builder, const Get &get, const char *what)
        {
            check(static_cast<Builder &>(builder).Append(get()), what);
        }

        void append_bool(const Column &, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            append_with<arrow::BooleanBuilder>(builder, [&] { return leaf.checked_as<Bool>(); }, "append bool");
        }

        void append_int(const Column &, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            append_with<arrow::Int64Builder>(builder, [&] { return leaf.checked_as<Int>(); }, "append int");
        }

        void append_float(const Column &, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            append_with<arrow::DoubleBuilder>(builder, [&] { return leaf.checked_as<Float>(); }, "append float");
        }

        void append_str(const Column &, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            check(static_cast<arrow::StringBuilder &>(builder).Append(leaf.checked_as<Str>()), "append str");
        }

        void append_date(const Column &, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            const auto days =
                std::chrono::sys_days{leaf.checked_as<Date>()}.time_since_epoch().count();
            append_with<arrow::Date32Builder>(builder, [&] { return static_cast<std::int32_t>(days); },
                                              "append date");
        }

        void append_datetime(const Column &, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            append_with<arrow::TimestampBuilder>(
                builder, [&] { return leaf.checked_as<DateTime>().time_since_epoch().count(); },
                "append datetime");
        }

        void append_timedelta(const Column &, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            append_with<arrow::DurationBuilder>(builder, [&] { return leaf.checked_as<TimeDelta>().count(); },
                                                "append timedelta");
        }

        void append_time(const Column &, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            append_with<arrow::Time64Builder>(builder, [&] { return leaf.checked_as<Time>().microseconds; },
                                              "append time");
        }

        Value read_bool(const Column &, const arrow::Array &array, std::int64_t row)
        {
            return Value{Bool{static_cast<const arrow::BooleanArray &>(array).Value(row)}};
        }

        Value read_int(const Column &, const arrow::Array &array, std::int64_t row)
        {
            return Value{Int{static_cast<const arrow::Int64Array &>(array).Value(row)}};
        }

        Value read_float(const Column &, const arrow::Array &array, std::int64_t row)
        {
            return Value{Float{static_cast<const arrow::DoubleArray &>(array).Value(row)}};
        }

        Value read_str(const Column &, const arrow::Array &array, std::int64_t row)
        {
            return Value{Str{static_cast<const arrow::StringArray &>(array).GetView(row)}};
        }

        Value read_date(const Column &, const arrow::Array &array, std::int64_t row)
        {
            const auto days = static_cast<const arrow::Date32Array &>(array).Value(row);
            return Value{Date{std::chrono::sys_days{std::chrono::days{days}}}};
        }

        Value read_datetime(const Column &, const arrow::Array &array, std::int64_t row)
        {
            const auto micros = static_cast<const arrow::TimestampArray &>(array).Value(row);
            return Value{DateTime{std::chrono::microseconds{micros}}};
        }

        Value read_timedelta(const Column &, const arrow::Array &array, std::int64_t row)
        {
            return Value{TimeDelta{static_cast<const arrow::DurationArray &>(array).Value(row)}};
        }

        Value read_time(const Column &, const arrow::Array &array, std::int64_t row)
        {
            return Value{Time{static_cast<const arrow::Time64Array &>(array).Value(row)}};
        }

        struct LeafOps
        {
            std::shared_ptr<arrow::DataType> type;
            Column::AppendFn                 append;
            Column::ReadFn                   read;
        };

        [[nodiscard]] LeafOps leaf_ops_for(const ValueTypeMetaData *meta)
        {
            if (meta == scalar_descriptor<Bool>::value_meta())
            {
                return {arrow::boolean(), &append_bool, &read_bool};
            }
            if (meta == scalar_descriptor<Int>::value_meta()) { return {arrow::int64(), &append_int, &read_int}; }
            if (meta == scalar_descriptor<Float>::value_meta())
            {
                return {arrow::float64(), &append_float, &read_float};
            }
            if (meta == scalar_descriptor<Str>::value_meta()) { return {arrow::utf8(), &append_str, &read_str}; }
            if (meta == scalar_descriptor<Date>::value_meta())
            {
                return {arrow::date32(), &append_date, &read_date};
            }
            if (meta == scalar_descriptor<DateTime>::value_meta())
            {
                return {arrow::timestamp(arrow::TimeUnit::MICRO), &append_datetime, &read_datetime};
            }
            if (meta == scalar_descriptor<TimeDelta>::value_meta())
            {
                return {arrow::duration(arrow::TimeUnit::MICRO), &append_timedelta, &read_timedelta};
            }
            if (meta == scalar_descriptor<Time>::value_meta())
            {
                return {arrow::time64(arrow::TimeUnit::MICRO), &append_time, &read_time};
            }
            throw std::logic_error(fmt::format("table codec: unsupported leaf scalar '{}'",
                                               meta != nullptr && meta->display_name ? meta->display_name : "?"));
        }

        // ---------------------------------------------------------------
        // Synthesis + interning. NO locks: wiring and evaluation are
        // single-threaded (the OperatorRegistry precedent) — push senders,
        // the only cross-thread entry, never touch converters.
        // ---------------------------------------------------------------

        std::unordered_map<const ValueTypeMetaData *, std::unique_ptr<TableConverter>> g_converters;

        [[nodiscard]] const TableConverter *build_converter(const ValueTypeMetaData *meta)
        {
            if (meta == nullptr) { throw std::logic_error("table codec: null value schema"); }

            auto converter  = std::make_unique<TableConverter>();
            converter->meta = meta;
            const auto &config     = record_replay::config();
            converter->date_key    = config.date_key;
            converter->as_of_key   = config.as_of_key;

            const auto add_leaf = [&](std::string name, const ValueTypeMetaData *leaf,
                                      std::vector<std::size_t> path) {
                LeafOps ops = leaf_ops_for(leaf);
                converter->columns.push_back(Column{.name      = std::move(name),
                                                    .leaf_meta = leaf,
                                                    .path      = std::move(path),
                                                    .type      = ops.type,
                                                    .append    = ops.append,
                                                    .read      = ops.read});
            };

            switch (meta->kind)
            {
                case ValueTypeKind::Atomic: add_leaf("value", meta, {}); break;
                case ValueTypeKind::Bundle: {
                    for (std::size_t i = 0; i < meta->field_count; ++i)
                    {
                        const auto &field = meta->fields[i];
                        if (field.type->kind != ValueTypeKind::Atomic)
                        {
                            throw std::logic_error(
                                "table codec: nested compound bundle fields are not supported yet "
                                "(depth-1 bundles only; see the design record)");
                        }
                        add_leaf(field.name != nullptr ? field.name : fmt::format("f{}", i), field.type, {i});
                    }
                    break;
                }
                default:
                    throw std::logic_error(fmt::format(
                        "table codec: unsupported value kind for '{}' (atomics and depth-1 bundles in v1)",
                        meta->display_name ? meta->display_name : "?"));
            }

            arrow::FieldVector fields;
            fields.reserve(converter->columns.size() + 2);
            fields.push_back(arrow::field(converter->date_key, arrow::timestamp(arrow::TimeUnit::MICRO)));
            fields.push_back(arrow::field(converter->as_of_key, arrow::timestamp(arrow::TimeUnit::MICRO)));
            for (const auto &column : converter->columns) { fields.push_back(arrow::field(column.name, column.type)); }
            converter->arrow_schema = arrow::schema(std::move(fields));

            const auto *raw = converter.get();
            g_converters.emplace(meta, std::move(converter));
            return raw;
        }

        [[nodiscard]] std::unique_ptr<arrow::ArrayBuilder> make_builder(const std::shared_ptr<arrow::DataType> &type)
        {
            std::unique_ptr<arrow::ArrayBuilder> builder;
            check(arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder), "make builder");
            return builder;
        }

        [[nodiscard]] std::shared_ptr<arrow::Array> finish(arrow::ArrayBuilder &builder)
        {
            std::shared_ptr<arrow::Array> array;
            check(builder.Finish(&array), "finish array");
            return array;
        }
    }  // namespace

    const TableConverter &table_converter(const ValueTypeMetaData *meta)
    {
        // Composed + interned once per schema. Per-tick operator paths do NOT
        // call this: nodes resolve their converter in ``start`` and carry it
        // in node State (the lifecycle "compose once" contract).
        if (const auto it = g_converters.find(meta); it != g_converters.end()) { return *it->second; }
        return *build_converter(meta);
    }

    void clear_table_converters() noexcept { g_converters.clear(); }

    Frame single_row_frame(const TableConverter &converter, DateTime value_time, DateTime as_of,
                           const ValueView &value)
    {
        arrow::ArrayVector arrays;
        arrays.reserve(converter.columns.size() + 2);

        const auto append_timestamp = [&](DateTime when) {
            auto builder = make_builder(arrow::timestamp(arrow::TimeUnit::MICRO));
            check(static_cast<arrow::TimestampBuilder &>(*builder).Append(when.time_since_epoch().count()),
                  "append timestamp");
            arrays.push_back(finish(*builder));
        };
        append_timestamp(value_time);
        append_timestamp(as_of);

        for (const auto &column : converter.columns)
        {
            auto builder = make_builder(column.type);
            // v1 paths are depth <= 1 (atomic or depth-1 bundle field).
            if (column.path.empty()) { column.append(column, value, *builder); }
            else { column.append(column, value.as_bundle().at(column.path.front()), *builder); }
            arrays.push_back(finish(*builder));
        }

        return Frame{arrow::Table::Make(converter.arrow_schema, std::move(arrays), 1)};
    }

    Value read_row(const TableConverter &converter, const Frame &frame, std::int64_t row)
    {
        if (!frame.has_value()) { throw std::invalid_argument("table codec: cannot read from an empty frame"); }
        const arrow::Table &table = *frame.table;
        if (row < 0 || row >= table.num_rows())
        {
            throw std::out_of_range("table codec: row index out of range");
        }

        const auto column_array = [&](const std::string &name) -> std::shared_ptr<arrow::Array> {
            const auto chunked = table.GetColumnByName(name);
            if (chunked == nullptr)
            {
                // The input-minimum rule: required columns must be present
                // (extra frame columns are simply never asked for).
                throw std::invalid_argument(fmt::format("table codec: frame is missing column '{}'", name));
            }
            if (chunked->num_chunks() != 1)
            {
                const auto combined = arrow::Concatenate(chunked->chunks());
                if (!combined.ok()) { fail_status(combined.status(), "concatenate chunks"); }
                return *combined;
            }
            return chunked->chunk(0);
        };

        if (converter.meta->kind == ValueTypeKind::Atomic)
        {
            const auto &column = converter.columns.front();
            return column.read(column, *column_array(column.name), row);
        }

        const auto *binding = ValuePlanFactory::instance().binding_for(converter.meta);
        BundleBuilder builder{*binding};
        for (const auto &column : converter.columns)
        {
            builder.set(column.path.front(), column.read(column, *column_array(column.name), row));
        }
        return builder.build();
    }
}  // namespace hgraph

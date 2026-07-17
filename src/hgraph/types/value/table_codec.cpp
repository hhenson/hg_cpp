#include <hgraph/types/value/table_codec.h>

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/value_builder.h>

#include <arrow/api.h>

#include <fmt/format.h>

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

        void append_null(arrow::ArrayBuilder &builder)
        {
            check(builder.AppendNull(), "append null");
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

        void append_bytes(const Column &, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            check(static_cast<arrow::BinaryBuilder &>(builder).Append(leaf.checked_as<Bytes>().data),
                  "append bytes");
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
            // polars-built tables carry large_utf8 (64-bit offsets); reading
            // one through StringArray misreads the offset buffer.
            if (array.type_id() == arrow::Type::LARGE_STRING)
            {
                return Value{Str{static_cast<const arrow::LargeStringArray &>(array).GetView(row)}};
            }
            return Value{Str{static_cast<const arrow::StringArray &>(array).GetView(row)}};
        }

        Value read_bytes(const Column &, const arrow::Array &array, std::int64_t row)
        {
            if (array.type_id() == arrow::Type::LARGE_BINARY)
            {
                return Value{
                    Bytes{std::string{static_cast<const arrow::LargeBinaryArray &>(array).GetView(row)}}};
            }
            return Value{Bytes{std::string{static_cast<const arrow::BinaryArray &>(array).GetView(row)}}};
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

        void append_sequence(const Column &, const ValueView &, arrow::ArrayBuilder &);
        Value read_sequence(const Column &, const arrow::Array &, std::int64_t);

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
            if (meta == scalar_descriptor<Bytes>::value_meta())
            {
                return {arrow::binary(), &append_bytes, &read_bytes};
            }
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
            if ((meta->value_kind() == ValueTypeKind::List ||
                 (meta->value_kind() == ValueTypeKind::Tuple && meta->has(ValueTypeFlags::VariadicTuple))) &&
                meta->element_type != nullptr)
            {
                return {arrow::list(leaf_ops_for(meta->element_type).type), &append_sequence, &read_sequence};
            }
            throw std::logic_error(fmt::format("table codec: unsupported leaf scalar '{}'",
                                               meta != nullptr && !meta->name().empty() ? meta->name()
                                                                                      : std::string_view{"?"}));
        }

        void append_sequence(const Column &column, const ValueView &leaf, arrow::ArrayBuilder &builder)
        {
            auto &list_builder = static_cast<arrow::ListBuilder &>(builder);
            check(list_builder.Append(), "append sequence");
            auto       &value_builder = *list_builder.value_builder();
            const auto *element_meta  = column.leaf_meta->element_type;
            const auto  element_ops   = leaf_ops_for(element_meta);
            const Column element_column{.leaf_meta = element_meta, .type = element_ops.type};

            const auto append_values = [&](const auto &sequence) {
                for (const ValueView value : sequence.values())
                {
                    if (value.has_value()) { element_ops.append(element_column, value, value_builder); }
                    else { append_null(value_builder); }
                }
            };
            if (leaf.schema()->value_kind() == ValueTypeKind::Tuple) { append_values(leaf.as_tuple()); }
            else { append_values(leaf.as_list()); }
        }

        Value read_sequence(const Column &column, const arrow::Array &array, std::int64_t row)
        {
            const auto &list         = static_cast<const arrow::ListArray &>(array);
            const auto *sequence_meta = column.leaf_meta;
            const auto *element_meta  = sequence_meta->element_type;
            const auto  element_binding = ValuePlanFactory::instance().type_for(element_meta);
            if (element_binding == nullptr)
            {
                throw std::logic_error("table codec: sequence element has no value binding");
            }

            const auto  element_ops = leaf_ops_for(element_meta);
            const Column element_column{.leaf_meta = element_meta, .type = element_ops.type};
            const auto  &values = *list.values();
            const auto   begin  = list.value_offset(row);
            const auto   end    = begin + list.value_length(row);
            ListBuilder builder{element_binding};
            for (std::int64_t index = begin; index < end; ++index)
            {
                if (values.IsNull(index))
                {
                    builder.push_back_unset();
                    continue;
                }
                Value value = element_ops.read(element_column, values, index);
                builder.push_back_copy(value.view().data());
            }
            ListStorage storage = builder.build_storage();
            return Value{compact_list_type(element_binding, *sequence_meta), &storage};
        }

        // ---------------------------------------------------------------
        // Synthesis + interning. NO locks: wiring and evaluation are
        // single-threaded (the OperatorRegistry precedent) — push senders,
        // the only cross-thread entry, never touch converters.
        // ---------------------------------------------------------------

        struct ConverterKey
        {
            const ValueTypeMetaData *meta{nullptr};
            std::string              date_key{};
            std::string              as_of_key{};

            bool operator==(const ConverterKey &) const = default;
        };

        struct ConverterKeyHash
        {
            std::size_t operator()(const ConverterKey &key) const noexcept
            {
                std::size_t seed = std::hash<const ValueTypeMetaData *>{}(key.meta);
                seed ^= std::hash<std::string>{}(key.date_key) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
                seed ^= std::hash<std::string>{}(key.as_of_key) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
                return seed;
            }
        };

        std::unordered_map<ConverterKey, std::unique_ptr<TableConverter>, ConverterKeyHash> g_converters;

        [[nodiscard]] const TableConverter *build_converter(const ValueTypeMetaData *meta,
                                                            std::string_view date_key,
                                                            std::string_view as_of_key)
        {
            if (meta == nullptr) { throw std::logic_error("table codec: null value schema"); }

            auto converter  = std::make_unique<TableConverter>();
            converter->meta = meta;
            converter->date_key = date_key;
            converter->as_of_key = as_of_key;

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

            switch (meta->value_kind())
            {
                case ValueTypeKind::Atomic: add_leaf("value", meta, {}); break;
                case ValueTypeKind::List:
                    if (!meta->has(ValueTypeFlags::VariadicTuple))
                    {
                        throw std::logic_error("table codec: only variadic tuple list leaves are supported");
                    }
                    add_leaf("value", meta, {});
                    break;
                case ValueTypeKind::Bundle: {
                    for (std::size_t i = 0; i < meta->field_count; ++i)
                    {
                        const auto &field = meta->fields[i];
                        if (field.type->value_kind() != ValueTypeKind::Atomic)
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
                        meta->name()));
            }

            arrow::FieldVector fields;
            fields.reserve(converter->columns.size() + 2);
            fields.push_back(arrow::field(converter->date_key, arrow::timestamp(arrow::TimeUnit::MICRO)));
            fields.push_back(arrow::field(converter->as_of_key, arrow::timestamp(arrow::TimeUnit::MICRO)));
            for (const auto &column : converter->columns) { fields.push_back(arrow::field(column.name, column.type)); }
            converter->arrow_schema = arrow::schema(std::move(fields));

            const auto *raw = converter.get();
            g_converters.emplace(ConverterKey{meta, std::string{date_key}, std::string{as_of_key}},
                                 std::move(converter));
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

        void append_column(const Column &column, const ValueView &value, arrow::ArrayBuilder &builder)
        {
            if (!value.has_value())
            {
                append_null(builder);
                return;
            }
            column.append(column, value, builder);
        }
    }  // namespace

    struct FrameRecorder::Impl
    {
        const TableConverter                             *converter{nullptr};
        std::unique_ptr<arrow::ArrayBuilder>              date_builder{};
        std::unique_ptr<arrow::ArrayBuilder>              as_of_builder{};
        std::vector<std::unique_ptr<arrow::ArrayBuilder>> column_builders{};
        std::int64_t                                      rows{0};
    };

    FrameRecorder::FrameRecorder(const TableConverter &converter) : impl_(std::make_unique<Impl>())
    {
        impl_->converter     = &converter;
        impl_->date_builder  = make_builder(arrow::timestamp(arrow::TimeUnit::MICRO));
        impl_->as_of_builder = make_builder(arrow::timestamp(arrow::TimeUnit::MICRO));
        impl_->column_builders.reserve(converter.columns.size());
        for (const auto &column : converter.columns) { impl_->column_builders.push_back(make_builder(column.type)); }
    }

    FrameRecorder::FrameRecorder(FrameRecorder &&) noexcept            = default;
    FrameRecorder &FrameRecorder::operator=(FrameRecorder &&) noexcept = default;
    FrameRecorder::~FrameRecorder()                                    = default;

    void FrameRecorder::append(DateTime value_time, DateTime as_of, const ValueView &value)
    {
        check(static_cast<arrow::TimestampBuilder &>(*impl_->date_builder)
                  .Append(value_time.time_since_epoch().count()),
              "append value time");
        check(static_cast<arrow::TimestampBuilder &>(*impl_->as_of_builder).Append(as_of.time_since_epoch().count()),
              "append as-of");
        const auto &columns = impl_->converter->columns;
        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            const auto &column = columns[i];
            if (column.path.empty()) { append_column(column, value, *impl_->column_builders[i]); }
            else { append_column(column, value.as_bundle().at(column.path.front()), *impl_->column_builders[i]); }
        }
        ++impl_->rows;
    }

    Frame FrameRecorder::finish()
    {
        arrow::ArrayVector arrays;
        arrays.reserve(impl_->converter->columns.size() + 2);
        arrays.push_back(hgraph::finish(*impl_->date_builder));
        arrays.push_back(hgraph::finish(*impl_->as_of_builder));
        for (auto &builder : impl_->column_builders) { arrays.push_back(hgraph::finish(*builder)); }
        return Frame{arrow::Table::Make(impl_->converter->arrow_schema, std::move(arrays), impl_->rows)};
    }

    DateTime frame_value_time(const TableConverter &converter, const Frame &frame, std::int64_t row)
    {
        const auto chunked = frame.table->GetColumnByName(converter.date_key);
        if (chunked == nullptr)
        {
            throw std::invalid_argument("table codec: frame is missing its value-time column");
        }
        const auto &array = static_cast<const arrow::TimestampArray &>(*chunked->chunk(0));
        return DateTime{std::chrono::microseconds{array.Value(row)}};
    }

    const TableConverter &table_converter(const ValueTypeMetaData *meta, std::string_view date_key,
                                          std::string_view as_of_key)
    {
        // Composed + interned once per schema. Per-tick operator paths do NOT
        // call this: nodes resolve their converter in ``start`` and carry it
        // in node State (the lifecycle "compose once" contract).
        ConverterKey key{meta, std::string{date_key}, std::string{as_of_key}};
        if (const auto it = g_converters.find(key); it != g_converters.end()) { return *it->second; }
        return *build_converter(meta, date_key, as_of_key);
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
            if (column.path.empty()) { append_column(column, value, *builder); }
            else { append_column(column, value.as_bundle().at(column.path.front()), *builder); }
            arrays.push_back(finish(*builder));
        }

        return Frame{arrow::Table::Make(converter.arrow_schema, std::move(arrays), 1)};
    }

    Frame frame_from_rows(const TableConverter &converter, std::span<const ValueView> rows,
                          std::size_t first_column)
    {
        const auto        &columns = converter.columns;
        arrow::FieldVector fields;
        fields.reserve(columns.size());
        for (const auto &column : columns) { fields.push_back(arrow::field(column.name, column.type)); }
        const auto schema = arrow::schema(std::move(fields));

        std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
        builders.reserve(columns.size());
        for (const auto &column : columns) { builders.push_back(make_builder(column.type)); }

        for (const ValueView &row : rows)
        {
            const auto tuple = row.as_tuple();
            for (std::size_t i = 0; i < columns.size(); ++i)
            {
                append_column(columns[i], tuple.at(first_column + i), *builders[i]);
            }
        }

        arrow::ArrayVector arrays;
        arrays.reserve(columns.size());
        for (auto &builder : builders) { arrays.push_back(finish(*builder)); }
        return Frame{arrow::Table::Make(schema, std::move(arrays),
                                        static_cast<std::int64_t>(rows.size()))};
    }

    Frame frame_from_values(const TableConverter &converter, std::span<const Value> rows)
    {
        const auto        &columns = converter.columns;
        arrow::FieldVector fields;
        fields.reserve(columns.size());
        for (const auto &column : columns) { fields.push_back(arrow::field(column.name, column.type)); }
        const auto schema = arrow::schema(std::move(fields));

        std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
        builders.reserve(columns.size());
        for (const auto &column : columns) { builders.push_back(make_builder(column.type)); }

        for (const Value &row : rows)
        {
            const ValueView view = row.view();
            for (std::size_t i = 0; i < columns.size(); ++i)
            {
                const auto &column = columns[i];
                if (column.path.empty()) { append_column(column, view, *builders[i]); }
                else { append_column(column, view.as_bundle().at(column.path.front()), *builders[i]); }
            }
        }

        arrow::ArrayVector arrays;
        arrays.reserve(columns.size());
        for (auto &builder : builders) { arrays.push_back(finish(*builder)); }
        return Frame{arrow::Table::Make(schema, std::move(arrays),
                                        static_cast<std::int64_t>(rows.size()))};
    }

    std::vector<std::string> frame_column_names(const Frame &frame)
    {
        std::vector<std::string> names;
        if (!frame.has_value()) { return names; }
        for (const auto &field : frame.table->schema()->fields()) { names.push_back(field->name()); }
        return names;
    }

    Value frame_cell(const Frame &frame, std::string_view column, const ValueTypeMetaData *leaf,
                     std::int64_t row)
    {
        if (!frame.has_value()) { throw std::invalid_argument("table codec: cannot read an empty frame"); }
        const auto chunked = frame.table->GetColumnByName(std::string{column});
        if (chunked == nullptr)
        {
            throw std::invalid_argument(fmt::format("table codec: frame is missing column '{}'", column));
        }
        std::shared_ptr<arrow::Array> array;
        if (chunked->num_chunks() != 1)
        {
            const auto combined = arrow::Concatenate(chunked->chunks());
            if (!combined.ok()) { fail_status(combined.status(), "concatenate chunks"); }
            array = *combined;
        }
        else { array = chunked->chunk(0); }
        if (array->IsNull(row)) { return Value{}; }
        const LeafOps ops = leaf_ops_for(leaf);
        const Column  temp{.name = std::string{column}, .leaf_meta = leaf, .type = ops.type};
        return ops.read(temp, *array, row);
    }

    Frame frame_rename_columns(const Frame &frame,
                               std::span<const std::pair<std::string, std::string>> renames)
    {
        if (!frame.has_value()) { return frame; }
        std::vector<std::string> names;
        for (const auto &field : frame.table->schema()->fields())
        {
            std::string name = field->name();
            for (const auto &[from, to] : renames)
            {
                if (name == from)
                {
                    name = to;
                    break;
                }
            }
            names.push_back(std::move(name));
        }
        auto renamed = frame.table->RenameColumns(names);
        if (!renamed.ok()) { fail_status(renamed.status(), "rename columns"); }
        return Frame{*renamed};
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

        if (converter.meta->value_kind() != ValueTypeKind::Bundle)
        {
            const auto &column = converter.columns.front();
            auto array = column_array(column.name);
            if (array->IsNull(row)) { return Value{*converter.meta}; }
            return column.read(column, *array, row);
        }

        const auto binding = ValuePlanFactory::instance().type_for(converter.meta);
        BundleBuilder builder{binding};
        for (const auto &column : converter.columns)
        {
            auto array = column_array(column.name);
            if (array->IsNull(row)) { continue; }
            builder.set(column.path.front(), column.read(column, *array, row));
        }
        return builder.build();
    }
}  // namespace hgraph

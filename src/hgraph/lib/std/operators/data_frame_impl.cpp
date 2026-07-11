#include <hgraph/lib/std/operators/impl/data_frame_impl.h>

#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/ts_input/bundle_view.h>
#include <hgraph/types/time_series/ts_input/dict_view.h>
#include <hgraph/types/time_series/ts_output/dict_view.h>
#include <hgraph/types/value/specialized_views.h>

#include <fmt/format.h>

#include <algorithm>
#include <memory>

namespace hgraph::stdlib
{
    namespace data_frame_detail
    {
        namespace
        {
            [[nodiscard]] const ValueTypeBinding &checked_binding(const ValueTypeMetaData *meta,
                                                                  const char              *what)
            {
                const auto *binding = ValuePlanFactory::instance().binding_for(meta);
                if (binding == nullptr)
                {
                    throw std::logic_error(fmt::format("{}: schema has no value binding", what));
                }
                return *binding;
            }

            [[nodiscard]] const ValueTypeMetaData *datetime_meta()
            {
                return TypeRegistry::instance().value_type("datetime");
            }

            [[nodiscard]] DateTime read_dt(const Frame &frame, const std::string &dt_col,
                                           std::int64_t row)
            {
                const Value cell = frame_cell(frame, dt_col, datetime_meta(), row);
                if (!cell.has_value())
                {
                    throw std::invalid_argument("from_data_frame: null value in the date column");
                }
                return cell.view().checked_as<DateTime>();
            }

            [[nodiscard]] const ValueTypeMetaData *frame_columns_schema(const ValueTypeMetaData *frame_meta,
                                                                        const char              *what)
            {
                if (frame_meta == nullptr || frame_meta->element_type == nullptr)
                {
                    throw std::invalid_argument(
                        fmt::format("{}: an untyped Frame cannot name columns (use Frame[Schema])", what));
                }
                return frame_meta->element_type;
            }

            void set_bundle_field(Value &row, std::size_t index, const ValueView &cell)
            {
                if (!cell.has_value()) { return; }
                ValueView root = row.view().begin_mutation();
                auto      mut  = root.as_bundle().begin_mutation();
                ValueView dest = mut.at(index);
                dest.copy_from(cell);
            }

            void set_bundle_field(Value &row, std::size_t index, const Value &cell)
            {
                if (!cell.has_value()) { return; }
                set_bundle_field(row, index, cell.view());
            }

            [[nodiscard]] std::size_t field_index_of(const ValueTypeMetaData *bundle,
                                                     std::string_view name, const char *what)
            {
                for (std::size_t i = 0; i < bundle->field_count; ++i)
                {
                    if (bundle->fields[i].name != nullptr && bundle->fields[i].name == name) { return i; }
                }
                throw std::invalid_argument(fmt::format("{}: no column matches field '{}'", what, name));
            }

            [[nodiscard]] bool key_equal(const ValueView &lhs, const ValueView &rhs)
            {
                return lhs.binding() == rhs.binding() &&
                       lhs.binding()->ops_ref().equals(lhs.data(), rhs.data());
            }
        }  // namespace

        // -----------------------------------------------------------------
        // from_data_frame
        // -----------------------------------------------------------------

        void start_from_frame(const Frame &frame, std::string_view dt_col, std::string_view key_col,
                              std::string_view value_col, TimeDelta offset, const TSOutputView &out,
                              SingleShotScheduler &sched, FromFramePlan *&plan_out)
        {
            auto plan      = std::make_unique<FromFramePlan>();
            plan->frame    = frame;
            plan->dt_col   = std::string{dt_col};
            plan->offset   = offset;
            plan->out_kind = out.schema()->kind;

            const auto add_bundle_fields = [&](const ValueTypeMetaData *bundle, const char *what) {
                plan->bundle_meta = bundle;
                for (std::size_t i = 0; i < bundle->field_count; ++i)
                {
                    const auto &field = bundle->fields[i];
                    if (field.type->kind != ValueTypeKind::Atomic)
                    {
                        throw std::invalid_argument(
                            fmt::format("{}: non-atomic field '{}' is not supported", what,
                                        field.name != nullptr ? field.name : "?"));
                    }
                    plan->fields.push_back(
                        FieldRead{field.name != nullptr ? field.name : "", field.type, i});
                }
            };

            switch (plan->out_kind)
            {
                case TSTypeKind::TS:
                    plan->fields.push_back(
                        FieldRead{std::string{value_col}, out.schema()->value_schema, 0});
                    break;
                case TSTypeKind::TSB:
                    add_bundle_fields(out.schema()->value_schema, "from_data_frame");
                    break;
                case TSTypeKind::TSD: {
                    plan->key_meta = out.schema()->key_type();
                    plan->key_col  = std::string{key_col};
                    const auto *child = out.schema()->element_ts();
                    if (child->kind == TSTypeKind::TSB)
                    {
                        add_bundle_fields(child->value_schema, "from_data_frame");
                    }
                    else if (child->kind == TSTypeKind::TS)
                    {
                        // The value column is "the remaining column" (the
                        // upstream k_v rule): first frame column that is
                        // neither the date nor the key column.
                        std::string column{value_col};
                        for (const auto &name : frame_column_names(frame))
                        {
                            if (name != plan->dt_col && name != plan->key_col)
                            {
                                column = name;
                                break;
                            }
                        }
                        plan->fields.push_back(FieldRead{std::move(column), child->value_schema, 0});
                    }
                    else
                    {
                        throw std::invalid_argument(
                            "from_data_frame: unsupported TSD element time-series kind");
                    }
                    break;
                }
                default: throw std::invalid_argument("from_data_frame: unsupported output kind");
            }

            if (plan->frame.has_value() && frame_rows(plan->frame) > 0)
            {
                sched.schedule(read_dt(plan->frame, plan->dt_col, 0) + plan->offset);
            }
            plan_out = plan.release();
        }

        namespace
        {
            void apply_from_frame_leafish(const FromFramePlan &plan, std::int64_t row,
                                          const TSOutputView &out)
            {
                if (plan.bundle_meta != nullptr)
                {
                    Value value{checked_binding(plan.bundle_meta, "from_data_frame")};
                    bool  any = false;
                    for (const auto &field : plan.fields)
                    {
                        const Value cell = frame_cell(plan.frame, field.column, field.leaf, row);
                        if (!cell.has_value()) { continue; }   // nulls do not tick
                        set_bundle_field(value, field.field_index, cell);
                        any = true;
                    }
                    if (any) { apply_delta(out, value.view()); }
                    return;
                }
                const auto &field = plan.fields.front();
                const Value cell  = frame_cell(plan.frame, field.column, field.leaf, row);
                if (cell.has_value()) { apply_current_value(out, cell.view()); }
            }
        }  // namespace

        void eval_from_frame(FromFramePlan &plan, DateTime now, NodeScheduler &sched,
                             const TSOutputView &out)
        {
            const auto rows = plan.frame.has_value() ? frame_rows(plan.frame) : 0;
            while (plan.row < rows && read_dt(plan.frame, plan.dt_col, plan.row) + plan.offset == now)
            {
                if (plan.out_kind == TSTypeKind::TSD)
                {
                    auto        dict     = out.as_dict();
                    auto        mutation = dict.begin_mutation(out.evaluation_time());
                    const Value key = frame_cell(plan.frame, plan.key_col, plan.key_meta, plan.row);
                    auto        element = mutation.at(key.view());
                    apply_from_frame_leafish(
                        plan, plan.row, TSOutputView{out.output(), element, out.evaluation_time()});
                }
                else { apply_from_frame_leafish(plan, plan.row, out); }
                ++plan.row;
            }
            if (plan.row < rows)
            {
                sched.schedule(read_dt(plan.frame, plan.dt_col, plan.row) + plan.offset);
            }
        }

        // -----------------------------------------------------------------
        // to_data_frame
        // -----------------------------------------------------------------

        const TSValueTypeMetaData *resolve_to_frame_output(const TSValueTypeMetaData *ts,
                                                           std::string_view dt_col,
                                                           std::string_view key_col,
                                                           std::string_view value_col)
        {
            std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields;
            fields.emplace_back(std::string{dt_col}, datetime_meta());

            const auto *leaf = ts;
            if (ts->kind == TSTypeKind::TSD)
            {
                if (ts->key_type()->kind != ValueTypeKind::Atomic) { return nullptr; }
                fields.emplace_back(std::string{key_col}, ts->key_type());
                leaf = ts->element_ts();
            }
            if (leaf->kind == TSTypeKind::TS)
            {
                if (leaf->value_schema->kind != ValueTypeKind::Atomic) { return nullptr; }
                fields.emplace_back(std::string{value_col}, leaf->value_schema);
            }
            else if (leaf->kind == TSTypeKind::TSB)
            {
                const auto *bundle = leaf->value_schema;
                for (std::size_t i = 0; i < bundle->field_count; ++i)
                {
                    const auto &field = bundle->fields[i];
                    if (field.type->kind != ValueTypeKind::Atomic) { return nullptr; }
                    fields.emplace_back(field.name != nullptr ? field.name : "", field.type);
                }
            }
            else { return nullptr; }

            auto &registry = TypeRegistry::instance();
            return registry.ts(registry.frame(registry.un_named_bundle(fields)));
        }

        void start_to_frame(const TSInputView &ts, std::string_view dt_col, std::string_view key_col,
                            std::string_view value_col, const TSOutputView &out, ToFramePlan *&plan_out)
        {
            auto        plan     = std::make_unique<ToFramePlan>();
            const auto *columns  = frame_columns_schema(out.schema()->value_schema, "to_data_frame");
            plan->row_meta       = columns;
            plan->converter      = &table_converter(columns);
            plan->dict           = ts.schema()->kind == TSTypeKind::TSD;

            const auto *leaf = plan->dict ? ts.schema()->element_ts() : ts.schema();
            const auto *leaf_bundle =
                leaf->kind == TSTypeKind::TSB ? leaf->value_schema : nullptr;

            for (std::size_t i = 0; i < columns->field_count; ++i)
            {
                const char            *name = columns->fields[i].name;
                const std::string_view column{name != nullptr ? name : ""};
                ToFramePlan::Column    entry;
                if (column == dt_col) { entry.source = ToFramePlan::Source::Date; }
                else if (plan->dict && column == key_col) { entry.source = ToFramePlan::Source::Key; }
                else if (leaf_bundle != nullptr)
                {
                    entry.source   = ToFramePlan::Source::Field;
                    entry.ts_field = field_index_of(leaf_bundle, column, "to_data_frame");
                }
                else
                {
                    // A plain TS leaf: the (single) value column, whatever
                    // its name (value_col by convention).
                    static_cast<void>(value_col);
                    entry.source = ToFramePlan::Source::Whole;
                }
                plan->columns.push_back(entry);
            }
            plan_out = plan.release();
        }

        namespace
        {
            [[nodiscard]] Value snapshot_row(const ToFramePlan &plan, DateTime now, const ValueView *key,
                                             const TSInputView &leaf)
            {
                Value row{checked_binding(plan.row_meta, "to_data_frame")};
                for (std::size_t i = 0; i < plan.columns.size(); ++i)
                {
                    const auto &column = plan.columns[i];
                    switch (column.source)
                    {
                        case ToFramePlan::Source::Date: {
                            Value cell{now};
                            set_bundle_field(row, i, cell);
                            break;
                        }
                        case ToFramePlan::Source::Key:
                            if (key != nullptr) { set_bundle_field(row, i, *key); }
                            break;
                        case ToFramePlan::Source::Field: {
                            auto bundle = leaf.as_bundle();
                            auto child  = bundle.at(column.ts_field);
                            if (child.valid()) { set_bundle_field(row, i, child.value()); }
                            break;
                        }
                        case ToFramePlan::Source::ValueField: {
                            if (!leaf.valid()) { break; }
                            const ValueView value = leaf.value();
                            auto            bundle = value.as_bundle();
                            set_bundle_field(row, i, bundle.at(plan.columns[i].ts_field));
                            break;
                        }
                        case ToFramePlan::Source::Whole:
                            if (leaf.valid()) { set_bundle_field(row, i, leaf.value()); }
                            break;
                    }
                }
                return row;
            }
        }  // namespace

        void eval_to_frame(const ToFramePlan &plan, const TSInputView &ts, DateTime now,
                           const TSOutputView &out)
        {
            std::vector<Value> rows;
            if (plan.dict)
            {
                auto dict = const_cast<TSInputView &>(ts).as_dict();
                for (auto &&[key, child] : dict.valid_items())
                {
                    rows.push_back(snapshot_row(plan, now, &key, child));
                }
            }
            else { rows.push_back(snapshot_row(plan, now, nullptr, ts)); }

            Frame frame = frame_from_values(*plan.converter, rows);
            Value boxed{checked_binding(out.schema()->value_schema, "to_data_frame")};
            *static_cast<Frame *>(const_cast<void *>(boxed.view().data())) = std::move(frame);
            apply_current_value(out, boxed.view());
        }

        // -----------------------------------------------------------------
        // group_by
        // -----------------------------------------------------------------

        const TSValueTypeMetaData *resolve_group_by_output(const TSValueTypeMetaData *ts,
                                                           const ValueView          &by)
        {
            if (ts->kind != TSTypeKind::TS) { return nullptr; }
            const auto *columns = ts->value_schema->element_type;
            if (columns == nullptr) { return nullptr; }
            auto &registry = TypeRegistry::instance();

            const auto field_meta = [&](std::string_view name) -> const ValueTypeMetaData * {
                for (std::size_t i = 0; i < columns->field_count; ++i)
                {
                    if (columns->fields[i].name != nullptr && columns->fields[i].name == name)
                    {
                        return columns->fields[i].type;
                    }
                }
                return nullptr;
            };

            const ValueTypeMetaData *key_meta = nullptr;
            if (by.schema()->kind == ValueTypeKind::Atomic)
            {
                key_meta = field_meta(by.checked_as<Str>());
            }
            else
            {
                std::vector<const ValueTypeMetaData *> parts;
                auto                                   list = by.as_indexed_view();
                for (std::size_t i = 0; i < list.size(); ++i)
                {
                    const auto *part = field_meta(std::string{list.at(i).checked_as<Str>()});
                    if (part == nullptr) { return nullptr; }
                    parts.push_back(part);
                }
                key_meta = registry.tuple(parts);
            }
            if (key_meta == nullptr) { return nullptr; }
            return registry.tsd(key_meta, registry.ts(ts->value_schema));
        }

        void start_group_by(const TSInputView &ts, const ValueView &by, const TSOutputView &out,
                            GroupByPlan *&plan_out)
        {
            static_cast<void>(out);
            auto        plan    = std::make_unique<GroupByPlan>();
            const auto *columns = frame_columns_schema(ts.schema()->value_schema, "group_by");
            plan->converter     = &table_converter(columns);

            const auto add_key = [&](std::string_view name) {
                const std::size_t index = field_index_of(columns, name, "group_by");
                plan->key_cols.push_back(
                    FieldRead{std::string{name}, columns->fields[index].type, index});
            };

            if (by.schema()->kind == ValueTypeKind::Atomic)
            {
                add_key(by.checked_as<Str>());
                plan->key_meta = plan->key_cols.front().leaf;
            }
            else
            {
                plan->tuple_key = true;
                std::vector<const ValueTypeMetaData *> parts;
                auto                                   list = by.as_indexed_view();
                for (std::size_t i = 0; i < list.size(); ++i)
                {
                    add_key(std::string{list.at(i).checked_as<Str>()});
                    parts.push_back(plan->key_cols.back().leaf);
                }
                plan->key_meta = TypeRegistry::instance().tuple(parts);
            }
            plan_out = plan.release();
        }

        void eval_group_by(const GroupByPlan &plan, const TSInputView &ts, const TSOutputView &out)
        {
            const ValueView view  = ts.value();
            const Frame    &frame = view.checked_as<Frame>();
            const auto      rows  = frame.has_value() ? frame_rows(frame) : 0;

            std::vector<std::pair<Value, std::vector<Value>>> buckets;
            for (std::int64_t r = 0; r < rows; ++r)
            {
                Value key{checked_binding(plan.key_meta, "group_by")};
                if (plan.tuple_key)
                {
                    for (std::size_t i = 0; i < plan.key_cols.size(); ++i)
                    {
                        const Value cell =
                            frame_cell(frame, plan.key_cols[i].column, plan.key_cols[i].leaf, r);
                        if (!cell.has_value()) { continue; }
                        ValueView root = key.view().begin_mutation();
                        auto      mut  = root.as_tuple().begin_mutation();
                        ValueView dest = mut.at(i);
                        dest.copy_from(cell.view());
                    }
                }
                else
                {
                    const Value cell =
                        frame_cell(frame, plan.key_cols.front().column, plan.key_cols.front().leaf, r);
                    if (!cell.has_value()) { continue; }
                    ValueView dest = key.view().begin_mutation();
                    dest.copy_from(cell.view());
                }

                auto bucket = std::find_if(buckets.begin(), buckets.end(), [&](const auto &entry) {
                    return key_equal(entry.first.view(), key.view());
                });
                if (bucket == buckets.end())
                {
                    buckets.emplace_back(std::move(key), std::vector<Value>{});
                    bucket = std::prev(buckets.end());
                }
                bucket->second.push_back(read_row(*plan.converter, frame, r));
            }

            auto dict     = out.as_dict();
            auto mutation = dict.begin_mutation(out.evaluation_time());

            // Removals first: collect current keys, drop the ones absent
            // from this tick's buckets.
            std::vector<Value> stale;
            for (ValueView key : dict.keys())
            {
                const bool present =
                    std::any_of(buckets.begin(), buckets.end(), [&](const auto &entry) {
                        return key_equal(entry.first.view(), key);
                    });
                if (!present) { stale.emplace_back(key); }
            }
            for (const Value &key : stale) { static_cast<void>(mutation.erase(key.view())); }

            const auto *child_schema = out.schema()->element_ts();
            for (auto &[key, bucket_rows] : buckets)
            {
                Frame sub = frame_from_values(*plan.converter, bucket_rows);
                Value boxed{checked_binding(child_schema->value_schema, "group_by")};
                *static_cast<Frame *>(const_cast<void *>(boxed.view().data())) = std::move(sub);
                auto element = mutation.at(key.view());
                apply_current_value(TSOutputView{out.output(), element, out.evaluation_time()},
                                    boxed.view());
            }
        }
        // -----------------------------------------------------------------
        // convert / combine frame targets
        // -----------------------------------------------------------------

        bool value_is_frame(const ValueTypeMetaData *meta)
        {
            const auto *base = TypeRegistry::instance().value_type("frame");
            if (meta == nullptr || base == nullptr) { return false; }
            return meta == base ||
                   (meta->display_name != nullptr && base->display_name != nullptr &&
                    std::string_view{meta->display_name} == std::string_view{base->display_name});
        }

        bool ts_value_is_frame(const TSValueTypeMetaData *ts)
        {
            return ts != nullptr && ts->kind == TSTypeKind::TS && value_is_frame(ts->value_schema);
        }

        std::string mapping_entry(const ValueView &mapping, std::string_view key)
        {
            if (!mapping.has_value() || !mapping.is_map()) { return {}; }
            auto        map = mapping.as_map();
            const Value probe{Str{std::string{key}}};
            if (!map.contains(probe.view())) { return {}; }
            return std::string{map.at(probe.view()).checked_as<Str>()};
        }

        void start_convert_tsd_frame(const TSInputView &ts, const ValueView &mapping,
                                     const TSOutputView &out, ToFramePlan *&plan_out)
        {
            auto        plan   = std::make_unique<ToFramePlan>();
            plan->dict         = true;
            const auto *child  = ts.schema()->element_ts();
            const auto *fields = child->value_schema;
            if (fields == nullptr || fields->kind != ValueTypeKind::Bundle)
            {
                throw std::invalid_argument("convert: TSD to Frame needs compound-valued elements");
            }
            const std::string key_col = mapping_entry(mapping, "key_col");

            const ValueTypeMetaData *columns = out.schema()->value_schema->element_type;
            if (columns == nullptr)
            {
                // Untyped Frame target: columns are the element's fields
                // (plus the key column when requested).
                std::vector<std::pair<std::string, const ValueTypeMetaData *>> spec;
                if (!key_col.empty()) { spec.emplace_back(key_col, ts.schema()->key_type()); }
                for (std::size_t i = 0; i < fields->field_count; ++i)
                {
                    spec.emplace_back(fields->fields[i].name != nullptr ? fields->fields[i].name : "",
                                      fields->fields[i].type);
                }
                columns = TypeRegistry::instance().un_named_bundle(spec);
            }
            plan->row_meta  = columns;
            plan->converter = &table_converter(columns);
            for (std::size_t i = 0; i < columns->field_count; ++i)
            {
                const char            *name = columns->fields[i].name;
                const std::string_view column{name != nullptr ? name : ""};
                ToFramePlan::Column    entry;
                if (!key_col.empty() && column == key_col) { entry.source = ToFramePlan::Source::Key; }
                else
                {
                    // The TSD child is a TS over a compound VALUE: cells walk
                    // the value's fields, not time-series children.
                    entry.source   = ToFramePlan::Source::ValueField;
                    entry.ts_field = field_index_of(fields, column, "convert");
                }
                plan->columns.push_back(entry);
            }
            plan_out = plan.release();
        }

        void start_convert_value_frame(const TSInputView &ts, const TSOutputView &out,
                                       ToFramePlan *&plan_out)
        {
            auto        plan  = std::make_unique<ToFramePlan>();
            const auto *value = ts.schema()->value_schema;
            const auto *element =
                value->kind == ValueTypeKind::List ? value->element_type : value;
            if (element == nullptr || element->kind != ValueTypeKind::Bundle)
            {
                throw std::invalid_argument("convert: value to Frame needs a compound payload");
            }
            const ValueTypeMetaData *columns = out.schema()->value_schema->element_type;
            if (columns == nullptr) { columns = element; }
            plan->row_meta  = columns;
            plan->converter = &table_converter(columns);
            for (std::size_t i = 0; i < columns->field_count; ++i)
            {
                const char *name = columns->fields[i].name;
                ToFramePlan::Column entry;
                entry.source   = ToFramePlan::Source::Field;
                entry.ts_field = field_index_of(element, name != nullptr ? name : "", "convert");
                plan->columns.push_back(entry);
            }
            plan_out = plan.release();
        }

        namespace
        {
            [[nodiscard]] Value value_row(const ToFramePlan &plan, const ValueView &element)
            {
                Value row{checked_binding(plan.row_meta, "convert")};
                for (std::size_t i = 0; i < plan.columns.size(); ++i)
                {
                    auto      bundle = element.as_bundle();
                    ValueView cell   = ValueView{bundle.at(plan.columns[i].ts_field).binding(),
                                                 bundle.at(plan.columns[i].ts_field).data()};
                    set_bundle_field(row, i, cell);
                }
                return row;
            }

            void publish_frame(const ToFramePlan &plan, std::vector<Value> rows,
                               const TSOutputView &out)
            {
                Frame frame = frame_from_values(*plan.converter, rows);
                Value boxed{checked_binding(out.schema()->value_schema, "convert")};
                *static_cast<Frame *>(const_cast<void *>(boxed.view().data())) = std::move(frame);
                apply_current_value(out, boxed.view());
            }
        }  // namespace

        void eval_convert_value_frame(const ToFramePlan &plan, const TSInputView &ts,
                                      const TSOutputView &out)
        {
            std::vector<Value> rows;
            const ValueView    value = ts.value();
            if (value.schema()->kind == ValueTypeKind::List)
            {
                auto list = value.as_list();
                for (ValueView element : list) { rows.push_back(value_row(plan, element)); }
            }
            else { rows.push_back(value_row(plan, value)); }
            publish_frame(plan, std::move(rows), out);
        }

        void eval_convert_frame_frame(const ValueView &mapping, const TSInputView &ts,
                                      const TSOutputView &out)
        {
            Frame frame = ts.value().checked_as<Frame>();
            if (mapping.has_value() && mapping.is_map() && mapping.as_map().size() > 0)
            {
                std::vector<std::pair<std::string, std::string>> renames;
                auto map = mapping.as_map();
                for (auto &&[key, value] : map.items())
                {
                    renames.emplace_back(std::string{key.checked_as<Str>()},
                                         std::string{value.checked_as<Str>()});
                }
                frame = frame_rename_columns(frame, renames);
            }
            Value boxed{checked_binding(out.schema()->value_schema, "convert")};
            *static_cast<Frame *>(const_cast<void *>(boxed.view().data())) = std::move(frame);
            apply_current_value(out, boxed.view());
        }

        void start_combine_frame(const TSInputView &ts, const TSOutputView &out, ToFramePlan *&plan_out)
        {
            auto        plan    = std::make_unique<ToFramePlan>();
            const auto *columns = frame_columns_schema(out.schema()->value_schema, "combine");
            const auto *bundle  = ts.schema()->value_schema;   // the structural TSB
            plan->row_meta      = columns;
            plan->converter     = &table_converter(columns);
            for (std::size_t i = 0; i < columns->field_count; ++i)
            {
                const char *name = columns->fields[i].name;
                ToFramePlan::Column entry;
                entry.source   = ToFramePlan::Source::Field;
                entry.ts_field = field_index_of(bundle, name != nullptr ? name : "", "combine");
                plan->columns.push_back(entry);
            }
            plan_out = plan.release();
        }

        void eval_combine_frame(const ToFramePlan &plan, const TSInputView &ts, const TSOutputView &out)
        {
            // Each input field is a TS[tuple[T, ...]] COLUMN; rows zip them.
            auto        bundle = const_cast<TSInputView &>(ts).as_bundle();
            std::size_t rows_n = 0;
            {
                auto first = bundle.at(plan.columns.front().ts_field);
                if (!first.valid()) { return; }
                rows_n = first.value().as_indexed_view().size();
            }
            std::vector<Value> rows;
            for (std::size_t r = 0; r < rows_n; ++r)
            {
                Value row{checked_binding(plan.row_meta, "combine")};
                for (std::size_t i = 0; i < plan.columns.size(); ++i)
                {
                    auto child = bundle.at(plan.columns[i].ts_field);
                    if (!child.valid()) { continue; }
                    auto column = child.value().as_indexed_view();
                    if (r >= column.size()) { continue; }
                    const ValueView &cell = column.at(r);
                    set_bundle_field(row, i, cell);
                }
                rows.push_back(std::move(row));
            }
            publish_frame(plan, std::move(rows), out);
        }
    }  // namespace data_frame_detail

    void register_data_frame_operators()
    {
        register_overload<from_data_frame, from_data_frame_impl>();
        register_overload<to_data_frame, to_data_frame_impl>();
        register_overload<group_by, group_by_impl>();
        register_overload<convert, convert_tsd_to_frame_impl>();
        register_overload<convert, convert_frame_to_frame_impl>();
        register_overload<convert, convert_value_to_frame_impl>();
        register_overload<combine, combine_frame_impl>();
    }
}  // namespace hgraph::stdlib

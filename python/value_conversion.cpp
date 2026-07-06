#include "module_internal.h"

#include <hgraph/lib/std/operators/arithmetic.h>

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <nanobind/stl/string.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::python_bridge
{
    PyObj::PyObj(nb::object value) noexcept
        : object(value.release().ptr())
    {
    }

    PyObj::PyObj(const PyObj &other) noexcept
        : object(other.object)
    {
        if (object != nullptr)
        {
            nb::gil_scoped_acquire gil;
            Py_INCREF(object);
        }
    }

    PyObj::PyObj(PyObj &&other) noexcept
        : object(other.object)
    {
        other.object = nullptr;
    }

    PyObj &PyObj::operator=(const PyObj &other) noexcept
    {
        if (this == &other) { return *this; }
        PyObj copy{other};
        std::swap(object, copy.object);
        return *this;
    }

    PyObj &PyObj::operator=(PyObj &&other) noexcept
    {
        std::swap(object, other.object);
        return *this;
    }

    PyObj::~PyObj()
    {
        if (object != nullptr)
        {
            nb::gil_scoped_acquire gil;
            Py_DECREF(object);
        }
    }

    nb::object PyObj::get() const { return nb::borrow(nb::handle(object)); }

    bool operator==(const PyObj &lhs, const PyObj &rhs) noexcept
    {
        if (lhs.object == rhs.object) { return true; }
        if (lhs.object == nullptr || rhs.object == nullptr) { return false; }
        nb::gil_scoped_acquire gil;
        const int result = PyObject_RichCompareBool(lhs.object, rhs.object, Py_EQ);
        return result == 1;
    }

    nb::object &cmp_result_enum_slot()
    {
        static auto *slot = new nb::object{};
        return *slot;
    }

    nb::object &divide_by_zero_enum_slot()
    {
        static auto *slot = new nb::object{};
        return *slot;
    }

    nb::object &removed_sentinel_slot()
    {
        static auto *slot = new nb::object{};
        return *slot;
    }

    nb::dict &bundle_class_registry()
    {
        static nb::dict *registry = new nb::dict();
        return *registry;
    }

    namespace
    {
        [[nodiscard]] bool is_py_date(nb::handle object)
        {
            return nb::hasattr(object, "year") && nb::hasattr(object, "month") && nb::hasattr(object, "day") &&
                   !nb::hasattr(object, "hour");
        }

        [[nodiscard]] bool is_py_time(nb::handle object)
        {
            return nb::hasattr(object, "hour") && nb::hasattr(object, "minute") && !nb::hasattr(object, "year");
        }

        [[nodiscard]] Value py_container_to_value(nb::handle object)
        {
            auto &registry = TypeRegistry::instance();
            const auto build = [&](const ValueTypeMetaData *schema, auto &&fill) {
                const auto *binding = ValuePlanFactory::instance().binding_for(schema);
                Value       value{*binding};
                fill(value.begin_mutation());
                return value;
            };
            if (nb::isinstance<nb::frozenset>(object) || nb::isinstance<nb::set>(object))
            {
                std::vector<Value> elements;
                for (nb::handle item : object) { elements.push_back(py_to_value(item)); }
                if (elements.empty()) { throw nb::type_error("cannot infer the element type of an empty set"); }
                return build(registry.mutable_set(elements.front().schema()), [&](ValueView view) {
                    MutableSetView set{std::move(view)};
                    for (const auto &element : elements) { set.add(element.view()); }
                });
            }
            if (nb::isinstance<nb::dict>(object))
            {
                std::vector<std::pair<Value, Value>> entries;
                for (auto [key, item] : nb::cast<nb::dict>(object))
                {
                    entries.emplace_back(py_to_value(key), py_to_value(item));
                }
                if (entries.empty()) { throw nb::type_error("cannot infer the key/value types of an empty dict"); }
                return build(registry.mutable_map(entries.front().first.schema(), entries.front().second.schema()),
                             [&](ValueView view) {
                                 MutableMapView map{std::move(view)};
                                 for (const auto &[key, item] : entries) { map.set_item(key.view(), item.view()); }
                             });
            }
            if (nb::isinstance<nb::tuple>(object) || nb::isinstance<nb::list>(object))
            {
                std::vector<Value> elements;
                for (nb::handle item : object) { elements.push_back(py_to_value(item)); }
                if (elements.empty()) { throw nb::type_error("cannot infer the element type of an empty sequence"); }
                return build(registry.mutable_list(elements.front().schema()), [&](ValueView view) {
                    MutableListView list{std::move(view)};
                    for (const auto &element : elements) { list.push_back(element.view()); }
                });
            }
            throw nb::type_error("unsupported Python value for hgraph");
        }

        [[nodiscard]] nb::object atomic_to_py(const ValueView &view)
        {
            const auto *meta = view.schema();
            if (meta == scalar_descriptor<Bool>::value_meta()) { return nb::cast(view.checked_as<Bool>()); }
            if (meta == scalar_descriptor<Int>::value_meta()) { return nb::cast(view.checked_as<Int>()); }
            if (meta == scalar_descriptor<Float>::value_meta()) { return nb::cast(view.checked_as<Float>()); }
            if (meta == scalar_descriptor<Str>::value_meta()) { return nb::cast(view.checked_as<Str>()); }
            if (meta == scalar_descriptor<DateTime>::value_meta()) { return nb::cast(view.checked_as<DateTime>()); }
            if (meta == scalar_descriptor<TimeDelta>::value_meta()) { return nb::cast(view.checked_as<TimeDelta>()); }
            if (meta == scalar_descriptor<Date>::value_meta())
            {
                const auto &date = view.checked_as<Date>();
                return nb::module_::import_("datetime").attr("date")(static_cast<int>(date.year()),
                                                                     static_cast<unsigned>(date.month()),
                                                                     static_cast<unsigned>(date.day()));
            }
            if (meta == scalar_descriptor<Time>::value_meta())
            {
                const auto micros = view.checked_as<Time>().microseconds;
                return nb::module_::import_("datetime").attr("time")(
                    static_cast<int>(micros / 3'600'000'000LL), static_cast<int>(micros / 60'000'000LL % 60),
                    static_cast<int>(micros / 1'000'000LL % 60), static_cast<int>(micros % 1'000'000LL));
            }
            if (meta == scalar_descriptor<Frame>::value_meta()) { return frame_to_py(view.checked_as<Frame>()); }
            if (meta == scalar_descriptor<PyObj>::value_meta()) { return view.checked_as<PyObj>().get(); }
            if (meta != nullptr && meta->display_name != nullptr &&
                std::string_view{meta->display_name} == "TimeSeriesReference")
            {
                return nb::cast(PyOpaqueRef{Value{view}});
            }
            if (meta == scalar_descriptor<stdlib::CmpResult>::value_meta())
            {
                const auto value = static_cast<std::int64_t>(view.checked_as<stdlib::CmpResult>());
                nb::object &enum_class = cmp_result_enum_slot();
                return enum_class.is_valid() ? enum_class(value) : nb::cast(value);
            }
            if (meta == scalar_descriptor<Bytes>::value_meta())
            {
                const auto &bytes = view.checked_as<Bytes>();
                return nb::cast(nb::bytes(bytes.data.data(), bytes.data.size()));
            }
            throw nb::type_error((std::string{"unsupported hgraph atomic for Python: "} +
                                  (meta != nullptr && meta->display_name ? meta->display_name : "?"))
                                     .c_str());
        }
    }  // namespace

    Value py_to_value(nb::handle object)
    {
        if (nb::isinstance<nb::bool_>(object)) { return Value{Bool{nb::cast<bool>(object)}}; }
        if (nb::isinstance<nb::int_>(object)) { return Value{Int{nb::cast<Int>(object)}}; }
        if (nb::isinstance<nb::float_>(object)) { return Value{Float{nb::cast<Float>(object)}}; }
        if (nb::isinstance<nb::str>(object)) { return Value{Str{nb::cast<std::string>(object)}}; }
        if (nb::isinstance<nb::bytes>(object))
        {
            auto raw = nb::cast<nb::bytes>(object);
            return Value{Bytes{std::string{raw.c_str(), raw.size()}}};
        }
        if (is_py_date(object))
        {
            return Value{Date{std::chrono::year{nb::cast<int>(object.attr("year"))},
                              std::chrono::month{nb::cast<unsigned>(object.attr("month"))},
                              std::chrono::day{nb::cast<unsigned>(object.attr("day"))}}};
        }
        if (is_py_time(object))
        {
            const std::int64_t micros = nb::cast<std::int64_t>(object.attr("hour")) * 3'600'000'000LL +
                                        nb::cast<std::int64_t>(object.attr("minute")) * 60'000'000LL +
                                        nb::cast<std::int64_t>(object.attr("second")) * 1'000'000LL +
                                        nb::cast<std::int64_t>(object.attr("microsecond"));
            return Value{Time{micros}};
        }
        DateTime when;
        if (nb::try_cast<DateTime>(object, when)) { return Value{when}; }
        TimeDelta delta;
        if (nb::try_cast<TimeDelta>(object, delta)) { return Value{delta}; }
        if (nb::hasattr(object, "__arrow_c_stream__")) { return py_arrow_to_frame(object); }
        if (nb::isinstance<PyOpaqueRef>(object)) { return Value{nb::cast<PyOpaqueRef &>(object).value.view()}; }
        if (cmp_result_enum_slot().is_valid() && nb::isinstance(object, cmp_result_enum_slot()))
        {
            return Value{static_cast<stdlib::CmpResult>(nb::cast<std::int64_t>(object.attr("value")))};
        }
        if (divide_by_zero_enum_slot().is_valid() && nb::isinstance(object, divide_by_zero_enum_slot()))
        {
            return Value{static_cast<stdlib::DivideByZero>(nb::cast<std::int64_t>(object.attr("value")))};
        }
        if (nb::isinstance<nb::frozenset>(object) || nb::isinstance<nb::set>(object) ||
            nb::isinstance<nb::dict>(object) || nb::isinstance<nb::tuple>(object) ||
            nb::isinstance<nb::list>(object))
        {
            return py_container_to_value(object);
        }
        return Value{PyObj{nb::borrow(object)}};
    }

    nb::object PyArrowStream::capsule() const
    {
        auto reader = std::make_shared<arrow::TableBatchReader>(*frame.table);
        auto *stream = new ArrowArrayStream{};
        const auto status = arrow::ExportRecordBatchReader(std::move(reader), stream);
        if (!status.ok())
        {
            delete stream;
            throw std::runtime_error("arrow export failed: " + status.ToString());
        }
        return nb::steal(PyCapsule_New(stream, "arrow_array_stream", [](PyObject *object) {
            auto *raw = static_cast<ArrowArrayStream *>(PyCapsule_GetPointer(object, "arrow_array_stream"));
            if (raw != nullptr)
            {
                if (raw->release != nullptr) { raw->release(raw); }
                delete raw;
            }
        }));
    }

    nb::object frame_to_py(const Frame &frame)
    {
        if (!frame.has_value()) { return nb::none(); }
        nb::object stream = nb::cast(PyArrowStream{frame});
        return nb::module_::import_("pyarrow").attr("table")(stream);
    }

    Value py_arrow_to_frame(nb::handle object)
    {
        nb::object capsule = object.attr("__arrow_c_stream__")();
        auto *stream = static_cast<ArrowArrayStream *>(PyCapsule_GetPointer(capsule.ptr(), "arrow_array_stream"));
        if (stream == nullptr) { throw nb::type_error("expected an arrow_array_stream capsule"); }
        auto reader = arrow::ImportRecordBatchReader(stream);
        if (!reader.ok()) { throw std::runtime_error("arrow import failed: " + reader.status().ToString()); }
        auto table = arrow::Table::FromRecordBatchReader(reader->get());
        if (!table.ok()) { throw std::runtime_error("arrow read failed: " + table.status().ToString()); }
        return Value{Frame{.table = std::move(*table)}};
    }

    nb::object value_to_py(const ValueView &view)
    {
        if (!view.valid()) { return nb::none(); }
        const auto *meta = view.schema();
        switch (meta->kind)
        {
            case ValueTypeKind::Atomic:
                return atomic_to_py(view);
            case ValueTypeKind::Tuple: {
                auto     tuple = view.as_tuple();
                nb::list items;
                for (std::size_t index = 0; index < tuple.size(); ++index) { items.append(value_to_py(tuple.at(index))); }
                return nb::tuple(items);
            }
            case ValueTypeKind::Bundle: {
                auto bundle = view.as_bundle();
                if (!meta->name().empty())
                {
                    nb::dict &classes = bundle_class_registry();
                    nb::str   key{std::string{meta->name()}.c_str()};
                    if (classes.contains(key))
                    {
                        nb::dict kwargs;
                        for (std::size_t index = 0; index < meta->field_count; ++index)
                        {
                            auto field = bundle.at(index);
                            kwargs[meta->fields[index].name] = field.has_value() ? value_to_py(field) : nb::none();
                        }
                        return classes[key](**kwargs);
                    }
                }
                nb::dict result;
                for (std::size_t index = 0; index < meta->field_count; ++index)
                {
                    auto field = bundle.at(index);
                    if (!field.has_value()) { continue; }
                    result[meta->fields[index].name] = value_to_py(field);
                }
                return result;
            }
            case ValueTypeKind::List: {
                nb::list result;
                for (const ValueView &element : view.as_list()) { result.append(value_to_py(element)); }
                if (meta->has(ValueTypeFlags::VariadicTuple)) { return nb::tuple(result); }
                return result;
            }
            case ValueTypeKind::Set: {
                nb::list items;
                for (const ValueView &element : view.as_set().values()) { items.append(value_to_py(element)); }
                return nb::steal(PyFrozenSet_New(nb::list(items).ptr()));
            }
            case ValueTypeKind::Map: {
                nb::dict result;
                for (const auto [key, item] : view.as_map()) { result[value_to_py(key)] = value_to_py(item); }
                return result;
            }
            case ValueTypeKind::Any:
                return value_to_py(view.as_any().get());
            default:
                throw nb::type_error("unsupported hgraph value kind for Python");
        }
    }

    const ValueTypeBinding &delta_binding(const ValueTypeMetaData *meta)
    {
        const auto *binding = ValuePlanFactory::instance().binding_for(meta);
        if (binding == nullptr) { throw nb::type_error("schema has no canonical binding"); }
        return *binding;
    }

    Value py_to_value_as(nb::handle object, const ValueTypeMetaData *meta)
    {
        if (meta == scalar_descriptor<Float>::value_meta() && nb::isinstance<nb::int_>(object) &&
            !nb::isinstance<nb::bool_>(object))
        {
            return Value{Float{static_cast<Float>(nb::cast<Int>(object))}};
        }
        switch (meta->kind)
        {
            case ValueTypeKind::Set: {
                SetBuilder builder{delta_binding(meta->element_type)};
                for (nb::handle item : object)
                {
                    (void)builder.insert_copy(py_to_value_as(item, meta->element_type).view().data());
                }
                return builder.build();
            }
            case ValueTypeKind::Map: {
                MapBuilder builder{delta_binding(meta->key_type), delta_binding(meta->element_type)};
                for (auto [key, item] : nb::cast<nb::dict>(object))
                {
                    builder.set_item_copy(py_to_value_as(key, meta->key_type).view().data(),
                                          py_to_value_as(item, meta->element_type).view().data());
                }
                return builder.build();
            }
            case ValueTypeKind::List: {
                ListBuilder builder{delta_binding(meta->element_type)};
                for (nb::handle item : object)
                {
                    builder.push_back_copy(py_to_value_as(item, meta->element_type).view().data());
                }
                return builder.build();
            }
            case ValueTypeKind::Tuple: {
                BundleBuilder builder{delta_binding(meta)};
                std::size_t   index = 0;
                for (nb::handle item : object)
                {
                    if (index >= meta->field_count) { throw nb::type_error("tuple value has too many items"); }
                    builder.set(index, py_to_value_as(item, meta->fields[index].type));
                    ++index;
                }
                if (index != meta->field_count) { throw nb::type_error("tuple value has too few items"); }
                return builder.build();
            }
            case ValueTypeKind::Bundle: {
                BundleBuilder builder{delta_binding(meta)};
                const bool    is_dict = nb::isinstance<nb::dict>(object);
                for (std::size_t index = 0; index < meta->field_count; ++index)
                {
                    const auto &field = meta->fields[index];
                    if (field.name == nullptr) { continue; }
                    nb::object item;
                    if (is_dict)
                    {
                        nb::dict spec = nb::cast<nb::dict>(object);
                        nb::str  key{field.name};
                        if (!spec.contains(key)) { continue; }
                        item = spec[key];
                    }
                    else
                    {
                        if (!nb::hasattr(object, field.name)) { continue; }
                        item = nb::getattr(object, field.name);
                    }
                    if (item.is_none()) { continue; }
                    builder.set(index, py_to_value_as(item, field.type));
                }
                return builder.build();
            }
            case ValueTypeKind::Any: {
                Value inner = py_to_value(object);
                Value boxed{delta_binding(meta)};
                MutableAnyView{boxed.begin_mutation()}.set(std::move(inner));
                return boxed;
            }
            default:
                break;
        }
        Value value = py_to_value(object);
        if (meta->kind == ValueTypeKind::Any && value.schema() != meta)
        {
            Value boxed{delta_binding(meta)};
            MutableAnyView{boxed.begin_mutation()}.set(std::move(value));
            return boxed;
        }
        return value;
    }

    Value py_to_delta(nb::handle object, const TSValueTypeMetaData *ts)
    {
        switch (ts->kind)
        {
            case TSTypeKind::TS:
                return py_to_value_as(object, ts->value_schema);
            case TSTypeKind::TSS: {
                const auto *elem = ts->value_schema->element_type;
                SetBuilder  added{delta_binding(elem)};
                SetBuilder  removed{delta_binding(elem)};
                nb::handle  add_from = object;
                nb::handle  remove_from{};
                if (nb::isinstance<nb::dict>(object))
                {
                    auto spec = nb::cast<nb::dict>(object);
                    add_from    = spec.contains("added") ? spec["added"] : nb::handle{};
                    remove_from = spec.contains("removed") ? spec["removed"] : nb::handle{};
                }
                if (add_from.is_valid())
                {
                    for (nb::handle item : add_from) { (void)added.insert_copy(py_to_value_as(item, elem).view().data()); }
                }
                if (remove_from.is_valid())
                {
                    for (nb::handle item : remove_from) { (void)removed.insert_copy(py_to_value_as(item, elem).view().data()); }
                }
                BundleBuilder bundle{delta_binding(ts->delta_value_schema)};
                bundle.set("added", added.build());
                bundle.set("removed", removed.build());
                return bundle.build();
            }
            case TSTypeKind::TSD: {
                const auto *key_meta = ts->key_type();
                const auto *child    = ts->element_ts();
                SetBuilder  removed{delta_binding(key_meta)};
                MapBuilder  modified{delta_binding(key_meta), delta_binding(child->delta_value_schema)};
                for (auto [key, item] : nb::cast<nb::dict>(object))
                {
                    Value key_value = py_to_value_as(key, key_meta);
                    const bool remove =
                        item.is_none() ||
                        (removed_sentinel_slot().is_valid() && item.is(removed_sentinel_slot()));
                    if (remove) { (void)removed.insert_copy(key_value.view().data()); }
                    else
                    {
                        Value child_delta = py_to_delta(item, child);
                        modified.set_item_copy(key_value.view().data(), child_delta.view().data());
                    }
                }
                BundleBuilder bundle{delta_binding(ts->delta_value_schema)};
                bundle.set("removed", removed.build());
                bundle.set("modified", modified.build());
                return bundle.build();
            }
            case TSTypeKind::TSL: {
                const auto *map_meta = ts->delta_value_schema;
                MapBuilder  builder{delta_binding(map_meta->key_type), delta_binding(map_meta->element_type)};
                if (nb::isinstance<nb::dict>(object))
                {
                    for (auto [key, item] : nb::cast<nb::dict>(object))
                    {
                        const auto index = nb::cast<std::int64_t>(key);
                        Value child_delta = py_to_delta(item, ts->element_ts());
                        builder.set_item_copy(std::addressof(index), child_delta.view().data());
                    }
                    return builder.build();
                }
                std::int64_t index = 0;
                for (nb::handle item : object)
                {
                    if (!item.is_none())
                    {
                        Value child_delta = py_to_delta(item, ts->element_ts());
                        builder.set_item_copy(std::addressof(index), child_delta.view().data());
                    }
                    ++index;
                }
                return builder.build();
            }
            default:
                return py_to_value_as(object, ts->delta_value_schema);
        }
    }
}  // namespace hgraph::python_bridge

std::size_t std::hash<hgraph::python_bridge::PyObj>::operator()(
    const hgraph::python_bridge::PyObj &value) const noexcept
{
    if (value.object == nullptr) { return 0; }
    nanobind::gil_scoped_acquire gil;
    const Py_hash_t result = PyObject_Hash(value.object);
    if (result == -1)
    {
        PyErr_Clear();
        return std::hash<const void *>{}(value.object);
    }
    return static_cast<std::size_t>(result);
}

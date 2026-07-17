#include "module_internal.h"

#include <hgraph/types/metadata/type_realization.h>

#include <hgraph/lib/std/operators/arithmetic.h>

#include <arrow/array.h>
#include <arrow/c/abi.h>
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
    std::unordered_map<const ValueTypeMetaData *, nb::object> &enum_class_registry()
    {
        static auto *registry = new std::unordered_map<const ValueTypeMetaData *, nb::object>{};
        return *registry;
    }

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
                const auto type = ValuePlanFactory::instance().type_for(schema);
                Value      value{type};
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
            if (!PyCapsule_IsValid(object, "arrow_array_stream")) { return; }
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

    nb::object PySeriesArray::arrow_c_array() const
    {
        auto *c_array  = new ArrowArray{};
        auto *c_schema = new ArrowSchema{};
        const auto status = arrow::ExportArray(*series.array, c_array, c_schema);
        if (!status.ok())
        {
            delete c_array;
            delete c_schema;
            throw std::runtime_error("arrow array export failed: " + status.ToString());
        }
        const auto array_release = [](PyObject *object) {
            if (!PyCapsule_IsValid(object, "arrow_array")) { return; }
            auto *raw = static_cast<ArrowArray *>(PyCapsule_GetPointer(object, "arrow_array"));
            if (raw != nullptr) { if (raw->release != nullptr) { raw->release(raw); } delete raw; }
        };
        const auto schema_release = [](PyObject *object) {
            if (!PyCapsule_IsValid(object, "arrow_schema")) { return; }
            auto *raw = static_cast<ArrowSchema *>(PyCapsule_GetPointer(object, "arrow_schema"));
            if (raw != nullptr) { if (raw->release != nullptr) { raw->release(raw); } delete raw; }
        };
        nb::object schema_capsule = nb::steal(PyCapsule_New(c_schema, "arrow_schema", schema_release));
        nb::object array_capsule  = nb::steal(PyCapsule_New(c_array, "arrow_array", array_release));
        return nb::make_tuple(schema_capsule, array_capsule);
    }

    nb::object series_to_py(const Series &series)
    {
        if (!series.has_value()) { return nb::none(); }
        nb::object holder = nb::cast(PySeriesArray{series});
        return nb::module_::import_("pyarrow").attr("array")(holder);
    }

    Value py_arrow_to_series(nb::handle object)
    {
        nb::object pair = object.attr("__arrow_c_array__")();
        nb::tuple  capsules = nb::cast<nb::tuple>(pair);
        auto *c_schema = static_cast<ArrowSchema *>(
            PyCapsule_GetPointer(capsules[0].ptr(), "arrow_schema"));
        auto *c_array = static_cast<ArrowArray *>(
            PyCapsule_GetPointer(capsules[1].ptr(), "arrow_array"));
        if (c_schema == nullptr || c_array == nullptr)
        {
            throw nb::type_error("expected an (arrow_schema, arrow_array) capsule pair");
        }
        auto array = arrow::ImportArray(c_array, c_schema);
        if (!array.ok()) { throw std::runtime_error("arrow array import failed: " + array.status().ToString()); }
        return Value{Series{.array = std::move(*array)}};
    }

    nb::object value_to_py(const ValueView &view)
    {
        if (!view.valid()) { return nb::none(); }
        // Frame/Series -> pyarrow dispatch through the type-erased ops (the
        // python_conversion_traits hooks installed at module init); no
        // kind-switch here (the type-erasure rule - conversion lives with the
        // ops).
        if (view.schema()->name() == "TimeSeriesReference")
        {
            return nb::cast(PyOpaqueRef{Value{view}, MIN_DT});   // opaque per the REF ruling
        }
        return view.binding().ops_ref().to_python(view.data());
    }

    ValueTypeRef delta_binding(const ValueTypeMetaData *meta)
    {
        if (const auto *snapshot = active_type_realization())
        {
            if (const auto realized = snapshot->type_for(meta)) { return realized; }
        }
        const auto type = ValuePlanFactory::instance().type_for(meta);
        if (!type) { throw nb::type_error("schema has no canonical type"); }
        return type;
    }

    [[nodiscard]] const ValueTypeMetaData *python_bundle_source_schema(
        nb::handle object, const ValueTypeMetaData *declared)
    {
        if (declared == nullptr || !declared->is_named_bundle() ||
            declared->bundle_hierarchy == nullptr || declared->bundle_hierarchy->children.empty())
        {
            return declared;
        }

        const auto alternatives = TypeRegistry::instance().bundle_descendants(declared);
        nb::object source = nb::borrow<nb::object>(object);
        if (nb::isinstance<nb::dict>(source))
        {
            nb::dict map = nb::cast<nb::dict>(source);
            nb::str key{std::string{declared->bundle_discriminator()}.c_str()};
            if (!map.contains(key))
            {
                throw std::invalid_argument(
                    "polymorphic Bundle dictionaries require the configured type discriminator");
            }
            const auto requested = nb::cast<std::string>(map[key]);
            const ValueTypeMetaData *match = nullptr;
            for (const auto *alternative : alternatives)
            {
                if (alternative->name() == requested || alternative->bundle_local_name() == requested)
                {
                    if (match != nullptr)
                    {
                        throw std::invalid_argument("polymorphic Bundle discriminator is ambiguous");
                    }
                    match = alternative;
                }
            }
            if (match == nullptr)
            {
                throw std::invalid_argument("polymorphic Bundle discriminator names no valid alternative");
            }
            return match;
        }

        const nb::object source_class = nb::getattr(source, "__class__");
        auto &classes = bundle_class_registry();
        for (const auto *alternative : alternatives)
        {
            nb::int_ key{reinterpret_cast<std::uintptr_t>(alternative)};
            if (classes.contains(key) && source_class.is(classes[key])) { return alternative; }
        }
        throw std::invalid_argument("value is not an instance of a closed Bundle alternative");
    }

    Value py_to_value_as(nb::handle object, const ValueTypeMetaData *meta)
    {
        // Frame/Series -> arrow go through the BINDING's from_python (the
        // type-erased python_conversion_traits hooks); no kind-switch.
        if (nb::isinstance<PyOpaqueRef>(object)) { return Value{nb::cast<PyOpaqueRef &>(object).value.view()}; }
        std::shared_ptr<const TypeRealizationSnapshot> conversion_snapshot;
        const auto *snapshot = active_type_realization();
        if (snapshot == nullptr && meta != nullptr &&
            meta->value_kind() != ValueTypeKind::Atomic &&
            meta->value_kind() != ValueTypeKind::Any)
        {
            conversion_snapshot = TypeRealizationSnapshot::capture(TypeRegistry::instance());
            snapshot = conversion_snapshot.get();
        }
        const auto canonical = ValuePlanFactory::instance().type_for(meta);
        const auto realized = snapshot != nullptr ? snapshot->type_for(meta) : ValueTypeRef{};
        const bool uses_realized_storage = realized && realized != canonical;
        const auto *storage_schema = uses_realized_storage
                                         ? meta
                                         : python_bundle_source_schema(object, meta);
        const auto type = uses_realized_storage
                              ? realized
                              : ValuePlanFactory::instance().type_for(storage_schema);
        if (!type) { throw nb::type_error("schema has no canonical type"); }
        TypeRealizationScope realization_scope{snapshot};
        Value result{type};
        result.view().assign_from_python(object);
        return result;
    }

    void install_arrow_conversion_hooks()
    {
        python_conversion_traits<Frame>::to_python_hook   = &frame_to_py;
        python_conversion_traits<Frame>::from_python_hook = [](nb::handle o) {
            return py_arrow_to_frame(o).view().checked_as<Frame>();
        };
        python_conversion_traits<Series>::to_python_hook   = &series_to_py;
        python_conversion_traits<Series>::from_python_hook = [](nb::handle o) {
            return py_arrow_to_series(o).view().checked_as<Series>();
        };
    }

    Value py_to_delta(nb::handle object, const TSValueTypeMetaData *ts)
    {
        std::shared_ptr<const TypeRealizationSnapshot> conversion_snapshot;
        const auto *snapshot = active_type_realization();
        if (snapshot == nullptr)
        {
            conversion_snapshot = TypeRealizationSnapshot::capture(TypeRegistry::instance());
            snapshot = conversion_snapshot.get();
        }
        TypeRealizationScope realization_scope{snapshot};

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
                    for (auto [key, item] : spec)
                    {
                        // Only the delta spec shape is a dict; anything else
                        // (e.g. {0: 1}) is not a set delta.
                        if (!nb::isinstance<nb::str>(key) ||
                            (nb::cast<std::string>(key) != "added" && nb::cast<std::string>(key) != "removed"))
                        {
                            throw nb::type_error("a TSS delta dict may only carry 'added'/'removed'");
                        }
                    }
                    add_from    = spec.contains("added") ? spec["added"] : nb::handle{};
                    remove_from = spec.contains("removed") ? spec["removed"] : nb::handle{};
                }
                if (add_from.is_valid())
                {
                    for (nb::handle item : add_from)
                    {
                        // hgraph's set-delta literal: Removed(x) members of a
                        // plain set are REMOVALS (TS-schema shaping - this is
                        // exactly py_to_delta's job). The registered Removed
                        // class decides; the name check only covers the
                        // pre-registration import window.
                        if (removed_class_slot().is_valid()
                                ? nb::isinstance(item, removed_class_slot())
                                : (nb::hasattr(item, "item") &&
                                   nb::cast<std::string>(item.type().attr("__name__")) == "Removed"))
                        {
                            (void)removed.insert_copy(py_to_value_as(item.attr("item"), elem).view().data());
                            continue;
                        }
                        (void)added.insert_copy(py_to_value_as(item, elem).view().data());
                    }
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

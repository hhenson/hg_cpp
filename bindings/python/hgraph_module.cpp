#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_ops.h>
#include <hgraph/util/date_time.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/string_view.h>

#include <chrono>
#include <compare>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph
{
    namespace
    {
        struct UnsupportedPythonScalar
        {
            int value{0};
        };

        void register_builtin_value_types()
        {
            auto &registry = TypeRegistry::instance();
            (void)registry.register_scalar<bool>("bool");
            (void)registry.register_scalar<int>("int");
            (void)registry.register_scalar<std::int64_t>("int64");
            (void)registry.register_scalar<double>("double");
            (void)registry.register_scalar<std::string>("string");
            (void)registry.register_scalar<engine_time_t>("engine_time");
            (void)registry.register_scalar<engine_time_delta_t>("engine_time_delta");
            (void)registry.register_scalar<engine_date_t>("engine_date");
        }

        [[nodiscard]] const ValueTypeBinding &binding_for(const ValueTypeMetaData *schema, const char *what)
        {
            if (schema == nullptr) { throw std::invalid_argument(std::string{what} + " requires a schema"); }
            const auto *binding = ValuePlanFactory::instance().binding_for(schema);
            if (binding == nullptr) { throw std::logic_error(std::string{what} + " schema has no binding"); }
            return *binding;
        }

        [[nodiscard]] Value value_from_python(const ValueTypeBinding &binding, nb::handle source)
        {
            Value out{binding};
            out.from_python(source);
            return out;
        }

        [[nodiscard]] Value value_from_python_schema(const ValueTypeMetaData &schema, nb::handle source)
        {
            return value_from_python(binding_for(&schema, "Value.from_python"), source);
        }

        [[nodiscard]] const ValueTypeMetaData *schema_from_handle(nb::handle source, const char *what)
        {
            const auto *schema = nb::cast<const ValueTypeMetaData *>(source);
            if (schema == nullptr) { throw std::invalid_argument(std::string{what} + " requires a schema"); }
            return schema;
        }

        [[nodiscard]] std::vector<const ValueTypeMetaData *> schema_sequence(nb::handle source, const char *what)
        {
            std::vector<const ValueTypeMetaData *> result;
            nb::iterator it = nb::iter(source);
            while (it != nb::iterator::sentinel())
            {
                result.push_back(schema_from_handle(*it, what));
                ++it;
            }
            return result;
        }

        [[nodiscard]] std::vector<std::pair<std::string, const ValueTypeMetaData *>>
        bundle_fields(nb::handle source, const char *what)
        {
            nb::object object = nb::borrow<nb::object>(source);
            nb::object items = nb::hasattr(object, "items") ? object.attr("items")() : object;

            std::vector<std::pair<std::string, const ValueTypeMetaData *>> result;
            nb::iterator it = nb::iter(items);
            while (it != nb::iterator::sentinel())
            {
                nb::tuple pair = nb::cast<nb::tuple>(*it);
                if (pair.size() != 2) { throw std::invalid_argument(std::string{what} + " expects name/schema pairs"); }
                result.emplace_back(nb::cast<std::string>(pair[0]), schema_from_handle(pair[1], what));
                ++it;
            }
            return result;
        }

        [[nodiscard]] const ValueTypeMetaData *int64_schema()
        {
            return TypeRegistry::instance().register_scalar<std::int64_t>("int64");
        }

        [[nodiscard]] const ValueTypeMetaData *string_schema()
        {
            return TypeRegistry::instance().register_scalar<std::string>("string");
        }

        [[nodiscard]] const ValueTypeMetaData *engine_time_schema()
        {
            return TypeRegistry::instance().register_scalar<engine_time_t>("engine_time");
        }

        [[nodiscard]] const ValueTypeMetaData *engine_delta_schema()
        {
            return TypeRegistry::instance().register_scalar<engine_time_delta_t>("engine_time_delta");
        }

        [[nodiscard]] const ValueTypeMetaData *engine_date_schema()
        {
            return TypeRegistry::instance().register_scalar<engine_date_t>("engine_date");
        }

        [[nodiscard]] Value typed_value_from_python(const ValueTypeMetaData *schema, nb::handle source, const char *what)
        {
            return value_from_python(binding_for(schema, what), source);
        }

        [[nodiscard]] Value int_value(nb::handle source)
        {
            return typed_value_from_python(int64_schema(), source, "int_value");
        }

        [[nodiscard]] Value string_value(nb::handle source)
        {
            return typed_value_from_python(string_schema(), source, "string_value");
        }

        [[nodiscard]] Value list_value(const ValueTypeMetaData &element_schema,
                                       nb::handle               source,
                                       std::size_t              fixed_size)
        {
            const auto *schema = TypeRegistry::instance().list(&element_schema, fixed_size);
            return typed_value_from_python(schema, source, "list_value");
        }

        [[nodiscard]] Value set_value(const ValueTypeMetaData &element_schema, nb::handle source)
        {
            const auto *schema = TypeRegistry::instance().set(&element_schema);
            return typed_value_from_python(schema, source, "set_value");
        }

        [[nodiscard]] Value queue_value(const ValueTypeMetaData &element_schema,
                                        nb::handle               source,
                                        std::size_t              max_capacity)
        {
            const auto *schema = TypeRegistry::instance().queue(&element_schema, max_capacity);
            return typed_value_from_python(schema, source, "queue_value");
        }

        [[nodiscard]] Value cyclic_buffer_value(const ValueTypeMetaData &element_schema,
                                                nb::handle               source,
                                                std::size_t              capacity)
        {
            const auto *schema = TypeRegistry::instance().cyclic_buffer(&element_schema, capacity);
            return typed_value_from_python(schema, source, "cyclic_buffer_value");
        }

        [[nodiscard]] Value tuple_value(nb::handle schemas, nb::handle source)
        {
            const auto *schema = TypeRegistry::instance().tuple(schema_sequence(schemas, "tuple_value"));
            return typed_value_from_python(schema, source, "tuple_value");
        }

        [[nodiscard]] Value bundle_value(std::string name, nb::handle fields, nb::handle source)
        {
            const auto *schema = TypeRegistry::instance().bundle(name, bundle_fields(fields, "bundle_value"));
            return typed_value_from_python(schema, source, "bundle_value");
        }

        [[nodiscard]] Value map_value(const ValueTypeMetaData &key_schema,
                                      const ValueTypeMetaData &value_schema,
                                      nb::handle               source)
        {
            const auto *schema = TypeRegistry::instance().map(&key_schema, &value_schema);
            return typed_value_from_python(schema, source, "map_value");
        }

        [[nodiscard]] Value engine_time_list(nb::handle microseconds)
        {
            const auto        &binding = binding_for(engine_time_schema(), "engine_time_list");
            ListBuilder        builder{binding};
            nb::iterator       it = nb::iter(microseconds);
            while (it != nb::iterator::sentinel())
            {
                const auto value = engine_time_t{engine_time_delta_t{nb::cast<std::int64_t>(*it)}};
                builder.push_back(value);
                ++it;
            }
            return builder.build();
        }

        [[nodiscard]] Value engine_delta_cyclic_buffer(nb::handle microseconds, std::size_t capacity)
        {
            const auto          &binding = binding_for(engine_delta_schema(), "engine_delta_cyclic_buffer");
            CyclicBufferBuilder  builder{binding, capacity};
            nb::iterator         it = nb::iter(microseconds);
            while (it != nb::iterator::sentinel())
            {
                const auto value = engine_time_delta_t{nb::cast<std::int64_t>(*it)};
                builder.push_back(value);
                ++it;
            }
            return builder.build();
        }

        [[nodiscard]] Value engine_date_queue(nb::handle days)
        {
            const auto  &binding = binding_for(engine_date_schema(), "engine_date_queue");
            QueueBuilder builder{binding};
            nb::iterator it = nb::iter(days);
            while (it != nb::iterator::sentinel())
            {
                const auto value = engine_date_t{std::chrono::sys_days{std::chrono::days{nb::cast<std::int64_t>(*it)}}};
                builder.push(value);
                ++it;
            }
            return builder.build();
        }

        void unsupported_scalar_to_python()
        {
            UnsupportedPythonScalar value{7};
            (void)ops_for<UnsupportedPythonScalar>().to_python(&value);
        }

        [[nodiscard]] std::string schema_name(const ValueTypeMetaData &schema)
        {
            return schema.display_name != nullptr ? std::string{schema.display_name} : std::string{};
        }

        [[nodiscard]] nb::list schema_fields(const ValueTypeMetaData &schema)
        {
            nb::list result;
            for (std::size_t index = 0; index < schema.field_count; ++index)
            {
                const ValueFieldMetaData &field = schema.fields[index];
                result.append(nb::make_tuple(field.name != nullptr ? std::string{field.name} : std::string{},
                                             nb::cast(field.type, nb::rv_policy::reference)));
            }
            return result;
        }

        [[nodiscard]] nb::object ordering_to_python(std::partial_ordering order)
        {
            if (order == std::partial_ordering::less) { return nb::cast(-1); }
            if (order == std::partial_ordering::greater) { return nb::cast(1); }
            if (order == std::partial_ordering::equivalent) { return nb::cast(0); }
            return nb::none();
        }

        [[nodiscard]] nb::list indexed_values_to_python(const IndexedValueView &view)
        {
            nb::list result;
            for (std::size_t index = 0; index < view.size(); ++index)
            {
                result.append(view.at(index).to_python());
            }
            return result;
        }

        [[nodiscard]] nb::list set_values_to_python(const SetView &view)
        {
            nb::list result;
            for (const auto value : view.values())
            {
                result.append(value.to_python());
            }
            return result;
        }

        [[nodiscard]] nb::list map_keys_to_python(const MapView &view)
        {
            nb::list result;
            for (const auto key : view.keys())
            {
                result.append(key.to_python());
            }
            return result;
        }

        [[nodiscard]] nb::list map_values_to_python(const MapView &view)
        {
            nb::list result;
            for (const auto value : view.values())
            {
                result.append(value.to_python());
            }
            return result;
        }

        [[nodiscard]] nb::list map_items_to_python(const MapView &view)
        {
            nb::list result;
            for (const auto entry : view.items())
            {
                result.append(nb::make_tuple(entry.first.to_python(), entry.second.to_python()));
            }
            return result;
        }

        void register_value_bindings(nb::module_ &m)
        {
            nb::module_ value_mod = m.def_submodule("value", "Value-layer C++ abstractions");

            nb::enum_<ValueTypeKind>(value_mod, "ValueTypeKind")
                .value("Atomic", ValueTypeKind::Atomic)
                .value("Tuple", ValueTypeKind::Tuple)
                .value("Bundle", ValueTypeKind::Bundle)
                .value("List", ValueTypeKind::List)
                .value("Set", ValueTypeKind::Set)
                .value("Map", ValueTypeKind::Map)
                .value("CyclicBuffer", ValueTypeKind::CyclicBuffer)
                .value("Queue", ValueTypeKind::Queue);

            nb::class_<ValueTypeMetaData>(value_mod, "ValueTypeMetaData")
                .def_prop_ro("name", &schema_name)
                .def_ro("kind", &ValueTypeMetaData::kind)
                .def_ro("field_count", &ValueTypeMetaData::field_count)
                .def_ro("fixed_size", &ValueTypeMetaData::fixed_size)
                .def_prop_ro("element_type",
                             [](const ValueTypeMetaData &self) { return self.element_type; },
                             nb::rv_policy::reference)
                .def_prop_ro("key_type",
                             [](const ValueTypeMetaData &self) { return self.key_type; },
                             nb::rv_policy::reference)
                .def_prop_ro("fields", &schema_fields)
                .def("is_buffer_compatible", &ValueTypeMetaData::is_buffer_compatible)
                .def("is_hashable", &ValueTypeMetaData::is_hashable)
                .def("is_equatable", &ValueTypeMetaData::is_equatable)
                .def("is_comparable", &ValueTypeMetaData::is_comparable)
                .def("__repr__", [](const ValueTypeMetaData &self) {
                    return std::string{"ValueTypeMetaData("} + schema_name(self) + ")";
                });

            nb::class_<TypeRegistry>(value_mod, "TypeRegistry")
                .def_static("instance", []() -> TypeRegistry & { return TypeRegistry::instance(); },
                            nb::rv_policy::reference)
                .def("register_builtin_value_types", [](TypeRegistry &) { register_builtin_value_types(); })
                .def("bool", [](TypeRegistry &self) { return self.register_scalar<bool>("bool"); },
                     nb::rv_policy::reference)
                .def("int", [](TypeRegistry &self) { return self.register_scalar<int>("int"); },
                     nb::rv_policy::reference)
                .def("int64", [](TypeRegistry &self) { return self.register_scalar<std::int64_t>("int64"); },
                     nb::rv_policy::reference)
                .def("double", [](TypeRegistry &self) { return self.register_scalar<double>("double"); },
                     nb::rv_policy::reference)
                .def("string", [](TypeRegistry &self) { return self.register_scalar<std::string>("string"); },
                     nb::rv_policy::reference)
                .def("engine_time", [](TypeRegistry &self) { return self.register_scalar<engine_time_t>("engine_time"); },
                     nb::rv_policy::reference)
                .def("engine_time_delta",
                     [](TypeRegistry &self) { return self.register_scalar<engine_time_delta_t>("engine_time_delta"); },
                     nb::rv_policy::reference)
                .def("engine_date", [](TypeRegistry &self) { return self.register_scalar<engine_date_t>("engine_date"); },
                     nb::rv_policy::reference)
                .def("value_type", &TypeRegistry::value_type, "name"_a, nb::rv_policy::reference)
                .def("tuple", [](TypeRegistry &self, nb::handle schemas) {
                         return self.tuple(schema_sequence(schemas, "TypeRegistry.tuple"));
                     }, "schemas"_a, nb::rv_policy::reference)
                .def("bundle", [](TypeRegistry &self, std::string name, nb::handle fields) {
                         return self.bundle(name, bundle_fields(fields, "TypeRegistry.bundle"));
                     }, "name"_a, "fields"_a, nb::rv_policy::reference)
                .def("list", [](TypeRegistry &self, const ValueTypeMetaData &element_schema, std::size_t fixed_size) {
                         return self.list(&element_schema, fixed_size);
                     }, "element_schema"_a, "fixed_size"_a = 0, nb::rv_policy::reference)
                .def("set", [](TypeRegistry &self, const ValueTypeMetaData &element_schema) {
                         return self.set(&element_schema);
                     }, "element_schema"_a, nb::rv_policy::reference)
                .def("map", [](TypeRegistry &self,
                                const ValueTypeMetaData &key_schema,
                                const ValueTypeMetaData &value_schema) {
                         return self.map(&key_schema, &value_schema);
                     }, "key_schema"_a, "value_schema"_a, nb::rv_policy::reference)
                .def("cyclic_buffer",
                     [](TypeRegistry &self, const ValueTypeMetaData &element_schema, std::size_t capacity) {
                         return self.cyclic_buffer(&element_schema, capacity);
                     }, "element_schema"_a, "capacity"_a, nb::rv_policy::reference)
                .def("queue", [](TypeRegistry &self, const ValueTypeMetaData &element_schema, std::size_t max_capacity) {
                         return self.queue(&element_schema, max_capacity);
                     }, "element_schema"_a, "max_capacity"_a = 0, nb::rv_policy::reference);

            nb::class_<ValueView>(value_mod, "ValueView")
                .def("valid", &ValueView::valid)
                .def("bound", &ValueView::bound)
                .def("has_value", &ValueView::has_value)
                .def("mutable_payload", &ValueView::mutable_payload)
                .def("writable_payload", &ValueView::writable_payload)
                .def("can_begin_mutation", &ValueView::can_begin_mutation)
                .def("begin_mutation", &ValueView::begin_mutation, nb::keep_alive<0, 1>())
                .def("end_mutation", &ValueView::end_mutation)
                .def("__bool__", &ValueView::valid)
                .def_prop_ro("schema", [](const ValueView &self) { return self.schema(); }, nb::rv_policy::reference)
                .def("to_python", &ValueView::to_python)
                .def("from_python", [](ValueView &self, nb::handle source) -> ValueView & {
                    self.from_python(source);
                    return self;
                }, "source"_a, nb::rv_policy::reference_internal)
                .def("to_string", &ValueView::to_string)
                .def("equals", &ValueView::equals, "other"_a)
                .def("compare", [](const ValueView &self, const ValueView &other) {
                    return ordering_to_python(self.compare(other));
                }, "other"_a)
                .def("clone", &ValueView::clone)
                .def("as_tuple", &ValueView::as_tuple, nb::keep_alive<0, 1>())
                .def("as_bundle", &ValueView::as_bundle, nb::keep_alive<0, 1>())
                .def("as_list", &ValueView::as_list, nb::keep_alive<0, 1>())
                .def("as_set", &ValueView::as_set, nb::keep_alive<0, 1>())
                .def("as_map", &ValueView::as_map, nb::keep_alive<0, 1>())
                .def("as_cyclic_buffer", &ValueView::as_cyclic_buffer, nb::keep_alive<0, 1>())
                .def("as_queue", &ValueView::as_queue, nb::keep_alive<0, 1>());

            nb::class_<IndexedValueView, ValueView>(value_mod, "IndexedValueView")
                .def("__len__", &IndexedValueView::size)
                .def("size", &IndexedValueView::size)
                .def("at", &IndexedValueView::at, "index"_a, nb::keep_alive<0, 1>())
                .def("__getitem__", &IndexedValueView::at, "index"_a, nb::keep_alive<0, 1>())
                .def("values", &indexed_values_to_python)
                .def("to_python_values", &indexed_values_to_python);

            nb::class_<TupleView, IndexedValueView>(value_mod, "TupleView")
                .def("to_python", [](const TupleView &self) { return nb::tuple(indexed_values_to_python(self)); });

            nb::class_<BundleView, IndexedValueView>(value_mod, "BundleView")
                .def("has_field", &BundleView::has_field, "name"_a)
                .def("field", &BundleView::field, "name"_a, nb::keep_alive<0, 1>())
                .def("at_name",
                     static_cast<const ValueView (BundleView::*)(std::string_view) const>(&BundleView::at),
                     "name"_a,
                     nb::keep_alive<0, 1>())
                .def("to_python", [](const BundleView &self) {
                    nb::dict result;
                    const auto *schema = self.schema();
                    for (std::size_t index = 0; index < schema->field_count; ++index)
                    {
                        const char *name = schema->fields[index].name;
                        if (name != nullptr) { result[nb::str{name}] = self.at(index).to_python(); }
                    }
                    return result;
                });

            nb::class_<ListView, IndexedValueView>(value_mod, "ListView")
                .def("is_fixed", &ListView::is_fixed)
                .def_prop_ro("element_schema", &ListView::element_schema, nb::rv_policy::reference)
                .def("empty", &ListView::empty)
                .def("front", &ListView::front, nb::keep_alive<0, 1>())
                .def("back", &ListView::back, nb::keep_alive<0, 1>());

            nb::class_<CyclicBufferView, IndexedValueView>(value_mod, "CyclicBufferView")
                .def("head", &CyclicBufferView::head)
                .def("capacity", &CyclicBufferView::capacity)
                .def_prop_ro("element_schema", &CyclicBufferView::element_schema, nb::rv_policy::reference)
                .def("empty", &CyclicBufferView::empty)
                .def("full", &CyclicBufferView::full)
                .def("front", &CyclicBufferView::front, nb::keep_alive<0, 1>())
                .def("back", &CyclicBufferView::back, nb::keep_alive<0, 1>());

            nb::class_<QueueView, IndexedValueView>(value_mod, "QueueView")
                .def("max_capacity", &QueueView::max_capacity)
                .def("has_max_capacity", &QueueView::has_max_capacity)
                .def_prop_ro("element_schema", &QueueView::element_schema, nb::rv_policy::reference)
                .def("empty", &QueueView::empty)
                .def("full", &QueueView::full)
                .def("front", &QueueView::front, nb::keep_alive<0, 1>())
                .def("back", &QueueView::back, nb::keep_alive<0, 1>());

            nb::class_<SetView, ValueView>(value_mod, "SetView")
                .def("__len__", &SetView::size)
                .def("size", &SetView::size)
                .def("empty", &SetView::empty)
                .def_prop_ro("element_schema", &SetView::element_schema, nb::rv_policy::reference)
                .def("contains", &SetView::contains, "key"_a)
                .def("values", &set_values_to_python)
                .def("to_python", &ValueView::to_python);

            nb::class_<MapView, ValueView>(value_mod, "MapView")
                .def("__len__", &MapView::size)
                .def("size", &MapView::size)
                .def("empty", &MapView::empty)
                .def_prop_ro("key_schema", &MapView::key_schema, nb::rv_policy::reference)
                .def_prop_ro("value_schema", &MapView::value_schema, nb::rv_policy::reference)
                .def("contains", &MapView::contains, "key"_a)
                .def("at", &MapView::at, "key"_a, nb::keep_alive<0, 1>())
                .def("key_set", &MapView::key_set, nb::keep_alive<0, 1>())
                .def("keys", &map_keys_to_python)
                .def("values", &map_values_to_python)
                .def("items", &map_items_to_python)
                .def("to_python", &ValueView::to_python);

            nb::class_<Value>(value_mod, "Value")
                .def(nb::init<>())
                .def_static("create", &value_from_python_schema, "schema"_a, "source"_a)
                .def("has_value", &Value::has_value)
                .def("__bool__", &Value::has_value)
                .def_prop_ro("schema", [](const Value &self) { return self.schema(); }, nb::rv_policy::reference)
                .def("view", [](Value &self) { return self.view(); }, nb::keep_alive<0, 1>())
                .def("begin_mutation", &Value::begin_mutation, nb::keep_alive<0, 1>())
                .def("to_python", &Value::to_python)
                .def("from_python", [](Value &self, nb::handle source) -> Value & {
                    self.from_python(source);
                    return self;
                }, "source"_a, nb::rv_policy::reference_internal)
                .def("clone", &Value::clone)
                .def("to_string", &Value::to_string)
                .def("equals", static_cast<bool (Value::*)(const Value &) const>(&Value::equals), "other"_a)
                .def("compare", [](const Value &self, const Value &other) {
                    return ordering_to_python(self.compare(other));
                }, "other"_a)
                .def("as_tuple", static_cast<TupleView (Value::*)() const>(&Value::as_tuple), nb::keep_alive<0, 1>())
                .def("as_bundle", static_cast<BundleView (Value::*)() const>(&Value::as_bundle), nb::keep_alive<0, 1>())
                .def("as_list", static_cast<ListView (Value::*)() const>(&Value::as_list), nb::keep_alive<0, 1>())
                .def("as_set", static_cast<SetView (Value::*)() const>(&Value::as_set), nb::keep_alive<0, 1>())
                .def("as_map", static_cast<MapView (Value::*)() const>(&Value::as_map), nb::keep_alive<0, 1>())
                .def("as_cyclic_buffer",
                     static_cast<CyclicBufferView (Value::*)() const>(&Value::as_cyclic_buffer),
                     nb::keep_alive<0, 1>())
                .def("as_queue", static_cast<QueueView (Value::*)() const>(&Value::as_queue), nb::keep_alive<0, 1>())
                .def("__repr__", [](const Value &self) {
                    return self.has_value() ? std::string{"Value("} + self.to_string() + ")"
                                            : std::string{"Value(None)"};
                });

            value_mod.def("register_builtin_value_types", &register_builtin_value_types);
            value_mod.def("value_from_python", &value_from_python_schema, "schema"_a, "source"_a);
            value_mod.def("int_value", &int_value, "source"_a);
            value_mod.def("string_value", &string_value, "source"_a);
            value_mod.def("list_value", &list_value, "element_schema"_a, "source"_a, "fixed_size"_a = 0);
            value_mod.def("set_value", &set_value, "element_schema"_a, "source"_a);
            value_mod.def("queue_value", &queue_value, "element_schema"_a, "source"_a, "max_capacity"_a = 0);
            value_mod.def("cyclic_buffer_value", &cyclic_buffer_value, "element_schema"_a, "source"_a, "capacity"_a);
            value_mod.def("tuple_value", &tuple_value, "schemas"_a, "source"_a);
            value_mod.def("bundle_value", &bundle_value, "name"_a, "fields"_a, "source"_a);
            value_mod.def("map_value", &map_value, "key_schema"_a, "value_schema"_a, "source"_a);
            value_mod.def("engine_time_list", &engine_time_list, "microseconds"_a);
            value_mod.def("engine_delta_cyclic_buffer", &engine_delta_cyclic_buffer, "microseconds"_a, "capacity"_a);
            value_mod.def("engine_date_queue", &engine_date_queue, "days_since_epoch"_a);
            value_mod.def("unsupported_scalar_to_python", &unsupported_scalar_to_python);

            m.attr("ValueTypeKind")    = value_mod.attr("ValueTypeKind");
            m.attr("TypeKind")         = value_mod.attr("ValueTypeKind");
            m.attr("ValueTypeMetaData") = value_mod.attr("ValueTypeMetaData");
            m.attr("TypeRegistry")     = value_mod.attr("TypeRegistry");
            m.attr("Value")            = value_mod.attr("Value");
            m.attr("ValueView")        = value_mod.attr("ValueView");
            m.attr("IndexedValueView") = value_mod.attr("IndexedValueView");
            m.attr("TupleView")        = value_mod.attr("TupleView");
            m.attr("BundleView")       = value_mod.attr("BundleView");
            m.attr("ListView")         = value_mod.attr("ListView");
            m.attr("SetView")          = value_mod.attr("SetView");
            m.attr("MapView")          = value_mod.attr("MapView");
            m.attr("CyclicBufferView") = value_mod.attr("CyclicBufferView");
            m.attr("QueueView")        = value_mod.attr("QueueView");

            m.def("register_builtin_value_types", &register_builtin_value_types);
            m.def("value_from_python", &value_from_python_schema, "schema"_a, "source"_a);
            m.def("int_value", &int_value, "source"_a);
            m.def("string_value", &string_value, "source"_a);
            m.def("list_value", &list_value, "element_schema"_a, "source"_a, "fixed_size"_a = 0);
            m.def("set_value", &set_value, "element_schema"_a, "source"_a);
            m.def("queue_value", &queue_value, "element_schema"_a, "source"_a, "max_capacity"_a = 0);
            m.def("cyclic_buffer_value", &cyclic_buffer_value, "element_schema"_a, "source"_a, "capacity"_a);
            m.def("tuple_value", &tuple_value, "schemas"_a, "source"_a);
            m.def("bundle_value", &bundle_value, "name"_a, "fields"_a, "source"_a);
            m.def("map_value", &map_value, "key_schema"_a, "value_schema"_a, "source"_a);
            m.def("engine_time_list", &engine_time_list, "microseconds"_a);
            m.def("engine_delta_cyclic_buffer", &engine_delta_cyclic_buffer, "microseconds"_a, "capacity"_a);
            m.def("engine_date_queue", &engine_date_queue, "days_since_epoch"_a);
            m.def("unsupported_scalar_to_python", &unsupported_scalar_to_python);
        }
    }  // namespace
}  // namespace hgraph

NB_MODULE(_hgraph, m)
{
    nb::set_leak_warnings(false);
    m.doc() = "The HGraph C++ runtime engine";

    hgraph::register_builtin_value_types();
    hgraph::register_value_bindings(m);
}

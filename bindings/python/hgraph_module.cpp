#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
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
            (void)registry.register_scalar<Int>("int");
            (void)registry.register_scalar<std::int32_t>("int32");
            (void)registry.register_scalar<std::int64_t>("int64");
            (void)registry.register_scalar<double>("double");
            (void)registry.register_scalar<std::string>("string");
            (void)registry.register_scalar<DateTime>("datetime");
            (void)registry.register_scalar<TimeDelta>("timedelta");
            (void)registry.register_scalar<Date>("date");
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

        [[nodiscard]] const TSValueTypeMetaData *ts_schema_from_handle(nb::handle source, const char *what)
        {
            const auto *schema = nb::cast<const TSValueTypeMetaData *>(source);
            if (schema == nullptr) { throw std::invalid_argument(std::string{what} + " requires a TS schema"); }
            return schema;
        }

        [[nodiscard]] std::vector<std::pair<std::string, const TSValueTypeMetaData *>>
        ts_bundle_fields(nb::handle source, const char *what)
        {
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> result;
            nb::object object = nb::borrow<nb::object>(source);
            nb::object items = nb::hasattr(object, "items") ? object.attr("items")() : object;

            nb::iterator it = nb::iter(items);
            while (it != nb::iterator::sentinel())
            {
                nb::tuple pair = nb::cast<nb::tuple>(*it);
                if (pair.size() != 2) { throw std::invalid_argument(std::string{what} + " expects name/schema pairs"); }
                result.emplace_back(nb::cast<std::string>(pair[0]), ts_schema_from_handle(pair[1], what));
                ++it;
            }
            return result;
        }

        [[nodiscard]] std::vector<TSEndpointSchema> endpoint_schema_sequence(nb::handle source, const char *)
        {
            std::vector<TSEndpointSchema> result;
            nb::iterator it = nb::iter(source);
            while (it != nb::iterator::sentinel())
            {
                result.push_back(nb::cast<TSEndpointSchema>(*it));
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

        [[nodiscard]] const ValueTypeMetaData *datetime_schema()
        {
            return TypeRegistry::instance().register_scalar<DateTime>("datetime");
        }

        [[nodiscard]] const ValueTypeMetaData *timedelta_schema()
        {
            return TypeRegistry::instance().register_scalar<TimeDelta>("timedelta");
        }

        [[nodiscard]] const ValueTypeMetaData *date_schema()
        {
            return TypeRegistry::instance().register_scalar<Date>("date");
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

        [[nodiscard]] Value datetime_list(nb::handle times)
        {
            const auto        &binding = binding_for(datetime_schema(), "datetime_list");
            ListBuilder        builder{binding};
            nb::iterator       it = nb::iter(times);
            while (it != nb::iterator::sentinel())
            {
                const auto value = nb::cast<DateTime>(*it);
                builder.push_back(value);
                ++it;
            }
            return builder.build();
        }

        [[nodiscard]] Value timedelta_cyclic_buffer(nb::handle durations, std::size_t capacity)
        {
            const auto          &binding = binding_for(timedelta_schema(), "timedelta_cyclic_buffer");
            CyclicBufferBuilder  builder{binding, capacity};
            nb::iterator         it = nb::iter(durations);
            while (it != nb::iterator::sentinel())
            {
                const auto value = nb::cast<TimeDelta>(*it);
                builder.push_back(value);
                ++it;
            }
            return builder.build();
        }

        [[nodiscard]] Value date_queue(nb::handle dates)
        {
            const auto  &binding = binding_for(date_schema(), "date_queue");
            QueueBuilder builder{binding};
            nb::iterator it = nb::iter(dates);
            while (it != nb::iterator::sentinel())
            {
                const auto value = nb::cast<Date>(*it);
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

        [[nodiscard]] std::string ts_schema_name(const TSValueTypeMetaData &schema)
        {
            return schema.display_name != nullptr ? std::string{schema.display_name} : std::string{};
        }

        [[nodiscard]] nb::list ts_schema_fields(const TSValueTypeMetaData &schema)
        {
            nb::list result;
            const auto *fields = schema.fields();
            for (std::size_t index = 0; index < schema.field_count(); ++index)
            {
                const TSFieldMetaData &field = fields[index];
                result.append(nb::make_tuple(field.name != nullptr ? std::string{field.name} : std::string{},
                                             nb::cast(field.type, nb::rv_policy::reference)));
            }
            return result;
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

        void apply_python_value_to_output_view(TSOutputView &view,
                                               nb::object   source,
                                               DateTime evaluation_time)
        {
            if (source.is_none()) { return; }
            auto  mutation = view.begin_mutation(evaluation_time);
            static_cast<void>(mutation.from_python(source));
        }

        TSOutput &apply_output_value(TSOutput &output, nb::object source, DateTime evaluation_time)
        {
            auto view = output.view(evaluation_time);
            apply_python_value_to_output_view(view, source, evaluation_time);
            return output;
        }

        TSOutputView &apply_output_view_value(TSOutputView &view, nb::object source)
        {
            apply_python_value_to_output_view(view, source, view.evaluation_time());
            return view;
        }

        [[nodiscard]] TSInput make_input(const TSValueTypeMetaData &root_schema,
                                         const TSEndpointSchema    &endpoint_schema)
        {
            return TSInput{TSInputBuilderFactory::checked_builder_for(root_schema, endpoint_schema)};
        }

        [[nodiscard]] nb::object ts_input_value_to_python(const TSInputView &view)
        {
            if (!view.valid()) { return nb::none(); }
            return view.data_view().value_to_python();
        }

        [[nodiscard]] nb::object ts_input_delta_value_to_python(const TSInputView &view)
        {
            if (!view.modified()) { return nb::none(); }
            return view.data_view().delta_value_to_python(view.evaluation_time());
        }

        [[nodiscard]] nb::object ts_output_value_to_python(const TSOutputView &view)
        {
            return view.valid() ? view.data_view().value_to_python() : nb::none();
        }

        [[nodiscard]] nb::object ts_output_delta_value_to_python(const TSOutputView &view)
        {
            if (!view.modified()) { return nb::none(); }
            return view.data_view().delta_value_to_python(view.evaluation_time());
        }

        [[nodiscard]] nb::list endpoint_schema_children(const TSEndpointSchema &schema)
        {
            nb::list result;
            for (const auto &child : schema.children()) { result.append(child); }
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
            nb::module_ ts_mod    = m.def_submodule("time_series", "Time-series endpoint C++ abstractions");

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

            nb::enum_<TSTypeKind>(ts_mod, "TSTypeKind")
                .value("TS", TSTypeKind::TS)
                .value("TSS", TSTypeKind::TSS)
                .value("TSD", TSTypeKind::TSD)
                .value("TSL", TSTypeKind::TSL)
                .value("TSW", TSTypeKind::TSW)
                .value("TSB", TSTypeKind::TSB)
                .value("REF", TSTypeKind::REF)
                .value("SIGNAL", TSTypeKind::SIGNAL);

            nb::enum_<TSEndpointRole>(ts_mod, "TSEndpointRole")
                .value("Peered", TSEndpointRole::Peered)
                .value("NonPeered", TSEndpointRole::NonPeered);

            nb::class_<TSValueTypeMetaData>(ts_mod, "TSValueTypeMetaData")
                .def_prop_ro("name", &ts_schema_name)
                .def_ro("kind", &TSValueTypeMetaData::kind)
                .def_prop_ro("value_type",
                             [](const TSValueTypeMetaData &self) { return self.value_type; },
                             nb::rv_policy::reference)
                .def_prop_ro("value_schema",
                             [](const TSValueTypeMetaData &self) { return self.value_schema; },
                             nb::rv_policy::reference)
                .def_prop_ro("delta_value_schema",
                             [](const TSValueTypeMetaData &self) { return self.delta_value_schema; },
                             nb::rv_policy::reference)
                .def_prop_ro("key_type", &TSValueTypeMetaData::key_type, nb::rv_policy::reference)
                .def_prop_ro("element_ts", &TSValueTypeMetaData::element_ts, nb::rv_policy::reference)
                .def_prop_ro("fixed_size", &TSValueTypeMetaData::fixed_size)
                .def_prop_ro("field_count", &TSValueTypeMetaData::field_count)
                .def_prop_ro("fields", &ts_schema_fields)
                .def("__repr__", [](const TSValueTypeMetaData &self) {
                    return std::string{"TSValueTypeMetaData("} + ts_schema_name(self) + ")";
                });

            nb::class_<TSEndpointSchema>(ts_mod, "TSEndpointSchema")
                .def(nb::init<>())
                .def_static("peered",
                            [](const TSValueTypeMetaData &schema) { return TSEndpointSchema::peered(&schema); },
                            "schema"_a)
                .def_static("non_peered",
                            [](const TSValueTypeMetaData &schema, nb::handle children) {
                                return TSEndpointSchema::non_peered(
                                    &schema,
                                    endpoint_schema_sequence(children, "TSEndpointSchema.non_peered"));
                            },
                            "schema"_a,
                            "children"_a)
                .def_static("non_peered_list",
                            [](const TSValueTypeMetaData &schema, const TSEndpointSchema &element) {
                                return TSEndpointSchema::non_peered_list(&schema, element);
                            },
                            "schema"_a,
                            "element"_a)
                .def("empty", &TSEndpointSchema::empty)
                .def_prop_ro("role", &TSEndpointSchema::role)
                .def_prop_ro("schema", &TSEndpointSchema::schema, nb::rv_policy::reference)
                .def("is_peered", &TSEndpointSchema::is_peered)
                .def("is_non_peered", &TSEndpointSchema::is_non_peered)
                .def("__len__", &TSEndpointSchema::child_count)
                .def("child_count", &TSEndpointSchema::child_count)
                .def("child", &TSEndpointSchema::child, "index"_a, nb::rv_policy::reference_internal)
                .def_prop_ro("children", &endpoint_schema_children);

            nb::class_<TypeRegistry>(value_mod, "TypeRegistry")
                .def_static("instance", []() -> TypeRegistry & { return TypeRegistry::instance(); },
                            nb::rv_policy::reference)
                .def("register_builtin_value_types", [](TypeRegistry &) { register_builtin_value_types(); })
                .def("bool", [](TypeRegistry &self) { return self.register_scalar<bool>("bool"); },
                     nb::rv_policy::reference)
                .def("int", [](TypeRegistry &self) { return self.register_scalar<Int>("int"); },
                     nb::rv_policy::reference)
                .def("int32", [](TypeRegistry &self) { return self.register_scalar<std::int32_t>("int32"); },
                     nb::rv_policy::reference)
                .def("int64", [](TypeRegistry &self) { return self.register_scalar<std::int64_t>("int64"); },
                     nb::rv_policy::reference)
                .def("double", [](TypeRegistry &self) { return self.register_scalar<double>("double"); },
                     nb::rv_policy::reference)
                .def("string", [](TypeRegistry &self) { return self.register_scalar<std::string>("string"); },
                     nb::rv_policy::reference)
                .def("datetime", [](TypeRegistry &self) { return self.register_scalar<DateTime>("datetime"); },
                     nb::rv_policy::reference)
                .def("timedelta",
                     [](TypeRegistry &self) { return self.register_scalar<TimeDelta>("timedelta"); },
                     nb::rv_policy::reference)
                .def("date", [](TypeRegistry &self) { return self.register_scalar<Date>("date"); },
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
                     }, "element_schema"_a, "max_capacity"_a = 0, nb::rv_policy::reference)
                .def("signal", [](TypeRegistry &self) { return self.signal(); }, nb::rv_policy::reference)
                .def("ts", [](TypeRegistry &self, const ValueTypeMetaData &value_schema) {
                         return self.ts(&value_schema);
                     }, "value_schema"_a, nb::rv_policy::reference)
                .def("tss", [](TypeRegistry &self, const ValueTypeMetaData &element_schema) {
                         return self.tss(&element_schema);
                     }, "element_schema"_a, nb::rv_policy::reference)
                .def("tsd", [](TypeRegistry &self,
                                const ValueTypeMetaData &key_schema,
                                const TSValueTypeMetaData &value_ts) {
                         return self.tsd(&key_schema, &value_ts);
                     }, "key_schema"_a, "value_ts"_a, nb::rv_policy::reference)
                .def("tsl", [](TypeRegistry &self, const TSValueTypeMetaData &element_ts, std::size_t fixed_size) {
                         return self.tsl(&element_ts, fixed_size);
                     }, "element_ts"_a, "fixed_size"_a = 0, nb::rv_policy::reference)
                .def("tsw", [](TypeRegistry &self,
                                const ValueTypeMetaData &value_schema,
                                std::size_t period,
                                std::size_t min_period) {
                         return self.tsw(&value_schema, period, min_period);
                     }, "value_schema"_a, "period"_a, "min_period"_a = 0, nb::rv_policy::reference)
                .def("tsw_duration", [](TypeRegistry &self,
                                         const ValueTypeMetaData &value_schema,
                                         TimeDelta time_range,
                                         TimeDelta min_time_range) {
                         return self.tsw_duration(&value_schema, time_range, min_time_range);
                     }, "value_schema"_a, "time_range"_a, "min_time_range"_a = TimeDelta{0},
                     nb::rv_policy::reference)
                .def("tsb", [](TypeRegistry &self, std::string name, nb::handle fields) {
                         return self.tsb(name, ts_bundle_fields(fields, "TypeRegistry.tsb"));
                     }, "name"_a, "fields"_a, nb::rv_policy::reference)
                .def("time_series_type", &TypeRegistry::time_series_type, "name"_a, nb::rv_policy::reference)
                .def("named_tsb", &TypeRegistry::named_tsb, "name"_a, nb::rv_policy::reference);

            nb::class_<TSOutputView>(ts_mod, "TSOutputView")
                .def("bound", &TSOutputView::bound)
                .def("valid", &TSOutputView::valid)
                .def("all_valid", &TSOutputView::all_valid)
                .def("modified", &TSOutputView::modified)
                .def_prop_ro("evaluation_time",
                             [](const TSOutputView &self) { return self.evaluation_time(); })
                .def_prop_ro("last_modified_time",
                             [](const TSOutputView &self) { return self.last_modified_time(); })
                .def_prop_ro("schema", [](const TSOutputView &self) { return self.schema(); },
                             nb::rv_policy::reference)
                .def_prop_ro("value", &ts_output_value_to_python)
                .def_prop_ro("delta_value", &ts_output_delta_value_to_python)
                .def("apply_value", &apply_output_view_value, nb::arg("value").none(),
                     nb::rv_policy::reference_internal)
                .def("as_bundle", [](TSOutputView &self) { return self.as_bundle(); }, nb::keep_alive<0, 1>())
                .def("as_list", [](TSOutputView &self) { return self.as_list(); }, nb::keep_alive<0, 1>());

            nb::class_<TSBOutputView>(ts_mod, "TSBOutputView")
                .def("__len__", &TSBOutputView::size)
                .def("size", &TSBOutputView::size)
                .def("empty", &TSBOutputView::empty)
                .def("has_field", &TSBOutputView::has_field, "name"_a)
                .def("field", [](TSBOutputView &self, std::string_view name) { return self.field(name); },
                     "name"_a,
                     nb::keep_alive<0, 1>())
                .def("at", [](TSBOutputView &self, std::size_t index) { return self.at(index); },
                     "index"_a,
                     nb::keep_alive<0, 1>())
                .def("__getitem__", [](TSBOutputView &self, std::string_view name) { return self.field(name); },
                     "name"_a,
                     nb::keep_alive<0, 1>());

            nb::class_<TSLOutputView>(ts_mod, "TSLOutputView")
                .def("__len__", &TSLOutputView::size)
                .def("size", &TSLOutputView::size)
                .def("empty", &TSLOutputView::empty)
                .def("at", [](TSLOutputView &self, std::size_t index) { return self.at(index); },
                     "index"_a,
                     nb::keep_alive<0, 1>())
                .def("__getitem__", [](TSLOutputView &self, std::size_t index) { return self.at(index); },
                     "index"_a,
                     nb::keep_alive<0, 1>());

            nb::class_<TSOutput>(ts_mod, "TSOutput")
                .def(nb::init<const TSValueTypeMetaData &>(), "schema"_a)
                .def("has_value", &TSOutput::has_value)
                .def("dirty", &TSOutput::dirty)
                .def("cleanup_delta", &TSOutput::cleanup_delta)
                .def("clear_dirty", &TSOutput::clear_dirty)
                .def_prop_ro("schema", [](const TSOutput &self) { return self.schema(); }, nb::rv_policy::reference)
                .def("view", [](TSOutput &self, DateTime evaluation_time) {
                         return self.view(evaluation_time);
                     }, "evaluation_time"_a = MIN_DT, nb::keep_alive<0, 1>())
                .def("apply_value", &apply_output_value, nb::arg("value").none(), "evaluation_time"_a,
                     nb::rv_policy::reference_internal);

            nb::class_<TSInputView>(ts_mod, "TSInputView")
                .def("bound", &TSInputView::bound)
                .def("is_bindable", &TSInputView::is_bindable)
                .def("valid", &TSInputView::valid)
                .def("all_valid", &TSInputView::all_valid)
                .def("modified", &TSInputView::modified)
                .def("active", &TSInputView::active)
                .def("make_active", &TSInputView::make_active)
                .def("make_passive", &TSInputView::make_passive)
                .def("bind_output", [](TSInputView &self, const TSOutputView &output) {
                         self.bind_output(output);
                         return true;
                     }, "output"_a, nb::keep_alive<1, 2>())
                .def("unbind_output", &TSInputView::unbind_output)
                .def_prop_ro("evaluation_time",
                             [](const TSInputView &self) { return self.evaluation_time(); })
                .def_prop_ro("last_modified_time",
                             [](const TSInputView &self) { return self.last_modified_time(); })
                .def_prop_ro("schema", [](const TSInputView &self) { return self.schema(); },
                             nb::rv_policy::reference)
                .def_prop_ro("value", &ts_input_value_to_python)
                .def_prop_ro("delta_value", &ts_input_delta_value_to_python)
                .def("as_bundle", [](TSInputView &self) { return self.as_bundle(); }, nb::keep_alive<0, 1>())
                .def("as_list", [](TSInputView &self) { return self.as_list(); }, nb::keep_alive<0, 1>());

            nb::class_<TSBInputView>(ts_mod, "TSBInputView")
                .def("__len__", &TSBInputView::size)
                .def("size", &TSBInputView::size)
                .def("empty", &TSBInputView::empty)
                .def("has_field", &TSBInputView::has_field, "name"_a)
                .def("bound", &TSBInputView::bound)
                .def("valid", &TSBInputView::valid)
                .def("all_valid", &TSBInputView::all_valid)
                .def("modified", &TSBInputView::modified)
                .def_prop_ro("value", [](const TSBInputView &self) { return ts_input_value_to_python(self.base()); })
                .def_prop_ro("delta_value",
                             [](const TSBInputView &self) { return ts_input_delta_value_to_python(self.base()); })
                .def("field", [](TSBInputView &self, std::string_view name) { return self.field(name); },
                     "name"_a,
                     nb::keep_alive<0, 1>())
                .def("at", [](TSBInputView &self, std::size_t index) { return self.at(index); },
                     "index"_a,
                     nb::keep_alive<0, 1>())
                .def("__getitem__", [](TSBInputView &self, std::string_view name) { return self.field(name); },
                     "name"_a,
                     nb::keep_alive<0, 1>());

            nb::class_<TSLInputView>(ts_mod, "TSLInputView")
                .def("__len__", &TSLInputView::size)
                .def("size", &TSLInputView::size)
                .def("empty", &TSLInputView::empty)
                .def("bound", &TSLInputView::bound)
                .def("valid", &TSLInputView::valid)
                .def("all_valid", &TSLInputView::all_valid)
                .def("modified", &TSLInputView::modified)
                .def_prop_ro("value", [](const TSLInputView &self) { return ts_input_value_to_python(self.base()); })
                .def_prop_ro("delta_value",
                             [](const TSLInputView &self) { return ts_input_delta_value_to_python(self.base()); })
                .def("at", [](TSLInputView &self, std::size_t index) { return self.at(index); },
                     "index"_a,
                     nb::keep_alive<0, 1>())
                .def("__getitem__", [](TSLInputView &self, std::size_t index) { return self.at(index); },
                     "index"_a,
                     nb::keep_alive<0, 1>());

            nb::class_<TSInput>(ts_mod, "TSInput")
                .def(nb::init<>())
                .def_static("create", &make_input, "schema"_a, "endpoint_schema"_a)
                .def("has_value", &TSInput::has_value)
                .def_prop_ro("schema", [](const TSInput &self) { return self.schema(); }, nb::rv_policy::reference)
                .def("view", [](TSInput &self, DateTime evaluation_time) {
                         return self.view(nullptr, evaluation_time);
                     }, "evaluation_time"_a = MIN_DT, nb::keep_alive<0, 1>());

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
            value_mod.def("datetime_list", &datetime_list, "times"_a);
            value_mod.def("timedelta_cyclic_buffer", &timedelta_cyclic_buffer, "durations"_a, "capacity"_a);
            value_mod.def("date_queue", &date_queue, "dates"_a);
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
            m.attr("TSTypeKind")       = ts_mod.attr("TSTypeKind");
            m.attr("TSEndpointRole")   = ts_mod.attr("TSEndpointRole");
            m.attr("TSValueTypeMetaData") = ts_mod.attr("TSValueTypeMetaData");
            m.attr("TSEndpointSchema") = ts_mod.attr("TSEndpointSchema");
            m.attr("TSOutput")         = ts_mod.attr("TSOutput");
            m.attr("TSOutputView")     = ts_mod.attr("TSOutputView");
            m.attr("TSBOutputView")    = ts_mod.attr("TSBOutputView");
            m.attr("TSLOutputView")    = ts_mod.attr("TSLOutputView");
            m.attr("TSInput")          = ts_mod.attr("TSInput");
            m.attr("TSInputView")      = ts_mod.attr("TSInputView");
            m.attr("TSBInputView")     = ts_mod.attr("TSBInputView");
            m.attr("TSLInputView")     = ts_mod.attr("TSLInputView");

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
            m.def("datetime_list", &datetime_list, "times"_a);
            m.def("timedelta_cyclic_buffer", &timedelta_cyclic_buffer, "durations"_a, "capacity"_a);
            m.def("date_queue", &date_queue, "dates"_a);
            m.def("unsupported_scalar_to_python", &unsupported_scalar_to_python);

            ts_mod.def("make_input", &make_input, "schema"_a, "endpoint_schema"_a);
            m.def("make_input", &make_input, "schema"_a, "endpoint_schema"_a);
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

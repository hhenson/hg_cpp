#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value_builder.h>

#include <stdexcept>
#include <string>

namespace hgraph
{
#if HGRAPH_ENABLE_PYTHON_USER_NODES
    namespace container_ops_detail
    {
        namespace
        {
            [[nodiscard]] const ValueTypeMetaData &checked_schema(const ValueTypeBinding &binding,
                                                                  ValueTypeKind           kind,
                                                                  const char             *what)
            {
                if (!binding.valid()) { throw std::logic_error(std::string{what} + " requires a valid binding"); }
                if (binding.type_meta->kind != kind)
                {
                    throw std::logic_error(std::string{what} + " received the wrong value kind");
                }
                return *binding.type_meta;
            }

            [[nodiscard]] const ValueTypeBinding &checked_binding_for(const ValueTypeMetaData *schema,
                                                                      const char             *what)
            {
                const auto *binding = ValuePlanFactory::instance().binding_for(schema);
                if (binding == nullptr)
                {
                    throw std::logic_error(std::string{what} + ": schema has no canonical value binding");
                }
                return *binding;
            }

            void require_non_none(nb::handle source, const char *what)
            {
                if (source.is_none()) { throw std::invalid_argument(std::string{what} + " requires a non-None value"); }
            }

            [[nodiscard]] bool is_sequence(nb::handle source)
            {
                nb::object object = nb::borrow<nb::object>(source);
                return nb::isinstance<nb::list>(object) || nb::isinstance<nb::tuple>(object);
            }

            template <typename Append>
            void for_each_sequence_item(nb::handle source, const char *what, Append append)
            {
                if (!is_sequence(source))
                {
                    throw std::invalid_argument(std::string{what} + " expects a Python list or tuple");
                }

                nb::object   object = nb::borrow<nb::object>(source);
                nb::iterator it     = nb::iter(object);
                while (it != nb::iterator::sentinel())
                {
                    nb::handle item = *it;
                    if (item.is_none()) { throw std::invalid_argument(std::string{what} + " does not allow None elements"); }
                    append(item);
                    ++it;
                }
            }

            [[nodiscard]] Value value_from_python(const ValueTypeBinding &binding, nb::handle source)
            {
                require_non_none(source, "from_python");
                Value out{binding};
                binding.checked_ops().from_python(binding, out.view().data(), source);
                return out;
            }
        }  // namespace

        void list_from_python(const void *, const ValueTypeBinding &binding, void *memory, nb::handle source)
        {
            if (memory == nullptr) { throw std::runtime_error("List from_python requires live storage"); }
            const auto &schema = checked_schema(binding, ValueTypeKind::List, "List from_python");
            if (schema.fixed_size != 0)
            {
                throw std::logic_error("List from_python compact storage is only valid for dynamic lists");
            }

            const auto &element_binding = checked_binding_for(schema.element_type, "List from_python");
            ListBuilder builder{element_binding};
            for_each_sequence_item(source, "List value", [&](nb::handle item) {
                Value element = value_from_python(element_binding, item);
                builder.push_back_copy(element.view().data());
            });

            *static_cast<ListStorage *>(memory) = builder.build_storage();
        }

        void cyclic_buffer_from_python(const void *, const ValueTypeBinding &binding, void *memory,
                                       nb::handle source)
        {
            if (memory == nullptr) { throw std::runtime_error("CyclicBuffer from_python requires live storage"); }
            const auto &schema = checked_schema(binding, ValueTypeKind::CyclicBuffer, "CyclicBuffer from_python");
            if (schema.fixed_size == 0)
            {
                throw std::invalid_argument("CyclicBuffer value requires a non-zero capacity");
            }

            const auto &element_binding = checked_binding_for(schema.element_type, "CyclicBuffer from_python");
            CyclicBufferBuilder builder{element_binding, schema.fixed_size};
            for_each_sequence_item(source, "CyclicBuffer value", [&](nb::handle item) {
                Value element = value_from_python(element_binding, item);
                builder.push_back_copy(element.view().data());
            });

            *static_cast<CyclicBufferStorage *>(memory) = builder.build_storage();
        }

        void queue_from_python(const void *, const ValueTypeBinding &binding, void *memory, nb::handle source)
        {
            if (memory == nullptr) { throw std::runtime_error("Queue from_python requires live storage"); }
            const auto &schema = checked_schema(binding, ValueTypeKind::Queue, "Queue from_python");

            const auto &element_binding = checked_binding_for(schema.element_type, "Queue from_python");
            QueueBuilder builder{element_binding, schema.fixed_size};
            for_each_sequence_item(source, "Queue value", [&](nb::handle item) {
                Value element = value_from_python(element_binding, item);
                builder.push_copy(element.view().data());
            });

            *static_cast<QueueStorage *>(memory) = builder.build_storage();
        }

        void set_from_python(const void *, const ValueTypeBinding &binding, void *memory, nb::handle source)
        {
            if (memory == nullptr) { throw std::runtime_error("Set from_python requires live storage"); }
            const auto &schema = checked_schema(binding, ValueTypeKind::Set, "Set from_python");

            nb::object object = nb::borrow<nb::object>(source);
            if (!nb::isinstance<nb::set>(object) && !nb::isinstance<nb::frozenset>(object) &&
                !nb::isinstance<nb::list>(object) && !nb::isinstance<nb::tuple>(object))
            {
                throw std::invalid_argument("Set value expects a Python set, frozenset, list, or tuple");
            }

            const auto &element_binding = checked_binding_for(schema.element_type, "Set from_python");
            SetBuilder builder{element_binding};
            nb::iterator it = nb::iter(object);
            while (it != nb::iterator::sentinel())
            {
                nb::handle item = *it;
                if (item.is_none()) { throw std::invalid_argument("Set value does not allow None elements"); }
                Value element = value_from_python(element_binding, item);
                builder.insert_copy(element.view().data());
                ++it;
            }

            *static_cast<SetStorage *>(memory) = builder.build_storage();
        }

        void map_from_python(const void *, const ValueTypeBinding &binding, void *memory, nb::handle source)
        {
            if (memory == nullptr) { throw std::runtime_error("Map from_python requires live storage"); }
            const auto &schema = checked_schema(binding, ValueTypeKind::Map, "Map from_python");

            nb::object object = nb::borrow<nb::object>(source);
            if (!nb::isinstance<nb::dict>(object) && !nb::hasattr(object, "items"))
            {
                throw std::invalid_argument("Map value expects a Python dict or dict-like object");
            }

            const auto &key_binding   = checked_binding_for(schema.key_type, "Map from_python");
            const auto &value_binding = checked_binding_for(schema.element_type, "Map from_python");
            MapBuilder  builder{key_binding, value_binding};

            nb::object   items = nb::hasattr(object, "items") ? object.attr("items")() : object;
            nb::iterator it    = nb::iter(items);
            while (it != nb::iterator::sentinel())
            {
                nb::tuple pair = nb::cast<nb::tuple>(*it);
                if (pair.size() != 2) { throw std::invalid_argument("Map items() must yield key/value pairs"); }
                if (pair[0].is_none()) { throw std::invalid_argument("Map value does not allow None keys"); }
                if (pair[1].is_none()) { throw std::invalid_argument("Map value does not allow None values"); }

                Value key   = value_from_python(key_binding, nb::borrow<nb::object>(pair[0]));
                Value value = value_from_python(value_binding, nb::borrow<nb::object>(pair[1]));
                builder.set_item_copy(key.view().data(), value.view().data());
                ++it;
            }

            *static_cast<MapStorage *>(memory) = builder.build_storage();
        }

        void map_key_adapter_from_python(const void *, const ValueTypeBinding &, void *, nb::handle)
        {
            throw std::logic_error("Map key-set adapter is read-only and cannot load from Python");
        }
    }  // namespace container_ops_detail
#endif
}  // namespace hgraph

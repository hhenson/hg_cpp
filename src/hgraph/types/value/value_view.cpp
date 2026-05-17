#include <hgraph/types/value/specialized_views.h>

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <compare>
#include <cstddef>
#include <fmt/format.h>
#include <stdexcept>
#include <string_view>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] const char *schema_name(const ValueTypeMetaData *schema) noexcept
        {
            if (schema == nullptr || schema->display_name == nullptr) { return "<unnamed>"; }
            return schema->display_name;
        }

        [[nodiscard]] const ValueTypeMetaData *structural_schema(const ValueTypeMetaData *schema) noexcept
        {
            if (schema != nullptr && schema->kind == ValueTypeKind::Bundle && schema->wrapped_un_named != nullptr)
            {
                return schema->wrapped_un_named;
            }
            return schema;
        }

        [[nodiscard]] bool value_schema_equivalent(const ValueTypeMetaData *lhs,
                                                   const ValueTypeMetaData *rhs) noexcept
        {
            lhs = structural_schema(lhs);
            rhs = structural_schema(rhs);
            if (lhs == rhs) { return true; }
            if (lhs == nullptr || rhs == nullptr || lhs->kind != rhs->kind) { return false; }

            switch (lhs->kind)
            {
                case ValueTypeKind::Atomic:
                    return false;

                case ValueTypeKind::Tuple:
                case ValueTypeKind::Bundle:
                    if (lhs->field_count != rhs->field_count) { return false; }
                    for (std::size_t index = 0; index < lhs->field_count; ++index)
                    {
                        if (!value_schema_equivalent(lhs->fields[index].type, rhs->fields[index].type))
                        {
                            return false;
                        }
                    }
                    return true;

                case ValueTypeKind::List:
                case ValueTypeKind::Set:
                case ValueTypeKind::CyclicBuffer:
                case ValueTypeKind::Queue:
                    return value_schema_equivalent(lhs->element_type, rhs->element_type);

                case ValueTypeKind::Map:
                    return value_schema_equivalent(lhs->key_type, rhs->key_type) &&
                           value_schema_equivalent(lhs->element_type, rhs->element_type);
            }

            return false;
        }

        [[nodiscard]] bool ordered_indexed_kind(ValueTypeKind kind) noexcept
        {
            return kind == ValueTypeKind::Tuple || kind == ValueTypeKind::Bundle ||
                   kind == ValueTypeKind::List || kind == ValueTypeKind::CyclicBuffer ||
                   kind == ValueTypeKind::Queue;
        }

        [[nodiscard]] bool semantic_indexed_equals(ValueView lhs, ValueView rhs)
        {
            const IndexedValueView a{lhs};
            const IndexedValueView b{rhs};
            const auto             size = a.size();
            if (size != b.size()) { return false; }
            for (std::size_t index = 0; index < size; ++index)
            {
                if (!a.at(index).equals(b.at(index))) { return false; }
            }
            return true;
        }

        [[nodiscard]] std::partial_ordering semantic_indexed_compare(ValueView lhs, ValueView rhs)
        {
            const IndexedValueView a{lhs};
            const IndexedValueView b{rhs};
            const auto             size = std::min(a.size(), b.size());
            for (std::size_t index = 0; index < size; ++index)
            {
                const auto child_order = a.at(index).compare(b.at(index));
                if (child_order != 0) { return child_order; }
            }
            if (a.size() < b.size()) { return std::partial_ordering::less; }
            if (a.size() > b.size()) { return std::partial_ordering::greater; }
            return std::partial_ordering::equivalent;
        }

        [[nodiscard]] const ValueTypeBinding *first_indexed_element_binding(ValueView view)
        {
            const IndexedValueView indexed{view};
            return indexed.size() == 0 ? nullptr : indexed.at(0).binding();
        }

        [[nodiscard]] bool semantic_set_equals(ValueView lhs, ValueView rhs)
        {
            const SetView a{lhs};
            const SetView b{rhs};
            if (a.size() != b.size()) { return false; }
            if (a.size() == 0) { return true; }
            if (first_indexed_element_binding(lhs) != first_indexed_element_binding(rhs)) { return false; }

            for (const auto key : a)
            {
                if (!b.contains(key)) { return false; }
            }
            return true;
        }

        [[nodiscard]] std::partial_ordering semantic_set_compare(ValueView lhs, ValueView rhs)
        {
            const SetView a{lhs};
            const SetView b{rhs};
            if (a.size() < b.size()) { return std::partial_ordering::less; }
            if (a.size() > b.size()) { return std::partial_ordering::greater; }
            return semantic_set_equals(lhs, rhs) ? std::partial_ordering::equivalent
                                                 : std::partial_ordering::unordered;
        }

        [[nodiscard]] bool semantic_map_equals(ValueView lhs, ValueView rhs)
        {
            const MapView a{lhs};
            const MapView b{rhs};
            if (a.size() != b.size()) { return false; }
            if (a.size() == 0) { return true; }
            if (first_indexed_element_binding(lhs) != first_indexed_element_binding(rhs)) { return false; }

            for (const auto entry : a)
            {
                const auto &key   = entry.first;
                const auto &value = entry.second;
                if (!b.contains(key)) { return false; }
                if (!value.equals(b.at(key))) { return false; }
            }
            return true;
        }

        [[nodiscard]] bool semantic_equals(ValueView lhs, ValueView rhs)
        {
            const auto *schema = structural_schema(lhs.schema());
            if (schema == nullptr) { return false; }

            if (ordered_indexed_kind(schema->kind)) { return semantic_indexed_equals(lhs, rhs); }
            if (schema->kind == ValueTypeKind::Set) { return semantic_set_equals(lhs, rhs); }
            if (schema->kind == ValueTypeKind::Map) { return semantic_map_equals(lhs, rhs); }
            return false;
        }

        [[nodiscard]] std::partial_ordering semantic_compare(ValueView lhs, ValueView rhs)
        {
            const auto *schema = structural_schema(lhs.schema());
            if (schema == nullptr) { return std::partial_ordering::unordered; }

            if (ordered_indexed_kind(schema->kind)) { return semantic_indexed_compare(lhs, rhs); }

            if (schema->kind == ValueTypeKind::Set) { return semantic_set_compare(lhs, rhs); }

            if (schema->kind == ValueTypeKind::Map)
            {
                return semantic_equals(lhs, rhs) ? std::partial_ordering::equivalent
                                                 : std::partial_ordering::unordered;
            }

            return std::partial_ordering::unordered;
        }
    }  // namespace

    Value ValueView::clone() const
    {
        if (binding() == nullptr) { throw std::logic_error("ValueView::clone requires a bound view"); }
        return Value{*this};
    }

    void ValueView::copy_from(const ValueView &other)
    {
        if (!has_value() || !other.has_value())
        {
            throw std::runtime_error("ValueView::copy_from requires non-empty views");
        }
        if (!mutable_payload()) { throw std::logic_error("ValueView::copy_from requires a mutable destination"); }
        if (data_ == other.data_) { return; }
        if (schema() != other.schema())
        {
            throw std::invalid_argument(fmt::format("ValueView::copy_from requires matching schemas: {} != {}",
                                                    schema_name(schema()),
                                                    schema_name(other.schema())));
        }
        const auto *bound       = binding();
        const auto *other_bound = other.binding();
        if (bound == nullptr || other_bound == nullptr)
        {
            throw std::invalid_argument("ValueView::copy_from requires matching storage plans");
        }

        const auto &other_ops    = other_bound->checked_ops();
        const auto &copy_binding = other_ops.owning_binding(*other_bound);
        if (bound->plan() != copy_binding.plan())
        {
            throw std::invalid_argument("ValueView::copy_from requires matching storage plans");
        }

        other_ops.copy_assign_view(*bound, mutable_data(), other.data_);
    }

    bool ValueView::try_copy_from(const ValueView &other)
    {
        if (!has_value() || !other.has_value()) { return false; }
        if (!mutable_payload()) { return false; }
        if (data_ == other.data_) { return true; }
        if (schema() != other.schema()) { return false; }
        const auto *bound       = binding();
        const auto *other_bound = other.binding();
        if (bound == nullptr || other_bound == nullptr)
        {
            return false;
        }

        const auto &other_ops    = other_bound->checked_ops();
        const auto &copy_binding = other_ops.owning_binding(*other_bound);
        if (bound->plan() != copy_binding.plan()) { return false; }
        other_ops.copy_assign_view(*bound, mutable_data(), other.data_);
        return true;
    }

    bool ValueView::equals(const ValueView &other) const noexcept
    {
        const auto *bound       = binding();
        const auto *other_bound = other.binding();
        if (bound == nullptr || other_bound == nullptr)
        {
            return bound == nullptr && other_bound == nullptr && data_ == other.data_;
        }
        if (data_ == nullptr || other.data_ == nullptr)
        {
            if (data_ != other.data_) { return false; }
            return bound == other_bound || value_schema_equivalent(schema(), other.schema());
        }

        return fallback_on_exception(false, [&] {
            if (bound == other_bound) { return bound->checked_ops().equals(data_, other.data_); }
            if (!value_schema_equivalent(schema(), other.schema())) { return false; }
            return semantic_equals(*this, other);
        });
    }

    std::partial_ordering ValueView::compare(const ValueView &other) const noexcept
    {
        const auto *bound       = binding();
        const auto *other_bound = other.binding();
        if (const auto order = value_ops_detail::null_order(bound, other_bound)) { return *order; }
        if (data_ == nullptr || other.data_ == nullptr)
        {
            if (data_ != other.data_) { return data_ == nullptr ? std::partial_ordering::less
                                                                : std::partial_ordering::greater; }
            if (bound == other_bound || value_schema_equivalent(schema(), other.schema()))
            {
                return std::partial_ordering::equivalent;
            }
            return std::partial_ordering::unordered;
        }

        return fallback_on_exception(std::partial_ordering::unordered, [&]() {
            if (bound == other_bound) { return bound->checked_ops().compare(data_, other.data_); }
            if (!value_schema_equivalent(schema(), other.schema())) { return std::partial_ordering::unordered; }
            return semantic_compare(*this, other);
        });
    }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
    nb::object ValueView::to_python() const
    {
        if (!valid()) { throw std::runtime_error("ValueView::to_python requires a non-empty view"); }
        return binding()->checked_ops().to_python(data_);
    }

    void ValueView::from_python(nb::handle source)
    {
        if (!valid()) { throw std::runtime_error("ValueView::from_python requires a non-empty view"); }
        if (source.is_none())
        {
            throw std::invalid_argument("ValueView::from_python cannot reset a view from None");
        }

        const auto *bound = binding();
        bound->checked_ops().from_python(*bound, mutable_data(), source);
    }

    nb::object Value::to_python() const
    {
        if (!has_value()) { return nb::none(); }
        return view().to_python();
    }

    void Value::from_python(nb::handle source)
    {
        if (source.is_none())
        {
            reset();
            return;
        }
        if (binding() == nullptr)
        {
            throw std::logic_error("Value::from_python requires a schema-bound Value");
        }

        Value replacement{*binding()};
        binding()->checked_ops().from_python(*binding(), const_cast<void *>(replacement.view().data()), source);
        *this = std::move(replacement);
    }
#endif
}  // namespace hgraph

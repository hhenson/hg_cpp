#include <hgraph/types/metadata/value_plan_factory.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <hgraph/python/bridge_state.h>
#endif

#include <hgraph/types/utils/intern_table.h>
#include <hgraph/types/value/any_ops.h>
#include <hgraph/types/value/compact_container_ops.h>
#include <hgraph/types/value/mutable_container_ops.h>
#include <hgraph/util/scope.h>

#include <compare>
#include <cstddef>
#include <cstdint>
#include <fmt/format.h>
#include <iterator>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        struct CompositeIndexedContext
        {
            const ValueTypeMetaData              *schema{nullptr};
            std::vector<const ValueTypeBinding *> child_bindings{};
            std::vector<std::size_t>              offsets{};
            std::size_t                           validity_offset{0};
            std::size_t                           validity_word_count{0};
        };

        using BundleValidityWord = std::uint64_t;
        static constexpr std::size_t bundle_validity_bits_per_word = sizeof(BundleValidityWord) * 8U;

        [[nodiscard]] constexpr std::size_t bundle_validity_word_count(std::size_t field_count) noexcept
        {
            return (field_count + bundle_validity_bits_per_word - 1U) / bundle_validity_bits_per_word;
        }

        [[nodiscard]] const MemoryUtils::StoragePlan &bundle_validity_plan(std::size_t field_count)
        {
            return MemoryUtils::array_plan<BundleValidityWord>(bundle_validity_word_count(field_count));
        }

        // ---- Bundle field validity (core_concepts.rst) ----
        [[nodiscard]] inline const BundleValidityWord *composite_validity(const CompositeIndexedContext *state,
                                                                          const void *memory) noexcept
        {
            if (state->validity_word_count == 0) { return nullptr; }   // dense Tuple, or empty Bundle
            return static_cast<const BundleValidityWord *>(MemoryUtils::advance(memory, state->validity_offset));
        }

        [[nodiscard]] inline BundleValidityWord *mutable_composite_validity(const CompositeIndexedContext *state,
                                                                            void *memory) noexcept
        {
            if (state->validity_word_count == 0) { return nullptr; }
            return static_cast<BundleValidityWord *>(MemoryUtils::advance(memory, state->validity_offset));
        }

        [[nodiscard]] inline bool composite_field_set(const CompositeIndexedContext *state, const void *memory,
                                                      std::size_t index) noexcept
        {
            const auto *words = composite_validity(state, memory);
            if (words == nullptr) { return true; }
            if (index >= state->schema->field_count) { return false; }
            const auto word = index / bundle_validity_bits_per_word;
            const auto bit  = index % bundle_validity_bits_per_word;
            return (words[word] & (BundleValidityWord{1} << bit)) != 0;
        }

        inline void composite_mark_field(const CompositeIndexedContext *state, void *memory, std::size_t index,
                                         bool set)
        {
            auto *words = mutable_composite_validity(state, memory);
            if (words == nullptr) { return; }
            if (index >= state->schema->field_count) { throw std::out_of_range("Bundle field index out of range"); }
            const auto word = index / bundle_validity_bits_per_word;
            const auto bit  = index % bundle_validity_bits_per_word;
            const auto mask = BundleValidityWord{1} << bit;
            if (set) { words[word] |= mask; }
            else { words[word] &= ~mask; }
        }

        [[maybe_unused]] inline void composite_mark_all(const CompositeIndexedContext *state, void *memory, bool set)
        {
            auto *words = mutable_composite_validity(state, memory);
            if (words == nullptr) { return; }
            const BundleValidityWord fill = set ? ~BundleValidityWord{0} : BundleValidityWord{0};
            for (std::size_t index = 0; index < state->validity_word_count; ++index) { words[index] = fill; }
            if (set && state->schema->field_count % bundle_validity_bits_per_word != 0)
            {
                const auto used_bits = state->schema->field_count % bundle_validity_bits_per_word;
                words[state->validity_word_count - 1U] = (BundleValidityWord{1} << used_bits) - 1U;
            }
        }

        struct ArrayIndexedContext
        {
            const ValueTypeMetaData *schema{nullptr};
            const ValueTypeBinding *element_binding{nullptr};
            std::size_t             size{0};
            std::size_t             stride{0};
        };

        [[nodiscard]] std::size_t combine_hash(std::size_t seed, std::size_t value) noexcept
        {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            return seed;
        }

        [[nodiscard]] std::size_t composite_indexed_size(const void *context, const void *) noexcept
        {
            return static_cast<const CompositeIndexedContext *>(context)->child_bindings.size();
        }

        [[nodiscard]] const void *composite_indexed_element_at(const void *context, const void *memory,
                                                               std::size_t index)
        {
            const auto *state = static_cast<const CompositeIndexedContext *>(context);
            if (!composite_field_set(state, memory, index)) { return nullptr; }   // UNSET field
            return static_cast<const std::byte *>(memory) + state->offsets[index];
        }

        [[nodiscard]] const ValueTypeBinding *composite_indexed_element_binding(const void *context, const void *,
                                                                                std::size_t index) noexcept
        {
            return static_cast<const CompositeIndexedContext *>(context)->child_bindings[index];
        }

        [[nodiscard]] ValueView composite_indexed_range_projector(const void *context, const void *memory,
                                                                  std::size_t index)
        {
            return ValueView{composite_indexed_element_binding(context, memory, index),
                             composite_indexed_element_at(context, memory, index)};
        }

        [[nodiscard]] void *composite_indexed_mutable_element_at(const void *context, void *memory,
                                                                  std::size_t index)
        {
            const auto *state = static_cast<const CompositeIndexedContext *>(context);
            // Mutable access marks the field LIVE (a writer is committing a
            // value) and always returns the real storage.
            composite_mark_field(state, memory, index, true);
            return static_cast<std::byte *>(memory) + state->offsets[index];
        }

        [[nodiscard]] ValueView composite_indexed_mutable_range_projector(const void *context,
                                                                          const void *memory,
                                                                          std::size_t index)
        {
            return ValueView{composite_indexed_element_binding(context, memory, index),
                             composite_indexed_mutable_element_at(context, const_cast<void *>(memory), index)}
                .begin_mutation();
        }

        [[nodiscard]] Range<ValueView> composite_indexed_make_range(const void *context, const void *memory)
        {
            return Range<ValueView>{
                .context   = context,
                .memory    = memory,
                .limit     = composite_indexed_size(context, memory),
                .predicate = nullptr,
                .projector = &composite_indexed_range_projector,
            };
        }

        [[nodiscard]] Range<ValueView> composite_indexed_make_mutable_range(const void *context, void *memory)
        {
            return Range<ValueView>{
                .context   = context,
                .memory    = memory,
                .limit     = composite_indexed_size(context, memory),
                .predicate = nullptr,
                .projector = &composite_indexed_mutable_range_projector,
            };
        }

        [[nodiscard]] std::size_t composite_value_hash(const void *context, const void *memory)
        {
            const auto *state = static_cast<const CompositeIndexedContext *>(context);
            std::size_t seed  = 0;
            for (std::size_t index = 0; index < state->child_bindings.size(); ++index)
            {
                if (!composite_field_set(state, memory, index))
                {
                    seed = combine_hash(seed, 0x9e3779b97f4a7c15ULL);   // UNSET marker
                    continue;
                }
                const auto &ops = state->child_bindings[index]->ops_ref();
                const auto *child = static_cast<const std::byte *>(memory) + state->offsets[index];
                seed = combine_hash(seed, ops.hash(child));
            }
            return seed;
        }

        [[nodiscard]] bool composite_value_equals(const void *context, const void *lhs, const void *rhs) noexcept
        {
            if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
            return fallback_on_exception(false, [&] {
                const auto *state = static_cast<const CompositeIndexedContext *>(context);
                for (std::size_t index = 0; index < state->child_bindings.size(); ++index)
                {
                    const bool lhs_set = composite_field_set(state, lhs, index);
                    const bool rhs_set = composite_field_set(state, rhs, index);
                    if (lhs_set != rhs_set) { return false; }
                    if (!lhs_set) { continue; }   // UNSET == UNSET
                    const auto &ops = state->child_bindings[index]->ops_ref();
                    const auto *a = static_cast<const std::byte *>(lhs) + state->offsets[index];
                    const auto *b = static_cast<const std::byte *>(rhs) + state->offsets[index];
                    if (!ops.equals(a, b)) { return false; }
                }
                return true;
            });
        }

        [[nodiscard]] std::partial_ordering composite_value_compare(const void *context, const void *lhs,
                                                                    const void *rhs) noexcept
        {
            if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }

            return fallback_on_exception(std::partial_ordering::unordered, [&]() {
                const auto *state = static_cast<const CompositeIndexedContext *>(context);
                for (std::size_t index = 0; index < state->child_bindings.size(); ++index)
                {
                    const bool lhs_set = composite_field_set(state, lhs, index);
                    const bool rhs_set = composite_field_set(state, rhs, index);
                    if (lhs_set != rhs_set)
                    {
                        return lhs_set ? std::partial_ordering::greater : std::partial_ordering::less;
                    }
                    if (!lhs_set) { continue; }
                    const auto &ops = state->child_bindings[index]->ops_ref();
                    const auto *a = static_cast<const std::byte *>(lhs) + state->offsets[index];
                    const auto *b = static_cast<const std::byte *>(rhs) + state->offsets[index];
                    const auto  c = ops.compare(a, b);
                    if (c != 0) { return c; }
                }
                return std::partial_ordering::equivalent;
            });
        }

        [[nodiscard]] std::string composite_value_to_string(const void *context, const void *memory)
        {
            if (memory == nullptr) { return {}; }
            const auto *state = static_cast<const CompositeIndexedContext *>(context);
            const bool  bundle = state->schema != nullptr && state->schema->kind == ValueTypeKind::Bundle;
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "{}", bundle ? '{' : '(');
            for (std::size_t index = 0; index < state->child_bindings.size(); ++index)
            {
                if (index > 0) { fmt::format_to(std::back_inserter(out), ", "); }
                if (bundle)
                {
                    const char *name = state->schema->fields[index].name;
                    fmt::format_to(std::back_inserter(out), "{}: ", name != nullptr ? name : "");
                }
                if (!composite_field_set(state, memory, index))
                {
                    fmt::format_to(std::back_inserter(out), "<unset>");
                    continue;
                }
                const auto &ops = state->child_bindings[index]->ops_ref();
                const auto *child = static_cast<const std::byte *>(memory) + state->offsets[index];
                fmt::format_to(std::back_inserter(out), "{}", ops.to_string(child));
            }
            fmt::format_to(std::back_inserter(out), "{}", bundle ? '}' : ')');
            return fmt::to_string(out);
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        void require_python_source(nb::handle source, const char *what)
        {
            if (source.is_none()) { throw std::invalid_argument(std::string{what} + " requires a non-None value"); }
        }

        [[nodiscard]] bool is_python_sequence(nb::handle source)
        {
            nb::object object = nb::borrow<nb::object>(source);
            return nb::isinstance<nb::list>(object) || nb::isinstance<nb::tuple>(object);
        }

        void assign_child_from_python(const ValueTypeBinding &binding,
                                      void                   *memory,
                                      nb::handle              source,
                                      const char             *what)
        {
            if (memory == nullptr) { throw std::runtime_error(std::string{what} + " child memory is not live"); }
            if (source.is_none()) { throw std::invalid_argument(std::string{what} + " does not allow None elements"); }
            binding.ops_ref().from_python(binding, memory, source);
        }

        [[nodiscard]] nb::object composite_value_to_python(const void *context, const void *memory)
        {
            if (memory == nullptr) { throw std::runtime_error("composite to_python requires live value memory"); }
            const auto *state = static_cast<const CompositeIndexedContext *>(context);
            const bool  bundle = state->schema != nullptr && state->schema->kind == ValueTypeKind::Bundle;

            if (bundle)
            {
                // A NAMED bundle with a registered python class rebuilds the
                // class (CompoundScalar read-back; UNSET fields -> None).
                nb::object bundle_class;
                if (!state->schema->name().empty())
                {
                    nb::dict &classes = python_bridge::bundle_class_registry();
                    nb::str   key{std::string{state->schema->name()}.c_str()};
                    if (classes.contains(key)) { bundle_class = classes[key]; }
                }
                nb::dict result;
                for (std::size_t index = 0; index < state->child_bindings.size(); ++index)
                {
                    const char *name = state->schema->fields[index].name;
                    if (name == nullptr || *name == '\0') { continue; }
                    const bool set = composite_field_set(state, memory, index);
                    if (!set)
                    {
                        if (bundle_class.is_valid()) { result[nb::str{name}] = nb::none(); }
                        continue;   // plain dicts OMIT unset fields
                    }
                    const auto &ops = state->child_bindings[index]->ops_ref();
                    const auto *child = static_cast<const std::byte *>(memory) + state->offsets[index];
                    result[nb::str{name}] = ops.to_python(child);
                }
                if (bundle_class.is_valid()) { return bundle_class(**result); }
                return result;
            }

            nb::list result;
            for (std::size_t index = 0; index < state->child_bindings.size(); ++index)
            {
                // UNSET tuple fields read back as None (field validity - the
                // relaxed combine/partial convert convention).
                if (!composite_field_set(state, memory, index))
                {
                    result.append(nb::none());
                    continue;
                }
                const auto &ops = state->child_bindings[index]->ops_ref();
                const auto *child = static_cast<const std::byte *>(memory) + state->offsets[index];
                result.append(ops.to_python(child));
            }
            return nb::tuple(result);
        }

        void fill_composite_from_sequence(const CompositeIndexedContext *state,
                                          void                          *memory,
                                          nb::handle                     source,
                                          const char                    *what)
        {
            if (!is_python_sequence(source))
            {
                throw std::invalid_argument(std::string{what} + " expects a Python list or tuple");
            }

            nb::object   object   = nb::borrow<nb::object>(source);
            nb::sequence sequence = nb::cast<nb::sequence>(object);
            const auto   count    = static_cast<std::size_t>(nb::len(sequence));
            // hgraph parity: a LONGER python sequence fills a fixed tuple's
            // leading fields (python tuples don't length-validate upstream).
            if (count > state->child_bindings.size()) { /* truncate below */ }
            else if (count != state->child_bindings.size())
            {
                throw std::invalid_argument(
                    fmt::format("{} expects {} elements, got {}", what, state->child_bindings.size(), count));
            }

            composite_mark_all(state, memory, true);   // sequences supply every field
            const std::size_t fill_count = std::min(count, state->child_bindings.size());
            for (std::size_t index = 0; index < fill_count; ++index)
            {
                nb::object element = sequence[index];
                // None = UNSET (field validity) - the TABLE row convention:
                // to_python reads holes back as None, so None round-trips.
                if (element.is_none())
                {
                    composite_mark_field(state, memory, index, false);
                    continue;
                }
                auto      *child   = static_cast<std::byte *>(memory) + state->offsets[index];
                assign_child_from_python(*state->child_bindings[index], child, element, what);
            }
        }

        void composite_value_from_python(const void *context, const ValueTypeBinding &, void *memory,
                                         nb::handle source)
        {
            if (memory == nullptr) { throw std::runtime_error("composite from_python requires live value memory"); }
            require_python_source(source, "Composite value");

            const auto *state  = static_cast<const CompositeIndexedContext *>(context);
            const bool  bundle = state->schema != nullptr && state->schema->kind == ValueTypeKind::Bundle;
            if (!bundle)
            {
                fill_composite_from_sequence(state, memory, source, "Tuple value");
                return;
            }

            nb::object object = nb::borrow<nb::object>(source);
            if (nb::isinstance<nb::dict>(object))
            {
                nb::dict map = nb::cast<nb::dict>(object);
                composite_mark_all(state, memory, false);
                for (std::size_t index = 0; index < state->child_bindings.size(); ++index)
                {
                    const char *name = state->schema->fields[index].name;
                    if (name == nullptr || *name == '\0')
                    {
                        throw std::invalid_argument("Bundle value has an unnamed field and cannot be loaded from dict");
                    }
                    nb::str key{name};
                    // PARTIAL dicts mark exactly the provided keys (field
                    // validity, core_concepts.rst) - absent = UNSET.
                    if (!map.contains(key)) { continue; }
                    nb::object value = map[key];
                    // None = UNSET (field validity - the same convention as
                    // the attribute form; eval_node bundles tick partially).
                    if (value.is_none()) { continue; }
                    auto      *child = static_cast<std::byte *>(memory) + state->offsets[index];
                    assign_child_from_python(*state->child_bindings[index], child, value, "Bundle value");
                    composite_mark_field(state, memory, index, true);
                }
                return;
            }

            if (is_python_sequence(source))
            {
                fill_composite_from_sequence(state, memory, source, "Bundle value");
                return;
            }

            // ATTRIBUTE form (dataclass / CompoundScalar instances): None
            // fields are UNSET (field validity - the CS convention).
            composite_mark_all(state, memory, false);
            for (std::size_t index = 0; index < state->child_bindings.size(); ++index)
            {
                const char *name = state->schema->fields[index].name;
                if (name == nullptr || *name == '\0')
                {
                    throw std::invalid_argument("Bundle value has an unnamed field and cannot be loaded from attributes");
                }
                if (!nb::hasattr(object, name)) { continue; }
                nb::object value = nb::getattr(object, name);
                if (value.is_none()) { continue; }
                auto *child = static_cast<std::byte *>(memory) + state->offsets[index];
                assign_child_from_python(*state->child_bindings[index], child, value, "Bundle value");
                composite_mark_field(state, memory, index, true);
            }
        }
#endif

        [[nodiscard]] std::size_t array_indexed_size(const void *context, const void *) noexcept
        {
            return static_cast<const ArrayIndexedContext *>(context)->size;
        }

        [[nodiscard]] const void *array_indexed_element_at(const void *context, const void *memory, std::size_t index)
        {
            const auto *state = static_cast<const ArrayIndexedContext *>(context);
            return static_cast<const std::byte *>(memory) + index * state->stride;
        }

        [[nodiscard]] const ValueTypeBinding *array_indexed_element_binding(const void *context, const void *,
                                                                            std::size_t) noexcept
        {
            return static_cast<const ArrayIndexedContext *>(context)->element_binding;
        }

        [[nodiscard]] ValueView array_indexed_range_projector(const void *context, const void *memory,
                                                              std::size_t index)
        {
            return ValueView{array_indexed_element_binding(context, memory, index),
                             array_indexed_element_at(context, memory, index)};
        }

        [[nodiscard]] ValueView array_indexed_mutable_range_projector(const void *context, const void *memory,
                                                                      std::size_t index)
        {
            return ValueView{array_indexed_element_binding(context, memory, index),
                             const_cast<void *>(array_indexed_element_at(context, memory, index))}
                .begin_mutation();
        }

        [[nodiscard]] Range<ValueView> array_indexed_make_range(const void *context, const void *memory)
        {
            return Range<ValueView>{
                .context   = context,
                .memory    = memory,
                .limit     = array_indexed_size(context, memory),
                .predicate = nullptr,
                .projector = &array_indexed_range_projector,
            };
        }

        [[nodiscard]] Range<ValueView> array_indexed_make_mutable_range(const void *context, void *memory)
        {
            return Range<ValueView>{
                .context   = context,
                .memory    = memory,
                .limit     = array_indexed_size(context, memory),
                .predicate = nullptr,
                .projector = &array_indexed_mutable_range_projector,
            };
        }

        [[nodiscard]] std::size_t array_value_hash(const void *context, const void *memory)
        {
            const auto *state = static_cast<const ArrayIndexedContext *>(context);
            const auto &ops   = state->element_binding->ops_ref();
            std::size_t seed  = 0;
            for (std::size_t index = 0; index < state->size; ++index)
            {
                const auto *child = static_cast<const std::byte *>(memory) + index * state->stride;
                seed = combine_hash(seed, ops.hash(child));
            }
            return seed;
        }

        [[nodiscard]] bool array_value_equals(const void *context, const void *lhs, const void *rhs) noexcept
        {
            if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
            return fallback_on_exception(false, [&] {
                const auto *state = static_cast<const ArrayIndexedContext *>(context);
                const auto &ops   = state->element_binding->ops_ref();
                for (std::size_t index = 0; index < state->size; ++index)
                {
                    const auto *a = static_cast<const std::byte *>(lhs) + index * state->stride;
                    const auto *b = static_cast<const std::byte *>(rhs) + index * state->stride;
                    if (!ops.equals(a, b)) { return false; }
                }
                return true;
            });
        }

        [[nodiscard]] std::partial_ordering array_value_compare(const void *context, const void *lhs,
                                                                const void *rhs) noexcept
        {
            if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }

            return fallback_on_exception(std::partial_ordering::unordered, [&]() {
                const auto *state = static_cast<const ArrayIndexedContext *>(context);
                const auto &ops   = state->element_binding->ops_ref();
                for (std::size_t index = 0; index < state->size; ++index)
                {
                    const auto *a = static_cast<const std::byte *>(lhs) + index * state->stride;
                    const auto *b = static_cast<const std::byte *>(rhs) + index * state->stride;
                    const auto  c = ops.compare(a, b);
                    if (c != 0) { return c; }
                }
                return std::partial_ordering::equivalent;
            });
        }

        [[nodiscard]] std::string array_value_to_string(const void *context, const void *memory)
        {
            if (memory == nullptr) { return "[]"; }
            const auto *state = static_cast<const ArrayIndexedContext *>(context);
            const auto &ops   = state->element_binding->ops_ref();
            fmt::memory_buffer out;
            fmt::format_to(std::back_inserter(out), "[");
            for (std::size_t index = 0; index < state->size; ++index)
            {
                if (index > 0) { fmt::format_to(std::back_inserter(out), ", "); }
                const auto *child = static_cast<const std::byte *>(memory) + index * state->stride;
                fmt::format_to(std::back_inserter(out), "{}", ops.to_string(child));
            }
            fmt::format_to(std::back_inserter(out), "]");
            return fmt::to_string(out);
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] nb::object array_value_to_python(const void *context, const void *memory)
        {
            if (memory == nullptr) { throw std::runtime_error("array to_python requires live value memory"); }
            const auto *state = static_cast<const ArrayIndexedContext *>(context);
            const auto &ops   = state->element_binding->ops_ref();
            if (ops.can_to_python_buffer(*state->element_binding))
            {
                struct ArrayBufferOwner
                {
                    const void                *memory{nullptr};
                    const ArrayIndexedContext *state{nullptr};
                };
                const ArrayBufferOwner owner{memory, state};
                const auto element_at = [](const void *owner_memory, std::size_t index) -> const void * {
                    const auto *owner_state = static_cast<const ArrayBufferOwner *>(owner_memory);
                    return static_cast<const std::byte *>(owner_state->memory) +
                           index * owner_state->state->stride;
                };
                return ops.to_python_buffer(*state->element_binding,
                                            ValueArraySource{
                                                .owner      = &owner,
                                                .size       = state->size,
                                                .element_at = element_at,
                                                .first      = ValueArraySpan{
                                                    .data   = memory,
                                                    .size   = state->size,
                                                    .stride = state->stride,
                                                },
                                            });
            }

            nb::list result;
            for (std::size_t index = 0; index < state->size; ++index)
            {
                const auto *child = static_cast<const std::byte *>(memory) + index * state->stride;
                result.append(ops.to_python(child));
            }
            return result;
        }

        void array_value_from_python(const void *context, const ValueTypeBinding &, void *memory, nb::handle source)
        {
            if (memory == nullptr) { throw std::runtime_error("array from_python requires live value memory"); }
            require_python_source(source, "Fixed List value");
            if (!is_python_sequence(source))
            {
                throw std::invalid_argument("Fixed List value expects a Python list or tuple");
            }

            const auto *state    = static_cast<const ArrayIndexedContext *>(context);
            nb::object  object   = nb::borrow<nb::object>(source);
            nb::sequence sequence = nb::cast<nb::sequence>(object);
            const auto   count    = static_cast<std::size_t>(nb::len(sequence));
            if (count != state->size)
            {
                throw std::invalid_argument(
                    fmt::format("Fixed List value expects {} elements, got {}", state->size, count));
            }

            for (std::size_t index = 0; index < count; ++index)
            {
                nb::object element = sequence[index];
                auto      *child   = static_cast<std::byte *>(memory) + index * state->stride;
                assign_child_from_python(*state->element_binding, child, element, "Fixed List value");
            }
        }
#endif

        struct CompositeIndexedOpsEntry
        {
            CompositeIndexedContext context{};
            IndexedValueOps         ops{};

            CompositeIndexedOpsEntry(const CompositeIndexedOpsEntry &other)
                : context{other.context}, ops{other.ops}
            {
                ops.context = &context;
            }

            CompositeIndexedOpsEntry(CompositeIndexedOpsEntry &&other) noexcept
                : context{std::move(other.context)}, ops{other.ops}
            {
                ops.context = &context;
            }

            CompositeIndexedOpsEntry(const ValueTypeMetaData &schema, const MemoryUtils::StoragePlan &plan)
            {
                const std::size_t validity_words =
                    (schema.kind == ValueTypeKind::Bundle || schema.kind == ValueTypeKind::Tuple)
                        ? bundle_validity_word_count(schema.field_count)
                        : 0;
                const std::size_t validity_components = validity_words == 0 ? 0 : 1;
                if (!plan.is_composite() || plan.component_count() != schema.field_count + validity_components)
                {
                    throw std::logic_error("ValuePlanFactory: composite indexed ops require matching composite plan");
                }

                context.child_bindings.reserve(schema.field_count);
                context.offsets.reserve(schema.field_count);
                const auto components = plan.components();
                for (std::size_t index = 0; index < schema.field_count; ++index)
                {
                    const auto *child_binding =
                        ValuePlanFactory::instance().binding_for(schema.fields[index].type);
                    if (child_binding == nullptr)
                    {
                        throw std::logic_error("ValuePlanFactory: composite field has no resolvable indexed binding");
                    }
                    context.child_bindings.push_back(child_binding);
                    context.offsets.push_back(components[index].offset);
                }

                context.schema = &schema;
                if (validity_components != 0)
                {
                    // The hidden fixed-word validity component sits right
                    // after the public fields (appended last at plan build).
                    context.validity_offset = components[schema.field_count].offset;
                    context.validity_word_count = validity_words;
                }

                ops = IndexedValueOps{
                    {&context,
                     true,
                     schema.is_hashable() ? &composite_value_hash : nullptr,
                     &composite_value_equals,
                     &composite_value_compare,
                     &composite_value_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                     ,
                     &composite_value_to_python,
                     &composite_value_from_python
#endif
                    },
                    &composite_indexed_size,
                    &composite_indexed_element_at,
                    &composite_indexed_element_binding,
                    &composite_indexed_make_range,
                    &composite_indexed_make_mutable_range,
                };
                ops.mutable_element_at = &composite_indexed_mutable_element_at;
            }
        };

        struct ArrayIndexedOpsEntry
        {
            ArrayIndexedContext context{};
            IndexedValueOps     ops{};

            ArrayIndexedOpsEntry(const ArrayIndexedOpsEntry &other)
                : context{other.context}, ops{other.ops}
            {
                ops.context = &context;
            }

            ArrayIndexedOpsEntry(ArrayIndexedOpsEntry &&other) noexcept
                : context{other.context}, ops{other.ops}
            {
                ops.context = &context;
            }

            ArrayIndexedOpsEntry(const ValueTypeMetaData &schema, const MemoryUtils::StoragePlan &plan)
            {
                if (!plan.is_array())
                {
                    throw std::logic_error("ValuePlanFactory: array indexed ops require an array plan");
                }

                const auto *element_binding = ValuePlanFactory::instance().binding_for(schema.element_type);
                if (element_binding == nullptr)
                {
                    throw std::logic_error("ValuePlanFactory: fixed list element has no resolvable binding");
                }

                context = ArrayIndexedContext{
                    .schema          = &schema,
                    .element_binding = element_binding,
                    .size            = plan.array_count(),
                    .stride          = plan.array_stride(),
                };

                ops = IndexedValueOps{
                    {&context,
                     true,
                     schema.is_hashable() ? &array_value_hash : nullptr,
                     &array_value_equals,
                     &array_value_compare,
                     &array_value_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                     ,
                     &array_value_to_python,
                     &array_value_from_python
#endif
                    },
                    &array_indexed_size,
                    &array_indexed_element_at,
                    &array_indexed_element_binding,
                    &array_indexed_make_range,
                    &array_indexed_make_mutable_range,
                };
            }
        };

        [[nodiscard]] InternTable<const ValueTypeMetaData *, CompositeIndexedOpsEntry> &
        composite_indexed_ops_cache() noexcept
        {
            static InternTable<const ValueTypeMetaData *, CompositeIndexedOpsEntry> cache;
            return cache;
        }

        [[nodiscard]] InternTable<const ValueTypeMetaData *, ArrayIndexedOpsEntry> &array_indexed_ops_cache() noexcept
        {
            static InternTable<const ValueTypeMetaData *, ArrayIndexedOpsEntry> cache;
            return cache;
        }

        [[nodiscard]] const IndexedValueOps &composite_indexed_ops(const ValueTypeMetaData &schema,
                                                                   const MemoryUtils::StoragePlan &plan)
        {
            return composite_indexed_ops_cache().emplace(&schema, schema, plan).ops;
        }

        [[nodiscard]] const IndexedValueOps &array_indexed_ops(const ValueTypeMetaData &schema,
                                                               const MemoryUtils::StoragePlan &plan)
        {
            return array_indexed_ops_cache().emplace(&schema, schema, plan).ops;
        }

        void clear_structured_indexed_ops() noexcept
        {
            composite_indexed_ops_cache().clear();
            array_indexed_ops_cache().clear();
        }
    }  // namespace

    ValuePlanFactory &ValuePlanFactory::instance()
    {
        // Immortal (see OperatorRegistry::instance): interned plans/bindings
        // must outlive every Value destructor, including static-teardown ones.
        static ValuePlanFactory *factory = new ValuePlanFactory();
        return *factory;
    }

    void ValuePlanFactory::register_atomic(const ValueTypeMetaData *schema, const MemoryUtils::StoragePlan *plan)
    {
        if (schema == nullptr || plan == nullptr) { return; }

        std::lock_guard<std::mutex> lock(mutex_);
        if (const auto it = cache_.find(schema); it != cache_.end())
        {
            if (it->second == plan) { return; }
            throw std::logic_error(std::string{"ValuePlanFactory: atomic schema already registered with a "
                                               "different plan: "} +
                                   (schema->display_name ? schema->display_name : "?"));
        }
        cache_.emplace(schema, plan);
    }

    void ValuePlanFactory::register_binding(const ValueTypeBinding &binding)
    {
        if (!binding.valid()) { return; }

        std::lock_guard<std::mutex> lock(mutex_);
        if (const auto it = binding_cache_.find(binding.type_meta); it != binding_cache_.end())
        {
            if (it->second == &binding) { return; }
            throw std::logic_error("ValuePlanFactory: schema already registered with a different binding");
        }

        if (const auto it = cache_.find(binding.type_meta); it != cache_.end())
        {
            if (it->second != binding.plan())
            {
                throw std::logic_error("ValuePlanFactory: schema already registered with a different plan");
            }
        }
        else
        {
            cache_.emplace(binding.type_meta, binding.plan());
        }

        binding_cache_.emplace(binding.type_meta, &binding);
    }

    const MemoryUtils::StoragePlan *ValuePlanFactory::plan_for(const ValueTypeMetaData *schema)
    {
        if (schema == nullptr) { return nullptr; }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = cache_.find(schema); it != cache_.end()) { return it->second; }
        }

        return synthesise(schema);
    }

    const MemoryUtils::StoragePlan *ValuePlanFactory::find(const ValueTypeMetaData *schema) const
    {
        if (schema == nullptr) { return nullptr; }

        std::lock_guard<std::mutex> lock(mutex_);
        const auto                  it = cache_.find(schema);
        return it == cache_.end() ? nullptr : it->second;
    }

    const ValueTypeBinding *ValuePlanFactory::binding_for(const ValueTypeMetaData *schema)
    {
        if (schema == nullptr) { return nullptr; }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (const auto it = binding_cache_.find(schema); it != binding_cache_.end()) { return it->second; }
        }

        return synthesise_binding(schema);
    }

    const ValueTypeBinding *ValuePlanFactory::find_binding(const ValueTypeMetaData *schema) const
    {
        if (schema == nullptr) { return nullptr; }

        std::lock_guard<std::mutex> lock(mutex_);
        const auto                  it = binding_cache_.find(schema);
        return it == binding_cache_.end() ? nullptr : it->second;
    }

    void ValuePlanFactory::reset() noexcept
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cache_.clear();
        binding_cache_.clear();
        clear_compact_container_plans();
        clear_mutable_container_plans();
        MemoryUtils::clear_synthesised_plans();
        clear_structured_indexed_ops();
    }

    const MemoryUtils::StoragePlan *ValuePlanFactory::synthesise(const ValueTypeMetaData *schema)
    {
        const MemoryUtils::StoragePlan *plan = nullptr;

        switch (schema->kind)
        {
            case ValueTypeKind::Atomic:
                throw std::logic_error(
                    "ValuePlanFactory: atomic schema has no canonical plan; register it via register_atomic "
                    "(typically through TypeRegistry::register_scalar<T>)");

            case ValueTypeKind::Tuple:
            {
                auto builder = MemoryUtils::tuple();
                const std::size_t validity_words = bundle_validity_word_count(schema->field_count);
                builder.reserve(schema->field_count + (validity_words == 0 ? 0 : 1));
                for (size_t index = 0; index < schema->field_count; ++index)
                {
                    const ValueTypeMetaData *field_type = schema->fields[index].type;
                    const MemoryUtils::StoragePlan *field_plan = plan_for(field_type);
                    if (field_plan == nullptr)
                    {
                        throw std::logic_error("ValuePlanFactory: tuple field has no resolvable plan");
                    }
                    builder.add_plan(*field_plan);
                }
                if (validity_words != 0)
                {
                    // Fixed tuples carry the same hidden field-validity words
                    // as bundles so a partial tuple (relaxed combine) reads
                    // its unset slots as None. Appended last: field i stays
                    // component i.
                    builder.add_hidden_plan(bundle_validity_plan(schema->field_count));
                }
                plan = &builder.build();
                break;
            }

            case ValueTypeKind::Bundle:
            {
                auto builder = MemoryUtils::named_tuple();
                const std::size_t validity_words = bundle_validity_word_count(schema->field_count);
                builder.reserve(schema->field_count + (validity_words == 0 ? 0 : 1));
                for (size_t index = 0; index < schema->field_count; ++index)
                {
                    const ValueFieldMetaData &field = schema->fields[index];
                    const MemoryUtils::StoragePlan *field_plan = plan_for(field.type);
                    if (field_plan == nullptr)
                    {
                        throw std::logic_error("ValuePlanFactory: bundle field has no resolvable plan");
                    }
                    builder.add_field(field.name != nullptr ? field.name : "", *field_plan);
                }
                if (validity_words != 0)
                {
                    // Hidden trailing validity words (design record: Bundle
                    // field validity, core_concepts.rst): one bit per field,
                    // default-zero. Appended LAST so field i remains
                    // component i; the component is intentionally unnamed.
                    builder.add_hidden_plan(bundle_validity_plan(schema->field_count));
                }
                plan = &builder.build();
                break;
            }

            case ValueTypeKind::List:
            {
                if (schema->fixed_size == 0)
                {
                    const ValueTypeBinding *binding = binding_for(schema);
                    plan = binding != nullptr ? binding->plan() : nullptr;
                    break;
                }
                const MemoryUtils::StoragePlan *element_plan = plan_for(schema->element_type);
                if (element_plan == nullptr)
                {
                    throw std::logic_error("ValuePlanFactory: fixed list has no element plan");
                }
                plan = &MemoryUtils::array_plan(*element_plan, schema->fixed_size);
                break;
            }

            case ValueTypeKind::Set:
            case ValueTypeKind::Map:
            case ValueTypeKind::CyclicBuffer:
            case ValueTypeKind::Queue:
            case ValueTypeKind::Any:
            {
                const ValueTypeBinding *binding = binding_for(schema);
                plan = binding != nullptr ? binding->plan() : nullptr;
                break;
            }
        }

        if (plan == nullptr)
        {
            throw std::logic_error("ValuePlanFactory: unhandled ValueTypeKind");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        const auto                  it = cache_.find(schema);
        if (it != cache_.end()) { return it->second; }
        cache_.emplace(schema, plan);
        return plan;
    }

    const ValueTypeBinding *ValuePlanFactory::synthesise_binding(const ValueTypeMetaData *schema)
    {
        const ValueTypeBinding *binding = nullptr;

        switch (schema->kind)
        {
            case ValueTypeKind::Atomic:
                throw std::logic_error(
                    "ValuePlanFactory: atomic schema has no canonical binding; register it via TypeRegistry::register_scalar<T>");

            case ValueTypeKind::Tuple:
            case ValueTypeKind::Bundle:
            {
                for (size_t index = 0; index < schema->field_count; ++index)
                {
                    if (binding_for(schema->fields[index].type) == nullptr)
                    {
                        throw std::logic_error("ValuePlanFactory: composite field has no resolvable binding");
                    }
                }
                const MemoryUtils::StoragePlan *plan = plan_for(schema);
                if (plan == nullptr) { throw std::logic_error("ValuePlanFactory: composite has no resolvable plan"); }
                binding = &ValueTypeBinding::intern(*schema, *plan, composite_indexed_ops(*schema, *plan));
                break;
            }

            case ValueTypeKind::List:
            {
                const ValueTypeBinding *element_binding = binding_for(schema->element_type);
                if (element_binding == nullptr)
                {
                    throw std::logic_error("ValuePlanFactory: list element has no resolvable binding");
                }
                if (schema->fixed_size == 0)
                {
                    binding = schema->is_mutable() ? &mutable_list_binding(*element_binding)
                                                   : &compact_list_binding(*element_binding, *schema);
                }
                else
                {
                    const MemoryUtils::StoragePlan *plan = plan_for(schema);
                    if (plan == nullptr) { throw std::logic_error("ValuePlanFactory: fixed list has no resolvable plan"); }
                    binding = &ValueTypeBinding::intern(*schema, *plan, array_indexed_ops(*schema, *plan));
                }
                break;
            }

            case ValueTypeKind::Set:
            {
                const ValueTypeBinding *element_binding = binding_for(schema->element_type);
                if (element_binding == nullptr)
                {
                    throw std::logic_error("ValuePlanFactory: set element has no resolvable binding");
                }
                binding = schema->is_mutable() ? &mutable_set_binding(*element_binding)
                                               : &compact_set_binding(*element_binding);
                break;
            }

            case ValueTypeKind::Map:
            {
                const ValueTypeBinding *key_binding = binding_for(schema->key_type);
                const ValueTypeBinding *value_binding = binding_for(schema->element_type);
                if (key_binding == nullptr || value_binding == nullptr)
                {
                    throw std::logic_error("ValuePlanFactory: map key/value has no resolvable binding");
                }
                binding = schema->is_mutable() ? &mutable_map_binding(*key_binding, *value_binding)
                                               : &compact_map_binding(*key_binding, *value_binding);
                break;
            }

            case ValueTypeKind::CyclicBuffer:
            {
                const ValueTypeBinding *element_binding = binding_for(schema->element_type);
                if (element_binding == nullptr)
                {
                    throw std::logic_error("ValuePlanFactory: cyclic buffer element has no resolvable binding");
                }
                binding = &compact_cyclic_buffer_binding(*element_binding, schema->fixed_size);
                break;
            }

            case ValueTypeKind::Queue:
            {
                const ValueTypeBinding *element_binding = binding_for(schema->element_type);
                if (element_binding == nullptr)
                {
                    throw std::logic_error("ValuePlanFactory: queue element has no resolvable binding");
                }
                binding = &compact_queue_binding(*element_binding, schema->fixed_size);
                break;
            }

            case ValueTypeKind::Any:
                binding = &any_binding(*schema);   // JSON keeps its identity
                break;
        }

        if (binding == nullptr)
        {
            throw std::logic_error("ValuePlanFactory: unhandled ValueTypeKind while synthesising binding");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        const auto                  it = binding_cache_.find(schema);
        if (it != binding_cache_.end()) { return it->second; }

        if (const auto plan_it = cache_.find(schema); plan_it != cache_.end())
        {
            if (plan_it->second != binding->plan())
            {
                throw std::logic_error("ValuePlanFactory: synthesised binding does not match cached plan");
            }
        }
        else
        {
            cache_.emplace(schema, binding->plan());
        }

        binding_cache_.emplace(schema, binding);
        return binding;
    }
}  // namespace hgraph

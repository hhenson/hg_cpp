#ifndef HGRAPH_CPP_ROOT_VALUE_H
#define HGRAPH_CPP_ROOT_VALUE_H

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value_ops.h>
#include <hgraph/types/value/value_view.h>

#include <compare>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace hgraph
{
    /**
     * Owning handle for a value-layer instance.
     *
     * ``Value`` is a thin wrapper over ``MemoryUtils::StorageHandle``
     * specialised on the value-layer ``ValueTypeBinding``. The storage
     * handle owns the bytes (inline or heap, deciding via the policy) and
     * retains the binding identity. The binding carries the schema, plan,
     * and ops, and ``Value`` adds the
     * typed convenience surface: scalar construction, ``ValueView`` lift,
     * and ``hash`` / ``equals`` / ``compare`` / ``to_string`` dispatch
     * through the lifted ``ValueView``.
     *
     * A ``Value`` may also be typed-null: ``storage_`` preserves the
     * schema/binding while its payload is empty. ``view()`` is valid only
     * when storage is populated.
     */
    class Value
    {
      public:
        using storage_type = MemoryUtils::StorageHandle<MemoryUtils::InlineStoragePolicy<>, ValueTypeBinding>;

        Value() noexcept = default;

        /**
         * Construct a typed-null ``Value`` for ``schema``. The canonical
         * binding is resolved and retained, but no payload is constructed
         * until the caller assigns or constructs from a binding/source.
         */
        explicit Value(const ValueTypeMetaData &schema)
        {
            const auto *binding = ValuePlanFactory::instance().binding_for(&schema);
            if (binding == nullptr) { throw std::logic_error("Value(schema): schema has no canonical binding"); }
            storage_ = storage_type::empty(*binding);
        }

        /**
         * Construct a default-valued ``Value`` for the given binding (the
         * binding's plan default-constructs the payload).
         */
        explicit Value(const ValueTypeBinding &binding)
            : storage_(binding)
        {
        }

        /**
         * Copy-construct a payload from external storage using ``binding``.
         */
        Value(const ValueTypeBinding &binding, const void *src)
            : storage_(storage_type::owning_copy(binding, src))
        {
        }

        /**
         * Construct an owning value for an atomic ``T`` whose schema has
         * been registered through ``TypeRegistry::register_scalar<T>``.
         * The matching binding is looked up; the storage handle drives
         * allocation and construction.
         */
        template <typename T,
                  typename = std::enable_if_t<!std::is_same_v<std::decay_t<T>, Value> &&
                                              !std::is_same_v<std::decay_t<T>, ValueTypeBinding>>>
        explicit Value(T value)
        {
            const ValueTypeBinding *binding = TypeRegistry::instance().scalar_binding<T>();
            if (binding == nullptr)
            {
                throw std::logic_error(
                    "Value(T): scalar type not registered — call register_scalar<T>(name) first");
            }
            storage_ = storage_type(*binding);
            // The handle has default-constructed the payload; assign the
            // caller-supplied value into the live storage.
            *static_cast<T *>(storage_.data()) = std::move(value);
        }

        // Copy and move are inherited from ``StorageHandle`` — copy
        // performs a deep allocation + copy-construct via the binding's
        // plan; move transfers ownership.
        Value(const Value &) = default;
        Value &operator=(const Value &) = default;
        Value(Value &&) noexcept = default;
        Value &operator=(Value &&) noexcept = default;
        ~Value() = default;

        /** True when storage is allocated and a value has been constructed in it. */
        [[nodiscard]] bool has_value() const noexcept { return storage_.has_value(); }

        /** The bound schema; ``nullptr`` for a default-constructed ``Value``. */
        [[nodiscard]] const ValueTypeMetaData *schema() const noexcept
        {
            const auto *bound = binding();
            return bound != nullptr ? bound->type_meta : nullptr;
        }
        [[nodiscard]] const ValueTypeBinding *binding() const noexcept { return storage_.binding(); }

        /** Tear down the constructed value (if any), preserving the binding/schema. */
        void reset() noexcept { storage_.reset_payload(); }

        // -- view access --
        [[nodiscard]] ValueView view()
        {
            return has_value() ? ValueView{binding(), storage_.data()} : ValueView{};
        }
        [[nodiscard]] ValueView view() const
        {
            return has_value() ? ValueView{binding(), const_cast<void *>(storage_.data())} : ValueView{};
        }

        // -- atomic access shortcuts --
        template <typename T>
        [[nodiscard]] const T &as() const
        {
            return view().template checked_as<T>();
        }
        template <typename T>
        [[nodiscard]] T &as()
        {
            return view().template checked_as<T>();
        }
        template <typename T>
        [[nodiscard]] const T *try_as() const noexcept
        {
            return view().template try_as<T>();
        }
        template <typename T>
        [[nodiscard]] T *try_as() noexcept
        {
            return view().template try_as<T>();
        }

        [[nodiscard]] TupleView as_tuple() const { return view().as_tuple(); }
        [[nodiscard]] std::optional<TupleView> try_as_tuple() const { return view().try_as_tuple(); }
        [[nodiscard]] BundleView as_bundle() const { return view().as_bundle(); }
        [[nodiscard]] std::optional<BundleView> try_as_bundle() const { return view().try_as_bundle(); }
        [[nodiscard]] ListView as_list() const { return view().as_list(); }
        [[nodiscard]] std::optional<ListView> try_as_list() const { return view().try_as_list(); }
        [[nodiscard]] SetView as_set() const { return view().as_set(); }
        [[nodiscard]] std::optional<SetView> try_as_set() const { return view().try_as_set(); }
        [[nodiscard]] MapView as_map() const { return view().as_map(); }
        [[nodiscard]] std::optional<MapView> try_as_map() const { return view().try_as_map(); }
        [[nodiscard]] CyclicBufferView as_cyclic_buffer() const { return view().as_cyclic_buffer(); }
        [[nodiscard]] std::optional<CyclicBufferView> try_as_cyclic_buffer() const
        {
            return view().try_as_cyclic_buffer();
        }
        [[nodiscard]] QueueView as_queue() const { return view().as_queue(); }
        [[nodiscard]] std::optional<QueueView> try_as_queue() const { return view().try_as_queue(); }

        // -- generic ops --
        [[nodiscard]] std::size_t hash() const noexcept { return view().hash(); }
        [[nodiscard]] bool equals(const Value &other) const noexcept
        {
            return view().equals(other.view());
        }
        [[nodiscard]] std::partial_ordering compare(const Value &other) const noexcept
        {
            const auto *lhs_binding = binding();
            const auto *rhs_binding = other.binding();
            if (const auto order = value_ops_detail::null_order(lhs_binding, rhs_binding)) { return *order; }
            if (lhs_binding != rhs_binding) { return std::partial_ordering::unordered; }
            if (!has_value() && !other.has_value()) { return std::partial_ordering::equivalent; }
            if (!has_value()) { return std::partial_ordering::less; }
            if (!other.has_value()) { return std::partial_ordering::greater; }
            return view().compare(other.view());
        }
        [[nodiscard]] std::string to_string() const { return view().to_string(); }

      private:
        storage_type storage_{};
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_H

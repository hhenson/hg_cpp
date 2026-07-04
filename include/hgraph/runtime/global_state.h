#ifndef HGRAPH_RUNTIME_GLOBAL_STATE_H
#define HGRAPH_RUNTIME_GLOBAL_STATE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/value/value.h>

#include <string>
#include <string_view>

namespace hgraph
{
    /**
     * Borrowing **view** over a graph's ``GlobalState`` — the access surface and
     * the node-level injectable. It is the value/view split applied to global
     * state: the owning ``GlobalState`` holds the data on the root graph /
     * builder; callers (and nodes, via the ``GlobalStateView`` selector) receive
     * this view to read and write it. Non-owning: it borrows the owner's value,
     * which must outlive the view.
     *
     * The store is a mutable ``Map<string, Any>``; values are boxed in ``Any`` on
     * the way in (``set`` assigns in place — no temporary) and unwrapped on the
     * way out (``get`` returns the contained value directly).
     */
    class HGRAPH_EXPORT GlobalStateView
    {
      public:
        explicit GlobalStateView(Value &map) noexcept : map_(&map) {}

        /** Number of keys currently stored. */
        [[nodiscard]] std::size_t size() const;
        /** True when ``key`` is present. */
        [[nodiscard]] bool contains(std::string_view key) const;

        /**
         * Read the value stored under ``key`` (the contained value, unwrapped
         * from its ``Any`` box). Returns an **invalid** ``ValueView`` when absent.
         *
         * The store is mutable by definition, so the read honours the stored
         * value's own mutability: a value boxed as **mutable** (e.g. a mutable
         * ``List``/``Map``) comes back as a **writable** view that can be mutated
         * in place; an **immutable** value comes back read-only (its ops refuse
         * ``begin_mutation``).
         */
        [[nodiscard]] ValueView get(std::string_view key) const;

        /** Typed read; throws when absent or the type does not match. */
        template <typename T>
        [[nodiscard]] const T &get_as(std::string_view key) const
        {
            return get(key).template checked_as<T>();
        }

        /** Insert or replace ``key`` with a copy of ``value`` (assigned in place). */
        void set(std::string_view key, const ValueView &value) const;
        void set(std::string_view key, const Value &value) const;
        /** Insert or replace ``key`` by MOVING ``value`` into the store (no copy). */
        void set(std::string_view key, Value &&value) const;

        /** Remove ``key``; returns whether a key was removed. */
        bool erase(std::string_view key) const;

        /** The underlying ``Map<string, Any>`` value. */
        [[nodiscard]] const Value &as_value() const noexcept { return *map_; }

      private:
        Value *map_;  // borrowed; owned by a GlobalState
    };

    /**
     * Owning ``GlobalState`` — a mutable, heterogeneous ``string -> value`` store
     * shared across a graph and the first **injectable**. It is created by the
     * top-level graph builder and carried (copied) onto the root graph, so the
     * same data is available at wiring time and at run time. It holds the value;
     * callers obtain a :cpp:class:`GlobalStateView` (via ``view()``,
     * ``GraphBuilder::global_state()`` or ``GraphView::global_state()``) to use
     * it — usage is explicit, a node only sees it when it declares it.
     *
     * Backed by a value-layer mutable ``Map<string, Any>`` ``Value`` — so it is a
     * single type-erased value: copyable, and bindable to Python.
     */
    class HGRAPH_EXPORT GlobalState
    {
      public:
        /** Construct an empty store (an empty mutable ``Map<string, Any>``). */
        GlobalState();

        GlobalState(const GlobalState &)                = default;
        GlobalState &operator=(const GlobalState &)     = default;
        GlobalState(GlobalState &&) noexcept            = default;
        GlobalState &operator=(GlobalState &&) noexcept = default;
        ~GlobalState()                                  = default;

        /** A view over this store. */
        [[nodiscard]] GlobalStateView view() noexcept { return GlobalStateView{map_}; }

        /** The underlying ``Map<string, Any>`` value. */
        [[nodiscard]] const Value &as_value() const noexcept { return map_; }

      private:
        Value map_;  // mutable Map<string, Any>
    };
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_GLOBAL_STATE_H

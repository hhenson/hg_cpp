#ifndef HGRAPH_CPP_ROOT_STATIC_NODE_H
#define HGRAPH_CPP_ROOT_STATIC_NODE_H

#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/time_series/ts_data/set_view.h>
#include <hgraph/types/time_series/ts_input/bundle_view.h>
#include <hgraph/types/time_series/ts_input/list_view.h>
#include <hgraph/types/time_series/ts_input/set_view.h>
#include <hgraph/types/time_series/ts_output/list_view.h>
#include <hgraph/types/time_series/ts_output/set_view.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_view.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgraph
{
    /**
     * Node-authoring selectors and the compile-time machinery that turns a
     * static node implementation into a runtime ``NodeBuilder``.
     *
     * A node implementation is a stateless struct with a static ``eval`` (and
     * optional ``start`` / ``stop``) whose parameters are the selectors below.
     * ``NodeBuilder::implementation<T>()`` reflects that signature and builds
     * the same ``(NodeTypeMetaData, NodeCallbacks, TSEndpointSchema)`` triple
     * that ``NodeBuilder::native`` already consumes, so static authoring is a
     * typed front-end over the one runtime node model rather than a parallel
     * mechanism.
     *
     * See ``data_structures`` developer guide: *Wiring* and
     * *Schemas > Static Schema*.
     *
     * The typed selector surface currently covers scalar ``TS<T>`` values,
     * set ``TSS<T>`` values, scalar ``State<T>``, wiring-time ``Scalar<Name, T>``
     * values, graph-level ``GlobalState<T>``, and scheduler injection. Other
     * container shapes and push-source selectors are still being filled in.
     */

    // -----------------------------------------------------------------
    // Set time-series delta
    // -----------------------------------------------------------------

    template <typename TValue>
    [[nodiscard]] inline Value make_set_delta_value(const std::vector<TValue> &added,
                                                    const std::vector<TValue> &removed);

    template <typename TValue>
    [[nodiscard]] inline const ValueTypeBinding &set_delta_value_binding()
    {
        auto       &registry = TypeRegistry::instance();
        const auto *t_meta   = scalar_descriptor<TValue>::value_meta();
        const auto *set_t    = registry.mutable_set(t_meta);
        const auto *schema   = registry.un_named_bundle({{"added", set_t}, {"removed", set_t}});
        const auto *binding  = ValuePlanFactory::instance().binding_for(schema);
        if (binding == nullptr) { throw std::logic_error("SetDelta could not resolve the delta value binding"); }
        return *binding;
    }

    template <typename TValue, typename BundleMutation>
    inline void populate_set_delta_bundle(BundleMutation &&bundle,
                                          const std::vector<TValue> &added,
                                          const std::vector<TValue> &removed)
    {
        {
            auto m = bundle.field("added").as_mutable_set();
            for (const auto &e : added) { (void)m.add(Value{e}.view()); }
        }
        {
            auto m = bundle.field("removed").as_mutable_set();
            for (const auto &e : removed) { (void)m.add(Value{e}.view()); }
        }
    }

    /**
     * Lightweight wrapper over a set-time-series **delta value** — a
     * ``Bundle{added, removed}`` whose fields are indexed containers of ``T`` (a
     * live ``TSS`` delta has ``Set`` fields; a built/recorded delta has mutable
     * ``Set`` fields — both read uniformly via the indexed view).
     *
     * ``SetDelta`` owns a snapshot of the bundle value. It is therefore safe to
     * copy out of an input selector and compare after the producing call stack has
     * unwound. Equality is order-independent (compares as sets). Build one from
     * elements with :cpp:func:`set_delta`.
     */
    template <typename TValue>
    class SetDelta
    {
      public:
        SetDelta() = default;
        explicit SetDelta(std::vector<TValue> added, std::vector<TValue> removed)
            : storage_(make_storage(added, removed))
        {
        }

        /** Copy an external delta bundle view into an owned snapshot. */
        explicit SetDelta(const ValueView &bundle)
        {
            if (bundle.valid())
            {
                storage_ = make_storage(elements_from(bundle, "added"), elements_from(bundle, "removed"));
            }
        }
        /** Copy a built delta bundle into this handle's storage. */
        explicit SetDelta(const Value &owned) : SetDelta(owned.view()) {}
        explicit SetDelta(Value &&owned) : SetDelta(owned.view()) {}

        [[nodiscard]] bool                valid() const noexcept { return storage_.has_value(); }
        [[nodiscard]] std::vector<TValue> added() const { return elements("added"); }
        [[nodiscard]] std::vector<TValue> removed() const { return elements("removed"); }

        /** The underlying delta bundle value (``Bundle{added, removed}``). */
        [[nodiscard]] ValueView value() const noexcept { return ValueView{storage_.binding(), storage_.data()}; }

        bool operator==(const SetDelta &other) const
        {
            return to_set(added()) == to_set(other.added()) && to_set(removed()) == to_set(other.removed());
        }

      private:
        [[nodiscard]] std::vector<TValue> elements(std::string_view field) const
        {
            std::vector<TValue> out;
            if (!valid()) { return out; }
            return elements_from(value(), field);
        }

        [[nodiscard]] static std::vector<TValue> elements_from(const ValueView &bundle, std::string_view field)
        {
            std::vector<TValue> out;
            if (!bundle.valid()) { return out; }
            const auto indexed = bundle.as_bundle().field(field).as_indexed_view();
            for (std::size_t i = 0; i < indexed.size(); ++i) { out.push_back(indexed.at(i).template checked_as<TValue>()); }
            return out;
        }
        static std::set<TValue> to_set(const std::vector<TValue> &v) { return std::set<TValue>{v.begin(), v.end()}; }

        using storage_type = Value::storage_type;

        [[nodiscard]] static storage_type make_storage(const std::vector<TValue> &added,
                                                       const std::vector<TValue> &removed)
        {
            const auto &binding = set_delta_value_binding<TValue>();
            storage_type storage{binding};
            populate_set_delta_bundle<TValue>(ValueView{&binding, storage.data()}.as_bundle().begin_mutation(),
                                              added,
                                              removed);
            return storage;
        }

        storage_type storage_{};
    };

    /**
     * Build a delta value ``Bundle{added: Set<T>, removed: Set<T>}`` from typed
     * elements. The fields are value-layer (mutable) ``Set``\ s so they match a
     * live ``TSS`` delta and compare order-independently via the hash-based set
     * equality (list fields would make delta comparison order-dependent and slow).
     */
    template <typename TValue>
    [[nodiscard]] inline Value make_set_delta_value(const std::vector<TValue> &added,
                                                    const std::vector<TValue> &removed)
    {
        const auto &binding = set_delta_value_binding<TValue>();
        Value       delta{binding};
        auto  bundle = delta.as_bundle().begin_mutation();
        populate_set_delta_bundle<TValue>(bundle, added, removed);
        return delta;
    }

    /** Build an owning :cpp:class:`SetDelta` from element lists (added, removed). */
    template <typename TValue>
    [[nodiscard]] inline SetDelta<TValue> set_delta(std::vector<TValue> added, std::vector<TValue> removed)
    {
        return SetDelta<TValue>{std::move(added), std::move(removed)};
    }

    // -----------------------------------------------------------------
    // List time-series delta
    // -----------------------------------------------------------------

    /**
     * Build a list-time-series **delta value** — an **immutable** value-layer
     * ``Map<int64, T>`` mapping a child index to that child's value, for the
     * children that ticked this cycle. The map is built once via ``MapBuilder``
     * (compact maps have no mutation path, which is exactly the immutability a
     * delta needs); the C++ surface speaks ``std::size_t`` indices while the
     * value-layer key is the registered ``int64`` scalar (a ``size_t`` scalar
     * would collide with ``uint64`` on LP64 platforms).
     */
    template <typename TValue>
    [[nodiscard]] inline Value make_list_delta_value(const std::map<std::size_t, TValue> &entries)
    {
        const auto *key_binding = ValuePlanFactory::instance().binding_for(scalar_descriptor<std::int64_t>::value_meta());
        const auto *val_binding = ValuePlanFactory::instance().binding_for(scalar_descriptor<TValue>::value_meta());
        if (key_binding == nullptr || val_binding == nullptr)
        {
            throw std::logic_error("ListDelta could not resolve the delta value binding");
        }
        MapBuilder builder{*key_binding, *val_binding};
        for (const auto &[index, value] : entries)
        {
            builder.set_item<std::int64_t, TValue>(static_cast<std::int64_t>(index), value);
        }
        return builder.build();
    }

    /**
     * Lightweight wrapper over a list-time-series **delta value** — an immutable
     * ``Map<int64, T>`` (child index -> value) for the children that ticked this
     * cycle. ``ListDelta`` owns a snapshot of that map value, so it is safe to copy
     * out of an input selector and compare after the producing call stack has
     * unwound. The elements are read on demand; equality is order-independent
     * (compares as maps). Build one from elements with :cpp:func:`list_delta`.
     */
    template <typename TValue>
    class ListDelta
    {
      public:
        ListDelta() = default;
        /** Build from a sparse ``index -> value`` map. */
        explicit ListDelta(const std::map<std::size_t, TValue> &entries) : storage_(make_storage(entries)) {}
        /** Copy an external delta map view into an owned snapshot. */
        explicit ListDelta(const ValueView &map) : storage_(make_storage(map)) {}
        /** Copy a built delta map into this handle's storage. */
        explicit ListDelta(const Value &owned) : ListDelta(owned.view()) {}
        explicit ListDelta(Value &&owned) : ListDelta(owned.view()) {}

        [[nodiscard]] bool valid() const noexcept { return storage_.has_value(); }

        /** The underlying delta map value (``Map<int64, T>``). */
        [[nodiscard]] ValueView value() const noexcept { return ValueView{storage_.binding(), storage_.data()}; }

        [[nodiscard]] bool contains(std::size_t index) const
        {
            if (!valid()) { return false; }
            return value().as_map().contains(index_key(index).view());
        }

        /** Value of the child at ``index`` (must be present). */
        [[nodiscard]] TValue at(std::size_t index) const
        {
            return value().as_map().at(index_key(index).view()).template checked_as<TValue>();
        }

        /** The modified child indices (unordered). */
        [[nodiscard]] std::vector<std::size_t> indices() const
        {
            std::vector<std::size_t> out;
            for (const auto &entry : items()) { out.push_back(entry.first); }
            return out;
        }

        /** The ``(index, value)`` pairs of the modified children. */
        [[nodiscard]] std::vector<std::pair<std::size_t, TValue>> items() const
        {
            std::vector<std::pair<std::size_t, TValue>> out;
            if (!valid()) { return out; }
            const auto map = value().as_map();
            for (const auto &[key, val] : map)
            {
                out.emplace_back(static_cast<std::size_t>(key.template checked_as<std::int64_t>()),
                                 val.template checked_as<TValue>());
            }
            return out;
        }

        bool operator==(const ListDelta &other) const { return to_map(items()) == to_map(other.items()); }

      private:
        [[nodiscard]] static Value index_key(std::size_t index)
        {
            (void)scalar_descriptor<std::int64_t>::value_meta();  // ensure int64 is registered
            return Value{static_cast<std::int64_t>(index)};
        }
        static std::map<std::size_t, TValue> to_map(const std::vector<std::pair<std::size_t, TValue>> &v)
        {
            return std::map<std::size_t, TValue>{v.begin(), v.end()};
        }

        using storage_type = Value::storage_type;

        /** Snapshot a delta-map view into an owning handle (deep copy via the binding). */
        [[nodiscard]] static storage_type make_storage(const ValueView &map)
        {
            if (!map.valid()) { return storage_type{}; }
            return storage_type::owning_copy(*map.binding(), map.data());
        }
        [[nodiscard]] static storage_type make_storage(const std::map<std::size_t, TValue> &entries)
        {
            return make_storage(make_list_delta_value<TValue>(entries).view());
        }

        storage_type storage_{};
    };

    /**
     * Build an owning :cpp:class:`ListDelta` from a sparse index -> value map, e.g.
     * ``list_delta<int>({{0, 10}, {2, 30}})`` (index 1 did not tick).
     */
    template <typename TValue>
    [[nodiscard]] inline ListDelta<TValue> list_delta(std::initializer_list<std::pair<std::size_t, TValue>> entries)
    {
        std::map<std::size_t, TValue> map;
        for (const auto &entry : entries) { map.insert_or_assign(entry.first, entry.second); }
        return ListDelta<TValue>{map};
    }

    /**
     * Build an owning :cpp:class:`ListDelta` from a positional sequence: position is
     * the child index and ``std::nullopt`` is "no tick at that index", e.g.
     * ``list_delta<int>({10, none, 30})``. (A no-tick *cycle* is expressed by
     * ``none`` at that cycle, not by an empty ``list_delta``.)
     */
    template <typename TValue>
    [[nodiscard]] inline ListDelta<TValue> list_delta(std::vector<std::optional<TValue>> positional)
    {
        std::map<std::size_t, TValue> map;
        for (std::size_t i = 0; i < positional.size(); ++i)
        {
            if (positional[i].has_value()) { map.emplace(i, *positional[i]); }
        }
        return ListDelta<TValue>{map};
    }

    // -----------------------------------------------------------------
    // Selector markers
    // -----------------------------------------------------------------

    /** Typed input selector. Only the scalar ``TS<T>`` form is defined so far. */
    template <fixed_string Name, typename TSchema>
    class In;

    template <fixed_string Name, typename TValue>
    class In<Name, TS<TValue>>
    {
      public:
        using schema                    = TS<TValue>;
        using value_type                = TValue;
        static constexpr auto field_name = Name;

        explicit In(TSInputView view) noexcept : view_(std::move(view)) {}

        /** Current value of the input. */
        [[nodiscard]] value_type value() const { return view_.value().template checked_as<TValue>(); }

        [[nodiscard]] bool modified() const { return view_.modified(); }
        [[nodiscard]] bool valid() const { return view_.valid(); }

        [[nodiscard]] const TSInputView &view() const noexcept { return view_; }

      private:
        TSInputView view_;
    };

    /**
     * Set time-series input (``TSS<T>``). Exposes the current set contents and this
     * cycle's added / removed elements (typed), plus membership and size.
     */
    template <fixed_string Name, typename TValue>
    class In<Name, TSS<TValue>>
    {
      public:
        using schema                     = TSS<TValue>;
        using value_type                 = TValue;  // the set's element type
        static constexpr auto field_name = Name;

        explicit In(TSInputView view) noexcept : view_(std::move(view)) {}

        [[nodiscard]] std::size_t size() const { return view_.as_set().size(); }
        [[nodiscard]] bool        empty() const { return view_.as_set().empty(); }
        [[nodiscard]] bool        contains(const TValue &key) const { return view_.as_set().contains(Value{key}.view()); }

        /** Current set contents / this cycle's added / removed elements, as typed vectors. */
        [[nodiscard]] std::vector<TValue> values() const { return collect(view_.as_set().values()); }
        [[nodiscard]] std::vector<TValue> added() const { return collect(view_.as_set().added()); }
        [[nodiscard]] std::vector<TValue> removed() const { return collect(view_.as_set().removed()); }

        /** This cycle's delta as an owned ``SetDelta`` snapshot. */
        [[nodiscard]] SetDelta<TValue> delta() const { return SetDelta<TValue>{view_.delta_value()}; }

        [[nodiscard]] bool modified() const { return view_.modified(); }
        [[nodiscard]] bool valid() const { return view_.valid(); }

        [[nodiscard]] TSSInputView       set_view() const { return view_.as_set(); }
        [[nodiscard]] const TSInputView &view() const noexcept { return view_; }

      private:
        template <typename RangeT>
        static std::vector<TValue> collect(RangeT range)
        {
            std::vector<TValue> out;
            for (const auto &v : range) { out.push_back(v.template checked_as<TValue>()); }
            return out;
        }

        TSInputView view_;
    };

    /**
     * List time-series input (``TSL<TS<T>, N>``). Exposes the fixed-size collection
     * of child time-series: their current values (by index or all-at-once), and this
     * cycle's delta (the children that ticked) as a :cpp:class:`ListDelta`. Only
     * scalar children (``TS<T>``) are supported in this slice.
     */
    template <fixed_string Name, typename TValue, std::size_t N>
    class In<Name, TSL<TS<TValue>, N>>
    {
      public:
        using schema                        = TSL<TS<TValue>, N>;
        using value_type                    = TValue;  // the child element type
        static constexpr auto        field_name = Name;
        static constexpr std::size_t fixed_size = N;

        explicit In(TSInputView view) noexcept : view_(std::move(view)) {}

        [[nodiscard]] std::size_t size() const
        {
            auto list = view_.as_list();
            return list.size();
        }

        /** Current value of child ``index``. */
        [[nodiscard]] TValue at(std::size_t index) const
        {
            auto list = view_.as_list();
            return list.at(index).value().template checked_as<TValue>();
        }

        /** All children's current values; ``std::nullopt`` where a child is not valid. */
        [[nodiscard]] std::vector<std::optional<TValue>> values() const
        {
            std::vector<std::optional<TValue>> out;
            auto                               list = view_.as_list();
            for (std::size_t i = 0; i < list.size(); ++i)
            {
                auto child = list.at(i);
                if (child.valid()) { out.push_back(child.value().template checked_as<TValue>()); }
                else { out.push_back(std::nullopt); }
            }
            return out;
        }

        /** This cycle's delta as an owned ``ListDelta`` (modified child index -> value). */
        [[nodiscard]] ListDelta<TValue> delta() const
        {
            std::map<std::size_t, TValue> entries;
            auto                          list = view_.as_list();
            for (const auto &[index, child] : list.modified_items())
            {
                entries.emplace(index, child.value().template checked_as<TValue>());
            }
            return ListDelta<TValue>{entries};
        }

        [[nodiscard]] bool modified() const { return view_.modified(); }
        [[nodiscard]] bool valid() const { return view_.valid(); }

        [[nodiscard]] TSLInputView      list_view() const { return view_.as_list(); }
        [[nodiscard]] const TSInputView &view() const noexcept { return view_; }

      private:
        TSInputView view_;
    };

    /** Typed output selector. The scalar ``TS<T>`` and set ``TSS<T>`` forms are defined. */
    template <typename TSchema>
    class Out;

    template <typename TValue>
    class Out<TS<TValue>>
    {
      public:
        using schema     = TS<TValue>;
        using value_type = TValue;

        Out(TSOutputView view, engine_time_t evaluation_time) noexcept
            : view_(std::move(view)), evaluation_time_(evaluation_time)
        {
        }

        /** Write ``value`` into the output and tick it at the current evaluation time. */
        template <typename U>
        void set(U &&value) const
        {
            Value wrapped{std::forward<U>(value)};
            auto  mutation = view_.begin_mutation(evaluation_time_);
            if (!mutation.copy_value_from(wrapped.view()))
            {
                throw std::logic_error("Out<TS<T>>::set failed to copy the value into the output");
            }
        }

        /**
         * Type-erased set: copy a value from ``value`` (a ``ValueView`` whose schema
         * matches the output) into the output and tick it. Lets value-layer code
         * drive the output without converting through the concrete ``T`` — the basis
         * for generalising tooling beyond scalar values.
         */
        void apply(const ValueView &value) const
        {
            auto mutation = view_.begin_mutation(evaluation_time_);
            if (!mutation.copy_value_from(value))
            {
                throw std::logic_error("Out<TS<T>>::apply failed to copy the value into the output");
            }
        }

        [[nodiscard]] bool modified() const { return view_.modified(); }
        [[nodiscard]] bool valid() const { return view_.valid(); }
        [[nodiscard]] const TSOutputView &view() const noexcept { return view_; }
        [[nodiscard]] engine_time_t evaluation_time() const noexcept { return evaluation_time_; }

      private:
        TSOutputView  view_;
        engine_time_t evaluation_time_{MIN_DT};
    };

    /**
     * Set time-series output (``TSS<T>``). Mutate the set with add / remove /
     * clear; the delta (added / removed) accumulates across calls within the cycle.
     */
    template <typename TValue>
    class Out<TSS<TValue>>
    {
      public:
        using schema     = TSS<TValue>;
        using value_type = TValue;

        Out(TSOutputView view, engine_time_t evaluation_time) noexcept
            : view_(std::move(view)), evaluation_time_(evaluation_time)
        {
        }

        /** Add ``key`` to the set; returns whether the set delta changed. */
        bool add(const TValue &key) const
        {
            auto mutation = view_.as_set().begin_mutation(evaluation_time_);
            return mutation.add(Value{key}.view());
        }
        /** Remove ``key`` from the set; returns whether the set delta changed. */
        bool remove(const TValue &key) const
        {
            auto mutation = view_.as_set().begin_mutation(evaluation_time_);
            return mutation.remove(Value{key}.view());
        }
        /** Remove all elements. */
        void clear() const { view_.as_set().begin_mutation(evaluation_time_).clear(); }

        [[nodiscard]] bool modified() const { return view_.modified(); }
        [[nodiscard]] bool valid() const { return view_.valid(); }
        [[nodiscard]] const TSOutputView &view() const noexcept { return view_; }
        [[nodiscard]] engine_time_t       evaluation_time() const noexcept { return evaluation_time_; }

      private:
        TSOutputView  view_;
        engine_time_t evaluation_time_{MIN_DT};
    };

    /**
     * List time-series output (``TSL<TS<T>, N>``). Tick a child time-series by index,
     * either flat (``out.set(i, v)``) or through a per-child sub-selector
     * (``out[i].set(v)`` / ``out[i].apply(view)``, which reuses the scalar
     * ``Out<TS<T>>`` surface). Only scalar children (``TS<T>``) are supported here.
     */
    template <typename TValue, std::size_t N>
    class Out<TSL<TS<TValue>, N>>
    {
      public:
        using schema                        = TSL<TS<TValue>, N>;
        using value_type                    = TValue;
        static constexpr std::size_t fixed_size = N;

        Out(TSOutputView view, engine_time_t evaluation_time) noexcept
            : view_(std::move(view)), evaluation_time_(evaluation_time)
        {
        }

        [[nodiscard]] std::size_t size() const
        {
            auto list = view_.as_list();
            return list.size();
        }

        /** Per-child output selector: ``out[i].set(v)`` ticks child ``i``. */
        [[nodiscard]] Out<TS<TValue>> operator[](std::size_t index) const
        {
            auto list = view_.as_list();
            return Out<TS<TValue>>{list.at(index), evaluation_time_};
        }

        /** Set child ``index`` to ``value`` and tick it (thin wrapper over ``operator[]``). */
        template <typename U>
        void set(std::size_t index, U &&value) const
        {
            (*this)[index].set(std::forward<U>(value));
        }

        /** Type-erased per-child set (see ``Out<TS<T>>::apply``). */
        void apply(std::size_t index, const ValueView &value) const { (*this)[index].apply(value); }

        [[nodiscard]] bool modified() const { return view_.modified(); }
        [[nodiscard]] bool valid() const { return view_.valid(); }
        [[nodiscard]] const TSOutputView &view() const noexcept { return view_; }
        [[nodiscard]] engine_time_t       evaluation_time() const noexcept { return evaluation_time_; }

      private:
        TSOutputView  view_;
        engine_time_t evaluation_time_{MIN_DT};
    };

    /** Typed handle into node-local (value-layer) state. One state slot per node. */
    template <typename TValue>
    class State
    {
      public:
        using value_type = TValue;

        explicit State(ValueView view) noexcept : view_(std::move(view)) {}

        [[nodiscard]] value_type get() const { return view_.template checked_as<TValue>(); }

        template <typename U>
        void set(U &&value)
        {
            auto mutation = view_.begin_mutation();
            mutation.set_scalar(std::forward<U>(value));
        }

        [[nodiscard]] ValueView       &view() noexcept { return view_; }
        [[nodiscard]] const ValueView &view() const noexcept { return view_; }

      private:
        ValueView view_;
    };

    /**
     * Typed handle to one of a graph or node's scalar inputs (wiring-time
     * configuration). Read-only: scalars are fixed at wiring time and do not change
     * during evaluation. The same marker is used in a node's ``eval`` (where the
     * value is read from the node's compound scalar configuration) and in a graph's
     * ``compose`` (where the value is supplied by the caller); it carries its value
     * directly so both paths are uniform.
     */
    template <fixed_string Name, typename TValue>
    class Scalar
    {
      public:
        using value_type                 = TValue;
        static constexpr auto field_name = Name;

        /** Supplied directly (graph ``compose`` parameter / wiring-time value). */
        explicit Scalar(TValue value) : value_(std::move(value)) {}

        /** Read from the node's compound scalar configuration (node ``eval`` path). */
        explicit Scalar(const ValueView &view) : value_(view.template checked_as<TValue>()) {}

        /** The configured value of this scalar input. */
        [[nodiscard]] const value_type &value() const noexcept { return value_; }

      private:
        TValue value_;
    };

    // The ``GlobalStateView`` injectable selector is the runtime view type from
    // ``<hgraph/runtime/global_state.h>`` (a node declares a ``GlobalStateView``
    // parameter to read/write the graph's shared state). It is *not* part of the
    // node's data contract: it adds no input field, scalar, or state, and does
    // not affect node-kind inference. Its ``arg_provider`` is below.

    // -----------------------------------------------------------------
    // Compile-time machinery
    // -----------------------------------------------------------------

    namespace static_node_detail
    {
        template <typename T>
        inline constexpr bool always_false_v = false;

        template <typename T>
        using selector_of = std::remove_cv_t<std::remove_reference_t<T>>;

        // ---- selector detection ----
        template <typename T> struct is_input_selector : std::false_type {};
        template <fixed_string N, typename S> struct is_input_selector<In<N, S>> : std::true_type {};

        template <typename T> struct is_output_selector : std::false_type {};
        template <typename S> struct is_output_selector<Out<S>> : std::true_type {};

        template <typename T> struct is_state_selector : std::false_type {};
        template <typename V> struct is_state_selector<State<V>> : std::true_type {};

        template <typename T> struct is_scalar_selector : std::false_type {};
        template <fixed_string N, typename V> struct is_scalar_selector<Scalar<N, V>> : std::true_type {};

        // The scheduler is an injectable, not part of the data contract; it only
        // flips the node's ``uses_scheduler`` flag so a per-node scheduler-state
        // slot is allocated.
        template <typename T> struct is_scheduler_selector : std::false_type {};
        template <> struct is_scheduler_selector<NodeScheduler> : std::true_type {};

        // ---- per-selector runtime metadata ----
        template <typename T> struct input_selector_traits;
        template <fixed_string N, typename S>
        struct input_selector_traits<In<N, S>>
        {
            static std::string                name() { return std::string{N.sv()}; }
            static const TSValueTypeMetaData *ts_meta() { return schema_descriptor<S>::ts_meta(); }
        };

        template <typename T> struct output_selector_traits;
        template <typename S>
        struct output_selector_traits<Out<S>>
        {
            static const TSValueTypeMetaData *ts_meta() { return schema_descriptor<S>::ts_meta(); }
        };

        template <typename T> struct state_selector_traits;
        template <typename V>
        struct state_selector_traits<State<V>>
        {
            static const ValueTypeMetaData *value_meta() { return scalar_descriptor<V>::value_meta(); }
        };

        template <typename T> struct scalar_selector_traits;
        template <fixed_string N, typename V>
        struct scalar_selector_traits<Scalar<N, V>>
        {
            static std::string              name() { return std::string{N.sv()}; }
            static const ValueTypeMetaData *value_meta() { return scalar_descriptor<V>::value_meta(); }
        };

        // ---- compile-time output schema type extraction ----
        // output_schema_of<E> is void for any non-output selector and S for Out<S>,
        // so picking the single non-void result yields the node's output schema type.
        template <typename T> struct output_schema_of { using type = void; };
        template <typename S> struct output_schema_of<Out<S>> { using type = S; };

        template <typename A, typename B> struct pick_non_void { using type = A; };
        template <typename B> struct pick_non_void<void, B> { using type = B; };

        template <typename... Es> struct output_type_of_pack { using type = void; };
        template <typename E, typename... Rest>
        struct output_type_of_pack<E, Rest...>
        {
            using type = typename pick_non_void<typename output_schema_of<E>::type,
                                                typename output_type_of_pack<Rest...>::type>::type;
        };

        template <typename Tuple> struct output_type_of_tuple;
        template <typename... Es>
        struct output_type_of_tuple<std::tuple<Es...>>
        {
            using type = typename output_type_of_pack<selector_of<Es>...>::type;
        };

        // ---- compile-time list of the input schema types (In args' ``S``, in order) ----
        // in_schema_tuple<E> is the empty tuple for non-input selectors and tuple<S>
        // for In<Name, S>, so tuple_cat over the args yields the input schema list.
        template <typename E> struct in_schema_tuple { using type = std::tuple<>; };
        template <fixed_string N, typename S> struct in_schema_tuple<In<N, S>> { using type = std::tuple<S>; };

        template <typename Tuple> struct input_schemas_of_tuple;
        template <typename... Es>
        struct input_schemas_of_tuple<std::tuple<Es...>>
        {
            using type = decltype(std::tuple_cat(std::declval<typename in_schema_tuple<selector_of<Es>>::type>()...));
        };

        // ---- compile-time list of the "wireable" parameters, in eval order ----
        // These are the parameters supplied at wiring time: the time-series inputs
        // (In) and the scalar inputs (Scalar). State / clock / scheduler parameters
        // are injected by the runtime and never appear at a wiring call site.
        template <typename E> struct wire_param_tuple { using type = std::tuple<>; };
        template <fixed_string N, typename S> struct wire_param_tuple<In<N, S>> { using type = std::tuple<In<N, S>>; };
        template <fixed_string N, typename V> struct wire_param_tuple<Scalar<N, V>> { using type = std::tuple<Scalar<N, V>>; };

        template <typename Tuple> struct wire_params_of_tuple;
        template <typename... Es>
        struct wire_params_of_tuple<std::tuple<Es...>>
        {
            using type = decltype(std::tuple_cat(std::declval<typename wire_param_tuple<selector_of<Es>>::type>()...));
        };

        // ---- function-pointer traits over a static hook ----
        template <typename F> struct fn_traits;
        template <typename R, typename... A>
        struct fn_traits<R (*)(A...)>
        {
            using return_type = R;
            using args_tuple  = std::tuple<A...>;
        };

        // ---- arg providers: build a selector from NodeView + evaluation time ----
        template <typename T>
        struct arg_provider
        {
            static_assert(always_false_v<T>, "Unsupported static node hook parameter type");
        };

        template <fixed_string N, typename V>
        struct arg_provider<In<N, V>>
        {
            static In<N, V> get(const NodeView &view, engine_time_t evaluation_time)
            {
                TSInputView root   = view.input(evaluation_time);
                auto        bundle = root.as_bundle();
                return In<N, V>{bundle.field(N.sv())};
            }
        };

        template <typename V>
        struct arg_provider<Out<V>>
        {
            static Out<V> get(const NodeView &view, engine_time_t evaluation_time)
            {
                return Out<V>{view.output(evaluation_time), evaluation_time};
            }
        };

        template <typename V>
        struct arg_provider<State<V>>
        {
            static State<V> get(const NodeView &view, engine_time_t) { return State<V>{view.state()}; }
        };

        template <fixed_string N, typename V>
        struct arg_provider<Scalar<N, V>>
        {
            static Scalar<N, V> get(const NodeView &view, engine_time_t)
            {
                return Scalar<N, V>{view.scalars().as_bundle().field(N.sv())};
            }
        };

        template <>
        struct arg_provider<GlobalStateView>
        {
            static GlobalStateView get(const NodeView &view, engine_time_t)
            {
                return view.graph().root().global_state();
            }
        };

        // Transparent, stateless injectable: no signature footprint, no scheduler
        // component (unlike NodeScheduler it is not an ``is_scheduler_selector``).
        template <>
        struct arg_provider<SingleShotScheduler>
        {
            static SingleShotScheduler get(const NodeView &view, engine_time_t evaluation_time)
            {
                return SingleShotScheduler{view.graph_value(), view.node_index(), evaluation_time};
            }
        };

        template <>
        struct arg_provider<NodeScheduler>
        {
            static NodeScheduler get(const NodeView &view, engine_time_t evaluation_time)
            {
                // ``started()`` is false while the node's ``start`` hook runs, which
                // is what lets a source schedule its first evaluation at the start
                // time (schedule(now())); during ``eval`` it is true (future only).
                return NodeScheduler{view.scheduler_state(), view.graph_value(), view.node_index(), evaluation_time,
                                     view.started()};
            }
        };

        template <>
        struct arg_provider<engine_time_t>
        {
            static engine_time_t get(const NodeView &, engine_time_t evaluation_time) noexcept
            {
                return evaluation_time;
            }
        };

        // ---- invoke a static hook by injecting each parameter by type ----
        template <auto Fn, std::size_t... I>
        void invoke_impl(const NodeView &view, engine_time_t evaluation_time, std::index_sequence<I...>)
        {
            using args = typename fn_traits<decltype(Fn)>::args_tuple;
            Fn(arg_provider<selector_of<std::tuple_element_t<I, args>>>::get(view, evaluation_time)...);
        }

        template <auto Fn>
        void invoke(const NodeView &view, engine_time_t evaluation_time)
        {
            using traits = fn_traits<decltype(Fn)>;
            static_assert(std::is_same_v<typename traits::return_type, void>,
                          "Static node hooks must return void");
            invoke_impl<Fn>(view, evaluation_time,
                            std::make_index_sequence<std::tuple_size_v<typename traits::args_tuple>>{});
        }

        // ---- hook / trait detection ----
        template <typename T> concept has_start     = requires { &T::start; };
        template <typename T> concept has_stop      = requires { &T::stop; };
        template <typename T> concept has_name      = requires { T::name; };
        // Optional ``static constexpr bool schedule_on_start`` attribute: when true
        // the framework schedules the node for the start cycle (see node.cpp).
        template <typename T> concept has_schedule_on_start = requires { T::schedule_on_start; };
    }  // namespace static_node_detail

    // -----------------------------------------------------------------
    // Static node signature
    // -----------------------------------------------------------------

    /**
     * Compile-time reflection of a static node implementation. Derives the
     * runtime node contract (input/output/state schema, node kind, input
     * endpoint annotation) from the parameter list of ``TImplementation::eval``.
     */
    template <typename TImplementation>
    struct StaticNodeSignature
    {
      private:
        using eval_args = typename static_node_detail::fn_traits<decltype(&TImplementation::eval)>::args_tuple;

        static constexpr std::size_t arg_count = std::tuple_size_v<eval_args>;
        using indices                          = std::make_index_sequence<arg_count>;

        template <std::size_t... I>
        static void collect_inputs(std::vector<std::pair<std::string, const TSValueTypeMetaData *>> &fields,
                                   std::index_sequence<I...>)
        {
            (
                [&] {
                    using E = static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>;
                    if constexpr (static_node_detail::is_input_selector<E>::value)
                    {
                        fields.emplace_back(static_node_detail::input_selector_traits<E>::name(),
                                            static_node_detail::input_selector_traits<E>::ts_meta());
                    }
                }(),
                ...);
        }

        template <std::size_t... I>
        static const TSValueTypeMetaData *find_output(std::index_sequence<I...>)
        {
            const TSValueTypeMetaData *out = nullptr;
            (
                [&] {
                    using E = static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>;
                    if constexpr (static_node_detail::is_output_selector<E>::value)
                    {
                        out = static_node_detail::output_selector_traits<E>::ts_meta();
                    }
                }(),
                ...);
            return out;
        }

        template <std::size_t... I>
        static const ValueTypeMetaData *find_state(std::index_sequence<I...>)
        {
            const ValueTypeMetaData *state = nullptr;
            (
                [&] {
                    using E = static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>;
                    if constexpr (static_node_detail::is_state_selector<E>::value)
                    {
                        state = static_node_detail::state_selector_traits<E>::value_meta();
                    }
                }(),
                ...);
            return state;
        }

        template <std::size_t... I>
        static void collect_scalars(std::vector<std::pair<std::string, const ValueTypeMetaData *>> &fields,
                                    std::index_sequence<I...>)
        {
            (
                [&] {
                    using E = static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>;
                    if constexpr (static_node_detail::is_scalar_selector<E>::value)
                    {
                        fields.emplace_back(static_node_detail::scalar_selector_traits<E>::name(),
                                            static_node_detail::scalar_selector_traits<E>::value_meta());
                    }
                }(),
                ...);
        }

        template <std::size_t... I>
        static constexpr std::size_t count_inputs(std::index_sequence<I...>)
        {
            return (std::size_t{0} + ... +
                    (static_node_detail::is_input_selector<
                         static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>>::value
                         ? std::size_t{1}
                         : std::size_t{0}));
        }

        template <std::size_t... I>
        static constexpr std::size_t count_outputs(std::index_sequence<I...>)
        {
            return (std::size_t{0} + ... +
                    (static_node_detail::is_output_selector<
                         static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>>::value
                         ? std::size_t{1}
                         : std::size_t{0}));
        }

        template <std::size_t... I>
        static constexpr std::size_t count_scalars(std::index_sequence<I...>)
        {
            return (std::size_t{0} + ... +
                    (static_node_detail::is_scalar_selector<
                         static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>>::value
                         ? std::size_t{1}
                         : std::size_t{0}));
        }

        template <typename ArgsTuple, std::size_t... I>
        static constexpr bool tuple_has_scheduler(std::index_sequence<I...>)
        {
            return (false || ... ||
                    static_node_detail::is_scheduler_selector<
                        static_node_detail::selector_of<std::tuple_element_t<I, ArgsTuple>>>::value);
        }

        template <typename ArgsTuple>
        static constexpr bool args_have_scheduler()
        {
            return tuple_has_scheduler<ArgsTuple>(std::make_index_sequence<std::tuple_size_v<ArgsTuple>>{});
        }

      public:
        /** The static output schema type (the ``Out<S>``'s ``S``), or ``void`` if no output. */
        using output_schema_type = typename static_node_detail::output_type_of_tuple<eval_args>::type;

        /** Tuple of the input schema *types* (each ``In<Name, S>``'s ``S``), in argument order. */
        using input_schema_types = typename static_node_detail::input_schemas_of_tuple<eval_args>::type;

        /** Tuple of the wiring-time parameter selector types (``In`` and ``Scalar``), in eval order. */
        using wire_param_types = typename static_node_detail::wire_params_of_tuple<eval_args>::type;

        [[nodiscard]] static constexpr std::size_t input_count() { return count_inputs(indices{}); }
        [[nodiscard]] static constexpr std::size_t output_count() { return count_outputs(indices{}); }
        [[nodiscard]] static constexpr std::size_t scalar_count() { return count_scalars(indices{}); }
        [[nodiscard]] static constexpr bool        has_output() { return output_count() > 0; }
        /** Whether the node declares ``static constexpr bool schedule_on_start = true``. */
        [[nodiscard]] static constexpr bool schedule_on_start()
        {
            if constexpr (static_node_detail::has_schedule_on_start<TImplementation>)
            {
                return TImplementation::schedule_on_start;
            }
            else
            {
                return false;
            }
        }
        /**
         * Whether any hook (``eval`` / ``start`` / ``stop``) injects a
         * ``NodeScheduler`` — so a scheduler-state slot is allocated. A source
         * that only schedules itself in ``start`` still needs the slot.
         */
        [[nodiscard]] static constexpr bool uses_scheduler()
        {
            bool result = args_have_scheduler<eval_args>();
            if constexpr (static_node_detail::has_start<TImplementation>)
            {
                result = result || args_have_scheduler<
                                       typename static_node_detail::fn_traits<decltype(&TImplementation::start)>::args_tuple>();
            }
            if constexpr (static_node_detail::has_stop<TImplementation>)
            {
                result = result || args_have_scheduler<
                                       typename static_node_detail::fn_traits<decltype(&TImplementation::stop)>::args_tuple>();
            }
            return result;
        }

        [[nodiscard]] static const TSValueTypeMetaData *input_schema()
        {
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            collect_inputs(fields, indices{});
            if (fields.empty()) { return nullptr; }
            return TypeRegistry::instance().un_named_tsb(fields);
        }

        [[nodiscard]] static const TSValueTypeMetaData *output_schema() { return find_output(indices{}); }
        [[nodiscard]] static const ValueTypeMetaData   *state_schema() { return find_state(indices{}); }

        /**
         * The node's scalar-configuration schema: a compound (un-named bundle)
         * value type with one field per ``Scalar<Name, T>`` argument, or
         * ``nullptr`` when the node takes no scalar inputs. Scalars are *not*
         * time-series inputs and never appear in the input TSB.
         */
        [[nodiscard]] static const ValueTypeMetaData *scalar_schema()
        {
            std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields;
            collect_scalars(fields, indices{});
            if (fields.empty()) { return nullptr; }
            return TypeRegistry::instance().un_named_bundle(fields);
        }

        [[nodiscard]] static NodeKind node_kind()
        {
            // The kind is always determined from the node's shape; there is no
            // override. A push source is distinguished by an apply_message hook
            // (not yet implemented); everything else is classified by which of
            // In / Out are present.
            const bool has_in  = input_count() > 0;
            const bool has_out = output_count() > 0;
            if (has_in && has_out) { return NodeKind::Compute; }
            if (has_out) { return NodeKind::PullSource; }
            if (has_in) { return NodeKind::Sink; }
            return NodeKind::Compute;
        }

        /** Input endpoint annotation: a non-peered TSB of peered terminals. */
        [[nodiscard]] static TSEndpointSchema input_endpoint()
        {
            const TSValueTypeMetaData *tsb = input_schema();
            if (tsb == nullptr) { return TSEndpointSchema{}; }
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            collect_inputs(fields, indices{});
            std::vector<TSEndpointSchema> children;
            children.reserve(fields.size());
            for (const auto &field : fields) { children.push_back(TSEndpointSchema::peered(field.second)); }
            return TSEndpointSchema::non_peered(tsb, std::move(children));
        }
    };

    // -----------------------------------------------------------------
    // NodeBuilder front-end
    // -----------------------------------------------------------------

    template <typename TImplementation>
    NodeBuilder &NodeBuilder::implementation()
    {
        static_assert(std::is_class_v<TImplementation>, "Static node implementations must be class/struct types");
        static_assert(std::is_empty_v<TImplementation>, "Static node implementations must be stateless");

        using signature = StaticNodeSignature<TImplementation>;
        static_assert(signature::output_count() <= 1, "Static nodes support at most one Out<...> parameter");

        NodeTypeMetaData schema;
        if constexpr (static_node_detail::has_name<TImplementation>) { schema.display_name = TImplementation::name; }
        schema.input_schema    = signature::input_schema();
        schema.output_schema   = signature::output_schema();
        schema.state_schema    = signature::state_schema();
        schema.scalar_schema   = signature::scalar_schema();
        schema.node_kind         = signature::node_kind();
        schema.uses_scheduler    = signature::uses_scheduler();
        schema.schedule_on_start = signature::schedule_on_start();

        NodeCallbacks callbacks;
        callbacks.evaluate = [](const NodeView &view, engine_time_t evaluation_time) {
            static_node_detail::invoke<&TImplementation::eval>(view, evaluation_time);
        };
        if constexpr (static_node_detail::has_start<TImplementation>)
        {
            callbacks.start = [](const NodeView &view, engine_time_t evaluation_time) {
                static_node_detail::invoke<&TImplementation::start>(view, evaluation_time);
            };
        }
        if constexpr (static_node_detail::has_stop<TImplementation>)
        {
            callbacks.stop = [](const NodeView &view, engine_time_t evaluation_time) {
                static_node_detail::invoke<&TImplementation::stop>(view, evaluation_time);
            };
        }

        std::string saved_label{label_};
        Value       saved_scalars{std::move(scalars_)};
        *this = NodeBuilder::native(std::move(schema), std::move(callbacks), signature::input_endpoint());
        if (!saved_label.empty()) { label(std::move(saved_label)); }
        if (saved_scalars.has_value()) { scalars(std::move(saved_scalars)); }
        return *this;
    }

}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_STATIC_NODE_H

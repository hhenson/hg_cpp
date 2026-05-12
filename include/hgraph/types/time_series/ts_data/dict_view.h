#ifndef HGRAPH_CPP_TS_DATA_DICT_VIEW_H
#define HGRAPH_CPP_TS_DATA_DICT_VIEW_H

#include <hgraph/types/time_series/ts_data/set_view.h>
#include <hgraph/types/value/value_range.h>
#include <hgraph/util/date_time.h>
#include <cstddef>
#include <utility>

namespace hgraph
{
    /** Read view over dictionary-shaped TSData. */
    class TSDDataView
    {
      public:
        explicit TSDDataView(TSDataView view);

        /** Underlying generic TSData view. */
        [[nodiscard]] const TSDataView &base() const noexcept;
        [[nodiscard]] TSDataView &base() noexcept;

        /** Binding, schema, layout, and value projections for the dictionary node. */
        [[nodiscard]] const TSDataBinding *binding() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;
        [[nodiscard]] const TSDDataLayout &layout() const;
        [[nodiscard]] ValueView value() const;
        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const;
        [[nodiscard]] engine_time_t last_modified_time() const;
        [[nodiscard]] bool modified(engine_time_t evaluation_time) const;
        void subscribe(Notifiable *observer) const;
        void unsubscribe(Notifiable *observer) const;
        [[nodiscard]] bool has_observers() const;
        [[nodiscard]] std::size_t observer_count() const;

        /** Number of live key/value entries. */
        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;

        /** Allocated slot count and slot-level delta predicates. */
        [[nodiscard]] std::size_t slot_capacity() const;
        [[nodiscard]] bool slot_occupied(std::size_t slot) const;
        [[nodiscard]] bool slot_live(std::size_t slot) const;
        [[nodiscard]] bool slot_added(std::size_t slot) const;
        [[nodiscard]] bool slot_removed(std::size_t slot) const;
        [[nodiscard]] bool slot_modified(std::size_t slot) const;

        /** Key or child TSData view at ``slot``; throws if the slot is not occupied. */
        [[nodiscard]] ValueView key_at_slot(std::size_t slot) const;
        [[nodiscard]] TSDataView at_slot(std::size_t slot) const;

        /** Key lookup helpers. Missing keys return an empty child view from ``at``. */
        [[nodiscard]] bool contains(const ValueView &key) const;
        [[nodiscard]] std::size_t find_slot(const ValueView &key) const;
        [[nodiscard]] TSDataView at(const ValueView &key) const;
        [[nodiscard]] TSDataView operator[](const ValueView &key) const;

        /** Live key, value, and item ranges. */
        [[nodiscard]] Range<ValueView> keys() const;
        [[nodiscard]] Range<TSDataView> values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSDataView> items() const;

        /** Entries whose child value is currently valid. */
        [[nodiscard]] Range<ValueView> valid_keys() const;
        [[nodiscard]] Range<TSDataView> valid_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSDataView> valid_items() const;

        /** Entries modified at ``evaluation_time``. */
        [[nodiscard]] Range<ValueView> modified_keys(engine_time_t evaluation_time) const;
        [[nodiscard]] Range<TSDataView> modified_values(engine_time_t evaluation_time) const;
        [[nodiscard]] KeyValueRange<ValueView, TSDataView> modified_items(engine_time_t evaluation_time) const;

        /** Added and removed key/value delta ranges for the current delta surface. */
        [[nodiscard]] Range<ValueView> added_keys() const;
        [[nodiscard]] Range<TSDataView> added_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSDataView> added_items() const;
        [[nodiscard]] Range<ValueView> removed_keys() const;
        [[nodiscard]] Range<TSDataView> removed_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSDataView> removed_items() const;

        /** Set-shaped view over the dictionary keys. */
        [[nodiscard]] TSSDataView key_set() const;

        /** Begin a mutation view over this dictionary. */
        [[nodiscard]] TSDDataMutationView begin_mutation(engine_time_t evaluation_time) const;

      private:
        [[nodiscard]] static Range<ValueView> empty_value_range() noexcept;
        [[nodiscard]] static Range<TSDataView> empty_ts_data_range() noexcept;
        [[nodiscard]] static KeyValueRange<ValueView, TSDataView> empty_ts_data_kv_range() noexcept;
        [[nodiscard]] Range<TSDataView> ts_data_values_range(Range<TSDataView>::predicate_fn predicate) const;
        [[nodiscard]] KeyValueRange<ValueView, TSDataView> ts_data_items_range(
            KeyValueRange<ValueView, TSDataView>::predicate_fn predicate) const;
        [[nodiscard]] static bool slot_live_predicate(const void *context, const void *, std::size_t slot);
        [[nodiscard]] static bool slot_valid_predicate(const void *context, const void *, std::size_t slot);
        [[nodiscard]] static bool slot_modified_predicate(const void *context, const void *, std::size_t slot);
        [[nodiscard]] static bool slot_added_predicate(const void *context, const void *, std::size_t slot);
        [[nodiscard]] static bool slot_removed_predicate(const void *context, const void *, std::size_t slot);
        [[nodiscard]] static TSDataView project_ts_value_at_slot(const void *context, const void *, std::size_t slot);
        [[nodiscard]] static std::pair<ValueView, TSDataView> project_ts_item_at_slot(const void *context,
                                                                                      const void *,
                                                                                      std::size_t slot);
        [[nodiscard]] const TSDDataOps &dict_ops() const;
        static void validate_kind(const TSDataView &view);

        TSDataView view_{};
    };

    /** Mutation view over dictionary-shaped TSData. */
    class TSDDataMutationView : public TSDDataView
    {
      public:
        TSDDataMutationView(TSDataView view, engine_time_t evaluation_time);

        TSDDataMutationView(const TSDDataMutationView &) = delete;
        TSDDataMutationView &operator=(const TSDDataMutationView &) = delete;
        TSDDataMutationView(TSDDataMutationView &&) noexcept;
        TSDDataMutationView &operator=(TSDDataMutationView &&) = delete;
        ~TSDDataMutationView() noexcept;

        /** Read view for the same underlying dictionary. */
        [[nodiscard]] TSDDataView view();

        /** Engine time associated with this mutation scope. */
        [[nodiscard]] engine_time_t current_mutation_time() const;

        /** Reserve storage for at least ``capacity`` slots. */
        void reserve(std::size_t capacity);

        /** Access or create the child TSData for ``key``. */
        [[nodiscard]] TSDataView at(const ValueView &key);
        [[nodiscard]] TSDataView operator[](const ValueView &key);

        /** Set the child value associated with ``key``. */
        void set(const ValueView &key, const ValueView &value);

        /** Remove ``key`` if present, returning true when the dictionary delta changes. */
        [[nodiscard]] bool erase(const ValueView &key);

        /** Remove all currently live entries. */
        void clear();

        /** Replace the dictionary from a value-layer map view. */
        [[nodiscard]] bool copy_value_from(const ValueView &source);

      private:
        [[nodiscard]] TSDataView at_slot(std::size_t slot);

        TSDataMutationView mutation_;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_TS_DATA_DICT_VIEW_H

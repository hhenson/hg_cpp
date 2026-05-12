#ifndef HGRAPH_CPP_TS_DATA_INDEXED_VIEW_H
#define HGRAPH_CPP_TS_DATA_INDEXED_VIEW_H

#include <hgraph/types/time_series/ts_data/base_view.h>
#include <hgraph/types/value/value_range.h>
#include <hgraph/util/date_time.h>
#include <cstddef>
#include <string_view>
#include <utility>

namespace hgraph
{
    /**
     * Common view for time-series data with integer indexed children.
     *
     * Fixed TSL and fixed TSB both expose a stable integer navigation surface.
     * The concrete storage shape remains behind ``IndexedTSDataOps``.
     */
    class IndexedTSDataView
    {
      public:
        /** Underlying generic TSData view. */
        [[nodiscard]] const TSDataView &base() const noexcept;
        [[nodiscard]] TSDataView &base() noexcept;

        /** Binding and schema carried by the underlying TSData view. */
        [[nodiscard]] const TSDataBinding *binding() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;

        /** Common layout and value projections for this TSData node. */
        [[nodiscard]] const TSDataLayout &layout() const;
        [[nodiscard]] ValueView value() const;
        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const;
        [[nodiscard]] engine_time_t last_modified_time() const;
        [[nodiscard]] bool modified(engine_time_t evaluation_time) const;

        /** Number of indexed children exposed by this view. */
        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;

        /** Child TSData view at ``index``; throws when the index is out of range. */
        [[nodiscard]] TSDataView at(std::size_t index) &;
        [[nodiscard]] TSDataView at(std::size_t index) const &;
        TSDataView at(std::size_t) && = delete;
        [[nodiscard]] TSDataView operator[](std::size_t index) &;
        [[nodiscard]] TSDataView operator[](std::size_t index) const &;
        TSDataView operator[](std::size_t) && = delete;

        /** All indexed child views in natural index order. */
        [[nodiscard]] Range<TSDataView> values() const;

        /** Children with a current value. */
        [[nodiscard]] Range<TSDataView> valid_values() const;

        /** Children modified at the parent view's current modification time. */
        [[nodiscard]] Range<TSDataView> modified_values(engine_time_t evaluation_time) const;

        /** ``index -> child`` pairs in natural index order. */
        [[nodiscard]] KeyValueRange<std::size_t, TSDataView> items() const;
        [[nodiscard]] KeyValueRange<std::size_t, TSDataView> valid_items() const;
        [[nodiscard]] KeyValueRange<std::size_t, TSDataView> modified_items(engine_time_t evaluation_time) const;

      protected:
        IndexedTSDataView(TSDataView &view, TSTypeKind expected_kind, const char *what);

        /** True when the indexed child has ever received a value. */
        [[nodiscard]] bool child_valid(std::size_t index) const;

        /** True when the indexed child was changed with the parent. */
        [[nodiscard]] bool child_modified_at_parent_time(std::size_t index) const;

        [[nodiscard]] const IndexedTSDataOps &indexed_ops() const;
        [[nodiscard]] Range<TSDataView> values_range(Range<TSDataView>::predicate_fn predicate) const;
        [[nodiscard]] KeyValueRange<std::size_t, TSDataView> items_range(
            KeyValueRange<std::size_t, TSDataView>::predicate_fn predicate) const;

      private:
        static void validate_kind(const TSDataView &view, TSTypeKind expected_kind, const char *what);
        [[nodiscard]] engine_time_t child_last_modified_time(std::size_t index) const;
        [[nodiscard]] TSDataView at_impl(std::size_t index);
        [[nodiscard]] static Range<TSDataView> empty_values_range() noexcept;
        [[nodiscard]] static KeyValueRange<std::size_t, TSDataView> empty_items_range() noexcept;
        [[nodiscard]] static bool child_valid_predicate(const void *context, const void *, std::size_t index);
        [[nodiscard]] static bool child_modified_predicate(const void *context, const void *, std::size_t index);
        [[nodiscard]] static TSDataView project_value(const void *context, const void *, std::size_t index);
        [[nodiscard]] static std::pair<std::size_t, TSDataView> project_item(const void *context, const void *,
                                                                             std::size_t index);

        TSDataView *view_{nullptr};
    };

    /** Named fixed bundle view over indexed TSData children. */
    class TSBDataView : public IndexedTSDataView
    {
      public:
        using IndexedTSDataView::at;
        using IndexedTSDataView::operator[];

        explicit TSBDataView(TSDataView &view);

        /** Child field view by schema field name. */
        [[nodiscard]] TSDataView at(std::string_view name) &;
        [[nodiscard]] TSDataView at(std::string_view name) const &;
        TSDataView at(std::string_view) && = delete;
        [[nodiscard]] TSDataView field(std::string_view name) &;
        [[nodiscard]] TSDataView field(std::string_view name) const &;
        TSDataView field(std::string_view) && = delete;
        [[nodiscard]] TSDataView operator[](std::string_view name) &;
        [[nodiscard]] TSDataView operator[](std::string_view name) const &;
        TSDataView operator[](std::string_view) && = delete;

        /** True when the bundle schema contains ``name``. */
        [[nodiscard]] bool has_field(std::string_view name) const noexcept;

        /** Field names in schema order. */
        [[nodiscard]] Range<std::string_view> keys() const;

        /** Named child ranges in schema order. */
        [[nodiscard]] KeyValueRange<std::string_view, TSDataView> items() const;
        [[nodiscard]] KeyValueRange<std::string_view, TSDataView> valid_items() const;
        [[nodiscard]] KeyValueRange<std::string_view, TSDataView> modified_items(engine_time_t evaluation_time) const;

      private:
        static constexpr std::size_t npos = static_cast<std::size_t>(-1);

        [[nodiscard]] std::size_t field_index(std::string_view name) const;
        [[nodiscard]] std::size_t find_field_index(std::string_view name) const noexcept;
        [[nodiscard]] std::string_view key_at(std::size_t index) const noexcept;
        [[nodiscard]] KeyValueRange<std::string_view, TSDataView> named_items_range(
            KeyValueRange<std::string_view, TSDataView>::predicate_fn predicate) const;
        [[nodiscard]] static KeyValueRange<std::string_view, TSDataView> empty_named_items_range() noexcept;
        [[nodiscard]] static bool named_child_valid_predicate(const void *context, const void *, std::size_t index);
        [[nodiscard]] static bool named_child_modified_predicate(const void *context, const void *, std::size_t index);
        [[nodiscard]] static std::string_view project_key(const void *context, const void *, std::size_t index);
        [[nodiscard]] static std::pair<std::string_view, TSDataView> project_named_item(const void *context,
                                                                                        const void *,
                                                                                        std::size_t index);
    };

    /** Fixed-size list view over indexed TSData children. */
    class TSLDataView : public IndexedTSDataView
    {
      public:
        explicit TSLDataView(TSDataView &view);
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_TS_DATA_INDEXED_VIEW_H

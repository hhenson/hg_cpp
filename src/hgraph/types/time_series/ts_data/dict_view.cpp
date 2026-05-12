#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/value/specialized_views.h>

#include <stdexcept>
#include <vector>

namespace hgraph
{
    TSDDataView::TSDDataView(TSDataView view)
        : view_(view)
    {
        validate_kind(view_);
    }

    const TSDataView &TSDDataView::base() const noexcept
    {
        return view_;
    }

    TSDataView &TSDDataView::base() noexcept
    {
        return view_;
    }

    const TSDataBinding *TSDDataView::binding() const noexcept
    {
        return view_.binding();
    }

    const TSValueTypeMetaData *TSDDataView::schema() const noexcept
    {
        return view_.schema();
    }

    const TSDDataLayout &TSDDataView::layout() const
    {
        return static_cast<const TSDDataLayout &>(view_.layout());
    }

    ValueView TSDDataView::value() const
    {
        return view_.value();
    }

    ValueView TSDDataView::delta_value(engine_time_t evaluation_time) const
    {
        return view_.delta_value(evaluation_time);
    }

    engine_time_t TSDDataView::last_modified_time() const
    {
        return view_.last_modified_time();
    }

    bool TSDDataView::modified(engine_time_t evaluation_time) const
    {
        return view_.modified(evaluation_time);
    }

    std::size_t TSDDataView::size() const
    {
        const auto &ops = dict_ops();
        return ops.size_impl(ops.context, view_.data());
    }

    bool TSDDataView::empty() const
    {
        return size() == 0;
    }

    std::size_t TSDDataView::slot_capacity() const
    {
        const auto &ops = dict_ops();
        return ops.slot_capacity_impl(ops.context, view_.data());
    }

    bool TSDDataView::slot_occupied(std::size_t slot) const
    {
        const auto &ops = dict_ops();
        return ops.slot_occupied_impl(ops.context, view_.data(), slot);
    }

    bool TSDDataView::slot_live(std::size_t slot) const
    {
        const auto &ops = dict_ops();
        return ops.slot_live_impl(ops.context, view_.data(), slot);
    }

    bool TSDDataView::slot_added(std::size_t slot) const
    {
        const auto &ops = dict_ops();
        return ops.slot_added_impl(ops.context, view_.data(), slot);
    }

    bool TSDDataView::slot_removed(std::size_t slot) const
    {
        const auto &ops = dict_ops();
        return ops.slot_removed_impl(ops.context, view_.data(), slot);
    }

    bool TSDDataView::slot_modified(std::size_t slot) const
    {
        const auto &ops = dict_ops();
        return ops.slot_modified_impl(ops.context, view_.data(), slot);
    }

    ValueView TSDDataView::key_at_slot(std::size_t slot) const
    {
        if (!slot_occupied(slot)) { throw std::out_of_range("TSDDataView::key_at_slot: slot is not occupied"); }
        const auto &ops = dict_ops();
        return ValueView{layout().key_binding, ops.key_at_slot_impl(ops.context, view_.data(), slot)};
    }

    bool TSDDataView::contains(const ValueView &key) const
    {
        const auto &ops = dict_ops();
        return ops.contains_impl(ops.context, view_.data(), key);
    }

    std::size_t TSDDataView::find_slot(const ValueView &key) const
    {
        const auto &ops = dict_ops();
        return ops.find_slot_impl(ops.context, view_.data(), key);
    }

    TSDataView TSDDataView::at_slot(std::size_t slot) const
    {
        if (!slot_occupied(slot)) { throw std::out_of_range("TSDDataView::at_slot: slot is not occupied"); }
        const auto &ops = dict_ops();
        const auto *child_memory = ops.child_at_slot_impl(ops.context, view_.data(), slot);
        if (!view_.ops().allows_mutation) { return TSDataView{layout().element_binding, child_memory}; }
        return TSDataView{layout().element_binding, child_memory, const_cast<TSDataView &>(view_), slot};
    }

    TSDataView TSDDataView::at(const ValueView &key) const
    {
        const auto slot = find_slot(key);
        if (slot == TS_DATA_NO_CHILD_ID) { return TSDataView{layout().element_binding, static_cast<const void *>(nullptr)}; }
        return at_slot(slot);
    }

    TSDataView TSDDataView::operator[](const ValueView &key) const
    {
        return at(key);
    }

    Range<ValueView> TSDDataView::keys() const
    {
        const auto &ops = dict_ops();
        return ops.make_values_range_impl(ops.context, view_.data());
    }

    Range<TSDataView> TSDDataView::values() const
    {
        return ts_data_values_range(&slot_live_predicate);
    }

    KeyValueRange<ValueView, TSDataView> TSDDataView::items() const
    {
        return ts_data_items_range(&slot_live_predicate);
    }

    Range<ValueView> TSDDataView::valid_keys() const
    {
        const auto &ops = dict_ops();
        return ops.make_valid_keys_range_impl(ops.context, view_.data());
    }

    Range<TSDataView> TSDDataView::valid_values() const
    {
        return ts_data_values_range(&slot_valid_predicate);
    }

    KeyValueRange<ValueView, TSDataView> TSDDataView::valid_items() const
    {
        return ts_data_items_range(&slot_valid_predicate);
    }

    Range<ValueView> TSDDataView::modified_keys(engine_time_t evaluation_time) const
    {
        if (!modified(evaluation_time)) { return empty_value_range(); }
        const auto &ops = dict_ops();
        return ops.make_modified_keys_range_impl(ops.context, view_.data());
    }

    Range<TSDataView> TSDDataView::modified_values(engine_time_t evaluation_time) const
    {
        if (!modified(evaluation_time)) { return empty_ts_data_range(); }
        return ts_data_values_range(&slot_modified_predicate);
    }

    KeyValueRange<ValueView, TSDataView> TSDDataView::modified_items(engine_time_t evaluation_time) const
    {
        if (!modified(evaluation_time)) { return empty_ts_data_kv_range(); }
        return ts_data_items_range(&slot_modified_predicate);
    }

    Range<ValueView> TSDDataView::added_keys() const
    {
        return key_set().added();
    }

    Range<TSDataView> TSDDataView::added_values() const
    {
        return ts_data_values_range(&slot_added_predicate);
    }

    KeyValueRange<ValueView, TSDataView> TSDDataView::added_items() const
    {
        return ts_data_items_range(&slot_added_predicate);
    }

    Range<ValueView> TSDDataView::removed_keys() const
    {
        return key_set().removed();
    }

    Range<TSDataView> TSDDataView::removed_values() const
    {
        return ts_data_values_range(&slot_removed_predicate);
    }

    KeyValueRange<ValueView, TSDataView> TSDDataView::removed_items() const
    {
        return ts_data_items_range(&slot_removed_predicate);
    }

    TSSDataView TSDDataView::key_set() const
    {
        const auto *key_set_binding = layout().key_set_binding;
        if (key_set_binding == nullptr)
        {
            throw std::logic_error("TSDDataView::key_set requires a key-set binding");
        }
        return TSSDataView{TSDataView{key_set_binding, view_.data()}};
    }

    TSDDataMutationView TSDDataView::begin_mutation(engine_time_t evaluation_time) const
    {
        return TSDDataMutationView{view_, evaluation_time};
    }

    Range<ValueView> TSDDataView::empty_value_range() noexcept
    {
        return Range<ValueView>{.context = nullptr, .memory = nullptr, .limit = 0, .predicate = nullptr,
                                .projector = nullptr};
    }

    Range<TSDataView> TSDDataView::empty_ts_data_range() noexcept
    {
        return Range<TSDataView>{.context = nullptr, .memory = nullptr, .limit = 0, .predicate = nullptr,
                                 .projector = nullptr};
    }

    KeyValueRange<ValueView, TSDataView> TSDDataView::empty_ts_data_kv_range() noexcept
    {
        return KeyValueRange<ValueView, TSDataView>{.context = nullptr, .memory = nullptr, .limit = 0,
                                                    .predicate = nullptr, .projector = nullptr};
    }

    Range<TSDataView> TSDDataView::ts_data_values_range(Range<TSDataView>::predicate_fn predicate) const
    {
        return Range<TSDataView>{
            .context   = this,
            .memory    = nullptr,
            .limit     = slot_capacity(),
            .predicate = predicate,
            .projector = &project_ts_value_at_slot,
        };
    }

    KeyValueRange<ValueView, TSDataView> TSDDataView::ts_data_items_range(
        KeyValueRange<ValueView, TSDataView>::predicate_fn predicate) const
    {
        return KeyValueRange<ValueView, TSDataView>{
            .context   = this,
            .memory    = nullptr,
            .limit     = slot_capacity(),
            .predicate = predicate,
            .projector = &project_ts_item_at_slot,
        };
    }

    bool TSDDataView::slot_live_predicate(const void *context, const void *, std::size_t slot)
    {
        return static_cast<const TSDDataView *>(context)->slot_live(slot);
    }

    bool TSDDataView::slot_valid_predicate(const void *context, const void *, std::size_t slot)
    {
        const auto *self = static_cast<const TSDDataView *>(context);
        return self->slot_live(slot) && self->at_slot(slot).last_modified_time() != MIN_DT;
    }

    bool TSDDataView::slot_modified_predicate(const void *context, const void *, std::size_t slot)
    {
        const auto *self = static_cast<const TSDDataView *>(context);
        return self->slot_live(slot) && self->slot_modified(slot);
    }

    bool TSDDataView::slot_added_predicate(const void *context, const void *, std::size_t slot)
    {
        const auto *self = static_cast<const TSDDataView *>(context);
        return self->slot_occupied(slot) && self->slot_added(slot);
    }

    bool TSDDataView::slot_removed_predicate(const void *context, const void *, std::size_t slot)
    {
        const auto *self = static_cast<const TSDDataView *>(context);
        return self->slot_occupied(slot) && self->slot_removed(slot);
    }

    TSDataView TSDDataView::project_ts_value_at_slot(const void *context, const void *, std::size_t slot)
    {
        return static_cast<const TSDDataView *>(context)->at_slot(slot);
    }

    std::pair<ValueView, TSDataView> TSDDataView::project_ts_item_at_slot(const void *context, const void *,
                                                                          std::size_t slot)
    {
        const auto *self = static_cast<const TSDDataView *>(context);
        return {self->key_at_slot(slot), self->at_slot(slot)};
    }

    const TSDDataOps &TSDDataView::dict_ops() const
    {
        return static_cast<const TSDDataOps &>(view_.ops());
    }

    void TSDDataView::validate_kind(const TSDataView &view)
    {
        if (!view.valid()) { throw std::logic_error("TSDDataView requires a live view"); }
        const auto *schema = view.schema();
        if (schema == nullptr || schema->kind != TSTypeKind::TSD)
        {
            throw std::invalid_argument("TSDDataView requires a TSD TSData kind");
        }
        (void)static_cast<const TSDDataOps &>(view.ops());
    }

    TSDDataMutationView::TSDDataMutationView(TSDataView view, engine_time_t evaluation_time)
        : TSDDataView(view),
          mutation_(view.begin_mutation(evaluation_time))
    {
        if (view.schema() == nullptr || view.schema()->kind != TSTypeKind::TSD)
        {
            throw std::invalid_argument("TSDDataMutationView requires a TSD TSData kind");
        }
    }

    TSDDataMutationView::TSDDataMutationView(TSDDataMutationView &&) noexcept = default;

    TSDDataMutationView::~TSDDataMutationView() noexcept = default;

    TSDDataView TSDDataMutationView::view()
    {
        return TSDDataView{base()};
    }

    engine_time_t TSDDataMutationView::current_mutation_time() const
    {
        return mutation_.current_mutation_time();
    }

    void TSDDataMutationView::reserve(std::size_t capacity)
    {
        const auto &ops = static_cast<const TSDDataOps &>(mutation_.ops());
        ops.reserve_impl(ops.context, mutation_.mutable_data(), capacity);
    }

    TSDataView TSDDataMutationView::at(const ValueView &key)
    {
        const auto &ops    = static_cast<const TSDDataOps &>(mutation_.ops());
        const auto  result = ops.insert_key_impl(ops.context, mutation_.mutable_data(), key, current_mutation_time());
        apply_slot_mutation_result(mutation_, result);
        return at_slot(result.slot);
    }

    TSDataView TSDDataMutationView::operator[](const ValueView &key)
    {
        return at(key);
    }

    void TSDDataMutationView::set(const ValueView &key, const ValueView &value)
    {
        auto child = at(key);
        auto child_mutation = child.begin_mutation(current_mutation_time());
        static_cast<void>(child_mutation.copy_value_from(value));
    }

    bool TSDDataMutationView::erase(const ValueView &key)
    {
        const auto &ops    = static_cast<const TSDDataOps &>(mutation_.ops());
        const auto  result = ops.remove_key_impl(ops.context, mutation_.mutable_data(), key, current_mutation_time());
        apply_slot_mutation_result(mutation_, result);
        return result.changed;
    }

    void TSDDataMutationView::clear()
    {
        std::vector<ValueView> current_keys;
        for (const auto key : keys()) { current_keys.push_back(key); }
        for (const auto key : current_keys) { static_cast<void>(erase(key)); }
    }

    bool TSDDataMutationView::copy_value_from(const ValueView &source)
    {
        if (!source.has_value())
        {
            throw std::invalid_argument("TSDDataMutationView::copy_value_from requires a live source");
        }
        if (source.schema() != layout().value_binding->type_meta)
        {
            throw std::invalid_argument("TSDDataMutationView::copy_value_from requires the map value schema");
        }

        const bool was_modified = mutation_.modified(current_mutation_time());
        auto       source_map   = source.as_map();
        for (const auto [key, value] : source_map.items()) { set(key, value); }

        std::vector<ValueView> removals;
        for (const auto key : keys())
        {
            if (!source_map.contains(key)) { removals.push_back(key); }
        }
        for (const auto key : removals) { static_cast<void>(erase(key)); }

        return !was_modified && mutation_.modified(current_mutation_time());
    }

    TSDataView TSDDataMutationView::at_slot(std::size_t slot)
    {
        const auto &ops = static_cast<const TSDDataOps &>(mutation_.ops());
        if (!ops.slot_occupied_impl(ops.context, mutation_.mutable_data(), slot))
        {
            throw std::out_of_range("TSDDataMutationView::at_slot: slot is not occupied");
        }
        const auto *child_memory = ops.child_at_slot_impl(ops.context, mutation_.mutable_data(), slot);
        return TSDataView{layout().element_binding, child_memory, mutation_.view(), slot};
    }
}  // namespace hgraph

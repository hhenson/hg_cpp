#include <hgraph/types/time_series/ts_data.h>

#include <stdexcept>
#include <vector>

namespace hgraph
{
    TSSDataView::TSSDataView(TSDataView view)
        : view_(view)
    {
        validate_kind(view_);
    }

    const TSDataView &TSSDataView::base() const noexcept
    {
        return view_;
    }

    TSDataView &TSSDataView::base() noexcept
    {
        return view_;
    }

    const TSDataBinding *TSSDataView::binding() const noexcept
    {
        return view_.binding();
    }

    const TSValueTypeMetaData *TSSDataView::schema() const noexcept
    {
        return view_.schema();
    }

    const TSSDataLayout &TSSDataView::layout() const
    {
        return static_cast<const TSSDataLayout &>(view_.layout());
    }

    ValueView TSSDataView::value() const
    {
        return view_.value();
    }

    ValueView TSSDataView::delta_value(engine_time_t evaluation_time) const
    {
        return view_.delta_value(evaluation_time);
    }

    engine_time_t TSSDataView::last_modified_time() const
    {
        return view_.last_modified_time();
    }

    bool TSSDataView::modified(engine_time_t evaluation_time) const
    {
        return view_.modified(evaluation_time);
    }

    void TSSDataView::subscribe(Notifiable *observer) const
    {
        view_.subscribe(observer);
    }

    void TSSDataView::unsubscribe(Notifiable *observer) const
    {
        view_.unsubscribe(observer);
    }

    bool TSSDataView::has_observers() const
    {
        return view_.has_observers();
    }

    std::size_t TSSDataView::observer_count() const
    {
        return view_.observer_count();
    }

    std::size_t TSSDataView::size() const
    {
        const auto &ops = set_ops();
        return ops.size_impl(ops.context, view_.data());
    }

    bool TSSDataView::empty() const
    {
        return size() == 0;
    }

    std::size_t TSSDataView::slot_capacity() const
    {
        const auto &ops = set_ops();
        return ops.slot_capacity_impl(ops.context, view_.data());
    }

    bool TSSDataView::slot_occupied(std::size_t slot) const
    {
        const auto &ops = set_ops();
        return ops.slot_occupied_impl(ops.context, view_.data(), slot);
    }

    bool TSSDataView::slot_live(std::size_t slot) const
    {
        const auto &ops = set_ops();
        return ops.slot_live_impl(ops.context, view_.data(), slot);
    }

    bool TSSDataView::slot_added(std::size_t slot) const
    {
        const auto &ops = set_ops();
        return ops.slot_added_impl(ops.context, view_.data(), slot);
    }

    bool TSSDataView::slot_removed(std::size_t slot) const
    {
        const auto &ops = set_ops();
        return ops.slot_removed_impl(ops.context, view_.data(), slot);
    }

    ValueView TSSDataView::at_slot(std::size_t slot) const
    {
        if (!slot_occupied(slot)) { throw std::out_of_range("TSSDataView::at_slot: slot is not occupied"); }
        const auto &ops = set_ops();
        return ValueView{layout().key_binding, ops.key_at_slot_impl(ops.context, view_.data(), slot)};
    }

    bool TSSDataView::contains(const ValueView &key) const
    {
        const auto &ops = set_ops();
        return ops.contains_impl(ops.context, view_.data(), key);
    }

    std::size_t TSSDataView::find_slot(const ValueView &key) const
    {
        const auto &ops = set_ops();
        return ops.find_slot_impl(ops.context, view_.data(), key);
    }

    Range<ValueView> TSSDataView::values() const
    {
        const auto &ops = set_ops();
        return ops.make_values_range_impl(ops.context, view_.data());
    }

    Range<ValueView> TSSDataView::added() const
    {
        const auto &ops = set_ops();
        return ops.make_added_values_range_impl(ops.context, view_.data());
    }

    Range<ValueView> TSSDataView::removed() const
    {
        const auto &ops = set_ops();
        return ops.make_removed_values_range_impl(ops.context, view_.data());
    }

    Range<ValueView> TSSDataView::added_values() const
    {
        return added();
    }

    Range<ValueView> TSSDataView::removed_values() const
    {
        return removed();
    }

    Range<ValueView>::iterator TSSDataView::begin() const
    {
        return values().begin();
    }

    Range<ValueView>::iterator TSSDataView::end() const
    {
        return values().end();
    }

    TSSDataMutationView TSSDataView::begin_mutation(engine_time_t evaluation_time) const
    {
        return TSSDataMutationView{view_, evaluation_time};
    }

    const TSSDataOps &TSSDataView::set_ops() const
    {
        return static_cast<const TSSDataOps &>(view_.ops());
    }

    void TSSDataView::validate_kind(const TSDataView &view)
    {
        if (!view.valid()) { throw std::logic_error("TSSDataView requires a live view"); }
        const auto *schema = view.schema();
        if (schema == nullptr || schema->kind != TSTypeKind::TSS)
        {
            throw std::invalid_argument("TSSDataView requires a TSS TSData kind");
        }
        (void)static_cast<const TSSDataOps &>(view.ops());
    }

    TSSDataMutationView::TSSDataMutationView(TSDataView view, engine_time_t evaluation_time)
        : TSSDataView(view),
          mutation_(view.begin_mutation(evaluation_time))
    {
        if (view.schema() == nullptr || view.schema()->kind != TSTypeKind::TSS)
        {
            throw std::invalid_argument("TSSDataMutationView requires a TSS TSData kind");
        }
    }

    TSSDataMutationView::TSSDataMutationView(TSSDataMutationView &&) noexcept = default;

    TSSDataMutationView::~TSSDataMutationView() noexcept = default;

    TSSDataView TSSDataMutationView::view()
    {
        return TSSDataView{base()};
    }

    engine_time_t TSSDataMutationView::current_mutation_time() const
    {
        return mutation_.current_mutation_time();
    }

    void TSSDataMutationView::reserve(std::size_t capacity)
    {
        const auto &ops = static_cast<const TSSDataOps &>(mutation_.ops());
        ops.reserve_impl(ops.context, mutation_.mutable_data(), capacity);
    }

    bool TSSDataMutationView::add(const ValueView &key)
    {
        const auto &ops    = static_cast<const TSSDataOps &>(mutation_.ops());
        const auto  result = ops.insert_key_impl(ops.context, mutation_.mutable_data(), key, current_mutation_time());
        apply_slot_mutation_result(mutation_, result);
        if (!result.changed && ops.touch_impl(ops.context, mutation_.mutable_data(), current_mutation_time()))
        {
            mutation_.mark_modified();
        }
        return result.changed;
    }

    bool TSSDataMutationView::remove(const ValueView &key)
    {
        const auto &ops    = static_cast<const TSSDataOps &>(mutation_.ops());
        const auto  result = ops.remove_key_impl(ops.context, mutation_.mutable_data(), key, current_mutation_time());
        apply_slot_mutation_result(mutation_, result);
        if (!result.changed && ops.touch_impl(ops.context, mutation_.mutable_data(), current_mutation_time()))
        {
            mutation_.mark_modified();
        }
        return result.changed;
    }

    void TSSDataMutationView::clear()
    {
        std::vector<ValueView> keys;
        for (const auto key : values()) { keys.push_back(key); }
        const auto &ops           = static_cast<const TSSDataOps &>(mutation_.ops());
        const bool  newly_touched = ops.touch_impl(ops.context, mutation_.mutable_data(), current_mutation_time());
        for (const auto key : keys) { static_cast<void>(remove(key)); }
        if (newly_touched) { mutation_.mark_modified(); }
    }

    bool TSSDataMutationView::copy_value_from(const ValueView &source)
    {
        return mutation_.copy_value_from(source);
    }
}  // namespace hgraph

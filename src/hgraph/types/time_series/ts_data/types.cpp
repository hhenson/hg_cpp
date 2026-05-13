#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <cassert>
#include <ranges>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph
{
    TSDataObserverSet::TSDataObserverSet(const TSDataObserverSet &) noexcept
    {
    }

    TSDataObserverSet &TSDataObserverSet::operator=(const TSDataObserverSet &other) noexcept
    {
        if (this != &other) { clear(); }
        return *this;
    }

    TSDataObserverSet::TSDataObserverSet(TSDataObserverSet &&other) noexcept
        : observers_(std::exchange(other.observers_, ObserverStorage{}))
    {
    }

    TSDataObserverSet &TSDataObserverSet::operator=(TSDataObserverSet &&other) noexcept
    {
        if (this != &other)
        {
            clear();
            observers_ = std::exchange(other.observers_, ObserverStorage{});
        }
        return *this;
    }

    TSDataObserverSet::~TSDataObserverSet() noexcept
    {
        clear();
    }

    bool TSDataObserverSet::empty() const noexcept
    {
        return size() == 0;
    }

    bool TSDataObserverSet::contains(const Notifiable *observer) const noexcept
    {
        if (observer == nullptr) { return false; }
        if (const auto *entry = single(); entry != nullptr) { return entry == observer; }
        const auto *entries = many();
        return entries != nullptr && std::find(entries->entries.begin(), entries->entries.end(), observer) != entries->entries.end();
    }

    std::size_t TSDataObserverSet::size() const noexcept
    {
        if (observers_.empty()) { return 0; }
        if (single() != nullptr) { return 1; }
        const auto *entries = many();
        if (entries == nullptr) { return 0; }
        return static_cast<std::size_t>(std::ranges::count_if(entries->entries, [](const auto *entry) {
            return entry != nullptr;
        }));
    }

    void TSDataObserverSet::subscribe(Notifiable *observer)
    {
        if (observer == nullptr) { return; }

        if (observers_.empty())
        {
            set_single(observer);
            return;
        }

        if (auto *entry = single(); entry != nullptr)
        {
            assert(entry != observer && "TSData observer registered twice");
            if (entry == observer) { return; }

            auto *entries = new ObserverList{};
            entries->entries.reserve(2);
            entries->entries.push_back(entry);
            entries->entries.push_back(observer);
            set_many(entries);
            return;
        }

        auto *entries = many();
        assert(entries != nullptr && "TSData observer storage is corrupt");
        if (entries == nullptr) { throw std::logic_error("TSData observer storage is corrupt"); }

        const auto it = std::find(entries->entries.begin(), entries->entries.end(), observer);
        assert(it == entries->entries.end() && "TSData observer registered twice");
        if (it == entries->entries.end()) { entries->entries.push_back(observer); }
    }

    void TSDataObserverSet::unsubscribe(Notifiable *observer)
    {
        if (observer == nullptr) { return; }

        if (auto *entry = single(); entry != nullptr)
        {
            assert(entry == observer && "removing unregistered TSData observer");
            if (entry == observer) { observers_.clear(); }
            return;
        }

        auto *entries = many();
        if (entries == nullptr)
        {
            assert(false && "removing unregistered TSData observer");
            return;
        }

        const auto it = std::find(entries->entries.begin(), entries->entries.end(), observer);
        assert(it != entries->entries.end() && "removing unregistered TSData observer");
        if (it == entries->entries.end()) { return; }

        if (entries->notify_depth > 0)
        {
            *it = nullptr;
            entries->compact_pending = true;
            return;
        }

        *it = entries->entries.back();
        entries->entries.pop_back();
        compact_many(*entries);
    }

    void TSDataObserverSet::notify(engine_time_t modified_time) const
    {
        if (auto *entry = single(); entry != nullptr)
        {
            entry->notify(modified_time);
            return;
        }

        auto *entries = many();
        if (entries == nullptr) { return; }
        ++entries->notify_depth;
        auto guard = make_scope_exit([this, entries]() noexcept {
            --entries->notify_depth;
            if (entries->notify_depth == 0 && entries->compact_pending)
            {
                const_cast<TSDataObserverSet *>(this)->compact_many(*entries);
            }
        });

        const auto limit = entries->entries.size();
        for (std::size_t index = 0; index < limit; ++index)
        {
            auto *observer = entries->entries[index];
            if (observer != nullptr) { observer->notify(modified_time); }
        }
    }

    void TSDataObserverSet::notify_invalidated() noexcept
    {
        if (auto *entry = single(); entry != nullptr)
        {
            static_cast<void>(fallback_on_exception(false, [&] {
                entry->notify_invalidated();
                return true;
            }));
            return;
        }

        const auto *entries = many();
        if (entries == nullptr) { return; }
        for (auto *observer : entries->entries)
        {
            if (observer == nullptr) { continue; }
            static_cast<void>(fallback_on_exception(false, [&] {
                observer->notify_invalidated();
                return true;
            }));
        }
    }

    void TSDataObserverSet::clear() noexcept
    {
        notify_invalidated();
        if (auto *entries = many(); entries != nullptr)
        {
            if (entries->notify_depth == 0) { delete entries; }
            else
            {
                std::ranges::fill(entries->entries, nullptr);
                entries->compact_pending = true;
            }
        }
        observers_.clear();
    }

    Notifiable *TSDataObserverSet::single() const noexcept
    {
        return observers_.get<Notifiable>();
    }

    TSDataObserverSet::ObserverList *TSDataObserverSet::many() const noexcept
    {
        return observers_.get<ObserverList>();
    }

    void TSDataObserverSet::set_single(Notifiable *observer) noexcept
    {
        observers_.set(observer);
    }

    void TSDataObserverSet::set_many(ObserverList *observers) noexcept
    {
        observers_.set(observers);
    }

    void TSDataObserverSet::compact_many(ObserverList &observers) noexcept
    {
        if (observers.notify_depth > 0)
        {
            observers.compact_pending = true;
            return;
        }

        for (std::size_t index = 0; index < observers.entries.size();)
        {
            if (observers.entries[index] != nullptr)
            {
                ++index;
                continue;
            }
            observers.entries[index] = observers.entries.back();
            observers.entries.pop_back();
        }
        observers.compact_pending = false;

        if (observers.entries.empty())
        {
            delete &observers;
            observers_.clear();
            return;
        }

        if (observers.entries.size() == 1)
        {
            auto *remaining = observers.entries.front();
            delete &observers;
            set_single(remaining);
        }
    }

    bool TSDataTracking::record_modified(engine_time_t modified_time)
    {
        if (modified_time == MIN_DT) { throw std::invalid_argument("TSDataTracking requires a concrete engine time"); }
        if (last_modified_time == modified_time) { return false; }

        last_modified_time = modified_time;
        observers.notify(modified_time);
        return true;
    }

    TSDataParentLinkKind TSDataParentLink::kind() const noexcept
    {
        return parent_.enum_value();
    }

    bool TSDataParentLink::has_parent() const noexcept
    {
        return has_ts_data_parent() || has_endpoint_parent();
    }

    bool TSDataParentLink::has_ts_data_parent() const noexcept
    {
        return kind() == TSDataParentLinkKind::TSData && parent_.ptr() != nullptr && payload_.ts_data != nullptr;
    }

    bool TSDataParentLink::has_endpoint_parent() const noexcept
    {
        return kind() == TSDataParentLinkKind::Endpoint && payload_.endpoint != nullptr;
    }

    const TSDataBinding *TSDataParentLink::parent_binding() const noexcept
    {
        return has_ts_data_parent() ? parent_.ptr() : nullptr;
    }

    const void *TSDataParentLink::parent_data() const noexcept
    {
        return has_ts_data_parent() ? payload_.ts_data : nullptr;
    }

    TSDataParent *TSDataParentLink::parent_endpoint() const noexcept
    {
        return has_endpoint_parent() ? payload_.endpoint : nullptr;
    }

    const TSDataTracking &TSDataParentLink::parent_tracking() const
    {
        if (!has_ts_data_parent()) { throw std::logic_error("TSDataParentLink requires a TSData parent"); }
        const auto *binding = parent_binding();
        const auto &table   = binding->checked_ops();
        return *table.tracking_impl(table.context, parent_data());
    }

    TSDataTracking &TSDataParentLink::mutable_parent_tracking() const
    {
        if (!has_ts_data_parent()) { throw std::logic_error("TSDataParentLink requires a TSData parent"); }
        const auto *binding = parent_binding();
        const auto &table   = binding->checked_ops();
        auto       *memory  = const_cast<void *>(parent_data());
        return *table.mutable_tracking_impl(table.context, memory);
    }

    void TSDataParentLink::notify_child_modified(engine_time_t mutation_time) const
    {
        if (!has_ts_data_parent())
        {
            if (auto *endpoint = parent_endpoint(); endpoint != nullptr)
            {
                endpoint->record_child_modified(child_id, mutation_time);
            }
            return;
        }

        const auto *binding = parent_binding();
        const auto &table   = binding->checked_ops();
        auto       *memory  = const_cast<void *>(parent_data());
        table.record_child_modified_impl(table.context, memory, child_id, mutation_time);

        auto &state = mutable_parent_tracking();
        if (state.record_modified(mutation_time)) { state.parent.notify_child_modified(mutation_time); }
    }

    std::vector<std::size_t> TSDataParentLink::path_from_root() const
    {
        std::vector<std::size_t> reversed_path;
        auto                     current = *this;
        while (current.has_ts_data_parent())
        {
            reversed_path.push_back(current.child_id);
            const auto &next = current.parent_tracking().parent;
            if (!next.has_ts_data_parent()) { break; }
            current = next;
        }

        std::reverse(reversed_path.begin(), reversed_path.end());
        return reversed_path;
    }

    TSDataView TSDataParentLink::root_view() const
    {
        if (!has_ts_data_parent()) { return TSDataView{}; }

        const TSDataBinding *root_binding = parent_binding();
        const void          *root_data    = parent_data();
        auto                 current      = *this;
        while (current.has_ts_data_parent())
        {
            root_binding = current.parent_binding();
            root_data    = current.parent_data();
            const auto &next = current.parent_tracking().parent;
            if (!next.has_ts_data_parent()) { break; }
            current = next;
        }
        return TSDataView{root_binding, root_data};
    }

    const ValueTypeBinding *FixedTSDataFieldLayout::value_binding() const noexcept
    {
        return layout != nullptr ? layout->value_binding : nullptr;
    }

    const ValueTypeBinding *FixedTSDataFieldLayout::delta_binding() const noexcept
    {
        return layout != nullptr ? layout->delta_binding : nullptr;
    }

    std::size_t FixedTSBDataLayout::size() const noexcept
    {
        return fields.size();
    }

    const FixedTSDataFieldLayout &FixedTSBDataLayout::field(std::size_t index) const
    {
        return fields.at(index);
    }

    std::size_t FixedTSLDataLayout::size() const noexcept
    {
        return element_count;
    }

    std::size_t FixedTSLDataLayout::element_value_offset(std::size_t index) const
    {
        if (index >= element_count) { throw std::out_of_range("FixedTSLDataLayout element index out of range"); }
        return value_offset + index * element_value_stride;
    }

    std::size_t FixedTSLDataLayout::element_auxiliary_offset_at(std::size_t index) const
    {
        if (index >= element_count) { throw std::out_of_range("FixedTSLDataLayout element index out of range"); }
        return element_auxiliary_offset + index * element_auxiliary_stride;
    }
}  // namespace hgraph

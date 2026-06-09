#ifndef HGRAPH_CPP_TS_DATA_PROXY_H
#define HGRAPH_CPP_TS_DATA_PROXY_H

#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/ts_data/dict_view.h>
#include <hgraph/types/utils/slot_observer.h>
#include <hgraph/types/utils/value_slot_store.h>

#include <cstddef>

namespace hgraph
{
    class TSDProxy;

    /** Slot-event adapter that drives one proxy storage from its source TSD. */
    class TSDProxySlotSync final : public SlotObserver, public Notifiable
    {
      public:
        explicit TSDProxySlotSync(TSDProxy &owner) noexcept;
        TSDProxySlotSync(const TSDProxySlotSync &) = delete;
        TSDProxySlotSync &operator=(const TSDProxySlotSync &) = delete;
        TSDProxySlotSync(TSDProxySlotSync &&) = delete;
        TSDProxySlotSync &operator=(TSDProxySlotSync &&) = delete;
        ~TSDProxySlotSync() override;

        void on_capacity(std::size_t old_capacity, std::size_t new_capacity) override;
        void on_insert(std::size_t slot) override;
        void on_remove(std::size_t slot) override;
        void on_erase(std::size_t slot) override;
        void on_clear() override;
        void notify(DateTime modified_time) override;

      private:
        TSDProxy *owner_{nullptr};
    };

    /**
     * Read-only proxy over a source ``TSD``.
     *
     * ``TSDProxy`` mirrors the source dictionary's key-set and stable slot ids,
     * but owns its value slots. A caller-provided value builder constructs each
     * proxy value from the source child at the same slot. Structural slot
     * callbacks construct and destroy the parallel value slots; source TSData
     * modification notification supplies the evaluation time used to materialise
     * added/removed proxy children.
     */
    class TSDProxy final
    {
      public:
        using ValueBuilder = void (*)(TSDProxy       &proxy,
                                      std::size_t     slot,
                                      const TSDataView &target,
                                      const TSDataView &source,
                                      DateTime   modified_time,
                                      const void     *context);

        TSDProxy() noexcept;
        TSDProxy(const TSDProxy &) = delete;
        TSDProxy &operator=(const TSDProxy &) = delete;
        TSDProxy(TSDProxy &&) = delete;
        TSDProxy &operator=(TSDProxy &&) = delete;
        ~TSDProxy();

        /**
         * Bind this proxy storage to ``source``.
         *
         * ``self_binding`` is the proxy TSData binding that owns this storage.
         * ``element_binding`` is the binding used for each constructed proxy
         * value. ``builder`` is called when a slot value first needs to be
         * materialised.
         */
        void bind(const TSDataBinding &self_binding,
                  const TSDataBinding &element_binding,
                  const TSDDataView   &source,
                  ValueBuilder         builder,
                  const void          *builder_context,
                  DateTime        modified_time);

        /** Clear transient delta state on constructed proxy values. */
        void cleanup_delta(DateTime modified_time);

        [[nodiscard]] TSDataView source_view() const noexcept;
        [[nodiscard]] TSDDataView source_dict() const;
        [[nodiscard]] TSDataView source_child_at_slot(std::size_t slot) const;

        [[nodiscard]] TSDataTracking &tracking() noexcept;
        [[nodiscard]] const TSDataTracking &tracking() const noexcept;
        [[nodiscard]] bool has_child(std::size_t slot) const noexcept;
        [[nodiscard]] bool child_updated(std::size_t slot) const noexcept;
        [[nodiscard]] const void *child_at_slot(std::size_t slot) const;
        [[nodiscard]] void *child_at_slot(std::size_t slot);

        /** Ops hooks used by the proxy TSData binding. */
        void record_child_modified(std::size_t slot, DateTime modified_time);
        void subscribe_slot_observer(SlotObserver *observer);
        void unsubscribe_slot_observer(SlotObserver *observer);

      private:
        friend class TSDProxySlotSync;

        /** Source slot lifecycle callbacks, invoked by ``TSDProxySlotSync``. */
        void on_slot_capacity(std::size_t old_capacity, std::size_t new_capacity);
        void on_slot_inserted(std::size_t slot);
        void on_slot_removed(std::size_t slot);
        void on_slot_erased(std::size_t slot);
        void on_slots_cleared();
        void on_source_modified(DateTime modified_time);

        void subscribe_source();
        void unsubscribe_source() noexcept;
        void sync_from_source(DateTime modified_time, bool force_modified);
        void construct_child_at_slot(std::size_t slot);
        void ensure_child_at_slot(std::size_t slot, DateTime modified_time);
        void refresh_child_at_slot(std::size_t slot, DateTime modified_time);
        void mark_modified(DateTime modified_time);

        const TSDataBinding          *self_binding_{nullptr};
        const TSDataBinding          *element_binding_{nullptr};
        TSDProxySlotSync              source_sync_;
        TSDDataStorageRef             source_storage_{};
        ValueBuilder                  value_builder_{nullptr};
        const void                   *value_builder_context_{nullptr};
        ValueSlotStore                values_{};
        TSDataTracking                tracking_{};
        SlotObserverList              slot_observers_{};
        bool                          subscribed_{false};
    };

    /**
     * Return the proxy binding for ``schema`` using ``element_binding`` for
     * proxy value slots.
     */
    [[nodiscard]] const TSDataBinding &tsd_proxy_binding_for(const TSValueTypeMetaData &schema,
                                                             const TSDataBinding      &element_binding);

    /** Bind a live proxy TSData view to a source dictionary. */
    void bind_tsd_proxy(const TSDataView       &proxy,
                        const TSDDataView      &source,
                        TSDProxy::ValueBuilder  builder,
                        const void             *builder_context,
                        DateTime           modified_time);
}  // namespace hgraph

#endif  // HGRAPH_CPP_TS_DATA_PROXY_H

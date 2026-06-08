#ifndef HGRAPH_CPP_ROOT_TS_OUTPUT_H
#define HGRAPH_CPP_ROOT_TS_OUTPUT_H

#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/time_series/ts_output/base_view.h>
#include <hgraph/types/time_series/ts_output/bundle_view.h>
#include <hgraph/types/time_series/ts_output/dict_view.h>
#include <hgraph/types/time_series/ts_output/list_view.h>
#include <hgraph/types/time_series/ts_output/mutation_view.h>
#include <hgraph/types/time_series/ts_output/set_view.h>
#include <hgraph/types/time_series/ts_output/window_view.h>

#include <memory>

namespace hgraph
{
    namespace detail
    {
        class TSOutputAlternativeStore;
    }

    class TSEndpointSchema;

    /**
     * Owning output-side time-series endpoint.
     *
     * ``TSOutput`` owns the root TSData allocation and exposes lifecycle hooks
     * used by nodes to clean up transient delta state after evaluation. Root
     * subscriptions are delegated to the root TSData observer set; child-level
     * subscriptions are registered on the projected child TSData views.
     */
    class TSOutput : private TSDataParent
    {
      public:
        TSOutput() noexcept;
        explicit TSOutput(const TSDataBinding &binding);
        explicit TSOutput(const TSValueTypeMetaData &schema);
        explicit TSOutput(const TSValueTypeMetaData *schema);
        explicit TSOutput(const TSEndpointSchema &endpoint_schema);
        ~TSOutput() noexcept;

        TSOutput(const TSOutput &other);
        TSOutput &operator=(const TSOutput &other);
        TSOutput(TSOutput &&other) noexcept;
        TSOutput &operator=(TSOutput &&other) noexcept;

        /** True when this output owns a bound TSData root. */
        [[nodiscard]] bool has_value() const noexcept;

        /** Root TSData binding and schema, or null when unbound. */
        [[nodiscard]] const TSDataBinding *binding() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;

        /** Borrowed root TSData view. */
        [[nodiscard]] TSDataView data_view();
        [[nodiscard]] TSDataView data_view() const;

        /** True after this output has been modified and before cleanup runs. */
        [[nodiscard]] bool dirty() const noexcept;

        /**
         * Clear transient delta state for the current dirty root.
         *
         * Nodes call this from their post-evaluation cleanup hook. The cleanup
         * walks only branches whose modification time matches the root's last
         * modified time.
         */
        void cleanup_delta();

        /** Clear the dirty flag without touching TSData delta state. */
        void clear_dirty() noexcept;

        /** Register / remove an observer at the root TSData level. */
        void subscribe(Notifiable *observer);
        void unsubscribe(Notifiable *observer);

        /** Read view at ``evaluation_time``. */
        [[nodiscard]] TSOutputView view(engine_time_t evaluation_time = MIN_DT);
        [[nodiscard]] TSOutputView view(engine_time_t evaluation_time = MIN_DT) const;

        /** Binding data for a canonical or alternative representation of ``source``. */
        [[nodiscard]] TSOutputHandle binding_for(const TSOutputView &source,
                                                 const TSValueTypeMetaData &requested_schema) const;

        /** Begin a root mutation scope. */
        [[nodiscard]] TSOutputMutationView begin_mutation(engine_time_t evaluation_time);

      private:
        friend class TSOutputMutationView;

        static const TSDataBinding &checked_binding_for(const TSValueTypeMetaData *schema);
        static const TSDataBinding &checked_binding_for(const TSEndpointSchema &endpoint_schema);
        static const TSData &copyable_data(const TSOutput &other);

        void attach_root_parent();
        void record_child_modified(std::size_t child_id, engine_time_t mutation_time) override;

        TSData                                      data_{};
        bool                                        dirty_{false};
        mutable std::unique_ptr<detail::TSOutputAlternativeStore> alternatives_{};
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_OUTPUT_H

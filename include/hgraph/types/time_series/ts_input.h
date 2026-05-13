#ifndef HGRAPH_CPP_ROOT_TS_INPUT_H
#define HGRAPH_CPP_ROOT_TS_INPUT_H

#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_input/base_view.h>
#include <hgraph/types/time_series/ts_input/bundle_view.h>
#include <hgraph/types/time_series/ts_input/dict_view.h>
#include <hgraph/types/time_series/ts_input/list_view.h>
#include <hgraph/types/time_series/ts_input/set_view.h>
#include <hgraph/types/time_series/ts_input/window_view.h>
#include <hgraph/types/time_series/ts_output.h>

#include <memory>

namespace hgraph
{
    namespace detail
    {
        struct TSInputNode;
    }

    class TSInput;
    class TSInputView;
    class TSBInputView;
    class TSLInputView;
    class TSSInputView;
    class TSDInputView;
    class TSWInputView;

    /**
     * Construction plan for one node input bundle.
     *
     * The plan is compiled from a canonical TS schema plus a generic endpoint
     * annotation tree. TSInput requires a non-peered TSB root, with nested
     * non-peered TSB/fixed-TSL prefixes and peered terminals beneath it. A
     * peered terminal compiles to input-side TargetLink state.
     */
    class TSInputConstructionPlan
    {
      public:
        TSInputConstructionPlan(const TSValueTypeMetaData &root_schema,
                                TSEndpointSchema           endpoint_schema);

        [[nodiscard]] const TSValueTypeMetaData &schema() const noexcept;
        [[nodiscard]] const TSEndpointSchema &endpoint_schema() const noexcept;

      private:
        const TSValueTypeMetaData *schema_{nullptr};
        TSEndpointSchema          endpoint_schema_{};
    };

    class TSInputPlanFactory
    {
      public:
        [[nodiscard]] static TSInputConstructionPlan compile(
            const TSValueTypeMetaData                  &root_schema,
            const TSEndpointSchema                     &endpoint_schema);
    };

    /**
     * Cached builder for TSInput endpoint state.
     *
     * Unlike TSOutput construction, an input builder does not allocate payload
     * storage for the visible time-series value. It builds the non-peered input
     * tree and peered terminal state used to borrow output TSData at runtime.
     */
    class TSInputBuilder
    {
      public:
        [[nodiscard]] const TSValueTypeMetaData &schema() const noexcept;
        [[nodiscard]] TSInput make_input() const;

      private:
        friend class TSInputBuilderFactory;
        friend class TSInput;

        explicit TSInputBuilder(TSInputConstructionPlan plan);

        TSInputConstructionPlan plan_;
    };

    class TSInputBuilderFactory
    {
      public:
        [[nodiscard]] static const TSInputBuilder *builder_for(const TSValueTypeMetaData &root_schema,
                                                               const TSEndpointSchema    &endpoint_schema);
        [[nodiscard]] static const TSInputBuilder &checked_builder_for(const TSValueTypeMetaData &root_schema,
                                                                       const TSEndpointSchema    &endpoint_schema);
        [[nodiscard]] static const TSInputBuilder *builder_for(const TSInputConstructionPlan &plan);
        [[nodiscard]] static const TSInputBuilder &checked_builder_for(const TSInputConstructionPlan &plan);
    };

    /**
     * Owning input-side time-series endpoint.
     *
     * TSInput owns binding and activation state. The root is always a
     * non-peered TSB; peered terminals inside that tree borrow TSOutput TSData
     * through input-side TargetLink state.
     */
    class TSInput
    {
      public:
        TSInput() noexcept;
        explicit TSInput(const TSInputBuilder &builder);
        TSInput(const TSInput &other);
        TSInput &operator=(const TSInput &other);
        TSInput(TSInput &&other) noexcept;
        TSInput &operator=(TSInput &&other) noexcept;
        ~TSInput();

        [[nodiscard]] bool has_value() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;

        /**
         * Root input view.
         *
         * ``scheduling_notifier`` is normally the owning node. Active views use
         * it as the final notification target.
         */
        [[nodiscard]] TSInputView view(Notifiable *scheduling_notifier = nullptr,
                                       engine_time_t evaluation_time = MIN_DT);
        [[nodiscard]] TSInputView view(Notifiable *scheduling_notifier = nullptr,
                                       engine_time_t evaluation_time = MIN_DT) const;

      private:
        friend class TSInputView;
        friend class TSBInputView;
        friend class TSLInputView;
        friend class TSSInputView;
        friend class TSDInputView;
        friend class TSWInputView;

        explicit TSInput(const TSInputConstructionPlan &plan);

        void rebuild_from_plan(const TSInputConstructionPlan &plan);
        void relink_nodes() noexcept;

        const TSInputBuilder             *builder_{nullptr};
        const TSValueTypeMetaData        *schema_{nullptr};
        std::unique_ptr<detail::TSInputNode> root_{};
    };

}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_H

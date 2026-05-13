#ifndef HGRAPH_CPP_ROOT_TS_INPUT_H
#define HGRAPH_CPP_ROOT_TS_INPUT_H

#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_output.h>

#include <cstddef>
#include <memory>
#include <string_view>
#include <vector>

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

        explicit TSInput(const TSInputConstructionPlan &plan);

        void rebuild_from_plan(const TSInputConstructionPlan &plan);
        void relink_nodes() noexcept;

        const TSInputBuilder             *builder_{nullptr};
        const TSValueTypeMetaData        *schema_{nullptr};
        std::unique_ptr<detail::TSInputNode> root_{};
    };

    /**
     * Input-side read/binding/activation view.
     *
     * A view can represent a non-peered input node or a position inside a
     * bound target output reached through a peered terminal.
     */
    class TSInputView
    {
      public:
        TSInputView() noexcept;

        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;
        [[nodiscard]] bool bound() const noexcept;
        [[nodiscard]] bool valid() const;
        [[nodiscard]] bool all_valid() const;
        [[nodiscard]] engine_time_t last_modified_time() const;
        [[nodiscard]] bool modified(engine_time_t evaluation_time) const;
        [[nodiscard]] bool modified() const;

        [[nodiscard]] ValueView value() const;
        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const;
        [[nodiscard]] ValueView delta_value() const;

        void bind_output(const TSOutputView &output);
        void unbind_output();

        void make_active();
        void make_passive();
        [[nodiscard]] bool active() const;

        [[nodiscard]] TSBInputView as_bundle() &;
        [[nodiscard]] TSBInputView as_bundle() const &;
        void as_bundle() && = delete;
        void as_bundle() const && = delete;
        [[nodiscard]] TSLInputView as_list() &;
        [[nodiscard]] TSLInputView as_list() const &;
        void as_list() && = delete;
        void as_list() const && = delete;

      private:
        friend class TSInput;
        friend class TSBInputView;
        friend class TSLInputView;

        TSInputView(TSInput                         *input,
                    detail::TSInputNode            *node,
                    TSDataView                      target_view,
                    std::vector<std::size_t>        target_path,
                    Notifiable                     *scheduling_notifier,
                    engine_time_t                   evaluation_time) noexcept;

        [[nodiscard]] bool is_target_position() const noexcept;
        [[nodiscard]] bool target_view_live() const noexcept;
        [[nodiscard]] bool is_target_root() const noexcept;
        [[nodiscard]] TSInputView child_from_target(TSDataView child, std::size_t index) const;
        [[nodiscard]] TSInputView child_from_node(detail::TSInputNode *child) const noexcept;

        TSInput                  *input_{nullptr};
        detail::TSInputNode      *node_{nullptr};
        TSDataView                target_view_{};
        std::vector<std::size_t>  target_path_{};
        Notifiable               *scheduling_notifier_{nullptr};
        engine_time_t             evaluation_time_{MIN_DT};
    };

    class TSBInputView
    {
      public:
        explicit TSBInputView(TSInputView view);

        [[nodiscard]] const TSInputView &base() const noexcept;
        [[nodiscard]] TSInputView &base() noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;

        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] bool has_field(std::string_view name) const noexcept;

        [[nodiscard]] TSInputView at(std::size_t index) &;
        [[nodiscard]] TSInputView at(std::size_t index) const &;
        TSInputView at(std::size_t) && = delete;
        [[nodiscard]] TSInputView operator[](std::size_t index) &;
        [[nodiscard]] TSInputView operator[](std::size_t index) const &;
        TSInputView operator[](std::size_t) && = delete;

        [[nodiscard]] TSInputView at(std::string_view name) &;
        [[nodiscard]] TSInputView at(std::string_view name) const &;
        TSInputView at(std::string_view) && = delete;
        [[nodiscard]] TSInputView field(std::string_view name) &;
        [[nodiscard]] TSInputView field(std::string_view name) const &;
        TSInputView field(std::string_view) && = delete;
        [[nodiscard]] TSInputView operator[](std::string_view name) &;
        [[nodiscard]] TSInputView operator[](std::string_view name) const &;
        TSInputView operator[](std::string_view) && = delete;

      private:
        static constexpr std::size_t npos = static_cast<std::size_t>(-1);

        [[nodiscard]] std::size_t field_index(std::string_view name) const;
        [[nodiscard]] std::size_t find_field_index(std::string_view name) const noexcept;

        TSInputView view_{};
    };

    class TSLInputView
    {
      public:
        explicit TSLInputView(TSInputView view);

        [[nodiscard]] const TSInputView &base() const noexcept;
        [[nodiscard]] TSInputView &base() noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;

        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;

        [[nodiscard]] TSInputView at(std::size_t index) &;
        [[nodiscard]] TSInputView at(std::size_t index) const &;
        TSInputView at(std::size_t) && = delete;
        [[nodiscard]] TSInputView operator[](std::size_t index) &;
        [[nodiscard]] TSInputView operator[](std::size_t index) const &;
        TSInputView operator[](std::size_t) && = delete;

      private:
        TSInputView view_{};
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_H

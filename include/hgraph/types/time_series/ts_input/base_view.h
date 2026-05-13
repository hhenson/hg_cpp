#ifndef HGRAPH_CPP_ROOT_TS_INPUT_BASE_VIEW_H
#define HGRAPH_CPP_ROOT_TS_INPUT_BASE_VIEW_H

#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/ts_output.h>
#include <cstddef>
#include <memory>
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
    class TSSInputView;
    class TSDInputView;
    class TSWInputView;

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

        /** Evaluation time associated with delta/modified checks. */
        [[nodiscard]] engine_time_t evaluation_time() const noexcept;

        /** Binding and schema for the input-side TSData projection. */
        [[nodiscard]] const TSDataBinding *binding() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;
        /** Underlying TSData projection; empty for unbound peered terminals. */
        [[nodiscard]] const TSDataView &data_view() const noexcept;

        /** True when this view or at least one structural child has a current value. */
        [[nodiscard]] bool valid() const;

        /** True when this view and all required structural descendants have current values. */
        [[nodiscard]] bool all_valid() const;

        /** Latest modification time observed at this view, including structural children. */
        [[nodiscard]] engine_time_t last_modified_time() const;

        /** True when this view or a structural child was modified at the view's evaluation time. */
        [[nodiscard]] bool modified() const;

        [[nodiscard]] ValueView value() const;
        [[nodiscard]] ValueView delta_value() const;

        /** For bindable target-link views, true when an output target is bound. */
        [[nodiscard]] bool bound() const noexcept;

        /** True when this view is backed by a peered input node whose target can be rebound. */
        [[nodiscard]] bool is_bindable() const noexcept;

        void bind_output(const TSOutputView &output);
        void unbind_output();

        void make_active();
        void make_passive();
        [[nodiscard]] bool active() const;

        [[nodiscard]] TSSInputView as_set() &;
        [[nodiscard]] TSSInputView as_set() const &;
        void as_set() && = delete;
        void as_set() const && = delete;
        [[nodiscard]] TSDInputView as_dict() &;
        [[nodiscard]] TSDInputView as_dict() const &;
        void as_dict() && = delete;
        void as_dict() const && = delete;
        [[nodiscard]] TSBInputView as_bundle() &;
        [[nodiscard]] TSBInputView as_bundle() const &;
        void as_bundle() && = delete;
        void as_bundle() const && = delete;
        [[nodiscard]] TSLInputView as_list() &;
        [[nodiscard]] TSLInputView as_list() const &;
        void as_list() && = delete;
        void as_list() const && = delete;
        [[nodiscard]] TSWInputView as_window() &;
        [[nodiscard]] TSWInputView as_window() const &;
        void as_window() && = delete;
        void as_window() const && = delete;

      private:
        friend class TSInput;
        friend class TSBInputView;
        friend class TSLInputView;
        friend class TSSInputView;
        friend class TSDInputView;
        friend class TSWInputView;

        TSInputView(TSInput                         *input,
                    detail::TSInputNode            *node,
                    TSDataView                      target_view,
                    std::vector<std::size_t>        target_path,
                    Notifiable                     *scheduling_notifier,
                    engine_time_t                   evaluation_time) noexcept;

        [[nodiscard]] bool is_target_position() const noexcept;
        [[nodiscard]] bool target_view_live() const noexcept;
        [[nodiscard]] TSDataView &checked_target_data_view(const char *what) const;
        [[nodiscard]] TSInputView child_from_target(TSDataView child, std::size_t index) const;
        [[nodiscard]] TSInputView child_from_node(detail::TSInputNode *child) const noexcept;

        TSInput                  *input_{nullptr};
        detail::TSInputNode      *node_{nullptr};
        TSDataView                data_view_{};
        std::vector<std::size_t>  target_path_{};
        Notifiable               *scheduling_notifier_{nullptr};
        engine_time_t             evaluation_time_{MIN_DT};
    };

}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_BASE_VIEW_H

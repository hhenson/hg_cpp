#ifndef HGRAPH_CPP_TS_INPUT_DETAIL_H
#define HGRAPH_CPP_TS_INPUT_DETAIL_H

#include <hgraph/types/time_series/ts_input.h>

#include <hgraph/types/time_series/ts_input/target_link.h>

#include <cstddef>
#include <memory>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace hgraph::detail
{
    struct TSInputEndpointOps;
    struct TSInputActiveTarget;
    struct TSInputViewOps;

    struct TSInputChildProjection
    {
        TSDataView visible{};
        TSDataView target_link{};
    };

    [[nodiscard]] bool output_view_bound(const TSOutputView &output) noexcept;
    [[nodiscard]] const TSDataView &empty_ts_data_view() noexcept;
    void validate_input_view_kind(const TSValueTypeMetaData *schema, TSTypeKind expected, const char *what);
    [[nodiscard]] const TSInputEndpointOps &input_endpoint_ops_for(const TSValueTypeMetaData *schema);
    [[nodiscard]] const TSDataBinding *input_data_binding_for(const TSEndpointSchema &endpoint_schema);
    [[nodiscard]] const TSDataBinding *regular_ts_data_binding_for(const TSValueTypeMetaData *schema);

    [[nodiscard]] TSInputChildProjection input_child_projection(const TSDataView &parent, std::size_t index);

    struct TSInputViewOps
    {
        void make_active(TSInputView &view) const;
        void make_passive(TSInputView &view) const;
        [[nodiscard]] bool active(const TSInputView &view) const;
    };

    [[nodiscard]] const TSInputViewOps &input_view_ops() noexcept;

    struct TSInputEndpointOps
    {
        using child_count_fn = std::size_t (*)(const TSValueTypeMetaData *schema) noexcept;
        using key_at_fn = std::string_view (*)(const TSValueTypeMetaData *schema, std::size_t index) noexcept;
        using find_key_fn = std::size_t (*)(const TSValueTypeMetaData *schema, std::string_view name) noexcept;
        using child_schema_fn = const TSValueTypeMetaData *(*)(const TSValueTypeMetaData *schema,
                                                               std::size_t                index) noexcept;
        using target_child_fn = TSDataView (*)(TSDataView parent, std::size_t index);
        /** Convert a non-peered input projection of this shape to a reference token. */
        using reference_fn = TimeSeriesReference (*)(const TSInputView &view);
#if HGRAPH_ENABLE_PYTHON_USER_NODES
        using to_python_fn = nb::object (*)(const void *context, const void *memory);
        using delta_to_python_fn = nb::object (*)(const void *context,
                                                  const void *memory,
                                                  engine_time_t evaluation_time);
#endif

        const char      *name{nullptr};
        bool             supports_input_projection{false};
        bool             named_value_projection{false};
        char             value_open{'['};
        char             value_close{']'};
        child_count_fn   child_count{nullptr};
        key_at_fn        key_at{nullptr};
        find_key_fn      find_key{nullptr};
        child_schema_fn  child_schema{nullptr};
        target_child_fn  target_child{nullptr};
        reference_fn     reference{nullptr};
#if HGRAPH_ENABLE_PYTHON_USER_NODES
        to_python_fn       to_python{nullptr};
        delta_to_python_fn delta_to_python{nullptr};
#endif
    };

    struct TSInputSchedulingNotifier final : Notifiable
    {
        Notifiable *target{nullptr};

        void notify(engine_time_t modified_time) override;
    };

    struct TSInputActiveTarget
    {
        TSInputActiveTarget() noexcept;
        TSInputActiveTarget(TSInputActiveTarget *parent_, std::size_t slot_) noexcept;
        TSInputActiveTarget(const TSInputActiveTarget &) = delete;
        TSInputActiveTarget &operator=(const TSInputActiveTarget &) = delete;
        ~TSInputActiveTarget() noexcept;

        [[nodiscard]] TSInputActiveTarget *child_at(std::size_t slot) const noexcept;
        [[nodiscard]] bool has_any_active() const noexcept;
        TSInputActiveTarget &ensure_child(std::size_t slot);
        void subscribe(const TSDataView &observed_, Notifiable *target_notifier);
        void unsubscribe() noexcept;

        TSInputActiveTarget *parent{nullptr};
        std::size_t          slot{0};
        bool                 active{false};
        TSDataView           observed{};
        TSInputSchedulingNotifier notifier{};
        std::unordered_map<std::size_t, std::unique_ptr<TSInputActiveTarget>> children{};
    };

}  // namespace hgraph::detail

#endif  // HGRAPH_CPP_TS_INPUT_DETAIL_H

#include <hgraph/types/time_series/ts_output.h>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/util/scope.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph
{
    namespace
    {
        void validate_endpoint_kind(const TSValueTypeMetaData *schema, TSTypeKind expected, const char *what)
        {
            if (schema == nullptr || schema->kind != expected)
            {
                throw std::invalid_argument(std::string{what} + " requires a matching time-series shape");
            }
        }

        template <typename T>
        [[nodiscard]] Range<T> empty_range() noexcept
        {
            return Range<T>{.context = nullptr, .memory = nullptr, .limit = 0, .predicate = nullptr,
                            .projector = nullptr};
        }

        template <typename K, typename V>
        [[nodiscard]] KeyValueRange<K, V> empty_kv_range() noexcept
        {
            return KeyValueRange<K, V>{.context = nullptr, .memory = nullptr, .limit = 0, .predicate = nullptr,
                                       .projector = nullptr};
        }

        [[nodiscard]] std::string_view field_name_at(const TSValueTypeMetaData *schema,
                                                     std::size_t                index) noexcept
        {
            if (schema == nullptr || schema->kind != TSTypeKind::TSB || index >= schema->field_count())
            {
                return {};
            }
            const auto *name = schema->fields()[index].name;
            return name != nullptr ? std::string_view{name} : std::string_view{};
        }

        [[nodiscard]] bool tsb_output_valid_child(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSBOutputView *>(context)->at(index).valid();
        }

        [[nodiscard]] bool tsb_output_modified_child(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSBOutputView *>(context)->at(index).modified();
        }

        [[nodiscard]] TSOutputView tsb_output_project_value(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSBOutputView *>(context)->at(index);
        }

        [[nodiscard]] std::pair<std::string_view, TSOutputView> tsb_output_project_item(
            const void *context,
            const void *,
            std::size_t index)
        {
            const auto *view = static_cast<const TSBOutputView *>(context);
            return {field_name_at(view->schema(), index), view->at(index)};
        }

        [[nodiscard]] bool tsl_output_valid_child(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSLOutputView *>(context)->at(index).valid();
        }

        [[nodiscard]] bool tsl_output_modified_child(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSLOutputView *>(context)->at(index).modified();
        }

        [[nodiscard]] TSOutputView tsl_output_project_value(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSLOutputView *>(context)->at(index);
        }

        [[nodiscard]] std::pair<std::size_t, TSOutputView> tsl_output_project_item(
            const void *context,
            const void *,
            std::size_t index)
        {
            return {index, static_cast<const TSLOutputView *>(context)->at(index)};
        }

        [[nodiscard]] bool tsd_output_live_slot(const void *context, const void *, std::size_t slot)
        {
            return static_cast<const TSDOutputView *>(context)->slot_live(slot);
        }

        [[nodiscard]] bool tsd_output_valid_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *view = static_cast<const TSDOutputView *>(context);
            return view->slot_live(slot) && view->at_slot(slot).valid();
        }

        [[nodiscard]] bool tsd_output_modified_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *view = static_cast<const TSDOutputView *>(context);
            return view->slot_live(slot) && view->slot_modified(slot);
        }

        [[nodiscard]] bool tsd_output_added_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *view = static_cast<const TSDOutputView *>(context);
            return view->slot_occupied(slot) && view->slot_added(slot);
        }

        [[nodiscard]] bool tsd_output_removed_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *view = static_cast<const TSDOutputView *>(context);
            return view->slot_occupied(slot) && view->slot_removed(slot);
        }

        [[nodiscard]] TSOutputView tsd_output_project_value(const void *context, const void *, std::size_t slot)
        {
            return static_cast<const TSDOutputView *>(context)->at_slot(slot);
        }

        [[nodiscard]] std::pair<ValueView, TSOutputView> tsd_output_project_item(
            const void *context,
            const void *,
            std::size_t slot)
        {
            const auto *view = static_cast<const TSDOutputView *>(context);
            return {view->key_at_slot(slot), view->at_slot(slot)};
        }
    }  // namespace

    TSOutput::TSOutput() noexcept = default;

    TSOutput::TSOutput(const TSDataBinding &binding)
        : data_(binding)
    {
        attach_root_parent();
    }

    TSOutput::TSOutput(const TSValueTypeMetaData &schema)
        : TSOutput(checked_binding_for(&schema))
    {
    }

    TSOutput::TSOutput(const TSValueTypeMetaData *schema)
        : TSOutput(checked_binding_for(schema))
    {
    }

    TSOutput::TSOutput(const TSOutput &other)
        : data_(copyable_data(other))
    {
        attach_root_parent();
    }

    TSOutput &TSOutput::operator=(const TSOutput &other)
    {
        if (this != &other)
        {
            data_ = other.data_;
            dirty_ = false;
            attach_root_parent();
        }
        return *this;
    }

    TSOutput::TSOutput(TSOutput &&other) noexcept
        : data_(std::move(other.data_)),
          dirty_(std::exchange(other.dirty_, false))
    {
        attach_root_parent();
    }

    TSOutput &TSOutput::operator=(TSOutput &&other) noexcept
    {
        if (this != &other)
        {
            data_ = std::move(other.data_);
            dirty_ = std::exchange(other.dirty_, false);
            attach_root_parent();
        }
        return *this;
    }

    bool TSOutput::has_value() const noexcept
    {
        return data_.has_value();
    }

    const TSDataBinding *TSOutput::binding() const noexcept
    {
        return data_.binding();
    }

    const TSValueTypeMetaData *TSOutput::schema() const noexcept
    {
        return data_.schema();
    }

    TSDataView TSOutput::data_view()
    {
        return data_.view();
    }

    TSDataView TSOutput::data_view() const
    {
        return data_.view();
    }

    bool TSOutput::dirty() const noexcept
    {
        return dirty_;
    }

    void TSOutput::cleanup_delta()
    {
        if (!dirty_) { return; }

        auto root = data_view();
        const auto modified_time = root.last_modified_time();
        root.cleanup_delta(modified_time);
        dirty_ = false;
    }

    void TSOutput::clear_dirty() noexcept
    {
        dirty_ = false;
    }

    void TSOutput::subscribe(Notifiable *observer)
    {
        if (!has_value()) { throw std::logic_error("TSOutput::subscribe requires a bound output"); }
        data_view().subscribe(observer);
    }

    void TSOutput::unsubscribe(Notifiable *observer)
    {
        if (!has_value()) { throw std::logic_error("TSOutput::unsubscribe requires a bound output"); }
        data_view().unsubscribe(observer);
    }

    TSOutputView TSOutput::view(engine_time_t evaluation_time)
    {
        return TSOutputView{this, data_view(), evaluation_time};
    }

    TSOutputView TSOutput::view(engine_time_t evaluation_time) const
    {
        return TSOutputView{this, data_view(), evaluation_time};
    }

    const TSDataBinding &TSOutput::checked_binding_for(const TSValueTypeMetaData *schema)
    {
        if (schema == nullptr) { throw std::invalid_argument("TSOutput requires a time-series schema"); }
        const auto *binding = TSDataPlanFactory::instance().binding_for(schema);
        if (binding == nullptr) { throw std::logic_error("TSOutput could not resolve a TSData binding"); }
        return *binding;
    }

    const TSData &TSOutput::copyable_data(const TSOutput &other)
    {
        return other.data_;
    }

    void TSOutput::attach_root_parent()
    {
        if (has_value()) { data_view().bind_parent(*this, TS_DATA_NO_CHILD_ID); }
    }

    void TSOutput::record_child_modified(std::size_t, engine_time_t)
    {
        dirty_ = true;
    }

    TSOutputMutationView TSOutput::begin_mutation(engine_time_t evaluation_time)
    {
        return TSOutputMutationView{*this, evaluation_time};
    }

    TSDataMutationView TSOutputMutationView::begin_root_mutation(TSOutput &output, engine_time_t evaluation_time)
    {
        if (evaluation_time == MIN_DT) { throw std::invalid_argument("TSOutput mutation requires a concrete time"); }
        if (!output.has_value()) { throw std::logic_error("TSOutput mutation requires a bound output"); }
        return output.data_view().begin_mutation(evaluation_time);
    }

    TSOutputMutationView::TSOutputMutationView(TSOutput &output, engine_time_t evaluation_time)
        : mutation_(begin_root_mutation(output, evaluation_time))
    {
    }

    TSOutputMutationView::TSOutputMutationView(TSOutputMutationView &&) noexcept = default;

    TSOutputMutationView::~TSOutputMutationView() noexcept = default;

    TSDataMutationView &TSOutputMutationView::data_mutation() noexcept
    {
        return mutation_;
    }

    const TSDataMutationView &TSOutputMutationView::data_mutation() const noexcept
    {
        return mutation_;
    }

    ValueView TSOutputMutationView::value() const
    {
        return mutation_.value();
    }

    ValueView TSOutputMutationView::delta_value() const
    {
        return mutation_.delta_value(current_mutation_time());
    }

    engine_time_t TSOutputMutationView::current_mutation_time() const
    {
        return mutation_.current_mutation_time();
    }

    bool TSOutputMutationView::modified() const
    {
        return mutation_.modified(current_mutation_time());
    }

    void TSOutputMutationView::mark_modified()
    {
        mutation_.mark_modified();
    }

    bool TSOutputMutationView::copy_value_from(const ValueView &source)
    {
        return mutation_.copy_value_from(source);
    }

    TSOutputView::TSOutputView() noexcept = default;

    TSOutputView::TSOutputView(const TSOutput *output, TSDataView data, engine_time_t evaluation_time) noexcept
        : output_(output),
          data_(data),
          evaluation_time_(evaluation_time)
    {
    }

    const TSOutput *TSOutputView::output() const noexcept
    {
        return output_;
    }

    const TSDataView &TSOutputView::data_view() const noexcept
    {
        return data_;
    }

    TSDataView &TSOutputView::data_view() noexcept
    {
        return data_;
    }

    engine_time_t TSOutputView::evaluation_time() const noexcept
    {
        return evaluation_time_;
    }

    const TSDataBinding *TSOutputView::binding() const noexcept
    {
        return data_.binding();
    }

    const TSValueTypeMetaData *TSOutputView::schema() const noexcept
    {
        return data_.schema();
    }

    bool TSOutputView::bound() const noexcept
    {
        return output_ != nullptr && data_.valid();
    }

    ValueView TSOutputView::value() const
    {
        return data_.valid() ? data_.value() : ValueView{};
    }

    ValueView TSOutputView::delta_value() const
    {
        return data_.valid() ? data_.delta_value(evaluation_time_) : ValueView{};
    }

    engine_time_t TSOutputView::last_modified_time() const
    {
        return data_.valid() ? data_.last_modified_time() : MIN_DT;
    }

    bool TSOutputView::modified() const
    {
        return evaluation_time_ != MIN_DT && data_.valid() && data_.modified(evaluation_time_);
    }

    bool TSOutputView::valid() const
    {
        return data_.valid() && data_.has_current_value();
    }

    bool TSOutputView::all_valid() const
    {
        return data_.valid() && data_.all_valid();
    }

    void TSOutputView::subscribe(Notifiable *observer) const
    {
        if (!data_.valid()) { throw std::logic_error("TSOutputView::subscribe requires a bound view"); }
        data_.subscribe(observer);
    }

    void TSOutputView::unsubscribe(Notifiable *observer) const
    {
        if (!data_.valid()) { throw std::logic_error("TSOutputView::unsubscribe requires a bound view"); }
        data_.unsubscribe(observer);
    }

    TSDataMutationView TSOutputView::begin_mutation(engine_time_t evaluation_time) const
    {
        if (!data_.valid()) { throw std::logic_error("TSOutputView::begin_mutation requires a bound view"); }
        return data_.begin_mutation(evaluation_time);
    }

    TSSOutputView TSOutputView::as_set() &
    {
        return TSSOutputView{*this};
    }

    TSSOutputView TSOutputView::as_set() const &
    {
        return TSSOutputView{*this};
    }

    TSDOutputView TSOutputView::as_dict() &
    {
        return TSDOutputView{*this};
    }

    TSDOutputView TSOutputView::as_dict() const &
    {
        return TSDOutputView{*this};
    }

    TSBOutputView TSOutputView::as_bundle() &
    {
        return TSBOutputView{*this};
    }

    TSBOutputView TSOutputView::as_bundle() const &
    {
        return TSBOutputView{*this};
    }

    TSLOutputView TSOutputView::as_list() &
    {
        return TSLOutputView{*this};
    }

    TSLOutputView TSOutputView::as_list() const &
    {
        return TSLOutputView{*this};
    }

    TSWOutputView TSOutputView::as_window() &
    {
        return TSWOutputView{*this};
    }

    TSWOutputView TSOutputView::as_window() const &
    {
        return TSWOutputView{*this};
    }

    TSBOutputView::TSBOutputView(TSOutputView view)
        : TSOutputTypedView<TSBOutputView>(std::move(view))
    {
        validate_endpoint_kind(schema(), TSTypeKind::TSB, "TSBOutputView");
    }

    TSBDataView TSBOutputView::data_view() const
    {
        return view_.data_view().as_bundle();
    }

    std::size_t TSBOutputView::size() const
    {
        return data_view().size();
    }

    bool TSBOutputView::empty() const
    {
        return data_view().empty();
    }

    bool TSBOutputView::has_field(std::string_view name) const noexcept
    {
        return fallback_on_exception(false, [&] { return data_view().has_field(name); });
    }

    Range<std::string_view> TSBOutputView::keys() const
    {
        return data_view().keys();
    }

    Range<TSOutputView> TSBOutputView::values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = size(), .predicate = nullptr,
                                   .projector = &tsb_output_project_value};
    }

    Range<TSOutputView> TSBOutputView::valid_values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = size(),
                                   .predicate = &tsb_output_valid_child,
                                   .projector = &tsb_output_project_value};
    }

    Range<TSOutputView> TSBOutputView::modified_values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = size(),
                                   .predicate = &tsb_output_modified_child,
                                   .projector = &tsb_output_project_value};
    }

    KeyValueRange<std::string_view, TSOutputView> TSBOutputView::items() const
    {
        return KeyValueRange<std::string_view, TSOutputView>{.context = this,
                                                             .memory = nullptr,
                                                             .limit = size(),
                                                             .predicate = nullptr,
                                                             .projector = &tsb_output_project_item};
    }

    KeyValueRange<std::string_view, TSOutputView> TSBOutputView::valid_items() const
    {
        return KeyValueRange<std::string_view, TSOutputView>{.context = this,
                                                             .memory = nullptr,
                                                             .limit = size(),
                                                             .predicate = &tsb_output_valid_child,
                                                             .projector = &tsb_output_project_item};
    }

    KeyValueRange<std::string_view, TSOutputView> TSBOutputView::modified_items() const
    {
        return KeyValueRange<std::string_view, TSOutputView>{.context = this,
                                                             .memory = nullptr,
                                                             .limit = size(),
                                                             .predicate = &tsb_output_modified_child,
                                                             .projector = &tsb_output_project_item};
    }

    TSOutputView TSBOutputView::at(std::size_t index) &
    {
        auto data = data_view();
        return TSOutputView{view_.output(), data.at(index), view_.evaluation_time()};
    }

    TSOutputView TSBOutputView::at(std::size_t index) const &
    {
        return const_cast<TSBOutputView *>(this)->at(index);
    }

    TSOutputView TSBOutputView::operator[](std::size_t index) &
    {
        return at(index);
    }

    TSOutputView TSBOutputView::operator[](std::size_t index) const &
    {
        return at(index);
    }

    TSOutputView TSBOutputView::at(std::string_view name) &
    {
        auto data = data_view();
        return TSOutputView{view_.output(), data.at(name), view_.evaluation_time()};
    }

    TSOutputView TSBOutputView::at(std::string_view name) const &
    {
        return const_cast<TSBOutputView *>(this)->at(name);
    }

    TSOutputView TSBOutputView::field(std::string_view name) &
    {
        return at(name);
    }

    TSOutputView TSBOutputView::field(std::string_view name) const &
    {
        return at(name);
    }

    TSOutputView TSBOutputView::operator[](std::string_view name) &
    {
        return at(name);
    }

    TSOutputView TSBOutputView::operator[](std::string_view name) const &
    {
        return at(name);
    }

    TSLOutputView::TSLOutputView(TSOutputView view)
        : TSOutputTypedView<TSLOutputView>(std::move(view))
    {
        validate_endpoint_kind(schema(), TSTypeKind::TSL, "TSLOutputView");
    }

    TSLDataView TSLOutputView::data_view() const
    {
        return view_.data_view().as_list();
    }

    std::size_t TSLOutputView::size() const
    {
        return data_view().size();
    }

    bool TSLOutputView::empty() const
    {
        return data_view().empty();
    }

    Range<TSOutputView> TSLOutputView::values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = size(), .predicate = nullptr,
                                   .projector = &tsl_output_project_value};
    }

    Range<TSOutputView> TSLOutputView::valid_values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = size(),
                                   .predicate = &tsl_output_valid_child,
                                   .projector = &tsl_output_project_value};
    }

    Range<TSOutputView> TSLOutputView::modified_values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = size(),
                                   .predicate = &tsl_output_modified_child,
                                   .projector = &tsl_output_project_value};
    }

    KeyValueRange<std::size_t, TSOutputView> TSLOutputView::items() const
    {
        return KeyValueRange<std::size_t, TSOutputView>{.context = this,
                                                        .memory = nullptr,
                                                        .limit = size(),
                                                        .predicate = nullptr,
                                                        .projector = &tsl_output_project_item};
    }

    KeyValueRange<std::size_t, TSOutputView> TSLOutputView::valid_items() const
    {
        return KeyValueRange<std::size_t, TSOutputView>{.context = this,
                                                        .memory = nullptr,
                                                        .limit = size(),
                                                        .predicate = &tsl_output_valid_child,
                                                        .projector = &tsl_output_project_item};
    }

    KeyValueRange<std::size_t, TSOutputView> TSLOutputView::modified_items() const
    {
        return KeyValueRange<std::size_t, TSOutputView>{.context = this,
                                                        .memory = nullptr,
                                                        .limit = size(),
                                                        .predicate = &tsl_output_modified_child,
                                                        .projector = &tsl_output_project_item};
    }

    TSOutputView TSLOutputView::at(std::size_t index) &
    {
        auto data = data_view();
        return TSOutputView{view_.output(), data.at(index), view_.evaluation_time()};
    }

    TSOutputView TSLOutputView::at(std::size_t index) const &
    {
        return const_cast<TSLOutputView *>(this)->at(index);
    }

    TSOutputView TSLOutputView::operator[](std::size_t index) &
    {
        return at(index);
    }

    TSOutputView TSLOutputView::operator[](std::size_t index) const &
    {
        return at(index);
    }

    TSSOutputView::TSSOutputView(TSOutputView view)
        : TSOutputTypedView<TSSOutputView>(std::move(view))
    {
        validate_endpoint_kind(schema(), TSTypeKind::TSS, "TSSOutputView");
    }

    TSSDataView TSSOutputView::data_view() const
    {
        return view_.data_view().as_set();
    }

    std::size_t TSSOutputView::size() const { return data_view().size(); }
    bool TSSOutputView::empty() const { return data_view().empty(); }
    std::size_t TSSOutputView::slot_capacity() const { return data_view().slot_capacity(); }
    bool TSSOutputView::slot_occupied(std::size_t slot) const { return data_view().slot_occupied(slot); }
    bool TSSOutputView::slot_live(std::size_t slot) const { return data_view().slot_live(slot); }
    bool TSSOutputView::slot_added(std::size_t slot) const { return data_view().slot_added(slot); }
    bool TSSOutputView::slot_removed(std::size_t slot) const { return data_view().slot_removed(slot); }
    ValueView TSSOutputView::at_slot(std::size_t slot) const { return data_view().at_slot(slot); }
    bool TSSOutputView::contains(const ValueView &key) const { return data_view().contains(key); }
    std::size_t TSSOutputView::find_slot(const ValueView &key) const { return data_view().find_slot(key); }
    Range<ValueView> TSSOutputView::values() const { return data_view().values(); }
    Range<ValueView> TSSOutputView::added() const { return data_view().added(); }
    Range<ValueView> TSSOutputView::removed() const { return data_view().removed(); }
    Range<ValueView> TSSOutputView::added_values() const { return data_view().added_values(); }
    Range<ValueView> TSSOutputView::removed_values() const { return data_view().removed_values(); }
    Range<ValueView>::iterator TSSOutputView::begin() const { return data_view().begin(); }
    Range<ValueView>::iterator TSSOutputView::end() const { return data_view().end(); }
    TSSDataMutationView TSSOutputView::begin_mutation(engine_time_t evaluation_time) const
    {
        return data_view().begin_mutation(evaluation_time);
    }

    TSDOutputView::TSDOutputView(TSOutputView view)
        : TSOutputTypedView<TSDOutputView>(std::move(view))
    {
        validate_endpoint_kind(schema(), TSTypeKind::TSD, "TSDOutputView");
    }

    TSDDataView TSDOutputView::data_view() const
    {
        return view_.data_view().as_dict();
    }

    std::size_t TSDOutputView::size() const { return data_view().size(); }
    bool TSDOutputView::empty() const { return data_view().empty(); }
    std::size_t TSDOutputView::slot_capacity() const { return data_view().slot_capacity(); }
    bool TSDOutputView::slot_occupied(std::size_t slot) const { return data_view().slot_occupied(slot); }
    bool TSDOutputView::slot_live(std::size_t slot) const { return data_view().slot_live(slot); }
    bool TSDOutputView::slot_added(std::size_t slot) const { return data_view().slot_added(slot); }
    bool TSDOutputView::slot_removed(std::size_t slot) const { return data_view().slot_removed(slot); }
    bool TSDOutputView::slot_modified(std::size_t slot) const { return data_view().slot_modified(slot); }
    ValueView TSDOutputView::key_at_slot(std::size_t slot) const { return data_view().key_at_slot(slot); }
    TSOutputView TSDOutputView::at_slot(std::size_t slot) const
    {
        return TSOutputView{view_.output(), data_view().at_slot(slot), view_.evaluation_time()};
    }
    bool TSDOutputView::contains(const ValueView &key) const { return data_view().contains(key); }
    std::size_t TSDOutputView::find_slot(const ValueView &key) const { return data_view().find_slot(key); }
    TSOutputView TSDOutputView::at(const ValueView &key) const
    {
        return TSOutputView{view_.output(), data_view().at(key), view_.evaluation_time()};
    }
    TSOutputView TSDOutputView::operator[](const ValueView &key) const { return at(key); }
    Range<ValueView> TSDOutputView::keys() const { return data_view().keys(); }

    Range<TSOutputView> TSDOutputView::values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                   .predicate = &tsd_output_live_slot,
                                   .projector = &tsd_output_project_value};
    }

    KeyValueRange<ValueView, TSOutputView> TSDOutputView::items() const
    {
        return KeyValueRange<ValueView, TSOutputView>{.context = this,
                                                      .memory = nullptr,
                                                      .limit = slot_capacity(),
                                                      .predicate = &tsd_output_live_slot,
                                                      .projector = &tsd_output_project_item};
    }

    Range<ValueView> TSDOutputView::valid_keys() const { return data_view().valid_keys(); }

    Range<TSOutputView> TSDOutputView::valid_values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                   .predicate = &tsd_output_valid_slot,
                                   .projector = &tsd_output_project_value};
    }

    KeyValueRange<ValueView, TSOutputView> TSDOutputView::valid_items() const
    {
        return KeyValueRange<ValueView, TSOutputView>{.context = this,
                                                      .memory = nullptr,
                                                      .limit = slot_capacity(),
                                                      .predicate = &tsd_output_valid_slot,
                                                      .projector = &tsd_output_project_item};
    }

    Range<ValueView> TSDOutputView::modified_keys() const { return data_view().modified_keys(view_.evaluation_time()); }

    Range<TSOutputView> TSDOutputView::modified_values() const
    {
        if (!modified()) { return empty_range<TSOutputView>(); }
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                   .predicate = &tsd_output_modified_slot,
                                   .projector = &tsd_output_project_value};
    }

    KeyValueRange<ValueView, TSOutputView> TSDOutputView::modified_items() const
    {
        if (!modified()) { return empty_kv_range<ValueView, TSOutputView>(); }
        return KeyValueRange<ValueView, TSOutputView>{.context = this,
                                                      .memory = nullptr,
                                                      .limit = slot_capacity(),
                                                      .predicate = &tsd_output_modified_slot,
                                                      .projector = &tsd_output_project_item};
    }

    Range<ValueView> TSDOutputView::added_keys() const { return data_view().added_keys(); }

    Range<TSOutputView> TSDOutputView::added_values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                   .predicate = &tsd_output_added_slot,
                                   .projector = &tsd_output_project_value};
    }

    KeyValueRange<ValueView, TSOutputView> TSDOutputView::added_items() const
    {
        return KeyValueRange<ValueView, TSOutputView>{.context = this,
                                                      .memory = nullptr,
                                                      .limit = slot_capacity(),
                                                      .predicate = &tsd_output_added_slot,
                                                      .projector = &tsd_output_project_item};
    }

    Range<ValueView> TSDOutputView::removed_keys() const { return data_view().removed_keys(); }

    Range<TSOutputView> TSDOutputView::removed_values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                   .predicate = &tsd_output_removed_slot,
                                   .projector = &tsd_output_project_value};
    }

    KeyValueRange<ValueView, TSOutputView> TSDOutputView::removed_items() const
    {
        return KeyValueRange<ValueView, TSOutputView>{.context = this,
                                                      .memory = nullptr,
                                                      .limit = slot_capacity(),
                                                      .predicate = &tsd_output_removed_slot,
                                                      .projector = &tsd_output_project_item};
    }

    TSDDataMutationView TSDOutputView::begin_mutation(engine_time_t evaluation_time) const
    {
        return data_view().begin_mutation(evaluation_time);
    }

    TSWOutputView::TSWOutputView(TSOutputView view)
        : TSOutputTypedView<TSWOutputView>(std::move(view))
    {
        validate_endpoint_kind(schema(), TSTypeKind::TSW, "TSWOutputView");
    }

    TSWDataView TSWOutputView::data_view() const
    {
        return view_.data_view().as_window();
    }

    bool TSWOutputView::duration_based() const noexcept
    {
        return fallback_on_exception(false, [&] { return data_view().duration_based(); });
    }
    bool TSWOutputView::size_based() const noexcept
    {
        return fallback_on_exception(false, [&] { return data_view().size_based(); });
    }
    bool TSWOutputView::time_based() const noexcept
    {
        return fallback_on_exception(false, [&] { return data_view().time_based(); });
    }
    std::size_t TSWOutputView::period() const { return data_view().period(); }
    std::size_t TSWOutputView::min_period() const { return data_view().min_period(); }
    engine_time_delta_t TSWOutputView::time_range() const { return data_view().time_range(); }
    engine_time_delta_t TSWOutputView::min_time_range() const { return data_view().min_time_range(); }
    std::size_t TSWOutputView::capacity() const { return data_view().capacity(); }
    std::size_t TSWOutputView::size() const { return data_view().size(); }
    bool TSWOutputView::empty() const { return data_view().empty(); }
    bool TSWOutputView::full() const { return data_view().full(); }
    engine_time_t TSWOutputView::first_modified_time() const { return data_view().first_modified_time(); }
    engine_time_t TSWOutputView::time_at(std::size_t index) const { return data_view().time_at(index); }
    ValueView TSWOutputView::time_value_at(std::size_t index) const { return data_view().time_value_at(index); }
    ValueView TSWOutputView::at(std::size_t index) const { return data_view().at(index); }
    ValueView TSWOutputView::operator[](std::size_t index) const { return data_view()[index]; }
    ValueView TSWOutputView::front() const { return data_view().front(); }
    ValueView TSWOutputView::back() const { return data_view().back(); }
    Range<ValueView> TSWOutputView::values() const { return data_view().values(); }
    Range<ValueView> TSWOutputView::time_values() const { return data_view().time_values(); }
    Range<engine_time_t> TSWOutputView::value_times() const { return data_view().value_times(); }
    Range<ValueView>::iterator TSWOutputView::begin() const { return data_view().begin(); }
    Range<ValueView>::iterator TSWOutputView::end() const { return data_view().end(); }
    TSWDataMutationView TSWOutputView::begin_mutation(engine_time_t evaluation_time) const
    {
        return data_view().begin_mutation(evaluation_time);
    }
}  // namespace hgraph

#include <hgraph/types/time_series/ts_output.h>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/util/scope.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph
{
    TSOutput::TSOutput() noexcept = default;

    TSOutput::TSOutput(const TSDataBinding &binding)
        : data_(binding)
    {
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
    }

    TSOutput &TSOutput::operator=(const TSOutput &other)
    {
        if (this != &other)
        {
            require_not_mutating("TSOutput copy target");
            other.require_not_mutating("TSOutput copy source");
            data_ = other.data_;
            clear_mutation_state();
        }
        return *this;
    }

    TSOutput::TSOutput(TSOutput &&other) noexcept
        : data_(std::move(other.data_))
    {
        other.clear_mutation_state();
    }

    TSOutput &TSOutput::operator=(TSOutput &&other) noexcept
    {
        if (this != &other)
        {
            data_ = std::move(other.data_);
            clear_mutation_state();
            other.clear_mutation_state();
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

    std::size_t TSOutput::mutation_depth() const noexcept
    {
        return mutation_depth_;
    }

    bool TSOutput::mutation_active() const noexcept
    {
        return mutation_depth_ != 0;
    }

    engine_time_t TSOutput::current_mutation_time() const noexcept
    {
        return mutation_time_;
    }

    TSOutputView TSOutput::view(engine_time_t evaluation_time)
    {
        return TSOutputView{this, data_view(), evaluation_time};
    }

    TSOutputView TSOutput::view(engine_time_t evaluation_time) const
    {
        return TSOutputView{this, data_view(), evaluation_time};
    }

    TSOutputMutationView TSOutput::begin_mutation(engine_time_t evaluation_time)
    {
        return TSOutputMutationView{*this, evaluation_time};
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
        other.require_not_mutating("TSOutput copy source");
        return other.data_;
    }

    void TSOutput::require_not_mutating(const char *what) const
    {
        if (mutation_active()) { throw std::logic_error(std::string{what} + " requires no active mutation"); }
    }

    void TSOutput::begin_mutation_scope(engine_time_t evaluation_time)
    {
        if (evaluation_time == MIN_DT) { throw std::invalid_argument("TSOutput mutation requires a concrete time"); }
        if (!has_value()) { throw std::logic_error("TSOutput mutation requires a bound output"); }
        if (mutation_depth_ != 0 && mutation_time_ != evaluation_time)
        {
            throw std::logic_error("TSOutput nested mutation requires the same engine time");
        }
        if (mutation_depth_ == 0) { mutation_time_ = evaluation_time; }
        ++mutation_depth_;
    }

    void TSOutput::end_mutation_scope() noexcept
    {
        if (mutation_depth_ == 0) { return; }
        --mutation_depth_;
        if (mutation_depth_ == 0) { mutation_time_ = MIN_DT; }
    }

    void TSOutput::clear_mutation_state() noexcept
    {
        mutation_depth_ = 0;
        mutation_time_  = MIN_DT;
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

    ValueView TSOutputView::delta_value(engine_time_t evaluation_time) const
    {
        return data_.valid() ? data_.delta_value(evaluation_time) : ValueView{};
    }

    engine_time_t TSOutputView::last_modified_time() const
    {
        return data_.valid() ? data_.last_modified_time() : MIN_DT;
    }

    bool TSOutputView::modified() const
    {
        return evaluation_time_ != MIN_DT && data_.valid() && data_.modified(evaluation_time_);
    }

    bool TSOutputView::modified(engine_time_t evaluation_time) const
    {
        return data_.valid() && data_.modified(evaluation_time);
    }

    bool TSOutputView::valid() const
    {
        return data_.valid() && data_.has_current_value();
    }

    bool TSOutputView::all_valid() const
    {
        return data_.valid() && data_.all_valid();
    }

    TSSDataView TSOutputView::as_set() &
    {
        return data_.as_set();
    }

    TSSDataView TSOutputView::as_set() const &
    {
        return data_.as_set();
    }

    TSDDataView TSOutputView::as_dict() &
    {
        return data_.as_dict();
    }

    TSDDataView TSOutputView::as_dict() const &
    {
        return data_.as_dict();
    }

    TSBDataView TSOutputView::as_bundle() &
    {
        return data_.as_bundle();
    }

    TSBDataView TSOutputView::as_bundle() const &
    {
        return data_.as_bundle();
    }

    TSLDataView TSOutputView::as_list() &
    {
        return data_.as_list();
    }

    TSLDataView TSOutputView::as_list() const &
    {
        return data_.as_list();
    }

    TSWDataView TSOutputView::as_window() &
    {
        return data_.as_window();
    }

    TSWDataView TSOutputView::as_window() const &
    {
        return data_.as_window();
    }

    TSOutputMutationView::TSOutputMutationView(TSOutput &output, engine_time_t evaluation_time)
        : output_(&output),
          mutation_(begin_root_mutation(output, evaluation_time))
    {
    }

    TSOutputMutationView::TSOutputMutationView(TSOutputMutationView &&other) noexcept
        : output_(std::exchange(other.output_, nullptr)),
          mutation_(std::move(other.mutation_))
    {
    }

    TSOutputMutationView::~TSOutputMutationView() noexcept
    {
        if (output_ != nullptr) { output_->end_mutation_scope(); }
    }

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

    ValueView TSOutputMutationView::delta_value(engine_time_t evaluation_time) const
    {
        return mutation_.delta_value(evaluation_time);
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

    TSDataMutationView TSOutputMutationView::begin_root_mutation(TSOutput &output, engine_time_t evaluation_time)
    {
        output.begin_mutation_scope(evaluation_time);
        auto rollback = UnwindCleanupGuard{[&output]() noexcept { output.end_mutation_scope(); }};
        return output.data_view().begin_mutation(evaluation_time);
    }
}  // namespace hgraph

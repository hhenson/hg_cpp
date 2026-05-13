#include <hgraph/types/time_series/ts_output.h>

#include <hgraph/types/metadata/ts_data_plan_factory.h>

#include <stdexcept>
#include <utility>

namespace hgraph
{
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

}  // namespace hgraph

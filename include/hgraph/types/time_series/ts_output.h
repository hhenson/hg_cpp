#ifndef HGRAPH_CPP_ROOT_TS_OUTPUT_H
#define HGRAPH_CPP_ROOT_TS_OUTPUT_H

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/util/date_time.h>
#include <hgraph/util/scope.h>

#include <cstddef>
#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph
{
    class TSOutputView;
    class TSOutputMutationView;

    /**
     * Owning output-side time-series endpoint.
     *
     * ``TSOutput`` is the first top-level holder over TSData. It owns the
     * root payload/delta storage and the root mutation scope state. External
     * subscriber fan-out and input binding are layered above this class.
     */
    class TSOutput
    {
      public:
        TSOutput() noexcept = default;

        explicit TSOutput(const TSDataBinding &binding)
            : data_(binding)
        {
        }

        explicit TSOutput(const TSValueTypeMetaData &schema)
            : TSOutput(checked_binding_for(&schema))
        {
        }

        explicit TSOutput(const TSValueTypeMetaData *schema)
            : TSOutput(checked_binding_for(schema))
        {
        }

        TSOutput(const TSOutput &other)
            : data_(copyable_data(other))
        {
        }

        TSOutput &operator=(const TSOutput &other)
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

        TSOutput(TSOutput &&other) noexcept
            : data_(std::move(other.data_))
        {
            other.clear_mutation_state();
        }

        TSOutput &operator=(TSOutput &&other) noexcept
        {
            if (this != &other)
            {
                data_ = std::move(other.data_);
                clear_mutation_state();
                other.clear_mutation_state();
            }
            return *this;
        }

        [[nodiscard]] bool has_value() const noexcept { return data_.has_value(); }
        [[nodiscard]] const TSDataBinding *binding() const noexcept { return data_.binding(); }
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept { return data_.schema(); }
        [[nodiscard]] TSDataView data_view() { return data_.view(); }
        [[nodiscard]] TSDataView data_view() const { return data_.view(); }

        [[nodiscard]] std::size_t mutation_depth() const noexcept { return mutation_depth_; }
        [[nodiscard]] bool mutation_active() const noexcept { return mutation_depth_ != 0; }
        [[nodiscard]] engine_time_t current_mutation_time() const noexcept { return mutation_time_; }

        [[nodiscard]] TSOutputView view(engine_time_t evaluation_time = MIN_DT);
        [[nodiscard]] TSOutputView view(engine_time_t evaluation_time = MIN_DT) const;
        [[nodiscard]] TSOutputMutationView begin_mutation(engine_time_t evaluation_time);

      private:
        friend class TSOutputMutationView;

        static const TSDataBinding &checked_binding_for(const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr) { throw std::invalid_argument("TSOutput requires a time-series schema"); }
            const auto *binding = TSDataPlanFactory::instance().binding_for(schema);
            if (binding == nullptr) { throw std::logic_error("TSOutput could not resolve a TSData binding"); }
            return *binding;
        }

        static const TSData &copyable_data(const TSOutput &other)
        {
            other.require_not_mutating("TSOutput copy source");
            return other.data_;
        }

        void require_not_mutating(const char *what) const
        {
            if (mutation_active()) { throw std::logic_error(std::string{what} + " requires no active mutation"); }
        }

        void begin_mutation_scope(engine_time_t evaluation_time)
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

        void end_mutation_scope() noexcept
        {
            if (mutation_depth_ == 0) { return; }
            --mutation_depth_;
            if (mutation_depth_ == 0) { mutation_time_ = MIN_DT; }
        }

        void clear_mutation_state() noexcept
        {
            mutation_depth_ = 0;
            mutation_time_  = MIN_DT;
        }

        TSData        data_{};
        std::size_t   mutation_depth_{0};
        engine_time_t mutation_time_{MIN_DT};
    };

    /**
     * Read-only endpoint view carrying an evaluation time.
     *
     * ``TSDataView::valid()`` means "live handle"; this view exposes the
     * time-series validity rule where ``MIN_DT`` means no current value.
     */
    class TSOutputView
    {
      public:
        TSOutputView() noexcept = default;

        TSOutputView(const TSOutput *output, TSDataView data, engine_time_t evaluation_time) noexcept
            : output_(output),
              data_(data),
              evaluation_time_(evaluation_time)
        {
        }

        [[nodiscard]] const TSOutput *output() const noexcept { return output_; }
        [[nodiscard]] const TSDataView &data_view() const noexcept { return data_; }
        [[nodiscard]] TSDataView &data_view() noexcept { return data_; }
        [[nodiscard]] engine_time_t evaluation_time() const noexcept { return evaluation_time_; }
        [[nodiscard]] const TSDataBinding *binding() const noexcept { return data_.binding(); }
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept { return data_.schema(); }
        [[nodiscard]] bool bound() const noexcept { return output_ != nullptr && data_.valid(); }

        [[nodiscard]] ValueView value() const
        {
            return data_.valid() ? data_.value() : ValueView{};
        }

        [[nodiscard]] ValueView delta_value() const
        {
            return data_.valid() ? data_.delta_value(evaluation_time_) : ValueView{};
        }

        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const
        {
            return data_.valid() ? data_.delta_value(evaluation_time) : ValueView{};
        }

        [[nodiscard]] engine_time_t last_modified_time() const
        {
            return data_.valid() ? data_.last_modified_time() : MIN_DT;
        }

        [[nodiscard]] bool modified() const
        {
            return evaluation_time_ != MIN_DT && data_.valid() && data_.modified(evaluation_time_);
        }

        [[nodiscard]] bool modified(engine_time_t evaluation_time) const
        {
            return data_.valid() && data_.modified(evaluation_time);
        }

        [[nodiscard]] bool valid() const
        {
            return data_.valid() && data_.has_current_value();
        }

        [[nodiscard]] bool all_valid() const
        {
            return data_.valid() && data_.all_valid();
        }

        [[nodiscard]] TSSDataView as_set() &
        {
            return data_.as_set();
        }
        [[nodiscard]] TSSDataView as_set() const &
        {
            return data_.as_set();
        }
        void as_set() && = delete;
        void as_set() const && = delete;

        [[nodiscard]] TSDDataView as_dict() &
        {
            return data_.as_dict();
        }
        [[nodiscard]] TSDDataView as_dict() const &
        {
            return data_.as_dict();
        }
        void as_dict() && = delete;
        void as_dict() const && = delete;

        [[nodiscard]] TSBDataView as_bundle() &
        {
            return data_.as_bundle();
        }
        [[nodiscard]] TSBDataView as_bundle() const &
        {
            return data_.as_bundle();
        }
        void as_bundle() && = delete;
        void as_bundle() const && = delete;

        [[nodiscard]] TSLDataView as_list() &
        {
            return data_.as_list();
        }
        [[nodiscard]] TSLDataView as_list() const &
        {
            return data_.as_list();
        }
        void as_list() && = delete;
        void as_list() const && = delete;

        [[nodiscard]] TSWDataView as_window() &
        {
            return data_.as_window();
        }
        [[nodiscard]] TSWDataView as_window() const &
        {
            return data_.as_window();
        }
        void as_window() && = delete;
        void as_window() const && = delete;

      private:
        const TSOutput *output_{nullptr};
        TSDataView      data_{};
        engine_time_t   evaluation_time_{MIN_DT};
    };

    class TSOutputMutationView
    {
      public:
        TSOutputMutationView(TSOutput &output, engine_time_t evaluation_time)
            : output_(&output),
              mutation_(begin_root_mutation(output, evaluation_time))
        {
        }

        TSOutputMutationView(const TSOutputMutationView &) = delete;
        TSOutputMutationView &operator=(const TSOutputMutationView &) = delete;

        TSOutputMutationView(TSOutputMutationView &&other) noexcept
            : output_(std::exchange(other.output_, nullptr)),
              mutation_(std::move(other.mutation_))
        {
        }

        TSOutputMutationView &operator=(TSOutputMutationView &&) = delete;

        ~TSOutputMutationView() noexcept
        {
            if (output_ != nullptr) { output_->end_mutation_scope(); }
        }

        [[nodiscard]] TSDataMutationView &data_mutation() noexcept { return mutation_; }
        [[nodiscard]] const TSDataMutationView &data_mutation() const noexcept { return mutation_; }
        [[nodiscard]] ValueView value() const { return mutation_.value(); }
        [[nodiscard]] ValueView delta_value(engine_time_t evaluation_time) const
        {
            return mutation_.delta_value(evaluation_time);
        }
        [[nodiscard]] engine_time_t current_mutation_time() const { return mutation_.current_mutation_time(); }
        [[nodiscard]] bool modified() const { return mutation_.modified(current_mutation_time()); }

        void mark_modified()
        {
            mutation_.mark_modified();
        }

        [[nodiscard]] bool copy_value_from(const ValueView &source)
        {
            return mutation_.copy_value_from(source);
        }

      private:
        static TSDataMutationView begin_root_mutation(TSOutput &output, engine_time_t evaluation_time)
        {
            output.begin_mutation_scope(evaluation_time);
            auto rollback = UnwindCleanupGuard{[&output]() noexcept { output.end_mutation_scope(); }};
            return output.data_view().begin_mutation(evaluation_time);
        }

        TSOutput          *output_{nullptr};
        TSDataMutationView mutation_;
    };

    inline TSOutputView TSOutput::view(engine_time_t evaluation_time)
    {
        return TSOutputView{this, data_view(), evaluation_time};
    }

    inline TSOutputView TSOutput::view(engine_time_t evaluation_time) const
    {
        return TSOutputView{this, data_view(), evaluation_time};
    }

    inline TSOutputMutationView TSOutput::begin_mutation(engine_time_t evaluation_time)
    {
        return TSOutputMutationView{*this, evaluation_time};
    }
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_OUTPUT_H

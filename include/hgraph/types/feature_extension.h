
#ifndef FEATURE_EXTENSION_H
#define FEATURE_EXTENSION_H

#include <any>
#include <functional>
#include <hgraph/builders/output_builder.h>
#include <hgraph/types/ts.h>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace hgraph
{

    struct FeatureOutputRequestTracker
    {
        explicit FeatureOutputRequestTracker(TimeSeriesOutput::ptr output_);
        TimeSeriesOutput::ptr            output;
        std::unordered_set<const void *> requesters;
    };

    template <typename T> struct FeatureOutputExtension
    {
        using ptr        = nb::ref<FeatureOutputExtension<T>>;
        using feature_fn = std::function<void(const TimeSeriesOutput &, TimeSeriesOutput &, const T&)>;

        FeatureOutputExtension(TimeSeriesOutput::ptr owning_output_, OutputBuilder::ptr output_builder_, feature_fn value_getter_,
                               std::optional<feature_fn> initial_value_getter_)
            : owning_output(std::move(owning_output_)), output_builder(std::move(output_builder_)),
              value_getter(std::move(value_getter_)), initial_value_getter(std::move(initial_value_getter_)) {}

        TimeSeriesOutput::ptr create_or_increment(const T &key, const void *requester) {
            auto it = _outputs.find(key);
            if (it == _outputs.end()) {
                auto new_output{output_builder->make_instance(&owning_output->owning_node())};

                auto [inserted_it, success] = _outputs.emplace(key, FeatureOutputRequestTracker(new_output));

                if (initial_value_getter) {
                    (*initial_value_getter)(*owning_output, *new_output, key);
                } else {
                    value_getter(*owning_output, *new_output, key);
                }

                it = inserted_it;
            }

            it->second.requesters.insert(requester);
            return it->second.output;
        }

        void update(const T &key) {
            if (auto it{_outputs.find(key)}; it != _outputs.end()) { value_getter(*owning_output, *(it->second.output), key); }
        }

        template <typename It> void update_all(It begin, It end) {
            if (!_outputs.empty()) {
                for (auto it = begin; it != end; ++it) { update(*it); }
            }
        }

        void release(const T &key, const void *requester) {
            if (auto it{_outputs.find(key)}; it != _outputs.end()) {
                it->second.requesters.erase(requester);
                if (it->second.requesters.empty()) { _outputs.erase(it); }
            }
        }

      private:
        TimeSeriesOutput::ptr     owning_output;
        OutputBuilder::ptr        output_builder;
        feature_fn                value_getter;
        std::optional<feature_fn> initial_value_getter;

        std::unordered_map<T, FeatureOutputRequestTracker> _outputs;
    };

    using FeatureOutputExtensionBool = FeatureOutputExtension<bool>;
    using FeatureOutputExtensionInt  = FeatureOutputExtension<int>;
    using FeatureOutputExtensionFloat = FeatureOutputExtension<double>;
    using FeatureOutputExtensionDate = FeatureOutputExtension<engine_date_t>;
    using FeatureOutputExtensionDateTime = FeatureOutputExtension<engine_time_t>;
    using FeatureOutputExtensionTimeDelta = FeatureOutputExtension<engine_time_delta_t>;

    using FeatureOutputExtensionObject = FeatureOutputExtension<nb::object>;

}  // namespace hgraph

#endif  // FEATURE_EXTENSION_H

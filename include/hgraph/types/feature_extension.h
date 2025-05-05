
#ifndef FEATURE_EXTENSION_H
#define FEATURE_EXTENSION_H

#include <any>
#include <functional>
#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/ts.h>
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

    template<typename T> struct FeatureOutputExtension
    {
        using feature_fn = std::function<void(const TimeSeriesOutput &, TimeSeriesOutput &, const T &)>;

        FeatureOutputExtension(TimeSeriesOutput::ptr owning_output_, output_builder_ptr output_builder_, feature_fn value_getter_,
                               std::optional<feature_fn> initial_value_getter_);

        TimeSeriesOutput::ptr create_or_increment(const T &key, const void *requester);

        void update(const T &key);

        void release(const T &key, const void *requester);

        template <typename It> void update_all(It begin, It end) {
            if (!_outputs.empty()) {
                for (auto it = begin; it != end; ++it) { update(*it); }
            }
        }

      private:
        TimeSeriesOutput::ptr     owning_output;
        output_builder_ptr        output_builder;
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

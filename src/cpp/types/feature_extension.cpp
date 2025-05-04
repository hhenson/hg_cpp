#include <hgraph/types/feature_extension.h>

namespace hgraph
{
    FeatureOutputRequestTracker::FeatureOutputRequestTracker(TimeSeriesOutput::ptr output_) : output(std::move(output_)) {}
}  // namespace hgraph

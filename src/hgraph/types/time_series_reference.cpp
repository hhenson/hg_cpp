#include <hgraph/types/time_series_reference.h>

#include <sstream>
#include <stdexcept>
#include <utility>

namespace hgraph
{
    namespace
    {
        constexpr std::size_t kHashMix = 0x9e3779b97f4a7c15ULL;

        std::size_t hash_combine(std::size_t seed, std::size_t value) noexcept
        {
            return seed ^ (value + kHashMix + (seed << 6U) + (seed >> 2U));
        }
    }  // namespace

    TimeSeriesReference::TimeSeriesReference(const TSValueTypeMetaData *target_schema) noexcept
        : kind_{Kind::EMPTY}, target_schema_{target_schema}
    {
    }

    TimeSeriesReference::TimeSeriesReference(const TSValueTypeMetaData    *target_schema,
                                              std::vector<TimeSeriesReference> items)
        : kind_{Kind::NON_PEERED}, target_schema_{target_schema}, items_{std::move(items)}
    {
    }

    TimeSeriesReference TimeSeriesReference::peered(const TSValueTypeMetaData *target_schema)
    {
        TimeSeriesReference ref;
        ref.kind_          = Kind::PEERED;
        ref.target_schema_ = target_schema;
        return ref;
    }

    const std::vector<TimeSeriesReference> &TimeSeriesReference::items() const
    {
        if (kind_ != Kind::NON_PEERED)
        {
            throw std::logic_error("TimeSeriesReference::items() requires a NON_PEERED reference");
        }
        return items_;
    }

    const TimeSeriesReference &TimeSeriesReference::operator[](size_t index) const
    {
        if (kind_ != Kind::NON_PEERED)
        {
            throw std::logic_error("TimeSeriesReference::operator[] requires a NON_PEERED reference");
        }
        if (index >= items_.size())
        {
            throw std::out_of_range("TimeSeriesReference::operator[] index out of range");
        }
        return items_[index];
    }

    bool TimeSeriesReference::operator==(const TimeSeriesReference &other) const noexcept
    {
        if (kind_ != other.kind_) { return false; }
        if (target_schema_ != other.target_schema_) { return false; }
        if (kind_ == Kind::NON_PEERED) { return items_ == other.items_; }
        // EMPTY and PEERED carry no further state today; once the runtime
        // layer adds a binding payload to PEERED, this will need to compare
        // it too.
        return true;
    }

    std::size_t TimeSeriesReference::hash() const noexcept
    {
        std::size_t seed = static_cast<std::size_t>(kind_);
        seed = hash_combine(seed, std::hash<const void *>{}(static_cast<const void *>(target_schema_)));
        if (kind_ == Kind::NON_PEERED)
        {
            for (const auto &item : items_) { seed = hash_combine(seed, item.hash()); }
        }
        return seed;
    }

    std::string TimeSeriesReference::to_string() const
    {
        switch (kind_)
        {
            case Kind::EMPTY:  return "TimeSeriesReference{empty}";
            case Kind::PEERED: return "TimeSeriesReference{peered}";
            case Kind::NON_PEERED:
            {
                std::ostringstream out;
                out << "TimeSeriesReference{[";
                for (size_t i = 0; i < items_.size(); ++i)
                {
                    if (i != 0) { out << ", "; }
                    out << items_[i].to_string();
                }
                out << "]}";
                return out.str();
            }
        }
        return "TimeSeriesReference{?}";
    }

    const TimeSeriesReference &TimeSeriesReference::empty_reference() noexcept
    {
        static const TimeSeriesReference empty{};
        return empty;
    }
}  // namespace hgraph

#include <hgraph/types/frame.h>

#include <arrow/table.h>

#include <ostream>

namespace hgraph
{
    std::ostream &operator<<(std::ostream &os, const Frame &value)
    {
        if (!value.has_value()) { return os << "frame[empty]"; }
        return os << "frame[" << value.table->num_rows() << " x " << value.table->num_columns() << "]";
    }
    std::int64_t frame_rows(const Frame &value) noexcept
    {
        return value.has_value() ? value.table->num_rows() : 0;
    }
}  // namespace hgraph

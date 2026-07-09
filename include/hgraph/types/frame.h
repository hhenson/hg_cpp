#ifndef HGRAPH_TYPES_FRAME_H
#define HGRAPH_TYPES_FRAME_H

#include <hgraph/types/static_schema.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <memory>

namespace arrow
{
    class Table;
}  // namespace arrow

namespace hgraph
{
    /**
     * The ``Frame`` scalar — a first-class table value backed by an **Apache
     * Arrow** table (design record: *Record/replay, tables and const_fn*,
     * P4 / Q-arrow-dep ruling 2026-07-04). Arrow is the interchange layer:
     * Polars, pandas and other Arrow-native frame libraries reach the data
     * zero-copy; user code names ``Frame``, never Arrow directly.
     *
     * A schema-less ``Frame`` is legal. When a schema qualifies a frame at a
     * boundary the rules are: an INPUT schema is a *minimum* requirement
     * (the arriving frame must contain at least those columns at those
     * types), an OUTPUT schema is *exact*. (The typed ``Frame<Schema>``
     * wiring marker lands with the schema-matching layer; the value kind
     * here is the substrate.)
     *
     * Value semantics: the handle is shared-immutable (Arrow tables are
     * immutable); copying a ``Frame`` copies the handle, not the data.
     * Equality is HANDLE IDENTITY (same table object, or both empty) — cheap
     * and honest for tick/delta suppression; content comparison is an
     * explicit operator when needed. The shared handle is an opaque
     * third-party resource (like ``Str``'s heap buffer), not runtime graph
     * structure — the no-shared-ptr rule governs the latter.
     */
    struct Frame
    {
        std::shared_ptr<arrow::Table> table{};

        [[nodiscard]] bool has_value() const noexcept { return table != nullptr; }

        friend bool operator==(const Frame &lhs, const Frame &rhs) noexcept
        {
            return lhs.table == rhs.table;
        }
    };

    /** Render as ``frame[rows x cols]`` (diagnostics; not a data format). */
    std::ostream &operator<<(std::ostream &os, const Frame &value);

    /** Row count (0 for an empty frame) without exposing Arrow headers. */
    [[nodiscard]] std::int64_t frame_rows(const Frame &value) noexcept;

    namespace static_schema_detail
    {
        template <>
        struct scalar_name<Frame>
        {
            static constexpr std::string_view value{"frame"};
        };
    }  // namespace static_schema_detail
}  // namespace hgraph

/** ``std::hash`` for ``Frame`` (handle identity, matching equality). */
template <>
struct std::hash<hgraph::Frame>
{
    [[nodiscard]] std::size_t operator()(const hgraph::Frame &value) const noexcept
    {
        return std::hash<const void *>{}(static_cast<const void *>(value.table.get()));
    }
};

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <hgraph/types/value/value_ops.h>

#include <nanobind/nanobind.h>

namespace hgraph
{
    /** Frame <-> pyarrow binds onto the type-erased ops (module installs the
        hooks; core dispatches uniformly - no kind-switch). */
    template <>
    struct python_conversion_traits<Frame>
    {
        inline static nanobind::object (*to_python_hook)(const Frame &) = nullptr;
        inline static Frame (*from_python_hook)(nanobind::handle)       = nullptr;

        static nanobind::object to_python(const Frame &value)
        {
            if (to_python_hook == nullptr)
            {
                throw std::logic_error("Frame python conversion hook not installed (import the module)");
            }
            return to_python_hook(value);
        }

        static Frame from_python(nanobind::handle source)
        {
            if (from_python_hook == nullptr)
            {
                throw std::logic_error("Frame python conversion hook not installed (import the module)");
            }
            return from_python_hook(source);
        }
    };
}  // namespace hgraph
#endif  // HGRAPH_ENABLE_PYTHON_USER_NODES

#endif  // HGRAPH_TYPES_FRAME_H

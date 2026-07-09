#ifndef HGRAPH_LIB_STD_OPERATORS_CONVERT_TARGET_H
#define HGRAPH_LIB_STD_OPERATORS_CONVERT_TARGET_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/type_pattern.h>

#include <span>
#include <string>

namespace hgraph::stdlib
{
    [[nodiscard]] HGRAPH_EXPORT const TSValueTypeMetaData *resolve_convert_target(
        const TypePattern &pattern,
        std::span<const TSValueTypeMetaData *const> inputs,
        std::span<const std::string> selected_keys = {});

    [[nodiscard]] HGRAPH_EXPORT const TSValueTypeMetaData *resolve_collect_target(
        const TypePattern &pattern,
        std::span<const TSValueTypeMetaData *const> inputs);
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_CONVERT_TARGET_H

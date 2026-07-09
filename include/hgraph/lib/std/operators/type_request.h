#ifndef HGRAPH_LIB_STD_OPERATORS_TYPE_REQUEST_H
#define HGRAPH_LIB_STD_OPERATORS_TYPE_REQUEST_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>

#include <span>
#include <string>

namespace hgraph::stdlib
{
    enum class TypeRequestKind
    {
        TSS,
        TSD,
        TSL,
        TSB,
        TSTuple,
        TSSet,
        TSMapping,
        TSCompoundScalar,
    };

    struct TypeRequest
    {
        TypeRequestKind kind{TypeRequestKind::TSTuple};
    };

    [[nodiscard]] HGRAPH_EXPORT std::string type_request_to_string(const TypeRequest &request);

    [[nodiscard]] HGRAPH_EXPORT const TSValueTypeMetaData *resolve_convert_target(
        const TypeRequest &request,
        std::span<const TSValueTypeMetaData *const> inputs,
        std::span<const std::string> selected_keys = {});

    [[nodiscard]] HGRAPH_EXPORT const TSValueTypeMetaData *resolve_collect_target(
        const TypeRequest &request,
        std::span<const TSValueTypeMetaData *const> inputs);
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_TYPE_REQUEST_H

#ifndef HGRAPH_LIB_STD_OPERATORS_TYPE_REQUEST_H
#define HGRAPH_LIB_STD_OPERATORS_TYPE_REQUEST_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <variant>
#include <vector>

namespace hgraph::stdlib
{
    struct ScalarRequest;
    struct TypeRequest;

    using ScalarRequestPtr = std::shared_ptr<const ScalarRequest>;
    using TypeRequestPtr   = std::shared_ptr<const TypeRequest>;

    struct ScalarVariableRequest
    {
        std::string name;
    };

    struct ScalarConcreteRequest
    {
        const ValueTypeMetaData *schema{nullptr};
    };

    struct ScalarUnknownTupleRequest
    {
        ScalarRequestPtr element;
    };

    struct ScalarHomogeneousTupleRequest
    {
        ScalarRequestPtr element;
    };

    struct ScalarFixedTupleRequest
    {
        std::vector<ScalarRequestPtr> elements;
    };

    struct ScalarSetRequest
    {
        ScalarRequestPtr element;
    };

    struct ScalarMapRequest
    {
        ScalarRequestPtr key;
        ScalarRequestPtr value;
    };

    struct ScalarBundleRequest
    {
        std::optional<std::string> schema_variable;
    };

    struct ScalarRequest
    {
        using Node = std::variant<
            ScalarVariableRequest,
            ScalarConcreteRequest,
            ScalarUnknownTupleRequest,
            ScalarHomogeneousTupleRequest,
            ScalarFixedTupleRequest,
            ScalarSetRequest,
            ScalarMapRequest,
            ScalarBundleRequest>;

        Node node;

        [[nodiscard]] static ScalarRequest variable(std::string name);
        [[nodiscard]] static ScalarRequest concrete(const ValueTypeMetaData *schema);
        [[nodiscard]] static ScalarRequest unknown_tuple(ScalarRequestPtr element = {});
        [[nodiscard]] static ScalarRequest homogeneous_tuple(ScalarRequest element);
        [[nodiscard]] static ScalarRequest fixed_tuple(std::vector<ScalarRequest> elements);
        [[nodiscard]] static ScalarRequest set(ScalarRequest element);
        [[nodiscard]] static ScalarRequest map(ScalarRequest key, ScalarRequest value);
        [[nodiscard]] static ScalarRequest bundle(std::optional<std::string> schema_variable = std::nullopt);
    };

    struct SizeVariableRequest
    {
        std::string name;
    };

    struct SizeConcreteRequest
    {
        std::size_t value{0};
    };

    struct SizeRequest
    {
        using Node = std::variant<SizeVariableRequest, SizeConcreteRequest>;

        Node node;

        [[nodiscard]] static SizeRequest variable(std::string name);
        [[nodiscard]] static SizeRequest concrete(std::size_t value);
    };

    struct TypeVariableRequest
    {
        std::string name;
    };

    struct TypeConcreteRequest
    {
        const TSValueTypeMetaData *schema{nullptr};
    };

    struct TypeTsRequest
    {
        ScalarRequest value;
    };

    struct TypeTssRequest
    {
        ScalarRequest element;
    };

    struct TypeTsdRequest
    {
        ScalarRequest key;
        TypeRequestPtr value;
    };

    struct TypeTslRequest
    {
        TypeRequestPtr element;
        SizeRequest    size;
    };

    struct TypeTsbRequest
    {
        std::optional<std::string> schema_variable;
    };

    struct TypeRefRequest
    {
        TypeRequestPtr target;
    };

    struct TypeRequest
    {
        using Node = std::variant<
            TypeVariableRequest,
            TypeConcreteRequest,
            TypeTsRequest,
            TypeTssRequest,
            TypeTsdRequest,
            TypeTslRequest,
            TypeTsbRequest,
            TypeRefRequest>;

        Node node;

        [[nodiscard]] static TypeRequest variable(std::string name);
        [[nodiscard]] static TypeRequest concrete(const TSValueTypeMetaData *schema);
        [[nodiscard]] static TypeRequest ts(ScalarRequest value);
        [[nodiscard]] static TypeRequest tss(ScalarRequest element);
        [[nodiscard]] static TypeRequest tsd(ScalarRequest key, TypeRequest value);
        [[nodiscard]] static TypeRequest tsl(TypeRequest element, SizeRequest size);
        [[nodiscard]] static TypeRequest tsb(std::optional<std::string> schema_variable = std::nullopt);
        [[nodiscard]] static TypeRequest ref(TypeRequest target);
    };

    [[nodiscard]] HGRAPH_EXPORT std::string scalar_request_to_string(const ScalarRequest &request);
    [[nodiscard]] HGRAPH_EXPORT std::string size_request_to_string(const SizeRequest &request);
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

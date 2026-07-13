#include <hgraph/types/metadata/type_record.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/type_pointer.h>
#include <hgraph/types/value/value.h>

#include <cstdint>
#include <stdexcept>
#include <type_traits>

int main()
{
    using namespace hgraph;

    static_assert(std::is_standard_layout_v<SchemaHeader>);
    static_assert(std::is_trivially_copyable_v<SchemaHeader>);
    static_assert(std::is_standard_layout_v<TypeRecord>);
    static_assert(std::is_trivially_copyable_v<TypeRecord>);
    static_assert(std::is_standard_layout_v<AnyPtr>);
    static_assert(std::is_trivially_copyable_v<AnyPtr>);

    auto &registry = TypeRegistry::instance();
    registry.register_scalar<std::int32_t>("int32");

    Value value{std::int32_t{41}};
    auto view = value.view();
    const AnyPtr pointer = AnyPtr::read_only(*view.record(), view.data());
    if (!pointer.valid() || pointer.family() != TypeFamily::Value || !pointer.read_only_access())
    {
        throw std::runtime_error("installed target produced an invalid erased pointer");
    }

    value.begin_mutation().as<std::int32_t>() = 73;
    if (value.view().checked_as<std::int32_t>() != 73)
    {
        throw std::runtime_error("installed target mutation round-trip failed");
    }

    return 0;
}

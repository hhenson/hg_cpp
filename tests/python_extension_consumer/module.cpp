#include <hgraph/types/metadata/type_registry.h>

#include <nanobind/nanobind.h>

#include <cstdint>

namespace nb = nanobind;

NB_MODULE(_hgraph_consumer, module)
{
    module.def("registry_address", [] {
        return reinterpret_cast<std::uintptr_t>(&hgraph::TypeRegistry::instance());
    });
}

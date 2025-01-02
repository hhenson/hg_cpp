#include <fmt/format.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/node.h>

namespace hgraph
{

    NodeException NodeException::capture_error(const std::exception &e, const Node &node, const std::string &msg) {
        return NodeException{fmt::format("{}: {} in node {}", msg, e.what(), node.signature().signature())};
    }

    NodeException NodeException::capture_error(std::exception_ptr e, const Node &node, const std::string &msg) {
        try {
            rethrow_exception(std::move(e));
        } catch (exception &e_) {
            return NodeException{fmt::format("{}: {} in node {}", msg, e_.what(), node.signature().signature())};
        }
    }
}  // namespace hgraph
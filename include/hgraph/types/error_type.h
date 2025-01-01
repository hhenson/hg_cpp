//
// Created by Howard Henson on 27/12/2024.
//

#ifndef ERROR_TYPE_H
#define ERROR_TYPE_H

#include <exception>
#include <hgraph/hgraph_export.h>
#include <hgraph/hgraph_forward_declarations.h>
#include <stdexcept>

namespace hgraph
{
    struct HGRAPH_EXPORT NodeError{};

    struct HGRAPH_EXPORT NodeException : std::runtime_error
    {
        using std::runtime_error::runtime_error;

        NodeException static capture_error(const std::exception &e, const Node &node, const std::string &msg);

        NodeException static capture_error(std::exception_ptr e, const Node &node, const std::string &msg);
    };
}  // namespace hgraph

#endif  // ERROR_TYPE_H

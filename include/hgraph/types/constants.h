//
// Created by Howard Henson on 02/07/2025.
//

#ifndef CONSTANTS_H
#define CONSTANTS_H

#include <hgraph/hgraph_base.h>

namespace hgraph
{
    /**
     * For Dictionary operations, REMOVE forces a remove with error if no value is present
     */
    nb::object get_remove();

    /**
     * For Dictionary operations, REMOVE_IF_EXISTS removes a key if it exists, otherwise ignores
     */
    nb::object get_remove_if_exists();

    nb::object get_removed();

    nb::object get_frozenset();

    nb::object get_frozendict();

    nb::object get_key_set_id();
}  // namespace hgraph

#endif  // CONSTANTS_H

//
// Created by Howard Henson on 27/12/2024.
//

#ifndef INPUT_BUILDER_H
#define INPUT_BUILDER_H

#include <hgraph/builders/builder.h>
#include <hgraph/hgraph_forward_declarations.h>

namespace hgraph
{

    // The InputBuilder class implementation

    struct  InputBuilder : Builder
    {
        /**
         * Create an instance of InputBuilder.
         * Either `owningNode` or `owningInput` must be provided.
         */
        virtual time_series_input_ptr make_instance(node_ptr owningNode = nullptr, time_series_input_ptr owningInput = nullptr) = 0;

        /**
         * Release an instance of the input type.
         * By default, do nothing.
         */
        virtual void release_instance(time_series_input_ptr item) {
        }
    };
}

#endif  // INPUT_BUILDER_H

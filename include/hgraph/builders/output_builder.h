
#ifndef OUTPUT_BUILDER_H
#define OUTPUT_BUILDER_H

#include<hgraph/builders/builder.h>
#include<hgraph/hgraph_forward_declarations.h>

namespace hgraph
{

    struct OutputBuilder : Builder
    {

        virtual time_series_output_ptr make_instance(node_ptr owning_node   = nullptr,
                                                                time_series_output_ptr owning_output = nullptr) = 0;

        virtual void release_instance(time_series_output_ptr item){};

    };
}

#endif //OUTPUT_BUILDER_H

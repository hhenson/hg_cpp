#include <hgraph/hgraph.h>

#include <iostream>

int main() {
    if (hgraph::version() != hgraph::version_string) {
        return 1;
    }

    if (hgraph::version_major != 0 || hgraph::version_minor != 1 || hgraph::version_patch != 0) {
        return 1;
    }

    std::cout << "hgraph " << hgraph::version() << '\n';
    return 0;
}

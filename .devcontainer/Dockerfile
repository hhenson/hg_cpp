FROM mcr.microsoft.com/devcontainers/cpp:1-ubuntu-22.04

ARG REINSTALL_CMAKE_VERSION_FROM_SOURCE="3.31.3"

# Optionally install the cmake for vcpkg
COPY ./reinstall-cmake.sh /tmp/

# Install GCC 10
# RUN apt-get update && \
#     apt-get install -y gcc-10 g++-10 && \
#     update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 10 && \
#     update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-10 10

RUN if [ "${REINSTALL_CMAKE_VERSION_FROM_SOURCE}" != "none" ]; then \
        chmod +x /tmp/reinstall-cmake.sh && /tmp/reinstall-cmake.sh ${REINSTALL_CMAKE_VERSION_FROM_SOURCE}; \
    fi \
    && rm -f /tmp/reinstall-cmake.sh

# [Optional] Uncomment this section to install additional vcpkg ports.
# RUN su vscode -c "${VCPKG_ROOT}/vcpkg install <your-port-name-here>"

# [Optional] Uncomment this section to install additional packages.
# RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
#     && apt-get -y install --no-install-recommends <your-package-list-here>

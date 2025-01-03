cmake_minimum_required(VERSION 3.30)
project(hgraph_cpp_engine)

if (NOT SKBUILD)
    message(WARNING "\
  This CMake file is meant to be executed using 'scikit-build'. Running
  it directly will almost certainly not produce the desired result. If
  you are a user trying to install this package, please use the command
  below, which will install all necessary build dependencies, compile
  the package in an isolated environment, and then install it.
  =====================================================================
   $ pip install .
  =====================================================================
  If you are a software developer, and this is your own package, then
  it is usually much more efficient to install the build dependencies
  in your environment once and use the following command that avoids
  a costly creation of a new virtual environment at every compilation:
  =====================================================================
   $ pip install nanobind scikit-build-core[pyproject]
   $ pip install --no-build-isolation -ve .
  =====================================================================
  You may optionally add -Ceditable.rebuild=true to auto-rebuild when
  the package is imported. Otherwise, you need to re-run the above
  after editing C++ files.")
endif()

set(CMAKE_CXX_STANDARD 20)

if (UNIX)
    set(PYBIND11_CPP_STANDARD -std=c++20)
    set(CMAKE_CXX_FLAGS_RELEASE "-g -O3")
    set(CMAKE_CXX_FLAGS_DEBUG "-g -v -fsanitize=address")
    set(CMAKE_CXX_FLAGS "-Wall")
endif (UNIX)

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    # Set properties specific to macOS
    # Check for Apple Silicon (ARM architecture)
    if (${CMAKE_SYSTEM_PROCESSOR} STREQUAL "arm64")
        message(STATUS "Configuring for Apple Silicon (ARM64)")
        include_directories(/opt/homebrew/include)
        link_directories(/opt/homebrew/lib)
    else (${CMAKE_SYSTEM_PROCESSOR} STREQUAL "arm64")
        message(STATUS "Configuring for Intel (x86_64)")
        include_directories(/usr/local/include)
        link_directories(/usr/local/lib)
    endif (${CMAKE_SYSTEM_PROCESSOR} STREQUAL "arm64")
endif (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

find_package(spdlog CONFIG REQUIRED)

find_package(Python 3.12 REQUIRED COMPONENTS Interpreter Development)
include_directories(${Python3_INCLUDE_DIRS})
link_libraries(${Python3_LIBRARIES})

#find_package(tsl-robin-map CONFIG REQUIRED)
execute_process(
        COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
        OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE nanobind_ROOT)
find_package(nanobind CONFIG REQUIRED)
include_directories(${NB_DIR}/include)
message(STATUS "Nanobind DIR: ${NB_DIR}/include")
#find_package(Boost REQUIRED)
find_package(Threads)
find_package(range-v3 CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
#find_package(immer CONFIG REQUIRED)

if (EXISTS "${CMAKE_SOURCE_DIR}/.git")
    execute_process(
            COMMAND git rev-parse --abbrev-ref HEAD
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE GIT_BRANCH
            OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    execute_process(
            COMMAND git log -1 --format=%H
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE GIT_COMMIT_HASH
            OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    execute_process(
            COMMAND git log -1 --format=%cD
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE GIT_COMMIT_DATE
            OUTPUT_STRIP_TRAILING_WHITESPACE
    )
else (EXISTS "${CMAKE_SOURCE_DIR}/.git")
    set(GIT_BRANCH "")
    set(GIT_COMMIT_HASH "")
    set(GIT_COMMIT_DATE "")
endif (EXISTS "${CMAKE_SOURCE_DIR}/.git")

message(STATUS "Git current branch: ${GIT_BRANCH}")
message(STATUS "Git commit hash: ${GIT_COMMIT_HASH}")
message(STATUS "Git commit date: ${GIT_COMMIT_DATE}")

message(STATUS "Generating version.h")
configure_file(
        ${CMAKE_SOURCE_DIR}/include/hgraph/version.h.in
        ${CMAKE_BINARY_DIR}/generated/version.h
)

include_directories(include)

set(HGRAPH_INCLUDES

        include/hgraph/hgraph_forward_declarations.h

        include/hgraph/builders/builder.h
        include/hgraph/builders/graph_builder.h
        include/hgraph/builders/input_builder.h
        include/hgraph/builders/node_builder.h
        include/hgraph/builders/output_builder.h

        include/hgraph/runtime/evaluation_context.h
        include/hgraph/runtime/evaluation_engine.h
        include/hgraph/runtime/graph_executor.h

        include/hgraph/types/error_type.h
        include/hgraph/types/graph.h
        include/hgraph/types/node.h
        include/hgraph/types/time_series_type.h
        include/hgraph/types/traits.h
        include/hgraph/types/tsb.h

        include/hgraph/python/pyb.h
        include/hgraph/python/pyb_wiring.h

        include/hgraph/util/date_time.h
        include/hgraph/util/lifecycle.h
        include/hgraph/util/reference_count_subscriber.h
        include/hgraph/util/sender_receiver_state.h

        include/hgraph/hgraph_export.h
)

list(TRANSFORM HGRAPH_INCLUDES PREPEND "${CMAKE_SOURCE_DIR}/")

list(APPEND HGRAPH_INCLUDES ${CMAKE_BINARY_DIR}/generated/version.h)

add_subdirectory(src/cpp)
#add_subdirectory(tests/cpp)

macro(dump_cmake_variables)
    message(STATUS "================ All cmake variables ==================")
    get_cmake_property(_variableNames VARIABLES)
    foreach (_variableName ${_variableNames})
        message(STATUS "${_variableName}=${${_variableName}}")
    endforeach ()
    message(STATUS "================ All cmake variables DONE ============")
endmacro()


set(DUMP_CMAKE_VARIABLES ON)

if (DUMP_CMAKE_VARIABLES)
    dump_cmake_variables()
endif ()
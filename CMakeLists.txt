cmake_minimum_required(VERSION 3.25)

project(
    hgraph
    VERSION 0.1.0
    DESCRIPTION "C++ first implementation of the hgraph runtime"
    HOMEPAGE_URL "https://github.com/hhenson/hgraph"
    LANGUAGES CXX
)

include(CMakePackageConfigHelpers)
include(CTest)
include(GNUInstallDirs)

set(HGRAPH_GENERATED_INCLUDE_DIR "${PROJECT_BINARY_DIR}/generated/include")
set(HGRAPH_VERSION_HEADER_IN "${PROJECT_SOURCE_DIR}/include/hgraph/version.h.in")
set(HGRAPH_VERSION_HEADER "${HGRAPH_GENERATED_INCLUDE_DIR}/hgraph/version.h")

set(HGRAPH_GIT_BRANCH "unknown")
set(HGRAPH_GIT_COMMIT_HASH "unknown")
set(HGRAPH_GIT_COMMIT_DATE "unknown")

find_package(Git QUIET)
if(Git_FOUND AND EXISTS "${PROJECT_SOURCE_DIR}/.git")
    function(hgraph_read_git_metadata output_variable)
        execute_process(
            COMMAND "${GIT_EXECUTABLE}" ${ARGN}
            WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
            OUTPUT_VARIABLE git_output
            OUTPUT_STRIP_TRAILING_WHITESPACE
            RESULT_VARIABLE git_result
            ERROR_QUIET
        )
        if(git_result EQUAL 0 AND NOT git_output STREQUAL "")
            set("${output_variable}" "${git_output}" PARENT_SCOPE)
        endif()
    endfunction()

    hgraph_read_git_metadata(HGRAPH_GIT_BRANCH rev-parse --abbrev-ref HEAD)
    hgraph_read_git_metadata(HGRAPH_GIT_COMMIT_HASH log -1 --format=%H)
    hgraph_read_git_metadata(HGRAPH_GIT_COMMIT_DATE log -1 --format=%cD)
endif()

message(STATUS "hgraph git branch: ${HGRAPH_GIT_BRANCH}")
message(STATUS "hgraph git commit: ${HGRAPH_GIT_COMMIT_HASH}")
message(STATUS "hgraph git commit date: ${HGRAPH_GIT_COMMIT_DATE}")

file(MAKE_DIRECTORY "${HGRAPH_GENERATED_INCLUDE_DIR}/hgraph")
configure_file("${HGRAPH_VERSION_HEADER_IN}" "${HGRAPH_VERSION_HEADER}" @ONLY)

option(HGRAPH_BUILD_SHARED "Build hgraph_core as a shared library" OFF)
option(HGRAPH_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" OFF)
option(HGRAPH_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(HGRAPH_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(HGRAPH_ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(HGRAPH_BUILD_PYTHON_BINDINGS "Build the Python extension bridge" OFF)
option(HGRAPH_ENABLE_PYTHON_USER_NODES "Enable C++ runtime support for Python user nodes" OFF)

if(HGRAPH_ENABLE_TSAN AND (HGRAPH_ENABLE_ASAN OR HGRAPH_ENABLE_UBSAN))
    message(FATAL_ERROR "HGRAPH_ENABLE_TSAN is mutually exclusive with ASAN and UBSAN")
endif()

add_library(hgraph_options INTERFACE)
add_library(hgraph::options ALIAS hgraph_options)

target_compile_features(hgraph_options INTERFACE cxx_std_23)
target_include_directories(hgraph_options INTERFACE
    $<BUILD_INTERFACE:${HGRAPH_GENERATED_INCLUDE_DIR}>
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

if(MSVC)
    target_compile_options(hgraph_options INTERFACE /W4 /permissive-)
    if(HGRAPH_WARNINGS_AS_ERRORS)
        target_compile_options(hgraph_options INTERFACE /WX)
    endif()
else()
    target_compile_options(hgraph_options INTERFACE -Wall -Wextra -Wpedantic)
    if(HGRAPH_WARNINGS_AS_ERRORS)
        target_compile_options(hgraph_options INTERFACE -Werror)
    endif()
endif()

set(_hgraph_sanitizers)
if(HGRAPH_ENABLE_ASAN)
    list(APPEND _hgraph_sanitizers address)
endif()
if(HGRAPH_ENABLE_UBSAN)
    list(APPEND _hgraph_sanitizers undefined)
endif()
if(HGRAPH_ENABLE_TSAN)
    list(APPEND _hgraph_sanitizers thread)
endif()

if(_hgraph_sanitizers)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        message(FATAL_ERROR "Sanitizers are only configured for Clang and GCC")
    endif()
    list(JOIN _hgraph_sanitizers "," _hgraph_sanitizer_flags)
    target_compile_options(hgraph_options INTERFACE
        -fsanitize=${_hgraph_sanitizer_flags}
        -fno-omit-frame-pointer
    )
    target_link_options(hgraph_options INTERFACE -fsanitize=${_hgraph_sanitizer_flags})
endif()

find_package(Threads REQUIRED)
target_link_libraries(hgraph_options INTERFACE Threads::Threads)

set(HGRAPH_CORE_TARGET_TYPE STATIC)
if(HGRAPH_BUILD_SHARED)
    set(HGRAPH_CORE_TARGET_TYPE SHARED)
endif()

if(EXISTS "${PROJECT_SOURCE_DIR}/src/CMakeLists.txt")
    add_subdirectory(src)
elseif(EXISTS "${PROJECT_SOURCE_DIR}/src/cpp/CMakeLists.txt")
    add_subdirectory(src/cpp)
elseif(EXISTS "${PROJECT_SOURCE_DIR}/src")
    file(GLOB_RECURSE HGRAPH_CORE_SOURCES CONFIGURE_DEPENDS
        "${PROJECT_SOURCE_DIR}/src/*.cc"
        "${PROJECT_SOURCE_DIR}/src/*.cpp"
        "${PROJECT_SOURCE_DIR}/src/*.cxx"
    )

    list(FILTER HGRAPH_CORE_SOURCES EXCLUDE REGEX "/python/")
    list(FILTER HGRAPH_CORE_SOURCES EXCLUDE REGEX "/bindings/python/")

    if(HGRAPH_CORE_SOURCES)
        add_library(hgraph_core ${HGRAPH_CORE_TARGET_TYPE} ${HGRAPH_CORE_SOURCES})
    else()
        add_library(hgraph_core INTERFACE)
    endif()
else()
    add_library(hgraph_core INTERFACE)
endif()

if(NOT TARGET hgraph_core)
    message(FATAL_ERROR "src/cpp/CMakeLists.txt must define the hgraph_core target")
endif()

if(NOT TARGET hgraph::core)
    add_library(hgraph::core ALIAS hgraph_core)
endif()
set_target_properties(hgraph_core PROPERTIES EXPORT_NAME core)

get_target_property(HGRAPH_CORE_ACTUAL_TYPE hgraph_core TYPE)
if(HGRAPH_CORE_ACTUAL_TYPE STREQUAL "INTERFACE_LIBRARY")
    target_link_libraries(hgraph_core INTERFACE hgraph::options)
else()
    target_link_libraries(hgraph_core PUBLIC hgraph::options)
endif()

if(HGRAPH_BUILD_PYTHON_BINDINGS OR HGRAPH_ENABLE_PYTHON_USER_NODES)
    if(DEFINED Python3_EXECUTABLE AND NOT DEFINED Python_EXECUTABLE)
        set(Python_EXECUTABLE "${Python3_EXECUTABLE}")
    endif()

    find_package(Python 3.12 COMPONENTS Interpreter Development.Module REQUIRED)

    execute_process(
        COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
        OUTPUT_STRIP_TRAILING_WHITESPACE
        OUTPUT_VARIABLE nanobind_ROOT
        RESULT_VARIABLE nanobind_RESULT
    )
    if(nanobind_RESULT EQUAL 0 AND nanobind_ROOT)
        set(nanobind_DIR "${nanobind_ROOT}" CACHE PATH "nanobind CMake package directory" FORCE)
        list(PREPEND CMAKE_PREFIX_PATH "${nanobind_ROOT}")
    endif()

    find_package(nanobind CONFIG REQUIRED)
    target_compile_definitions(hgraph_options INTERFACE HGRAPH_ENABLE_PYTHON_USER_NODES=1)
else()
    target_compile_definitions(hgraph_options INTERFACE HGRAPH_ENABLE_PYTHON_USER_NODES=0)
endif()

if(HGRAPH_BUILD_PYTHON_BINDINGS)
    if(EXISTS "${PROJECT_SOURCE_DIR}/bindings/python/CMakeLists.txt")
        add_subdirectory(bindings/python)
    elseif(EXISTS "${PROJECT_SOURCE_DIR}/python/CMakeLists.txt")
        add_subdirectory(python)
    else()
        message(STATUS "HGRAPH_BUILD_PYTHON_BINDINGS=ON but no Python binding CMake subtree exists yet")
    endif()
endif()

if(BUILD_TESTING AND EXISTS "${PROJECT_SOURCE_DIR}/tests/CMakeLists.txt")
    add_subdirectory(tests)
endif()

set(_hgraph_export_targets hgraph_options hgraph_core)
install(TARGETS ${_hgraph_export_targets}
    EXPORT hgraphTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

if(EXISTS "${PROJECT_SOURCE_DIR}/include")
    install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        FILES_MATCHING
            PATTERN "*.h"
            PATTERN "*.hh"
            PATTERN "*.hpp"
    )
endif()
install(FILES "${HGRAPH_VERSION_HEADER}" DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/hgraph)

write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/hgraphConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

file(WRITE "${PROJECT_BINARY_DIR}/hgraphConfig.cmake"
    "include(CMakeFindDependencyMacro)\n"
    "find_dependency(Threads)\n"
    "include(\"\${CMAKE_CURRENT_LIST_DIR}/hgraphTargets.cmake\")\n"
)

install(EXPORT hgraphTargets
    NAMESPACE hgraph::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph
)
install(FILES
    "${PROJECT_BINARY_DIR}/hgraphConfig.cmake"
    "${PROJECT_BINARY_DIR}/hgraphConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph
)

cmake_minimum_required(VERSION 3.25)

project(
    hgraph
    VERSION 0.3.0
    DESCRIPTION "C++ first implementation of the hgraph runtime"
    HOMEPAGE_URL "https://github.com/hhenson/hg_cpp"
    LANGUAGES CXX
)

include(CMakePackageConfigHelpers)
include(CTest)
include(FetchContent)
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
option(HGRAPH_PYTHON_STABLE_ABI "Build the Python bridge against the CPython 3.12 stable ABI" ON)
option(HGRAPH_ENABLE_IDE_PYTHON_HEADER_HINTS "Use local .venv Python/nanobind headers for IDE code models when Python support is disabled" ON)
option(HGRAPH_ENABLE_COMPILER_CACHE "Use sccache or ccache when available" ON)
option(HGRAPH_ENABLE_PCH "Use private precompiled headers for repository targets" ON)

set(_hgraph_default_use_pyarrow_arrow OFF)
if(HGRAPH_BUILD_PYTHON_BINDINGS)
    set(_hgraph_default_use_pyarrow_arrow ON)
endif()
option(HGRAPH_USE_PYARROW_ARROW "Use pyarrow's bundled Arrow libraries for Python bridge builds" "${_hgraph_default_use_pyarrow_arrow}")

if(HGRAPH_ENABLE_COMPILER_CACHE AND NOT CMAKE_CXX_COMPILER_LAUNCHER)
    find_program(HGRAPH_COMPILER_CACHE_PROGRAM sccache)
    if(NOT HGRAPH_COMPILER_CACHE_PROGRAM)
        find_program(HGRAPH_COMPILER_CACHE_PROGRAM ccache)
    endif()
    if(HGRAPH_COMPILER_CACHE_PROGRAM)
        set(CMAKE_CXX_COMPILER_LAUNCHER "${HGRAPH_COMPILER_CACHE_PROGRAM}" CACHE STRING "C++ compiler launcher" FORCE)
        message(STATUS "hgraph compiler cache: ${HGRAPH_COMPILER_CACHE_PROGRAM}")
    endif()
endif()

function(hgraph_enable_private_pch target_name)
    if(HGRAPH_ENABLE_PCH)
        target_precompile_headers(${target_name} PRIVATE
            "$<$<COMPILE_LANGUAGE:CXX>:${PROJECT_SOURCE_DIR}/src/hgraph/pch.h>"
        )
    endif()
endfunction()

if(HGRAPH_ENABLE_TSAN AND (HGRAPH_ENABLE_ASAN OR HGRAPH_ENABLE_UBSAN))
    message(FATAL_ERROR "HGRAPH_ENABLE_TSAN is mutually exclusive with ASAN and UBSAN")
endif()

add_library(hgraph_options INTERFACE)
add_library(hgraph::options ALIAS hgraph_options)

target_compile_features(hgraph_options INTERFACE cxx_std_23)
target_include_directories(hgraph_options INTERFACE
    $<BUILD_INTERFACE:${HGRAPH_GENERATED_INCLUDE_DIR}>
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include/third_party>
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

if(HGRAPH_BUILD_PYTHON_BINDINGS)
    # Wheels must not retain a dependency on a package-manager shared library.
    # Build simdjson as a private static dependency of the extension instead.
    FetchContent_Declare(
        simdjson
        GIT_REPOSITORY https://github.com/simdjson/simdjson.git
        GIT_TAG        v4.6.4
        GIT_SHALLOW    TRUE
    )
    set(SIMDJSON_INSTALL OFF CACHE BOOL "Generate simdjson install targets" FORCE)
    set(SIMDJSON_DEVELOPER_MODE OFF CACHE BOOL "Build simdjson developer targets" FORCE)
    FetchContent_MakeAvailable(simdjson)
    target_link_libraries(hgraph_options INTERFACE
        $<BUILD_INTERFACE:simdjson::simdjson>
    )
else()
    find_package(simdjson CONFIG REQUIRED)
    target_link_libraries(hgraph_options INTERFACE simdjson::simdjson)
endif()

if(NOT HGRAPH_BUILD_PYTHON_BINDINGS)
    find_package(fmt CONFIG QUIET)
endif()
if(NOT fmt_FOUND)
    FetchContent_Declare(
        fmt
        GIT_REPOSITORY https://github.com/fmtlib/fmt.git
        GIT_TAG        12.1.0
        GIT_SHALLOW    TRUE
    )
    set(FMT_DOC OFF CACHE BOOL "Build fmt documentation" FORCE)
    set(FMT_INSTALL OFF CACHE BOOL "Generate fmt install target" FORCE)
    set(FMT_TEST OFF CACHE BOOL "Build fmt tests" FORCE)
    FetchContent_MakeAvailable(fmt)
endif()
if(HGRAPH_BUILD_PYTHON_BINDINGS)
    target_link_libraries(hgraph_options INTERFACE
        $<BUILD_INTERFACE:fmt::fmt-header-only>
    )
else()
    target_link_libraries(hgraph_options INTERFACE fmt::fmt)
endif()

# Apache Arrow is a FORMAL dependency (design record: record_replay_table.rst,
# ruling 2026-07-04): the Frame value kind and the table serialization family
# are Arrow-native. A system/package-manager install is expected for normal
# C++ builds (`brew install apache-arrow` / `apt install libarrow-dev`);
# Arrow is too heavy to FetchContent by default. Python bridge builds may use
# pyarrow's bundled Arrow dylibs instead so one Python process does not load
# both system Arrow and wheel-bundled Arrow.
set(HGRAPH_PYARROW_LIBRARY_DIR "" CACHE PATH "Directory containing pyarrow's bundled Arrow libraries")
set(HGRAPH_PYARROW_ABI_MAJOR "24" CACHE STRING "Required pyarrow/Arrow ABI major for Python bridge builds")
if(HGRAPH_USE_PYARROW_ARROW)
    if(NOT (HGRAPH_BUILD_PYTHON_BINDINGS OR HGRAPH_ENABLE_PYTHON_USER_NODES))
        message(FATAL_ERROR "HGRAPH_USE_PYARROW_ARROW requires Python support to be enabled")
    endif()
    if(DEFINED Python3_EXECUTABLE AND NOT DEFINED Python_EXECUTABLE)
        set(Python_EXECUTABLE "${Python3_EXECUTABLE}")
    endif()
    find_package(Python 3.12 COMPONENTS Interpreter REQUIRED)
    set(_hgraph_pyarrow_probe [=[
import pathlib, pyarrow, sys
expected_major = int(sys.argv[1])
actual_major = int(pyarrow.__version__.split(".", 1)[0])
if actual_major != expected_major:
    raise SystemExit(
        f"pyarrow {pyarrow.__version__} provides Arrow ABI major {actual_major}; "
        f"this build requires major {expected_major}"
    )
root = pathlib.Path(pyarrow.__file__).resolve().parent
def pick(pattern):
    matches = sorted(root.glob(pattern))
    if not matches:
        raise SystemExit(f"missing pyarrow library matching {pattern!r} under {root}")
    return matches[0]
if sys.platform == "win32":
    arrow_link = pick("arrow.lib")
    compute_link = pick("arrow_compute.lib")
    arrow_runtime = pick("arrow.dll")
    compute_runtime = pick("arrow_compute.dll")
else:
    arrow_link = arrow_runtime = pick("libarrow.*")
    compute_link = compute_runtime = pick("libarrow_compute.*")
print(root)
print(root / "include")
print(arrow_link)
print(compute_link)
print(arrow_runtime)
print(compute_runtime)
]=])
    execute_process(
        COMMAND "${Python_EXECUTABLE}" -c "${_hgraph_pyarrow_probe}" "${HGRAPH_PYARROW_ABI_MAJOR}"
        OUTPUT_STRIP_TRAILING_WHITESPACE
        OUTPUT_VARIABLE _hgraph_pyarrow_info
        ERROR_VARIABLE _hgraph_pyarrow_error
        RESULT_VARIABLE _hgraph_pyarrow_result
    )
    if(NOT _hgraph_pyarrow_result EQUAL 0)
        message(FATAL_ERROR
            "HGRAPH_USE_PYARROW_ARROW=ON requires a compatible pyarrow installation: "
            "${_hgraph_pyarrow_error}")
    endif()
    string(REPLACE "\n" ";" _hgraph_pyarrow_info "${_hgraph_pyarrow_info}")
    list(GET _hgraph_pyarrow_info 0 _hgraph_pyarrow_dir)
    list(GET _hgraph_pyarrow_info 1 _hgraph_pyarrow_include)
    list(GET _hgraph_pyarrow_info 2 _hgraph_pyarrow_arrow_lib)
    list(GET _hgraph_pyarrow_info 3 _hgraph_pyarrow_compute_lib)
    list(GET _hgraph_pyarrow_info 4 HGRAPH_PYARROW_ARROW_RUNTIME)
    list(GET _hgraph_pyarrow_info 5 HGRAPH_PYARROW_COMPUTE_RUNTIME)
    if(NOT EXISTS "${_hgraph_pyarrow_include}/arrow/api.h")
        message(FATAL_ERROR "pyarrow headers not found under ${_hgraph_pyarrow_include}")
    endif()
    add_library(Arrow::arrow_shared SHARED IMPORTED GLOBAL)
    add_library(ArrowCompute::arrow_compute_shared SHARED IMPORTED GLOBAL)
    if(WIN32)
        set_target_properties(Arrow::arrow_shared PROPERTIES
            IMPORTED_IMPLIB "${_hgraph_pyarrow_arrow_lib}"
            IMPORTED_LOCATION "${HGRAPH_PYARROW_ARROW_RUNTIME}"
            INTERFACE_INCLUDE_DIRECTORIES "${_hgraph_pyarrow_include}"
        )
        set_target_properties(ArrowCompute::arrow_compute_shared PROPERTIES
            IMPORTED_IMPLIB "${_hgraph_pyarrow_compute_lib}"
            IMPORTED_LOCATION "${HGRAPH_PYARROW_COMPUTE_RUNTIME}"
            INTERFACE_INCLUDE_DIRECTORIES "${_hgraph_pyarrow_include}"
        )
    else()
        set_target_properties(Arrow::arrow_shared PROPERTIES
            IMPORTED_LOCATION "${_hgraph_pyarrow_arrow_lib}"
            INTERFACE_INCLUDE_DIRECTORIES "${_hgraph_pyarrow_include}"
        )
        set_target_properties(ArrowCompute::arrow_compute_shared PROPERTIES
            IMPORTED_LOCATION "${_hgraph_pyarrow_compute_lib}"
            INTERFACE_INCLUDE_DIRECTORIES "${_hgraph_pyarrow_include}"
        )
    endif()
    set(HGRAPH_PYARROW_LIBRARY_DIR "${_hgraph_pyarrow_dir}" CACHE PATH
        "Directory containing pyarrow's bundled Arrow libraries" FORCE)
else()
    find_package(Arrow CONFIG REQUIRED)
    find_package(ArrowCompute CONFIG REQUIRED)  # Arrow 24 splits compute into its own lib
endif()
target_link_libraries(hgraph_options INTERFACE Arrow::arrow_shared ArrowCompute::arrow_compute_shared)

# spdlog backs the LOGGER injectable (ruling 2026-07-04), built in
# SPDLOG_FMT_EXTERNAL mode against the fmt above so there is exactly one fmt
# in the process. A system spdlog is only trusted when fmt also came from the
# system (a fetched fmt paired with a system spdlog risks ODR mismatches).
if(fmt_FOUND)
    find_package(spdlog CONFIG QUIET)
endif()
if(NOT spdlog_FOUND)
    FetchContent_Declare(
        spdlog
        GIT_REPOSITORY https://github.com/gabime/spdlog.git
        GIT_TAG        v1.15.3
        GIT_SHALLOW    TRUE
    )
    set(SPDLOG_FMT_EXTERNAL ON CACHE BOOL "spdlog uses the project fmt" FORCE)
    set(SPDLOG_INSTALL OFF CACHE BOOL "Generate spdlog install target" FORCE)
    set(SPDLOG_BUILD_EXAMPLE OFF CACHE BOOL "Build spdlog examples" FORCE)
    FetchContent_MakeAvailable(spdlog)
endif()
if(spdlog_FOUND)
    target_link_libraries(hgraph_options INTERFACE spdlog::spdlog)
else()
    # The FetchContent'd spdlog target cannot join the install export set;
    # link it for the build tree only and let installed consumers resolve
    # spdlog via the exported find_dependency below.
    target_link_libraries(hgraph_options INTERFACE $<BUILD_INTERFACE:spdlog::spdlog>)
endif()

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

set(_hgraph_use_python_stable_abi OFF)
if(HGRAPH_BUILD_PYTHON_BINDINGS AND HGRAPH_PYTHON_STABLE_ABI)
    if(CMAKE_VERSION VERSION_LESS 3.26)
        message(FATAL_ERROR "HGRAPH_PYTHON_STABLE_ABI requires CMake 3.26 or newer")
    endif()
    set(_hgraph_use_python_stable_abi ON)
endif()

if(HGRAPH_BUILD_PYTHON_BINDINGS OR HGRAPH_ENABLE_PYTHON_USER_NODES)
    if(DEFINED Python3_EXECUTABLE AND NOT DEFINED Python_EXECUTABLE)
        set(Python_EXECUTABLE "${Python3_EXECUTABLE}")
    endif()

    if(_hgraph_use_python_stable_abi)
        # nanobind's package config validates Python::Module even though the
        # extension itself links only Python::SABIModule.
        set(_hgraph_python_components Interpreter Development.Module Development.SABIModule)
    else()
        set(_hgraph_python_components Interpreter Development.Module)
    endif()
    if(HGRAPH_ENABLE_PYTHON_USER_NODES AND NOT _hgraph_use_python_stable_abi)
        list(APPEND _hgraph_python_components Development.Embed)
    endif()

    find_package(Python 3.12 COMPONENTS ${_hgraph_python_components} REQUIRED)

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
    if(DEFINED NB_DIR AND EXISTS "${NB_DIR}/include")
        target_include_directories(hgraph_options INTERFACE
            $<BUILD_INTERFACE:${NB_DIR}/include>
        )
    endif()
    target_include_directories(hgraph_options INTERFACE
        $<BUILD_INTERFACE:${Python_INCLUDE_DIRS}>
    )
    if(_hgraph_use_python_stable_abi)
        target_link_libraries(hgraph_options INTERFACE Python::SABIModule)
        set(_hgraph_nanobind_library nanobind-static-abi3)
    else()
        target_link_libraries(hgraph_options INTERFACE Python::Module)
        set(_hgraph_nanobind_library nanobind-static)
    endif()
    if(HGRAPH_ENABLE_PYTHON_USER_NODES AND NOT _hgraph_use_python_stable_abi AND TARGET Python::Python)
        target_link_libraries(hgraph_options INTERFACE Python::Python)
    endif()
    if(COMMAND nanobind_build_library)
        nanobind_build_library(${_hgraph_nanobind_library} AS_SYSINCLUDE)
        target_link_libraries(hgraph_options INTERFACE $<BUILD_INTERFACE:${_hgraph_nanobind_library}>)
    endif()
    # Standalone user-node builds embed Python and link Python::Python. Stable
    # bridge builds resolve their limited-API symbols from the importing Python
    # process, keeping the extension independent of the interpreter minor.
    if(HGRAPH_ENABLE_PYTHON_USER_NODES)
        target_compile_definitions(hgraph_options INTERFACE HGRAPH_ENABLE_PYTHON_USER_NODES=1)
    else()
        target_compile_definitions(hgraph_options INTERFACE HGRAPH_ENABLE_PYTHON_USER_NODES=0)
    endif()
else()
    if(HGRAPH_ENABLE_IDE_PYTHON_HEADER_HINTS)
        if(WIN32)
            set(_hgraph_ide_python "${PROJECT_SOURCE_DIR}/.venv/Scripts/python.exe")
        else()
            set(_hgraph_ide_python "${PROJECT_SOURCE_DIR}/.venv/bin/python")
        endif()

        if(EXISTS "${_hgraph_ide_python}")
            execute_process(
                COMMAND "${_hgraph_ide_python}" -m nanobind --include_dir
                OUTPUT_STRIP_TRAILING_WHITESPACE
                OUTPUT_VARIABLE _hgraph_ide_nanobind_include
                RESULT_VARIABLE _hgraph_ide_nanobind_result
                ERROR_QUIET
            )
            execute_process(
                COMMAND "${_hgraph_ide_python}" -c "import sysconfig; print(sysconfig.get_path('include') or '')"
                OUTPUT_STRIP_TRAILING_WHITESPACE
                OUTPUT_VARIABLE _hgraph_ide_python_include
                RESULT_VARIABLE _hgraph_ide_python_result
                ERROR_QUIET
            )

            if(_hgraph_ide_nanobind_result EQUAL 0 AND EXISTS "${_hgraph_ide_nanobind_include}")
                target_include_directories(hgraph_options INTERFACE
                    $<BUILD_INTERFACE:${_hgraph_ide_nanobind_include}>
                )
            endif()
            if(_hgraph_ide_python_result EQUAL 0 AND EXISTS "${_hgraph_ide_python_include}")
                target_include_directories(hgraph_options INTERFACE
                    $<BUILD_INTERFACE:${_hgraph_ide_python_include}>
                )
            endif()
        endif()
    endif()
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

set(_hgraph_export_targets hgraph_options hgraph_runtime hgraph_wiring hgraph_stdlib hgraph_core)
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

if(EXISTS "${PROJECT_SOURCE_DIR}/tools/debugger")
    install(DIRECTORY tools/debugger/ DESTINATION ${CMAKE_INSTALL_DATADIR}/hgraph/debugger
        FILES_MATCHING
            PATTERN "*.py"
            PATTERN "README.md"
    )
endif()

write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/hgraphConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

set(_hgraph_config_dependencies
    "find_dependency(Threads)\n"
    "find_dependency(fmt CONFIG)\n"
    "find_dependency(Arrow CONFIG)\n"
    "find_dependency(ArrowCompute CONFIG)\n"
    "find_dependency(spdlog CONFIG)\n"
)
if(HGRAPH_BUILD_PYTHON_BINDINGS OR HGRAPH_ENABLE_PYTHON_USER_NODES)
    if(_hgraph_use_python_stable_abi)
        set(_hgraph_config_python_components "Interpreter Development.Module Development.SABIModule")
    else()
        set(_hgraph_config_python_components "Interpreter Development.Module")
    endif()
    if(HGRAPH_ENABLE_PYTHON_USER_NODES AND NOT _hgraph_use_python_stable_abi)
        set(_hgraph_config_python_components "Interpreter Development.Module Development.Embed")
    endif()
    string(APPEND _hgraph_config_dependencies
        "find_dependency(Python 3.12 COMPONENTS ${_hgraph_config_python_components})\n"
        "find_dependency(nanobind CONFIG)\n"
    )
endif()

file(WRITE "${PROJECT_BINARY_DIR}/hgraphConfig.cmake"
    "include(CMakeFindDependencyMacro)\n"
    "${_hgraph_config_dependencies}"
    "include(\"\${CMAKE_CURRENT_LIST_DIR}/hgraphTargets.cmake\")\n"
)
if(HGRAPH_BUILD_PYTHON_BINDINGS)
    file(APPEND "${PROJECT_BINARY_DIR}/hgraphConfig.cmake"
        "if(TARGET hgraph::options)\n"
        "  target_link_libraries(hgraph::options INTERFACE simdjson::simdjson fmt::fmt spdlog::spdlog)\n"
        "endif()\n"
    )
endif()
if(HGRAPH_BUILD_PYTHON_BINDINGS OR HGRAPH_ENABLE_PYTHON_USER_NODES)
    file(APPEND "${PROJECT_BINARY_DIR}/hgraphConfig.cmake"
        "if(TARGET hgraph::options AND DEFINED NB_DIR AND EXISTS \"\${NB_DIR}/include\")\n"
        "  target_include_directories(hgraph::options INTERFACE \"\${NB_DIR}/include\")\n"
        "endif()\n"
        "if(TARGET hgraph::options AND Python_INCLUDE_DIRS)\n"
        "  target_include_directories(hgraph::options INTERFACE \${Python_INCLUDE_DIRS})\n"
        "endif()\n"
    )
endif()

install(EXPORT hgraphTargets
    NAMESPACE hgraph::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph
)
install(FILES
    "${PROJECT_BINARY_DIR}/hgraphConfig.cmake"
    "${PROJECT_BINARY_DIR}/hgraphConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hgraph
)

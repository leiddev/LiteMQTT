cmake_minimum_required(VERSION 3.16)

project(LiteMQTT
    VERSION 1.0.0
    DESCRIPTION "A small header-only MQTT client built on standalone Asio"
    LANGUAGES CXX)

include(GNUInstallDirs)

# ---------------------------------------------------------------------------
# Layout of this file
#
#   1. options
#   2. the library target (what a consumer links: LiteMQTT::litemqtt)
#   3. the executables this repository builds for itself (demo, tools, tests)
#   4. install / export rules
#
# The split is deliberate: everything a consumer needs lives on `litemqtt`, and
# everything that is only about building *here* stays out of it.  That is what
# lets the project be dropped into another tree with add_subdirectory without
# dragging a C++ standard, a charset flag or a test dependency along.
# ---------------------------------------------------------------------------

# A plain ${CMAKE_SOURCE_DIR} comparison also works on CMake < 3.21, which is
# still what several distributions ship.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    set(LITEMQTT_IS_TOP_LEVEL ON)
else()
    set(LITEMQTT_IS_TOP_LEVEL OFF)
endif()

option(LITEMQTT_BUILD_EXAMPLES "Build the demo and the two reconnect tools" ${LITEMQTT_IS_TOP_LEVEL})
option(LITEMQTT_BUILD_TESTS    "Build the unit tests"                     ${LITEMQTT_IS_TOP_LEVEL})
option(LITEMQTT_INSTALL        "Generate install and package rules"       ${LITEMQTT_IS_TOP_LEVEL})
option(LITEMQTT_REQUIRE_TESTS  "Fail configure when GoogleTest is missing" OFF)
option(LITEMQTT_FETCH_GTEST    "Download googletest when it is not installed" OFF)

# ---------------------------------------------------------------------------
# The library
#
# LiteMQTT is header only, so the target is an INTERFACE library.  It carries
# the include directory, the C++14 requirement, the Windows spellings Asio
# expects, and Asio itself: client.hpp and connection.hpp include <asio.hpp>,
# so Asio is part of the public interface and not an implementation detail.
# ---------------------------------------------------------------------------
add_library(litemqtt INTERFACE)
add_library(LiteMQTT::litemqtt ALIAS litemqtt)

target_include_directories(litemqtt INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/inc>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

target_compile_features(litemqtt INTERFACE cxx_std_14)

# `_WIN32_WINNT` is deliberately *not* set here.  It is an application-wide
# decision -- other libraries in the same program may require a newer value
# (cpp-httplib wants Windows 10, for one) -- and a header-only library must not
# lower it for its consumers.  Asio itself falls back to 0x0601 in its headers.

# A parent project that already provides an `asio` target (a vendored copy of
# its own, or a package that defines one) keeps it.  Otherwise the standalone
# Asio that ships under 3rdparty/ is added here.
if(NOT TARGET asio)
    if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/asio/CMakeLists.txt")
        message(FATAL_ERROR
            "Asio is not available: no `asio` target was defined by the parent "
            "project and 3rdparty/asio is missing.  Provide an asio target or "
            "restore the vendored copy.")
    endif()
    add_subdirectory(3rdparty/asio ${CMAKE_CURRENT_BINARY_DIR}/3rdparty/asio)
    set(LITEMQTT_BUNDLED_ASIO ON)
else()
    set(LITEMQTT_BUNDLED_ASIO OFF)
endif()

find_package(Threads REQUIRED)
target_link_libraries(litemqtt INTERFACE
    $<BUILD_INTERFACE:asio>
    Threads::Threads)

# ---------------------------------------------------------------------------
# Flags for the executables of this repository.  They are not part of the
# library interface: a consumer does not have to inherit our warnings policy.
# Keeping them on a target instead of add_compile_options() also stops them
# leaking into subprojects -- MSVC rejects /source-charset:utf-8 next to the
# /utf-8 googletest sets for itself (D8016), and CI fetches googletest as a
# subproject.
# ---------------------------------------------------------------------------
add_library(litemqtt_build_flags INTERFACE)
if(MSVC)
    target_compile_options(litemqtt_build_flags INTERFACE /utf-8)
    target_compile_definitions(litemqtt_build_flags INTERFACE
        _CRT_SECURE_NO_WARNINGS
        _WINSOCK_DEPRECATED_NO_WARNINGS
        WIN32_LEAN_AND_MEAN
        NOMINMAX
        _WIN32_WINNT=0x0601)
endif()

# ---------------------------------------------------------------------------
# The demo and the reconnect tools.  They bind loopback ports and are meant to
# be run by hand; only the unit tests are registered with CTest.
# ---------------------------------------------------------------------------
if(LITEMQTT_BUILD_EXAMPLES)
    add_executable(LiteMQTTDemo examples/demo.cc)
    target_link_libraries(LiteMQTTDemo PRIVATE LiteMQTT::litemqtt litemqtt_build_flags)

    add_executable(reconnect_probe tools/reconnect_probe.cc)
    target_link_libraries(reconnect_probe PRIVATE LiteMQTT::litemqtt litemqtt_build_flags)

    add_executable(reconnect_stress tools/reconnect_stress.cc)
    target_link_libraries(reconnect_stress PRIVATE LiteMQTT::litemqtt litemqtt_build_flags)
endif()

# ---------------------------------------------------------------------------
# Unit tests
#
# GoogleTest is optional so that the library and the demo build anywhere.  CI
# turns LITEMQTT_REQUIRE_TESTS on, which makes a missing GoogleTest a configure
# error instead of a quiet "skipping unit tests", and LITEMQTT_FETCH_GTEST on,
# which downloads googletest when the system does not have it (the Windows
# runners have no package manager that ships it).
# ---------------------------------------------------------------------------
if(LITEMQTT_BUILD_TESTS)
    enable_testing()

    find_package(GTest QUIET)
    if(NOT GTest_FOUND AND LITEMQTT_FETCH_GTEST)
        include(FetchContent)
        # 1.14 is the last googletest that still supports C++14; newer releases
        # require C++17, which this project does not.
        FetchContent_Declare(googletest
            GIT_REPOSITORY https://github.com/google/googletest.git
            GIT_TAG v1.14.0
            GIT_SHALLOW TRUE)
        set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
        set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
        FetchContent_MakeAvailable(googletest)
        set(GTest_FOUND TRUE)
    endif()

    if(GTest_FOUND)
        # An installed GoogleTest comes through CMake's FindGTest and also offers
        # GTest::GTest / GTest::Main; the fetched one only has the upstream aliases.
        if(TARGET GTest::GTest)
            set(LITEMQTT_GTEST_LIBS GTest::GTest GTest::Main)
        else()
            set(LITEMQTT_GTEST_LIBS GTest::gtest GTest::gtest_main)
        endif()

        function(litemqtt_add_test name)
            add_executable(${name} test/${name}.cc)
            target_link_libraries(${name} PRIVATE
                LiteMQTT::litemqtt ${LITEMQTT_GTEST_LIBS} litemqtt_build_flags)
            add_test(NAME ${name} COMMAND ${name})
        endfunction()

        litemqtt_add_test(codec_test)
        litemqtt_add_test(robustness_test)
        litemqtt_add_test(client_test)
        litemqtt_add_test(qos_test)
        litemqtt_add_test(log_test)
        litemqtt_add_test(connection_test)
        litemqtt_add_test(helper_test)
    elseif(LITEMQTT_REQUIRE_TESTS)
        message(FATAL_ERROR
            "GoogleTest not found and LITEMQTT_REQUIRE_TESTS is ON. "
            "Install it (libgtest-dev / vcpkg) or configure with -DLITEMQTT_FETCH_GTEST=ON.")
    else()
        message(STATUS "GoogleTest not found, skipping unit tests")
    endif()
endif()

# ---------------------------------------------------------------------------
# Install and package
#
# Only the interface target is exported; the bundled Asio is not a separate
# target in the installed tree but a second header tree in the same include
# root, so `find_package(LiteMQTT)` gives a self-contained target and no Asio
# package has to be located.  When the parent supplied its own asio target the
# header tree is not ours to install, and the interface export is still valid
# because the build-interface dependency is not exported.
# ---------------------------------------------------------------------------
if(LITEMQTT_INSTALL)
    include(CMakePackageConfigHelpers)

    install(TARGETS litemqtt EXPORT LiteMQTTTargets)
    install(DIRECTORY inc/litemqtt DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
    if(LITEMQTT_BUNDLED_ASIO)
        # Only the headers: the asio tree also carries automake files that have
        # no business in an installed prefix.
        install(FILES 3rdparty/asio/include/asio.hpp
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
        install(DIRECTORY 3rdparty/asio/include/asio
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
    endif()

    set(LITEMQTT_CMAKE_DIR ${CMAKE_INSTALL_LIBDIR}/cmake/LiteMQTT)
    install(EXPORT LiteMQTTTargets
        NAMESPACE LiteMQTT::
        FILE LiteMQTTTargets.cmake
        DESTINATION ${LITEMQTT_CMAKE_DIR})

    configure_package_config_file(cmake/LiteMQTTConfig.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/LiteMQTTConfig.cmake
        INSTALL_DESTINATION ${LITEMQTT_CMAKE_DIR})
    write_basic_package_version_file(
        ${CMAKE_CURRENT_BINARY_DIR}/LiteMQTTConfigVersion.cmake
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion)

    install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/LiteMQTTConfig.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/LiteMQTTConfigVersion.cmake
        DESTINATION ${LITEMQTT_CMAKE_DIR})
endif()

if(LITEMQTT_IS_TOP_LEVEL)
    message(STATUS "LiteMQTT ${PROJECT_VERSION} on ${CMAKE_SYSTEM_NAME}")
    message(STATUS "  C++ compiler : ${CMAKE_CXX_COMPILER}")
    message(STATUS "  library target: LiteMQTT::litemqtt")
    message(STATUS "  examples     : ${LITEMQTT_BUILD_EXAMPLES}")
    message(STATUS "  tests        : ${LITEMQTT_BUILD_TESTS}")
    message(STATUS "  install rules: ${LITEMQTT_INSTALL}")
endif()

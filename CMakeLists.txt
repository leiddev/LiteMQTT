cmake_minimum_required(VERSION 3.14)
project(LiteMQTT)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(ExternalProject)
include(3rdparty/asio.cmake)

add_executable(LiteMQTTDemo main.cc)

if(WIN32)
    add_definitions(-DWIN32_LEAN_AND_MEAN)
    add_definitions(-DNOMINMAX)
else()
    find_package(Threads REQUIRED)
endif()

# Compiler settings live in an interface target rather than in plain
# add_compile_options() so they do not leak into third-party subprojects:
# MSVC rejects /source-charset:utf-8 next to the /utf-8 that googletest sets for
# itself (D8016), and CI fetches googletest as a subproject.
add_library(litemqtt_options INTERFACE)
if(CMAKE_CXX_COMPILER_ID MATCHES MSVC)
    target_compile_options(litemqtt_options INTERFACE
        /source-charset:utf-8
        /execution-charset:utf-8
        -D_CRT_SECURE_NO_WARNINGS
        -D_WINSOCK_DEPRECATED_NO_WARNINGS)
endif()

target_include_directories(LiteMQTTDemo PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/inc)
target_link_libraries(LiteMQTTDemo asio litemqtt_options)
target_compile_definitions(LiteMQTTDemo PRIVATE ASIO_STANDALONE _WIN32_WINNT=0x0601)

# Reconnect probe suite: manual tool (binds loopback ports), not part of ctest.
add_executable(reconnect_probe tools/reconnect_probe.cc)
target_include_directories(reconnect_probe PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/inc)
target_link_libraries(reconnect_probe asio litemqtt_options)
target_compile_definitions(reconnect_probe PRIVATE ASIO_STANDALONE _WIN32_WINNT=0x0601)

# Reconnect stress harness: manual tool (chaos broker on a loopback port, keeps
# publishing while the link flaps / blackholes / disappears), not part of ctest.
add_executable(reconnect_stress tools/reconnect_stress.cc)
target_include_directories(reconnect_stress PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/inc)
target_link_libraries(reconnect_stress asio litemqtt_options)
target_compile_definitions(reconnect_stress PRIVATE ASIO_STANDALONE _WIN32_WINNT=0x0601)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang" AND UNIX)
    target_link_libraries(reconnect_stress pthread)
endif()

# Unit tests
#
# GoogleTest is optional so that the library and the demo build anywhere.  CI
# turns LITEMQTT_REQUIRE_TESTS on, which makes a missing GoogleTest a configure
# error instead of a quiet "skipping unit tests", and LITEMQTT_FETCH_GTEST on,
# which downloads googletest when the system does not have it (Windows runners
# have no package manager that ships it).
option(LITEMQTT_REQUIRE_TESTS "Fail configure when GoogleTest is missing" OFF)
option(LITEMQTT_FETCH_GTEST "Download googletest when it is not installed" OFF)

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
    # An installed GoogleTest comes through CMake's FindGTest and exposes
    # GTest::GTest / GTest::Main; the fetched one only has the upstream aliases.
    if(TARGET GTest::GTest)
        set(LITEMQTT_GTEST_LIBS GTest::GTest GTest::Main)
    else()
        set(LITEMQTT_GTEST_LIBS GTest::gtest GTest::gtest_main)
    endif()

    function(litemqtt_add_test name)
        add_executable(${name} test/${name}.cc)
        target_include_directories(${name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/inc)
        target_link_libraries(${name} ${LITEMQTT_GTEST_LIBS} asio litemqtt_options)
        target_compile_definitions(${name} PRIVATE ASIO_STANDALONE _WIN32_WINNT=0x0601)
        add_test(NAME ${name} COMMAND ${name})
    endfunction()

    litemqtt_add_test(codec_test)
    litemqtt_add_test(client_test)
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

message(STATUS "Build completed for ${CMAKE_SYSTEM_NAME}")
message(STATUS "C compiler: ${CMAKE_C_COMPILER}")
message(STATUS "C++ compiler: ${CMAKE_CXX_COMPILER}")

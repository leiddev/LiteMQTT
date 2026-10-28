cmake_minimum_required(VERSION 3.10)
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

if(CMAKE_CXX_COMPILER_ID MATCHES MSVC)
    add_compile_options(/source-charset:utf-8)
    add_compile_options(/execution-charset:utf-8)
    add_compile_options(-D_CRT_SECURE_NO_WARNINGS)
    add_compile_options(-D_WINSOCK_DEPRECATED_NO_WARNINGS)
    add_compile_options(-DNOMINMAX) # Prevent Windows.h from defining min/max macros
endif()

target_include_directories(LiteMQTTDemo PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/inc)
target_link_libraries(LiteMQTTDemo asio)
target_compile_definitions(LiteMQTTDemo PRIVATE ASIO_STANDALONE _WIN32_WINNT=0x0601)

# Reconnect probe suite: manual tool (binds loopback ports), not part of ctest.
add_executable(reconnect_probe tools/reconnect_probe.cc)
target_include_directories(reconnect_probe PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/inc)
target_link_libraries(reconnect_probe asio)
target_compile_definitions(reconnect_probe PRIVATE ASIO_STANDALONE _WIN32_WINNT=0x0601)

# Reconnect stress harness: manual tool (chaos broker on a loopback port, keeps
# publishing while the link flaps / blackholes / disappears), not part of ctest.
add_executable(reconnect_stress tools/reconnect_stress.cc)
target_include_directories(reconnect_stress PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/inc)
target_link_libraries(reconnect_stress asio)
target_compile_definitions(reconnect_stress PRIVATE ASIO_STANDALONE _WIN32_WINNT=0x0601)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang" AND UNIX)
    target_link_libraries(reconnect_stress pthread)
endif()

# Unit tests
enable_testing()
find_package(GTest QUIET)
if(GTest_FOUND)
    add_executable(codec_test test/codec_test.cc)
    target_include_directories(codec_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/inc)
    target_link_libraries(codec_test GTest::GTest GTest::Main asio)
    target_compile_definitions(codec_test PRIVATE ASIO_STANDALONE _WIN32_WINNT=0x0601)
    add_test(NAME codec_test COMMAND codec_test)

    add_executable(client_test test/client_test.cc)
    target_include_directories(client_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/inc)
    target_link_libraries(client_test GTest::GTest GTest::Main asio)
    target_compile_definitions(client_test PRIVATE ASIO_STANDALONE _WIN32_WINNT=0x0601)
    add_test(NAME client_test COMMAND client_test)

    add_executable(log_test test/log_test.cc)
    target_include_directories(log_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/inc)
    target_link_libraries(log_test GTest::GTest GTest::Main asio)
    target_compile_definitions(log_test PRIVATE ASIO_STANDALONE _WIN32_WINNT=0x0601)
    add_test(NAME log_test COMMAND log_test)

    add_executable(connection_test test/connection_test.cc)
    target_include_directories(connection_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/inc)
    target_link_libraries(connection_test GTest::GTest GTest::Main asio)
    target_compile_definitions(connection_test PRIVATE ASIO_STANDALONE _WIN32_WINNT=0x0601)
    add_test(NAME connection_test COMMAND connection_test)

    add_executable(helper_test test/helper_test.cc)
    target_include_directories(helper_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/inc)
    target_link_libraries(helper_test GTest::GTest GTest::Main asio)
    target_compile_definitions(helper_test PRIVATE ASIO_STANDALONE _WIN32_WINNT=0x0601)
    add_test(NAME helper_test COMMAND helper_test)
else()
    message(STATUS "GoogleTest not found, skipping unit tests")
endif()

message(STATUS "Build completed for ${CMAKE_SYSTEM_NAME}")
message(STATUS "C compiler: ${CMAKE_C_COMPILER}")
message(STATUS "C++ compiler: ${CMAKE_CXX_COMPILER}")

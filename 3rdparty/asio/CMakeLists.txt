# Standalone Asio (headers only).
#
# The tree under include/ is a verbatim copy of the upstream release; this file
# only wraps it in a target so the rest of the build can depend on `asio`
# instead of repeating -I flags and Threads plumbing in every executable.
add_library(asio INTERFACE)

target_include_directories(asio INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

target_compile_features(asio INTERFACE cxx_std_11)

find_package(Threads REQUIRED)
target_link_libraries(asio INTERFACE Threads::Threads)

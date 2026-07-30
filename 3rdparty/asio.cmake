add_library(asio INTERFACE)

target_include_directories(asio INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/asio/include
)

find_package(Threads REQUIRED)
target_link_libraries(asio INTERFACE Threads::Threads)

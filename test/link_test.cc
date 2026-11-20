// Two translation units, one program.  A header-only library is included by
// every one of them, so anything it defines rather than declares has to link
// once.  gcc 9 in C++14 puts an out-of-class `constexpr` static data member in
// .rodata as a strong symbol, so a definition in the header is "multiple
// definition" as soon as a second .cc includes it -- a library whose own tests
// are one .cc each never sees that.
//
// link_test_other.cc is the second unit; it includes the same headers.

#include <gtest/gtest.h>

#include <cstddef>
#include <memory>

#include "litemqtt/connection.hpp"

// Defined in link_test_other.cc.
std::size_t link_test_packet_cap();
std::size_t link_test_write_queue();

namespace {

TEST(HeaderOnlyLibrary, DefaultsAgreeAcrossTranslationUnits) {
    asio::io_context io;
    auto conn = std::make_shared<litemqtt::connection>(io);
    EXPECT_EQ(conn->max_packet_size(), link_test_packet_cap());
    EXPECT_EQ(conn->max_write_queue(), link_test_write_queue());
}

}  // namespace

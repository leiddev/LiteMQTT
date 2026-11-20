// The second translation unit of link_test (see the comment there).  It has to
// include every public header: this is the unit that turns a definition left in
// one of them into a duplicate symbol.

#include <cstddef>

#include "litemqtt/mqtt_helper.hpp"

std::size_t link_test_packet_cap() {
    return litemqtt::connection::kDefaultMaxPacketSize;
}

std::size_t link_test_write_queue() {
    return litemqtt::connection::kDefaultMaxWriteQueue;
}

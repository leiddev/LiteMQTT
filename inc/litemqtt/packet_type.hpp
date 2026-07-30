#ifndef LITEMQTT_PACKET_TYPE_HPP
#define LITEMQTT_PACKET_TYPE_HPP

#include <cstdint>

namespace litemqtt {

enum class packet_type : uint8_t {
    reserved0 = 0,
    connect = 1,
    connack = 2,
    publish = 3,
    puback = 4,
    pubrec = 5,
    pubrel = 6,
    pubcomp = 7,
    subscribe = 8,
    suback = 9,
    unsubscribe = 10,
    unsuback = 11,
    pingreq = 12,
    pingresp = 13,
    disconnect = 14,
    reserved15 = 15,
};

constexpr uint8_t to_uint8(packet_type pt) {
    return static_cast<uint8_t>(pt);
}

constexpr bool is_valid_packet_type(uint8_t byte) {
    return byte >= 1 && byte <= 14 && byte != 0 && byte != 15;
}

constexpr packet_type to_packet_type(uint8_t byte) {
    return static_cast<packet_type>(byte);
}

constexpr const char* to_string(packet_type pt) {
    switch (pt) {
        case packet_type::connect: return "CONNECT";
        case packet_type::connack: return "CONNACK";
        case packet_type::publish: return "PUBLISH";
        case packet_type::puback: return "PUBACK";
        case packet_type::pubrec: return "PUBREC";
        case packet_type::pubrel: return "PUBREL";
        case packet_type::pubcomp: return "PUBCOMP";
        case packet_type::subscribe: return "SUBSCRIBE";
        case packet_type::suback: return "SUBACK";
        case packet_type::unsubscribe: return "UNSUBSCRIBE";
        case packet_type::unsuback: return "UNSUBACK";
        case packet_type::pingreq: return "PINGREQ";
        case packet_type::pingresp: return "PINGRESP";
        case packet_type::disconnect: return "DISCONNECT";
        default: return "UNKNOWN";
    }
}

}  // namespace litemqtt

#endif  // LITEMQTT_PACKET_TYPE_HPP

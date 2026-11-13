#ifndef LITEMQTT_PACKETS_HPP
#define LITEMQTT_PACKETS_HPP

#include <string>
#include <vector>
#include <utility>

#include "buffer_reader.hpp"
#include "buffer_writer.hpp"
#include "packet_type.hpp"

namespace litemqtt {

constexpr uint16_t MQTT_PROTOCOL_LEVEL = 4;
constexpr const char* MQTT_PROTOCOL_NAME = "MQTT";

namespace detail {

/// Reads the fixed header of @p data and checks that it is well formed: a
/// known packet type, a remaining length that fits in four bytes, and a body
/// of exactly that many bytes.  Every parse() starts here, so a truncated or
/// padded packet is rejected instead of yielding a half-filled struct.
inline bool read_fixed_header(buffer_reader& reader, packet_type expected) {
    uint8_t packet_type_byte = 0;
    if (!reader.read_uint8(packet_type_byte)) {
        return false;
    }
    if (to_packet_type(packet_type_byte >> 4) != expected) {
        return false;
    }
    std::size_t remaining_len = 0;
    if (!reader.read_remaining_length(remaining_len)) {
        return false;
    }
    // The reader is positioned right after the length field; whatever follows
    // has to be exactly the declared body.
    return remaining_len == reader.remaining();
}

}  // namespace detail

struct connect_packet {
    std::string client_id;
    uint16_t keep_alive_seconds = 0;
    bool clean_session = false;
    std::string username;
    std::string password;

    std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> buf;
        buffer_writer writer(buf);

        buffer_writer variable_header;
        variable_header.write_string(MQTT_PROTOCOL_NAME);
        variable_header.write_uint8(MQTT_PROTOCOL_LEVEL);

        uint8_t connect_flags = 0;
        if (clean_session) {
            connect_flags |= 0x02;
        }
        if (!username.empty()) {
            connect_flags |= 0x80;
        }
        if (!password.empty()) {
            connect_flags |= 0x40;
        }
        variable_header.write_uint8(connect_flags);

        variable_header.write_uint16(keep_alive_seconds);
        variable_header.write_string(client_id);

        if (!username.empty()) {
            variable_header.write_string(username);
        }
        if (!password.empty()) {
            variable_header.write_string(password);
        }

        writer.write_uint8(to_uint8(packet_type::connect) << 4);
        writer.write_remaining_length(variable_header.size());
        writer.write_bytes(variable_header.data(), variable_header.size());

        return buf;
    }

    /// Parses a CONNECT into @p pkt.  Returns false when the packet is
    /// malformed or truncated; @p pkt is left unspecified in that case.
    static bool parse(const std::vector<uint8_t>& data, connect_packet& pkt) {
        buffer_reader reader(data);
        if (!detail::read_fixed_header(reader, packet_type::connect)) {
            return false;
        }

        std::string protocol_name;
        if (!reader.read_string(protocol_name)) {
            return false;
        }
        if (protocol_name != MQTT_PROTOCOL_NAME) {
            return false;
        }

        uint8_t protocol_level = 0;
        if (!reader.read_uint8(protocol_level)) {
            return false;
        }
        if (protocol_level != MQTT_PROTOCOL_LEVEL) {
            return false;
        }

        uint8_t connect_flags = 0;
        if (!reader.read_uint8(connect_flags)) {
            return false;
        }
        pkt.clean_session = (connect_flags & 0x02) != 0;
        const bool has_username = (connect_flags & 0x80) != 0;
        const bool has_password = (connect_flags & 0x40) != 0;

        // MQTT 3.1.1 section 3.1.2.9: a password without a user name is invalid.
        if (has_password && !has_username) {
            return false;
        }

        if (!reader.read_uint16(pkt.keep_alive_seconds)) {
            return false;
        }
        if (!reader.read_string(pkt.client_id)) {
            return false;
        }
        if (has_username && !reader.read_string(pkt.username)) {
            return false;
        }
        if (has_password && !reader.read_string(pkt.password)) {
            return false;
        }
        return reader.empty();
    }
};


struct connack_packet {
    uint8_t session_present = 0;
    uint8_t return_code = 0;

    std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> buf;
        buffer_writer writer(buf);

        writer.write_uint8(to_uint8(packet_type::connack) << 4);
        writer.write_uint8(2);
        writer.write_uint8(session_present & 0x01);
        writer.write_uint8(return_code);

        return buf;
    }

    /// Parses a CONNACK into @p pkt.  Returns false when the packet is
    /// malformed or truncated.
    static bool parse(const std::vector<uint8_t>& data, connack_packet& pkt) {
        buffer_reader reader(data);
        if (!detail::read_fixed_header(reader, packet_type::connack)) {
            return false;
        }
        if (!reader.read_uint8(pkt.session_present)) {
            return false;
        }
        return reader.read_uint8(pkt.return_code);
    }
};

struct publish_packet {
    uint16_t packet_id = 0;
    uint8_t qos = 0;
    bool dup = false;
    std::string topic_name;
    std::string payload;

    std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> buf;
        buffer_writer writer(buf);

        buffer_writer variable_header;
        variable_header.write_string(topic_name);
        if (qos > 0) {
            variable_header.write_uint16(packet_id);
        }
        variable_header.write_bytes(
            reinterpret_cast<const uint8_t*>(payload.data()),
            payload.size()
        );

        uint8_t flags = (qos & 0x03) << 1;
        if (dup) {
            flags |= 0x08;
        }
        writer.write_uint8((to_uint8(packet_type::publish) << 4) | flags);
        writer.write_remaining_length(variable_header.size());
        writer.write_bytes(variable_header.data(), variable_header.size());

        return buf;
    }

    /// Parses a PUBLISH into @p pkt.  Returns false when the packet is
    /// malformed or truncated, including a QoS of 3, which the flags cannot
    /// legally encode.
    static bool parse(const std::vector<uint8_t>& data, publish_packet& pkt) {
        buffer_reader reader(data);

        uint8_t packet_type_byte = 0;
        if (!reader.read_uint8(packet_type_byte)) {
            return false;
        }
        if (to_packet_type(packet_type_byte >> 4) != packet_type::publish) {
            return false;
        }
        pkt.dup = ((packet_type_byte >> 3) & 0x01) != 0;
        pkt.qos = (packet_type_byte >> 1) & 0x03;
        if (pkt.qos > 2) {
            return false;
        }

        std::size_t remaining_len = 0;
        if (!reader.read_remaining_length(remaining_len)) {
            return false;
        }
        if (remaining_len != reader.remaining()) {
            return false;
        }

        if (!reader.read_string(pkt.topic_name)) {
            return false;
        }
        if (pkt.qos > 0 && !reader.read_uint16(pkt.packet_id)) {
            return false;
        }

        const std::size_t payload_len = reader.remaining();
        if (payload_len > 0) {
            pkt.payload.resize(payload_len);
            if (!reader.read_bytes(reinterpret_cast<uint8_t*>(&pkt.payload[0]), payload_len)) {
                return false;
            }
        }
        return true;
    }
};

struct subscribe_packet {
    uint16_t packet_id = 0;
    std::vector<std::pair<std::string, uint8_t>> topic_filters;

    std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> buf;
        buffer_writer writer(buf);

        buffer_writer variable_header;
        variable_header.write_uint16(packet_id);

        for (const auto& tf : topic_filters) {
            variable_header.write_string(tf.first);
            variable_header.write_uint8(tf.second);
        }

        writer.write_uint8((to_uint8(packet_type::subscribe) << 4) | 0x02);
        writer.write_remaining_length(variable_header.size());
        writer.write_bytes(variable_header.data(), variable_header.size());

        return buf;
    }
};

struct suback_packet {
    uint16_t packet_id = 0;
    std::vector<uint8_t> return_codes;

    std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> buf;
        buffer_writer writer(buf);

        buffer_writer payload;
        payload.write_uint16(packet_id);
        for (uint8_t rc : return_codes) {
            payload.write_uint8(rc);
        }

        writer.write_uint8(to_uint8(packet_type::suback) << 4);
        writer.write_remaining_length(payload.size());
        writer.write_bytes(payload.data(), payload.size());

        return buf;
    }

    /// Parses a SUBACK into @p pkt.  Returns false when the packet is
    /// malformed or truncated.  An empty return-code list is legal on the wire
    /// but useless; parse() accepts it and leaves the check to the caller.
    static bool parse(const std::vector<uint8_t>& data, suback_packet& pkt) {
        buffer_reader reader(data);
        if (!detail::read_fixed_header(reader, packet_type::suback)) {
            return false;
        }
        if (!reader.read_uint16(pkt.packet_id)) {
            return false;
        }
        while (!reader.empty()) {
            uint8_t rc = 0;
            if (!reader.read_uint8(rc)) {
                return false;
            }
            pkt.return_codes.push_back(rc);
        }
        return true;
    }
};

struct unsubscribe_packet {
    uint16_t packet_id = 0;
    std::vector<std::string> topic_filters;

    std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> buf;
        buffer_writer writer(buf);

        buffer_writer variable_header;
        variable_header.write_uint16(packet_id);

        for (const auto& tf : topic_filters) {
            variable_header.write_string(tf);
        }

        writer.write_uint8((to_uint8(packet_type::unsubscribe) << 4) | 0x02);
        writer.write_remaining_length(variable_header.size());
        writer.write_bytes(variable_header.data(), variable_header.size());

        return buf;
    }
};

struct puback_packet {
    uint16_t packet_id = 0;

    std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> buf;
        buffer_writer writer(buf);
        writer.write_uint8(to_uint8(packet_type::puback) << 4);
        writer.write_uint8(2);
        writer.write_uint16(packet_id);
        return buf;
    }

    /// Parses a PUBACK into @p pkt.  Returns false when the packet is
    /// malformed or truncated.
    static bool parse(const std::vector<uint8_t>& data, puback_packet& pkt) {
        buffer_reader reader(data);
        if (!detail::read_fixed_header(reader, packet_type::puback)) {
            return false;
        }
        return reader.read_uint16(pkt.packet_id);
    }
};

struct pubcomp_packet {
    uint16_t packet_id = 0;

    std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> buf;
        buffer_writer writer(buf);
        writer.write_uint8(to_uint8(packet_type::pubcomp) << 4);
        writer.write_uint8(2);
        writer.write_uint16(packet_id);
        return buf;
    }

    /// Parses a PUBCOMP into @p pkt.  Returns false when the packet is
    /// malformed or truncated.
    static bool parse(const std::vector<uint8_t>& data, pubcomp_packet& pkt) {
        buffer_reader reader(data);
        if (!detail::read_fixed_header(reader, packet_type::pubcomp)) {
            return false;
        }
        return reader.read_uint16(pkt.packet_id);
    }
};

struct pubrel_packet {
    uint16_t packet_id = 0;

    std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> buf;
        buffer_writer writer(buf);
        writer.write_uint8((to_uint8(packet_type::pubrel) << 4) | 0x02);
        writer.write_uint8(2);
        writer.write_uint16(packet_id);
        return buf;
    }

    /// Parses a PUBREL into @p pkt.  Returns false when the packet is
    /// malformed or truncated.
    static bool parse(const std::vector<uint8_t>& data, pubrel_packet& pkt) {
        buffer_reader reader(data);
        if (!detail::read_fixed_header(reader, packet_type::pubrel)) {
            return false;
        }
        return reader.read_uint16(pkt.packet_id);
    }
};

struct pubrec_packet {
    uint16_t packet_id = 0;

    std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> buf;
        buffer_writer writer(buf);
        writer.write_uint8(to_uint8(packet_type::pubrec) << 4);
        writer.write_uint8(2);
        writer.write_uint16(packet_id);
        return buf;
    }

    /// Parses a PUBREC into @p pkt.  Returns false when the packet is
    /// malformed or truncated.
    static bool parse(const std::vector<uint8_t>& data, pubrec_packet& pkt) {
        buffer_reader reader(data);
        if (!detail::read_fixed_header(reader, packet_type::pubrec)) {
            return false;
        }
        return reader.read_uint16(pkt.packet_id);
    }
};

struct unsuback_packet {
    uint16_t packet_id = 0;

    std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> buf;
        buffer_writer writer(buf);

        writer.write_uint8(to_uint8(packet_type::unsuback) << 4);
        writer.write_uint8(2);
        writer.write_uint16(packet_id);

        return buf;
    }

    /// Parses an UNSUBACK into @p pkt.  Returns false when the packet is
    /// malformed or truncated.
    static bool parse(const std::vector<uint8_t>& data, unsuback_packet& pkt) {
        buffer_reader reader(data);
        if (!detail::read_fixed_header(reader, packet_type::unsuback)) {
            return false;
        }
        return reader.read_uint16(pkt.packet_id);
    }
};

struct pingreq_packet {
    std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> buf;
        buffer_writer writer(buf);
        writer.write_uint8(to_uint8(packet_type::pingreq) << 4);
        writer.write_uint8(0);
        return buf;
    }

    static pingreq_packet parse(const std::vector<uint8_t>& /*data*/) {
        return pingreq_packet{};
    }
};

struct pingresp_packet {
    std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> buf;
        buffer_writer writer(buf);
        writer.write_uint8(to_uint8(packet_type::pingresp) << 4);
        writer.write_uint8(0);
        return buf;
    }

    static pingresp_packet parse(const std::vector<uint8_t>& /*data*/) {
        return pingresp_packet{};
    }
};

struct disconnect_packet {
    std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> buf;
        buffer_writer writer(buf);
        writer.write_uint8(to_uint8(packet_type::disconnect) << 4);
        writer.write_uint8(0);
        return buf;
    }

    static disconnect_packet parse(const std::vector<uint8_t>& /*data*/) {
        return disconnect_packet{};
    }
};

}  // namespace litemqtt

#endif  // LITEMQTT_PACKETS_HPP

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

    static connect_packet parse(const std::vector<uint8_t>& data) {
        buffer_reader reader(data);

        uint8_t packet_type_byte = 0;
        reader.read_uint8(packet_type_byte);

        std::size_t remaining_len = 0;
        reader.read_remaining_length(remaining_len);

        connect_packet pkt;

        std::string protocol_name;
        reader.read_string(protocol_name);

        uint8_t protocol_level = 0;
        reader.read_uint8(protocol_level);

        uint8_t connect_flags = 0;
        reader.read_uint8(connect_flags);
        pkt.clean_session = (connect_flags & 0x02) != 0;
        bool has_username = (connect_flags & 0x80) != 0;
        bool has_password = (connect_flags & 0x40) != 0;

        reader.read_uint16(pkt.keep_alive_seconds);
        reader.read_string(pkt.client_id);

        if (has_username) {
            reader.read_string(pkt.username);
        }
        if (has_password) {
            reader.read_string(pkt.password);
        }

        return pkt;
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

    static connack_packet parse(const std::vector<uint8_t>& data) {
        buffer_reader reader(data);

        uint8_t packet_type_byte = 0;
        reader.read_uint8(packet_type_byte);

        std::size_t remaining_len = 0;
        reader.read_remaining_length(remaining_len);

        connack_packet pkt;
        reader.read_uint8(pkt.session_present);
        reader.read_uint8(pkt.return_code);

        return pkt;
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

    static publish_packet parse(const std::vector<uint8_t>& data) {
        buffer_reader reader(data);

        uint8_t packet_type_byte = 0;
        reader.read_uint8(packet_type_byte);

        std::size_t remaining_len = 0;
        reader.read_remaining_length(remaining_len);

        publish_packet pkt;
        pkt.dup = ((packet_type_byte >> 3) & 0x01) != 0;
        pkt.qos = (packet_type_byte >> 1) & 0x03;
        reader.read_string(pkt.topic_name);
        if (pkt.qos > 0) {
            reader.read_uint16(pkt.packet_id);
        }

        std::size_t payload_len = reader.remaining();
        if (payload_len > 0) {
            pkt.payload.resize(payload_len);
            reader.read_bytes(
                reinterpret_cast<uint8_t*>(&pkt.payload[0]),
                payload_len
            );
        }

        return pkt;
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

    static suback_packet parse(const std::vector<uint8_t>& data) {
        buffer_reader reader(data);

        uint8_t packet_type_byte = 0;
        reader.read_uint8(packet_type_byte);

        std::size_t remaining_len = 0;
        reader.read_remaining_length(remaining_len);

        suback_packet pkt;
        reader.read_uint16(pkt.packet_id);

        while (!reader.empty()) {
            uint8_t rc = 0;
            reader.read_uint8(rc);
            pkt.return_codes.push_back(rc);
        }

        return pkt;
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

    static puback_packet parse(const std::vector<uint8_t>& data) {
        buffer_reader reader(data);
        uint8_t packet_type_byte = 0;
        reader.read_uint8(packet_type_byte);
        std::size_t remaining_len = 0;
        reader.read_remaining_length(remaining_len);
        puback_packet pkt;
        reader.read_uint16(pkt.packet_id);
        return pkt;
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

    static unsuback_packet parse(const std::vector<uint8_t>& data) {
        buffer_reader reader(data);

        uint8_t packet_type_byte = 0;
        reader.read_uint8(packet_type_byte);

        std::size_t remaining_len = 0;
        reader.read_remaining_length(remaining_len);

        unsuback_packet pkt;
        reader.read_uint16(pkt.packet_id);

        return pkt;
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

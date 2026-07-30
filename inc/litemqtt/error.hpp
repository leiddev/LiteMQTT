#ifndef LITEMQTT_ERROR_HPP
#define LITEMQTT_ERROR_HPP

namespace litemqtt {

enum class error_code : int {
    success = 0,
    invalid_packet = 1,
    invalid_packet_type = 2,
    buffer_overflow = 3,
    invalid_remaining_length = 4,
    invalid_mqtt_protocol = 5,
    network_error = 6,
    connection_refused = 7,
    keepalive_timeout = 8,
    operation_in_progress = 9,
    not_connected = 10,
    unknown = 255,
};

constexpr int to_int(error_code ec) {
    return static_cast<int>(ec);
}

constexpr const char* to_string(error_code ec) {
    switch (ec) {
        case error_code::success: return "success";
        case error_code::invalid_packet: return "invalid packet";
        case error_code::invalid_packet_type: return "invalid packet type";
        case error_code::buffer_overflow: return "buffer overflow";
        case error_code::invalid_remaining_length: return "invalid remaining length";
        case error_code::invalid_mqtt_protocol: return "invalid MQTT protocol";
        case error_code::network_error: return "network error";
        case error_code::connection_refused: return "connection refused";
        case error_code::keepalive_timeout: return "keepalive timeout";
        case error_code::operation_in_progress: return "operation in progress";
        case error_code::not_connected: return "not connected";
        default: return "unknown error";
    }
}

}  // namespace litemqtt

#endif  // LITEMQTT_ERROR_HPP

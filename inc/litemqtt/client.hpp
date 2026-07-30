#ifndef LITEMQTT_CLIENT_HPP
#define LITEMQTT_CLIENT_HPP

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <asio.hpp>

#include "buffer_reader.hpp"
#include "connection.hpp"
#include "packets.hpp"
#include "packet_type.hpp"

namespace litemqtt {

using connect_cb = std::function<void(bool success, uint8_t return_code)>;
using message_cb = std::function<void(std::string topic, std::string payload)>;
using close_cb = std::function<void()>;

class mqtt_client : public std::enable_shared_from_this<mqtt_client> {
public:
    explicit mqtt_client(asio::io_context& io);
    ~mqtt_client();

    void set_client_id(const std::string& id);
    void set_keep_alive(uint16_t seconds);
    void set_clean_session(bool clean);

    void async_connect(const std::string& host, uint16_t port,
                        connect_cb callback = nullptr);
    void async_disconnect();

    void async_publish(const std::string& topic, const std::string& payload);
    void async_subscribe(const std::string& topic);

    void on_connect(connect_cb callback);
    void on_message(message_cb callback);
    void on_close(close_cb callback);

    connection_state state() const;

private:
    void send_connect_packet();
    void handle_packet(const std::vector<uint8_t>& data);
    void handle_connack(const std::vector<uint8_t>& data);
    void handle_publish(const std::vector<uint8_t>& data);
    void start_read_loop();
    void schedule_pingreq();
    void cancel_ping_timer();

    asio::io_context& io_;
    std::shared_ptr<connection> conn_;

    std::string client_id_ = "litemqtt_cpp";
    uint16_t keep_alive_seconds_ = 60;
    bool clean_session_ = true;

    uint16_t next_packet_id_ = 1;

    connect_cb on_connect_cb_;
    message_cb on_message_cb_;
    close_cb on_close_cb_;

    std::shared_ptr<asio::steady_timer> ping_timer_;
};

inline mqtt_client::mqtt_client(asio::io_context& io)
    : io_(io), conn_(std::make_shared<connection>(io)) {
    ping_timer_ = std::make_shared<asio::steady_timer>(io);
}

inline mqtt_client::~mqtt_client() {
    cancel_ping_timer();
    if (conn_->state() != connection_state::disconnected) {
        conn_->close();
    }
}

inline void mqtt_client::set_client_id(const std::string& id) { client_id_ = id; }
inline void mqtt_client::set_keep_alive(uint16_t seconds) { keep_alive_seconds_ = seconds; }
inline void mqtt_client::set_clean_session(bool clean) { clean_session_ = clean; }

inline void mqtt_client::async_connect(const std::string& host, uint16_t port, connect_cb callback) {
    if (callback) {
        on_connect_cb_ = std::move(callback);
    }

    auto self = shared_from_this();
    std::cerr << "[DEBUG] async_connect called, host=" << host << " port=" << port << std::endl;
    conn_->async_connect(host, port,
        [this, self](const asio::error_code& ec) {
            std::cerr << "[DEBUG] connection::async_connect callback, ec=" << ec.value() << std::endl;
            if (ec) {
                std::cerr << "Socket connect failed: " << ec.message() << std::endl;
                if (on_connect_cb_) on_connect_cb_(false, 255);
                return;
            }
            send_connect_packet();
        });
}

inline void mqtt_client::send_connect_packet() {
    connect_packet pkt;
    pkt.client_id = client_id_;
    pkt.keep_alive_seconds = keep_alive_seconds_;
    pkt.clean_session = clean_session_;

    std::vector<uint8_t> data = pkt.serialize();
    std::cerr << "[DEBUG] send_connect_packet: " << data.size() << " bytes" << std::endl;

    auto self = shared_from_this();
    conn_->async_write_packet(data, [this, self](const asio::error_code& ec) {
        std::cerr << "[DEBUG] CONNECT packet written, ec=" << ec.value() << std::endl;
        if (!ec) {
            schedule_pingreq();
            start_read_loop();
        }
    });
}

inline void mqtt_client::start_read_loop() {
    auto self = shared_from_this();
    conn_->async_read_packet(
        [this, self](const asio::error_code& ec, const std::vector<uint8_t>& data) {
            std::cerr << "[DEBUG] read callback: ec=" << ec.value() << " data.size=" << data.size() << std::endl;
            if (ec) {
                conn_->close();
                if (on_close_cb_) on_close_cb_();
                return;
            }
            handle_packet(data);
            start_read_loop();
        });
}

inline void mqtt_client::handle_packet(const std::vector<uint8_t>& data) {
    if (data.empty()) return;
    buffer_reader reader(data);
    uint8_t packet_type_byte = 0;
    reader.read_uint8(packet_type_byte);

    auto pt = to_packet_type(packet_type_byte >> 4);
    std::cerr << "[DEBUG] handle_packet: type=" << to_string(pt) << " (0x" << std::hex << (int)packet_type_byte << std::dec << "), size=" << data.size() << std::endl;

    switch (pt) {
        case packet_type::connack:
            handle_connack(data);
            break;
        case packet_type::publish:
            handle_publish(data);
            break;
        case packet_type::pingresp:
            cancel_ping_timer();
            break;
        default:
            std::cerr << "[DEBUG] unhandled packet type: " << to_string(pt) << std::endl;
            break;
    }
}

inline void mqtt_client::handle_connack(const std::vector<uint8_t>& data) {
    std::cerr << "[DEBUG] handle_connack called, on_connect_cb_=" << (bool)on_connect_cb_ << std::endl;
    connack_packet pkt = connack_packet::parse(data);
    if (on_connect_cb_) on_connect_cb_(pkt.return_code == 0, pkt.return_code);
}

inline void mqtt_client::handle_publish(const std::vector<uint8_t>& data) {
    publish_packet pkt = publish_packet::parse(data);
    if (on_message_cb_) on_message_cb_(pkt.topic_name, pkt.payload);
}

inline void mqtt_client::schedule_pingreq() {
    if (keep_alive_seconds_ == 0) return;

    auto self = shared_from_this();
    ping_timer_->expires_after(std::chrono::seconds(keep_alive_seconds_));
    ping_timer_->async_wait([this, self](const asio::error_code& ec) {
        if (ec) return;

        pingreq_packet pkt;
        std::vector<uint8_t> data = pkt.serialize();

        conn_->async_write_packet(data, [this, self](const asio::error_code& write_ec) {
            if (write_ec) {
                conn_->close();
                if (on_close_cb_) on_close_cb_();
                return;
            }

            ping_timer_->expires_after(std::chrono::seconds(keep_alive_seconds_ / 2));
            ping_timer_->async_wait([this, self](const asio::error_code& timer_ec) {
                if (timer_ec) return;
                conn_->close();
                if (on_close_cb_) on_close_cb_();
            });
        });
    });
}

inline void mqtt_client::cancel_ping_timer() {
    asio::error_code ec;
    ping_timer_->cancel(ec);
}

inline void mqtt_client::async_disconnect() {
    cancel_ping_timer();
    disconnect_packet pkt;
    auto self = shared_from_this();
    conn_->async_write_packet(pkt.serialize(), [this, self](const asio::error_code&) {
        conn_->close();
    });
}

inline void mqtt_client::async_publish(const std::string& topic, const std::string& payload) {
    publish_packet pkt;
    pkt.topic_name = topic;
    pkt.payload = payload;
    conn_->async_write_packet(pkt.serialize(), [](const asio::error_code&) {});
}

inline void mqtt_client::async_subscribe(const std::string& topic) {
    subscribe_packet pkt;
    pkt.packet_id = next_packet_id_++;
    pkt.topic_filters.push_back(std::make_pair(topic, 0));
    conn_->async_write_packet(pkt.serialize(), [](const asio::error_code&) {});
}

inline void mqtt_client::on_connect(connect_cb callback) { on_connect_cb_ = std::move(callback); }
inline void mqtt_client::on_message(message_cb callback) { on_message_cb_ = std::move(callback); }
inline void mqtt_client::on_close(close_cb callback) { on_close_cb_ = std::move(callback); }

inline connection_state mqtt_client::state() const { return conn_->state(); }

}  // namespace litemqtt

#endif  // LITEMQTT_CLIENT_HPP

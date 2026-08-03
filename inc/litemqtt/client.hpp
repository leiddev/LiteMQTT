#ifndef LITEMQTT_CLIENT_HPP
#define LITEMQTT_CLIENT_HPP

#include <cstdint>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <asio.hpp>

#include "buffer_reader.hpp"
#include "connection.hpp"
#include "packets.hpp"
#include "packet_type.hpp"

namespace litemqtt {

using connect_cb = std::function<void(bool success, uint8_t return_code)>;
using message_cb = std::function<void(std::string topic, std::string payload, uint8_t qos, uint16_t packet_id)>;
using close_cb = std::function<void()>;
using subscribe_cb = std::function<void(bool success, std::string topic, uint8_t qos_granted)>;
using publish_cb = std::function<void(bool success, std::string topic, uint8_t qos, uint16_t packet_id)>;

class mqtt_client : public std::enable_shared_from_this<mqtt_client> {
public:
    explicit mqtt_client(asio::io_context& io);
    ~mqtt_client();

    void set_client_id(const std::string& id);
    void set_keep_alive(uint16_t seconds);
    void set_clean_session(bool clean);
    void set_username(const std::string& username);
    void set_password(const std::string& password);

    void async_connect(const std::string& host, uint16_t port,
                        connect_cb callback = nullptr);
    void async_disconnect();

    void async_publish(const std::string& topic, const std::string& payload,
                       uint8_t qos = 0, publish_cb callback = nullptr);
    void async_subscribe(const std::string& topic, uint8_t qos = 0, subscribe_cb callback = nullptr);

    void on_connect(connect_cb callback);
    void on_message(message_cb callback);
    void on_close(close_cb callback);
    void on_subscribe(subscribe_cb callback);
    void on_publish(publish_cb callback);

    connection_state state() const;

    void handle_packet(const std::vector<uint8_t>& data);
    void handle_puback(const std::vector<uint8_t>& data);
    void handle_pubrec(const std::vector<uint8_t>& data);
    void handle_pubcomp(const std::vector<uint8_t>& data);
    void handle_pubrel(const std::vector<uint8_t>& data);
    void handle_suback(const std::vector<uint8_t>& data);
    void handle_publish(const std::vector<uint8_t>& data);

private:
    void send_connect_packet();
    void handle_connack(const std::vector<uint8_t>& data);
    void start_read_loop();
    void schedule_pingreq();
    void cancel_ping_timer();

    uint16_t acquire_packet_id();
    void send_publish_entry(uint16_t packet_id);
    void async_publish_impl(const std::string& topic, const std::string& payload,
                            uint8_t qos, publish_cb callback);
    void async_subscribe_impl(const std::string& topic, uint8_t qos, subscribe_cb callback);
    void send_pubrel(uint16_t packet_id);
    void arm_publish_retry_timer(uint16_t packet_id);
    void arm_pubrec_retry_timer(uint16_t packet_id);
    void cancel_publish_retry_timer(uint16_t packet_id);
    void finish_publish_success(uint16_t packet_id);
    void finish_publish_failure(uint16_t packet_id);
    void schedule_dedup_cleanup();
    void notify_closed_once();

public:
    // Test hooks (do not use in production code).
    uint16_t acquire_packet_id_for_test() { return acquire_packet_id(); }

    void debug_insert_pending_publish(uint16_t pid) {
        pending_publish entry;
        entry.topic = "test/topic";
        entry.qos = 1;
        entry.payload = "payload";
        entry.timer = std::make_shared<asio::steady_timer>(io_);
        entry.attempts = 1;
        pending_publishes_[pid] = std::move(entry);
    }

    void debug_insert_pending_publish_with_cb(uint16_t pid, publish_cb cb) {
        pending_publish entry;
        entry.topic = "test/topic";
        entry.qos = 1;
        entry.payload = "payload";
        entry.callback = std::move(cb);
        entry.timer = std::make_shared<asio::steady_timer>(io_);
        entry.attempts = 1;
        pending_publishes_[pid] = std::move(entry);
    }

    void debug_clear_pending_publishes() { pending_publishes_.clear(); }

    std::size_t debug_pending_publish_count() const { return pending_publishes_.size(); }

    void debug_insert_pending_subscribe(uint16_t pid,
                                        std::vector<std::pair<std::string, uint8_t>> filters) {
        subscribe_cb cb;
        pending_subscribes_[pid] = {std::move(filters), std::move(cb)};
    }

    void debug_insert_pending_subscribe_with_cb(uint16_t pid,
                                                std::vector<std::pair<std::string, uint8_t>> filters,
                                                subscribe_cb cb) {
        pending_subscribes_[pid] = {std::move(filters), std::move(cb)};
    }

    std::size_t debug_pending_subscribe_count() const { return pending_subscribes_.size(); }

private:
    asio::io_context& io_;
    std::shared_ptr<connection> conn_;

    std::string client_id_ = "litemqtt_cpp";
    uint16_t keep_alive_seconds_ = 60;
    bool clean_session_ = true;
    std::string username_;
    std::string password_;

    uint16_t next_packet_id_ = 1;

    static constexpr std::chrono::seconds kPublishRetryInterval{5};

    connect_cb on_connect_cb_;
    message_cb on_message_cb_;
    close_cb on_close_cb_;
    subscribe_cb on_subscribe_cb_;
    publish_cb on_publish_cb_;

    struct pending_publish {
        std::string topic;
        uint8_t qos = 0;
        std::string payload;
        publish_cb callback;
        std::shared_ptr<asio::steady_timer> timer;
        int attempts = 0;
    };

    struct pending_pubrec {
        std::shared_ptr<asio::steady_timer> timer;
        int attempts = 0;
    };

    std::map<uint16_t, std::pair<std::vector<std::pair<std::string, uint8_t>>, subscribe_cb>> pending_subscribes_;
    std::map<uint16_t, pending_publish> pending_publishes_;
    std::map<uint16_t, pending_pubrec> pending_pubrecs_;

    std::unordered_set<uint16_t> recently_seen_publish_ids_;
    std::shared_ptr<asio::steady_timer> dedup_cleanup_timer_;

    std::shared_ptr<asio::steady_timer> ping_timer_;
    bool closed_ = false;
};

inline mqtt_client::mqtt_client(asio::io_context& io)
    : io_(io), conn_(std::make_shared<connection>(io)) {
    ping_timer_ = std::make_shared<asio::steady_timer>(io);
    dedup_cleanup_timer_ = std::make_shared<asio::steady_timer>(io);
}

inline mqtt_client::~mqtt_client() {
    cancel_ping_timer();

    // Cancel all pending pubrec timers
    for (auto it = pending_pubrecs_.begin(); it != pending_pubrecs_.end(); ++it) {
        asio::error_code ec;
        it->second.timer->cancel(ec);
    }
    pending_pubrecs_.clear();

    if (conn_->state() != connection_state::disconnected) {
        conn_->close();
    }
}

inline void mqtt_client::set_client_id(const std::string& id) { client_id_ = id; }
inline void mqtt_client::set_keep_alive(uint16_t seconds) { keep_alive_seconds_ = seconds; }
inline void mqtt_client::set_clean_session(bool clean) { clean_session_ = clean; }
inline void mqtt_client::set_username(const std::string& username) { username_ = username; }
inline void mqtt_client::set_password(const std::string& password) { password_ = password; }

inline void mqtt_client::async_connect(const std::string& host, uint16_t port, connect_cb callback) {
    if (callback) {
        on_connect_cb_ = std::move(callback);
    }

    auto self = shared_from_this();
    conn_->async_connect(host, port,
        [this, self](const asio::error_code& ec) {
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
    pkt.username = username_;
    pkt.password = password_;

    auto self = shared_from_this();
    conn_->async_write_packet(pkt.serialize(), [this, self](const asio::error_code& ec) {
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
            if (ec) {
                notify_closed_once();
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
    switch (pt) {
        case packet_type::connack:
            handle_connack(data);
            break;
        case packet_type::publish:
            handle_publish(data);
            break;
        case packet_type::suback:
            handle_suback(data);
            break;
        case packet_type::puback:
            handle_puback(data);
            break;
        case packet_type::pubrec:
            handle_pubrec(data);
            break;
        case packet_type::pubrel:
            handle_pubrel(data);
            break;
        case packet_type::pubcomp:
            handle_pubcomp(data);
            break;
        case packet_type::pingresp:
            cancel_ping_timer();
            schedule_pingreq();
            break;
        default:
            break;
    }
}

inline void mqtt_client::handle_connack(const std::vector<uint8_t>& data) {
    connack_packet pkt = connack_packet::parse(data);
    if (pkt.return_code == 0) {
        conn_->mark_connected();
    }
    if (on_connect_cb_) on_connect_cb_(pkt.return_code == 0, pkt.return_code);
}

inline void mqtt_client::handle_publish(const std::vector<uint8_t>& data) {
    publish_packet pkt = publish_packet::parse(data);

    if (pkt.qos > 2) {
        std::cerr << "Protocol error: PUBLISH with invalid QoS " << static_cast<int>(pkt.qos) << std::endl;
        notify_closed_once();
        return;
    }

    bool is_duplicate = false;
    if (pkt.qos > 0) {
        auto inserted = recently_seen_publish_ids_.insert(pkt.packet_id);
        is_duplicate = !inserted.second;
        if (recently_seen_publish_ids_.size() == 1) {
            schedule_dedup_cleanup();
        }
    }

    if (!is_duplicate) {
        uint16_t message_id = (pkt.qos == 0) ? 0 : pkt.packet_id;
        if (on_message_cb_) on_message_cb_(pkt.topic_name, pkt.payload, pkt.qos, message_id);
    }

    if (pkt.qos == 1) {
        puback_packet puback;
        puback.packet_id = pkt.packet_id;
        auto self = shared_from_this();
        conn_->async_write_packet(puback.serialize(), [self](const asio::error_code&) {});
    } else if (pkt.qos == 2) {
        // QoS 2: respond with PUBREC
        pubrec_packet pubrec;
        pubrec.packet_id = pkt.packet_id;
        auto self = shared_from_this();
        conn_->async_write_packet(pubrec.serialize(), [self](const asio::error_code&) {});
    }
}

inline void mqtt_client::handle_suback(const std::vector<uint8_t>& data) {
    suback_packet pkt = suback_packet::parse(data);
    auto it = pending_subscribes_.find(pkt.packet_id);
    if (it == pending_subscribes_.end()) {
        return;
    }

    const auto& filters_ref = it->second.first;
    subscribe_cb cb = it->second.second;
    std::vector<std::pair<std::string, uint8_t>> filters{filters_ref};
    pending_subscribes_.erase(it);

    if (!cb) {
        return;
    }

    if (pkt.return_codes.size() < filters.size()) {
        notify_closed_once();
        return;
    }

    for (std::size_t i = 0; i < filters.size(); ++i) {
        uint8_t rc = pkt.return_codes[i];
        bool success = rc != 0x80;
        uint8_t qos_granted = success ? rc : 0;
        cb(success, filters[i].first, qos_granted);
    }
}

inline void mqtt_client::handle_puback(const std::vector<uint8_t>& data) {
    puback_packet pkt = puback_packet::parse(data);
    auto it = pending_publishes_.find(pkt.packet_id);
    if (it == pending_publishes_.end()) {
        return;
    }

    std::string topic = it->second.topic;
    uint8_t qos = it->second.qos;
    publish_cb cb = it->second.callback;
    cancel_publish_retry_timer(pkt.packet_id);
    pending_publishes_.erase(it);
    if (cb) {
        cb(true, topic, qos, pkt.packet_id);
    }
}

inline void mqtt_client::handle_pubrec(const std::vector<uint8_t>& data) {
    pubrec_packet pkt = pubrec_packet::parse(data);

    // Check if this is for an outgoing publish (we sent PUBLISH Q2, now got PUBREC)
    auto pub_it = pending_publishes_.find(pkt.packet_id);
    if (pub_it != pending_publishes_.end()) {
        // This is a response to our outgoing QoS 2 publish
        // Cancel the retry timer and send PUBREL
        cancel_publish_retry_timer(pkt.packet_id);

        pending_pubrec entry;
        entry.timer = std::make_shared<asio::steady_timer>(io_);
        entry.attempts = 0;
        pending_pubrecs_[pkt.packet_id] = entry;

        send_pubrel(pkt.packet_id);
        return;
    }

    // Check if this is a duplicate PUBREC for a pending_pubrel
    auto rec_it = pending_pubrecs_.find(pkt.packet_id);
    if (rec_it != pending_pubrecs_.end()) {
        // Duplicate PUBREC, ignore
        return;
    }
}

inline void mqtt_client::handle_pubrel(const std::vector<uint8_t>& data) {
    pubrel_packet pkt = pubrel_packet::parse(data);

    // Send PUBCOMP to complete the QoS 2 incoming flow
    pubcomp_packet pubcomp;
    pubcomp.packet_id = pkt.packet_id;
    auto self = shared_from_this();
    conn_->async_write_packet(pubcomp.serialize(), [self](const asio::error_code&) {});
}

inline void mqtt_client::handle_pubcomp(const std::vector<uint8_t>& data) {
    pubcomp_packet pkt = pubcomp_packet::parse(data);

    auto rec_it = pending_pubrecs_.find(pkt.packet_id);
    if (rec_it != pending_pubrecs_.end()) {
        // Cancel the timer and remove from pending
        asio::error_code ec;
        rec_it->second.timer->cancel(ec);
        pending_pubrecs_.erase(rec_it);
    }

    auto pub_it = pending_publishes_.find(pkt.packet_id);
    if (pub_it != pending_publishes_.end()) {
        std::string topic = pub_it->second.topic;
        uint8_t qos = pub_it->second.qos;
        publish_cb cb = pub_it->second.callback;
        cancel_publish_retry_timer(pkt.packet_id);
        pending_publishes_.erase(pub_it);
        if (cb) {
            cb(true, topic, qos, pkt.packet_id);
        }
    }
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
                notify_closed_once();
                return;
            }

            ping_timer_->expires_after(std::chrono::seconds(keep_alive_seconds_ / 2));
            ping_timer_->async_wait([this, self](const asio::error_code& timer_ec) {
                if (timer_ec) return;
                notify_closed_once();
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

inline void mqtt_client::async_publish(const std::string& topic, const std::string& payload,
                                        uint8_t qos, publish_cb callback) {
    auto self = shared_from_this();
    asio::post(io_, [self, topic, payload, qos, callback = std::move(callback)]() mutable {
        self->async_publish_impl(topic, payload, qos, std::move(callback));
    });
}

inline void mqtt_client::async_publish_impl(const std::string& topic, const std::string& payload,
                                             uint8_t qos, publish_cb callback) {
    if (closed_) {
        if (callback) callback(false, topic, qos, 0);
        return;
    }

    if (qos > 2) {
        std::cerr << "async_publish: invalid QoS " << static_cast<int>(qos) << std::endl;
        if (callback) callback(false, topic, qos, 0);
        return;
    }

    if (qos == 0) {
        publish_packet pkt;
        pkt.topic_name = topic;
        pkt.payload = payload;
        pkt.qos = 0;
        pkt.dup = false;

        auto self = shared_from_this();
        conn_->async_write_packet(pkt.serialize(),
            [self, callback, topic](const asio::error_code& ec) {
                if (callback) callback(ec ? false : true, topic, 0, 0);
            });
        return;
    }

    uint16_t packet_id = acquire_packet_id();
    if (packet_id == 0) {
        // All packet IDs exhausted
        if (callback) callback(false, topic, qos, 0);
        return;
    }

    // Backpressure: reject new publishes if pending queue is full
    constexpr std::size_t kMaxPendingPublishes = 1000;
    if (pending_publishes_.size() >= kMaxPendingPublishes) {
        std::cerr << "async_publish: backpressure limit reached (pending="
                  << pending_publishes_.size() << ")" << std::endl;
        if (callback) callback(false, topic, qos, 0);
        return;
    }

    pending_publish entry;
    entry.topic = topic;
    entry.qos = qos;
    entry.payload = payload;
    entry.callback = callback;
    entry.timer = std::make_shared<asio::steady_timer>(io_);
    entry.attempts = 1;
    pending_publishes_[packet_id] = std::move(entry);

    send_publish_entry(packet_id);
}

inline void mqtt_client::async_subscribe(const std::string& topic, uint8_t qos, subscribe_cb callback) {
    auto self = shared_from_this();
    asio::post(io_, [self, topic, qos, callback = std::move(callback)]() mutable {
        self->async_subscribe_impl(topic, qos, std::move(callback));
    });
}

inline void mqtt_client::async_subscribe_impl(const std::string& topic, uint8_t qos, subscribe_cb callback) {
    if (closed_) {
        if (callback) callback(false, topic, 0);
        return;
    }

    if (qos > 2) {
        std::cerr << "async_subscribe: invalid QoS " << static_cast<int>(qos) << std::endl;
        if (callback) callback(false, topic, 0);
        return;
    }

    uint16_t packet_id = acquire_packet_id();
    if (packet_id == 0) {
        // All packet IDs exhausted
        if (callback) callback(false, topic, 0);
        return;
    }

    subscribe_packet pkt;
    pkt.packet_id = packet_id;
    pkt.topic_filters.push_back(std::make_pair(topic, qos));

    pending_subscribes_[packet_id] = {pkt.topic_filters, callback};

    auto self = shared_from_this();
    conn_->async_write_packet(pkt.serialize(),
        [this, self, packet_id](const asio::error_code& ec) {
            if (!ec) return;
            auto it = pending_subscribes_.find(packet_id);
            if (it == pending_subscribes_.end()) return;
            const auto& filters = it->second.first;
            subscribe_cb cb = it->second.second;
            pending_subscribes_.erase(it);
            if (!cb) return;
            for (const auto& f : filters) {
                cb(false, f.first, 0);
            }
        });
}

inline void mqtt_client::on_connect(connect_cb callback) { on_connect_cb_ = std::move(callback); }
inline void mqtt_client::on_message(message_cb callback) { on_message_cb_ = std::move(callback); }
inline void mqtt_client::on_close(close_cb callback) { on_close_cb_ = std::move(callback); }
inline void mqtt_client::on_subscribe(subscribe_cb callback) { on_subscribe_cb_ = std::move(callback); }
inline void mqtt_client::on_publish(publish_cb callback) { on_publish_cb_ = std::move(callback); }

inline connection_state mqtt_client::state() const { return conn_->state(); }

// Must be called on the io_context thread that owns the pending_* maps.
// Returns 0 if all packet IDs are exhausted (all 65535 IDs are in use).
// async_publish / async_subscribe enforce this via asio::post.
inline uint16_t mqtt_client::acquire_packet_id() {
    uint16_t start = next_packet_id_;
    for (;;) {
        ++next_packet_id_;
        if (next_packet_id_ == 0) {
            ++next_packet_id_;  // Skip reserved value 0
        }
        if (next_packet_id_ == start) {
            // All packet IDs exhausted, trigger connection close
            std::cerr << "acquire_packet_id: all packet IDs exhausted" << std::endl;
            notify_closed_once();
            return 0;
        }
        if (!pending_publishes_.count(next_packet_id_) &&
            !pending_subscribes_.count(next_packet_id_) &&
            !pending_pubrecs_.count(next_packet_id_)) {
            return next_packet_id_;
        }
    }
}

inline void mqtt_client::send_publish_entry(uint16_t packet_id) {
    auto it = pending_publishes_.find(packet_id);
    if (it == pending_publishes_.end()) {
        return;
    }

    pending_publish& entry = it->second;

    publish_packet pkt;
    pkt.topic_name = entry.topic;
    pkt.payload = entry.payload;
    pkt.qos = entry.qos;
    pkt.packet_id = packet_id;
    pkt.dup = (entry.attempts > 1);

    arm_publish_retry_timer(packet_id);

    auto self = shared_from_this();
    conn_->async_write_packet(pkt.serialize(),
        [this, self, packet_id](const asio::error_code& ec) {
            if (ec) {
                // Underlying socket is broken; abort this publish attempt.
                finish_publish_failure(packet_id);
                return;
            }
            auto cur = pending_publishes_.find(packet_id);
            if (cur == pending_publishes_.end()) return;

            // Reset the retry window - bytes are on the wire, wait for PUBACK/PUBREC
            // (or for the timer to fire and trigger a DUP retransmit).
            arm_publish_retry_timer(packet_id);
        });
}

inline void mqtt_client::send_pubrel(uint16_t packet_id) {
    pubrel_packet pkt;
    pkt.packet_id = packet_id;

    arm_pubrec_retry_timer(packet_id);

    auto self = shared_from_this();
    conn_->async_write_packet(pkt.serialize(),
        [this, self, packet_id](const asio::error_code& ec) {
            if (ec) {
                // Underlying socket is broken; abort this pubrel attempt.
                auto rec_it = pending_pubrecs_.find(packet_id);
                if (rec_it != pending_pubrecs_.end()) {
                    asio::error_code cancel_ec;
                    rec_it->second.timer->cancel(cancel_ec);
                    pending_pubrecs_.erase(rec_it);
                }
                return;
            }
            // Reset the retry timer
            arm_pubrec_retry_timer(packet_id);
        });
}

inline void mqtt_client::arm_pubrec_retry_timer(uint16_t packet_id) {
    auto rec_it = pending_pubrecs_.find(packet_id);
    if (rec_it == pending_pubrecs_.end()) return;

    auto timer = rec_it->second.timer;
    timer->expires_after(std::chrono::seconds(5));

    auto self = shared_from_this();
    timer->async_wait([this, self, packet_id](const asio::error_code& ec) {
        if (ec) return;

        auto rec_it = pending_pubrecs_.find(packet_id);
        if (rec_it == pending_pubrecs_.end()) return;

        pending_pubrec& e = rec_it->second;
        if (e.attempts >= 3) {
            // Give up, close connection
            asio::error_code cancel_ec;
            e.timer->cancel(cancel_ec);
            notify_closed_once();
            return;
        }
        e.attempts += 1;
        send_pubrel(packet_id);
    });
}

inline void mqtt_client::arm_publish_retry_timer(uint16_t packet_id) {
    auto it = pending_publishes_.find(packet_id);
    if (it == pending_publishes_.end()) return;

    auto timer = it->second.timer;
    timer->expires_after(std::chrono::seconds(5));

    auto self = shared_from_this();
    timer->async_wait([this, self, packet_id](const asio::error_code& ec) {
        if (ec) {
            return;
        }
        auto cur = pending_publishes_.find(packet_id);
        if (cur == pending_publishes_.end()) {
            return;
        }
        pending_publish& e = cur->second;
        if (e.attempts >= 3) {
            finish_publish_failure(packet_id);
            return;
        }
        e.attempts += 1;
        send_publish_entry(packet_id);
    });
}
inline void mqtt_client::cancel_publish_retry_timer(uint16_t packet_id) {
    auto it = pending_publishes_.find(packet_id);
    if (it == pending_publishes_.end()) return;
    asio::error_code ec;
    it->second.timer->cancel(ec);
}

inline void mqtt_client::finish_publish_success(uint16_t packet_id) {
    auto it = pending_publishes_.find(packet_id);
    if (it == pending_publishes_.end()) return;

    std::string topic = it->second.topic;
    uint8_t qos = it->second.qos;
    publish_cb cb = it->second.callback;

    cancel_publish_retry_timer(packet_id);
    pending_publishes_.erase(it);

    if (cb) {
        cb(true, topic, qos, packet_id);
    }
}

inline void mqtt_client::finish_publish_failure(uint16_t packet_id) {
    auto it = pending_publishes_.find(packet_id);
    if (it != pending_publishes_.end()) {
        std::string topic = it->second.topic;
        uint8_t qos = it->second.qos;
        publish_cb cb = it->second.callback;

        cancel_publish_retry_timer(packet_id);
        pending_publishes_.erase(it);

        if (cb) {
            cb(false, topic, qos, packet_id);
        }
    }

    notify_closed_once();
}

inline void mqtt_client::schedule_dedup_cleanup() {
    if (!dedup_cleanup_timer_) return;
    dedup_cleanup_timer_->expires_after(std::chrono::seconds(120));
    auto self = shared_from_this();
    dedup_cleanup_timer_->async_wait([this, self](const asio::error_code& ec) {
        if (ec) return;
        recently_seen_publish_ids_.clear();
    });
}

inline void mqtt_client::notify_closed_once() {
    if (closed_) return;
    closed_ = true;

    cancel_ping_timer();
    if (dedup_cleanup_timer_) {
        asio::error_code ec;
        dedup_cleanup_timer_->cancel(ec);
    }

    for (auto& kv : pending_publishes_) {
        if (kv.second.callback) {
            kv.second.callback(false, kv.second.topic, kv.second.qos, kv.first);
        }
        if (kv.second.timer) {
            asio::error_code ec;
            kv.second.timer->cancel(ec);
        }
    }
    pending_publishes_.clear();

    for (auto& kv : pending_pubrecs_) {
        if (kv.second.timer) {
            asio::error_code ec;
            kv.second.timer->cancel(ec);
        }
    }
    pending_pubrecs_.clear();

    for (auto& kv : pending_subscribes_) {
        subscribe_cb cb = kv.second.second;
        const auto& filters = kv.second.first;
        if (cb) {
            for (const auto& f : filters) {
                cb(false, f.first, 0);
            }
        }
    }
    pending_subscribes_.clear();

    conn_->close();
    if (on_close_cb_) on_close_cb_();
}

}  // namespace litemqtt

#endif  // LITEMQTT_CLIENT_HPP

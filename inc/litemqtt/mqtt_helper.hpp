#ifndef LITEMQTT_HELPER_HPP
#define LITEMQTT_HELPER_HPP

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "client.hpp"

namespace litemqtt {

using message_callback = std::function<void(
    std::string topic,      // Topic
    std::string payload,    // Message payload
    uint8_t qos,            // QoS level
    uint16_t packet_id      // Packet ID
)>;

class mqtt_helper : public std::enable_shared_from_this<mqtt_helper> {
public:
    mqtt_helper() = delete;
    explicit mqtt_helper(const std::string& host, uint16_t port,
                          const std::string& client_id);
    ~mqtt_helper();

    mqtt_helper(const mqtt_helper&) = delete;
    mqtt_helper& operator=(const mqtt_helper&) = delete;
    mqtt_helper(mqtt_helper&&) = delete;
    mqtt_helper& operator=(mqtt_helper&&) = delete;

    // Configuration methods
    void set_credentials(const std::string& username, const std::string& password);
    void set_keep_alive(uint16_t seconds);
    void set_clean_session(bool clean);
    void set_reconnect_config(int max_attempts, int base_delay_ms);

    // Core operations
    void on_message(message_callback callback);
    void subscribe(const std::string& topic, uint8_t qos = 1);
    void subscribe(const std::string& topic, uint8_t qos,
                   std::function<void(bool success, std::string topic, uint8_t qos)> callback);
    void publish(const std::string& topic, const std::string& payload,
                 uint8_t qos = 0);
    void publish(const std::string& topic, const std::string& payload,
                 uint8_t qos, std::function<void(bool success, std::string topic,
                                                uint8_t qos, uint16_t packet_id)> callback);

    // Lifecycle
    void start();
    void stop();
    bool is_connected() const;
    bool is_running() const;

private:
    void init_client();
    void start_io_thread();
    void stop_io_thread();
    void do_connect();
    void do_reconnect();
    void setup_callbacks();

    std::string host_;
    uint16_t port_;
    std::string client_id_;
    std::string username_;
    std::string password_;
    uint16_t keep_alive_seconds_ = 60;
    bool clean_session_ = true;

    int max_reconnect_attempts_ = 10;
    int base_reconnect_delay_ms_ = 1000;
    std::atomic<int> reconnect_attempts_{0};
    std::atomic<bool> initial_connect_{true};

    std::atomic<bool> running_{false};
    std::atomic<bool> should_reconnect_{false};
    std::atomic<bool> user_stopped_{false};

    asio::io_context io_;
    std::unique_ptr<asio::io_context::work> work_;
    std::shared_ptr<mqtt_client> client_;
    std::thread io_thread_;

    std::vector<std::string> pending_topics_;
    std::vector<std::pair<std::string, uint8_t>> subscribes_;
    std::mutex pending_mutex_;

    message_callback message_callback_;
    std::mutex callback_mutex_;
};

inline mqtt_helper::mqtt_helper(const std::string& host, uint16_t port,
                                const std::string& client_id)
    : host_(host),
      port_(port),
      client_id_(client_id),
      should_reconnect_(true) {
}

inline mqtt_helper::~mqtt_helper() {
    stop();
}

inline void mqtt_helper::set_credentials(const std::string& username,
                                        const std::string& password) {
    username_ = username;
    password_ = password;
}

inline void mqtt_helper::set_keep_alive(uint16_t seconds) {
    keep_alive_seconds_ = seconds;
}

inline void mqtt_helper::set_clean_session(bool clean) {
    clean_session_ = clean;
}

inline void mqtt_helper::set_reconnect_config(int max_attempts,
                                              int base_delay_ms) {
    max_reconnect_attempts_ = max_attempts;
    base_reconnect_delay_ms_ = base_delay_ms;
}

inline void mqtt_helper::on_message(message_callback callback) {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    message_callback_ = std::move(callback);
}

inline void mqtt_helper::subscribe(const std::string& topic, uint8_t qos) {
    subscribe(topic, qos, nullptr);
}

inline void mqtt_helper::subscribe(
    const std::string& topic, uint8_t qos,
    std::function<void(bool success, std::string topic, uint8_t qos)> callback) {
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        subscribes_.emplace_back(topic, qos);
    }

    if (!client_ || !is_connected()) {
        return;
    }

    client_->async_subscribe(topic, qos, std::move(callback));
}

inline void mqtt_helper::publish(const std::string& topic,
                                  const std::string& payload, uint8_t qos) {
    publish(topic, payload, qos, nullptr);
}

inline void mqtt_helper::publish(
    const std::string& topic, const std::string& payload, uint8_t qos,
    std::function<void(bool success, std::string topic, uint8_t qos,
                       uint16_t packet_id)> callback) {
    if (!client_) return;

    client_->async_publish(topic, payload, qos, std::move(callback));
}

inline void mqtt_helper::start() {
    if (running_.load()) {
        return;
    }

    user_stopped_.store(false);
    should_reconnect_.store(true);
    work_ = std::make_unique<asio::io_context::work>(io_);

    init_client();
    setup_callbacks();
    do_connect();
    start_io_thread();
}

inline void mqtt_helper::stop() {
    if (!running_.load()) {
        return;
    }

    user_stopped_.store(true);
    should_reconnect_.store(false);

    if (client_) {
        client_->async_disconnect();
    }

    work_.reset();
    stop_io_thread();
}

inline bool mqtt_helper::is_connected() const {
    return client_ && client_->state() == connection_state::connected;
}

inline bool mqtt_helper::is_running() const {
    return running_.load();
}

inline void mqtt_helper::init_client() {
    client_ = std::make_shared<mqtt_client>(io_);
    client_->set_client_id(client_id_);
    client_->set_keep_alive(keep_alive_seconds_);
    client_->set_clean_session(clean_session_);
    if (!username_.empty()) {
        client_->set_username(username_);
    }
    if (!password_.empty()) {
        client_->set_password(password_);
    }
}

inline void mqtt_helper::start_io_thread() {
    running_.store(true);
    io_thread_ = std::thread([this]() {
        try {
            io_.run();
        } catch (const std::exception& e) {
            std::cerr << "[mqtt_helper] io_context exception: " << e.what() << std::endl;
        } catch (...) {
            std::cerr << "[mqtt_helper] io_context unknown exception" << std::endl;
        }
        std::cout << "[mqtt_helper] io_context thread exiting" << std::endl;
    });
}

inline void mqtt_helper::stop_io_thread() {
    if (io_thread_.joinable()) {
        io_thread_.join();
    }
    running_.store(false);
}

inline void mqtt_helper::setup_callbacks() {
    auto self = shared_from_this();

    client_->on_message([self](std::string topic, std::string payload,
                                uint8_t qos, uint16_t packet_id) {
        std::lock_guard<std::mutex> lock(self->callback_mutex_);
        if (self->message_callback_) {
            self->message_callback_(std::move(topic), std::move(payload),
                                      qos, packet_id);
        }
    });

    client_->on_close([self]() {
        std::cout << "[mqtt_helper] Connection closed" << std::endl;
        if (self->should_reconnect_.load() && !self->user_stopped_.load()) {
            self->do_reconnect();
        }
    });

    client_->on_connect([self](bool success, uint8_t rc) {
        std::cout << "[mqtt_helper] Connection callback fired: success=" << success << ", rc=" << static_cast<int>(rc) << std::endl;
        if (!success) {
            std::cerr << "[mqtt_helper] Connection failed, return code: "
                    << static_cast<int>(rc) << std::endl;
            if (self->initial_connect_.load()) {
                self->should_reconnect_.store(false);
                return;
            }
            self->do_reconnect();
            return;
        }

        std::cout << "[mqtt_helper] Connected to broker" << std::endl;
        self->reconnect_attempts_.store(0);
        self->initial_connect_.store(false);

        // Restore subscriptions
        {
            std::lock_guard<std::mutex> lock(self->pending_mutex_);
            for (auto& item : self->subscribes_) {
                self->client_->async_subscribe(item.first, item.second, nullptr);
            }
        }
    });
}

inline void mqtt_helper::do_connect() {
    if (!client_) {
        std::cerr << "[mqtt_helper] client_ is null!" << std::endl;
        return;
    }

    std::cout << "[mqtt_helper] Initiating connection to " << host_ << ":" << port_ << "..." << std::endl;

    client_->async_connect(host_, port_);
}

inline void mqtt_helper::do_reconnect() {
    if (!should_reconnect_.load() || user_stopped_.load()) {
        return;
    }

    int attempts = reconnect_attempts_.load();
    if (attempts >= max_reconnect_attempts_) {
        std::cerr << "[mqtt_helper] Max reconnect attempts reached ("
                  << max_reconnect_attempts_ << "), giving up" << std::endl;
        should_reconnect_.store(false);
        return;
    }

    int delay_ms = std::min(base_reconnect_delay_ms_ * (1 << attempts), 30000);
    reconnect_attempts_.fetch_add(1);

    std::cout << "[mqtt_helper] Reconnecting in " << delay_ms << "ms (attempt "
              << (attempts + 1) << "/" << max_reconnect_attempts_ << ")..." << std::endl;

    std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    if (should_reconnect_.load() && !user_stopped_.load()) {
        do_connect();
    }
}

}  // namespace litemqtt

#endif  // LITEMQTT_HELPER_HPP

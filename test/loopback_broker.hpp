#ifndef LITEMQTT_TEST_LOOPBACK_BROKER_HPP
#define LITEMQTT_TEST_LOOPBACK_BROKER_HPP

// A tiny scripted MQTT peer for the unit tests.  It owns one blocking thread
// that accepts a single connection, answers CONNECT, and then runs whatever the
// test told it to do.
//
// Everything the peer receives is recorded so a test can assert on the exact
// bytes the client put on the wire (DUP flags, packet ids, PUBREL, ...), and
// every byte it sends is taken from an outgoing queue the test fills through
// reply(), so a test can simply not answer a packet to exercise the client's
// retry timers.
//
// Teardown used to be the weak point of the hand written brokers in these
// tests: closing the acceptor does not interrupt a thread blocked in accept()
// or read_some(), so a test whose client never connected hung in join().
// stop() now shuts both sockets down first, which makes every blocked call
// return.

#include <chrono>
#include <cstdint>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

namespace litemqtt_test {

using namespace std::chrono;

inline bool wait_for(const std::function<bool()>& pred, int timeout_ms) {
    const auto deadline = steady_clock::now() + milliseconds(timeout_ms);
    while (steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(milliseconds(5));
    }
    return pred();
}

/// Splits one MQTT frame off the front of @p buffer.
/// Returns false when the buffer does not hold a whole frame yet.
inline bool take_frame(std::vector<uint8_t>& buffer, std::vector<uint8_t>& frame) {
    if (buffer.size() < 2) {
        return false;
    }
    std::size_t remaining = 0;
    std::size_t multiplier = 1;
    std::size_t index = 1;
    uint8_t digit = 0;
    do {
        if (index >= buffer.size()) {
            return false;
        }
        digit = buffer[index++];
        remaining += (digit & 0x7F) * multiplier;
        multiplier *= 128;
    } while ((digit & 0x80) != 0);

    if (buffer.size() < index + remaining) {
        return false;
    }
    frame.assign(buffer.begin(), buffer.begin() + index + remaining);
    buffer.erase(buffer.begin(), buffer.begin() + index + remaining);
    return true;
}

/// Builds a raw QoS 0 PUBLISH with the remaining length encoded in as few bytes
/// as the payload requires (1 below 128, 2 below 16384, 3 above).
inline std::vector<uint8_t> make_raw_publish(const std::string& topic, const std::string& payload) {
    std::vector<uint8_t> pkt;
    pkt.push_back(0x30);
    std::size_t remaining = 2 + topic.size() + payload.size();
    do {
        uint8_t b = static_cast<uint8_t>(remaining % 128);
        remaining /= 128;
        if (remaining != 0) {
            b |= 0x80;
        }
        pkt.push_back(b);
    } while (remaining != 0);
    pkt.push_back(static_cast<uint8_t>(topic.size() >> 8));
    pkt.push_back(static_cast<uint8_t>(topic.size() & 0xff));
    pkt.insert(pkt.end(), topic.begin(), topic.end());
    pkt.insert(pkt.end(), payload.begin(), payload.end());
    return pkt;
}

inline std::vector<uint8_t> connack_bytes(uint8_t return_code, uint8_t session_present = 0) {
    return {0x20, 0x02, static_cast<uint8_t>(session_present), return_code};
}

/// Packet id of a PUBACK / PUBREC / PUBREL / PUBCOMP frame (fixed header is 2
/// bytes, the id follows immediately).  Not valid for PUBLISH, whose variable
/// header starts with the topic: use publish_packet_id() for that.
inline uint16_t frame_packet_id(const std::vector<uint8_t>& frame) {
    if (frame.size() < 4) {
        return 0;
    }
    return static_cast<uint16_t>((frame[2] << 8) | frame[3]);
}

/// Packet id of a PUBLISH frame, skipping the topic name and, for multi-byte
/// remaining lengths, the extra length bytes.
inline uint16_t publish_packet_id(const std::vector<uint8_t>& frame) {
    std::size_t index = 1;
    while (index < frame.size() && (frame[index] & 0x80) != 0) {
        ++index;
    }
    ++index;  // the last remaining-length byte
    if (index + 2 > frame.size()) {
        return 0;
    }
    const std::size_t topic_len = (static_cast<std::size_t>(frame[index]) << 8) | frame[index + 1];
    index += 2 + topic_len;
    if (index + 2 > frame.size()) {
        return 0;
    }
    return static_cast<uint16_t>((frame[index] << 8) | frame[index + 1]);
}

/// DUP flag of a PUBLISH frame.
inline bool publish_dup(const std::vector<uint8_t>& frame) {
    return !frame.empty() && (frame[0] & 0x08) != 0;
}

/// QoS of a PUBLISH frame.
inline uint8_t publish_qos(const std::vector<uint8_t>& frame) {
    return frame.empty() ? 0 : static_cast<uint8_t>((frame[0] >> 1) & 0x03);
}

class loopback_broker {
public:
    using frame_handler = std::function<void(const std::vector<uint8_t>&)>;

    enum class handshake {
        answer_ok,    ///< reply CONNACK 0 and start serving
        refuse,       ///< reply CONNACK with refuse_code and close
        silent,       ///< read the CONNECT and never answer (blackhole)
        accept_only,  ///< accept the socket and never even read
    };

    loopback_broker() : acc_(io_, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), 0)) {
        port_ = acc_.local_endpoint().port();
        thread_ = std::thread([this]() { serve(); });
    }

    ~loopback_broker() { stop(); }

    uint16_t port() const { return port_; }

    // ---- configuration, set before the client connects ----

    void set_handshake(handshake h) { config_handshake_ = h; }
    void set_refuse_code(uint8_t rc) { refuse_code_ = rc; }
    /// Runs on the broker thread for every frame after the handshake.
    void set_frame_handler(frame_handler handler) { config_handler_ = std::move(handler); }

    // ---- driven by the test thread while it runs ----

    /// Queues raw bytes to be written to the client.
    void reply(std::vector<uint8_t> bytes) {
        std::lock_guard<std::mutex> lock(mutex_);
        outbox_.push_back(std::move(bytes));
        has_outbox_ = true;
        cv_.notify_all();
    }

    void close_client_connection() {
        std::lock_guard<std::mutex> lock(mutex_);
        close_requested_ = true;
        cv_.notify_all();
    }

    std::vector<std::vector<uint8_t>> received() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return inbox_;
    }

    std::size_t received_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return inbox_.size();
    }

    bool client_connected() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return client_connected_;
    }

    void stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopped_ = true;
            close_requested_ = true;
            cv_.notify_all();
        }
        asio::error_code ec;
        // shutdown() is what releases a thread blocked in read_some(); closing
        // the socket from here while the broker thread may still touch it would
        // be a data race, so the acceptor is the only thing closed outright.
        if (client_socket_.is_open()) {
            client_socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
        }
        acc_.close(ec);
        if (thread_.joinable()) {
            thread_.join();
        }
        if (client_socket_.is_open()) {
            client_socket_.close(ec);
        }
    }

private:
    void serve() {
        asio::error_code ec;
        acc_.accept(client_socket_, ec);
        if (ec) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            client_connected_ = true;
            cv_.notify_all();
        }

        if (config_handshake_ == handshake::accept_only) {
            wait_until_stopped();
            return;
        }

        std::vector<uint8_t> buffer;
        std::vector<uint8_t> frame;
        // First frame is CONNECT.
        if (!read_one_frame(buffer, frame)) {
            return;
        }

        if (config_handshake_ == handshake::refuse) {
            write_bytes(connack_bytes(refuse_code_));
            wait_until_stopped();
            return;
        }
        if (config_handshake_ == handshake::silent) {
            wait_until_stopped();
            return;
        }
        write_bytes(connack_bytes(0));

        // Serve until stopped.  The loop always reads (so a client's writes
        // never block on a full socket buffer) and always pumps the outbox (so
        // a test that queued a reply with no handler still gets it sent).
        for (;;) {
            if (drain_outbox() == drain::closed) {
                return;
            }
            if (stopped_or_closed()) {
                return;
            }
            if (!read_one_frame(buffer, frame, /*blocking=*/false)) {
                std::this_thread::sleep_for(milliseconds(1));
                continue;
            }
            if (config_handler_) {
                config_handler_(frame);
            }
        }
    }

    enum class drain { ok, closed };

    drain drain_outbox() {
        std::deque<std::vector<uint8_t>> pending;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            pending.swap(outbox_);
            has_outbox_ = false;
        }
        for (auto& bytes : pending) {
            if (!write_bytes(bytes)) {
                return drain::closed;
            }
        }
        return drain::ok;
    }

    bool stopped_or_closed() {
        std::lock_guard<std::mutex> lock(mutex_);
        return stopped_ || close_requested_;
    }

    void wait_until_stopped() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]() { return stopped_ || close_requested_; });
    }

    bool write_bytes(const std::vector<uint8_t>& bytes) {
        asio::error_code ec;
        asio::write(client_socket_, asio::buffer(bytes), ec);
        return !ec;
    }

    /// Reads exactly one frame.  With blocking=false it returns as soon as no
    /// whole frame is buffered (the caller loops).
    bool read_one_frame(std::vector<uint8_t>& buffer, std::vector<uint8_t>& frame,
                        bool blocking = true) {
        for (;;) {
            if (take_frame(buffer, frame)) {
                std::lock_guard<std::mutex> lock(mutex_);
                inbox_.push_back(frame);
                return true;
            }
            if (!blocking && client_socket_.available() == 0) {
                return false;
            }
            char tmp[512];
            asio::error_code ec;
            std::size_t got = client_socket_.read_some(asio::buffer(tmp), ec);
            if (ec) {
                return false;
            }
            buffer.insert(buffer.end(), tmp, tmp + got);
        }
    }

    asio::io_context io_;
    asio::ip::tcp::acceptor acc_;
    asio::ip::tcp::socket client_socket_{io_};
    std::thread thread_;
    uint16_t port_ = 0;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<std::vector<uint8_t>> outbox_;
    std::vector<std::vector<uint8_t>> inbox_;
    bool has_outbox_ = false;
    bool stopped_ = false;
    bool close_requested_ = false;
    bool client_connected_ = false;

    handshake config_handshake_ = handshake::answer_ok;
    uint8_t refuse_code_ = 0;
    frame_handler config_handler_;
};

}  // namespace litemqtt_test

#endif  // LITEMQTT_TEST_LOOPBACK_BROKER_HPP

// Regression tests for the inbound framing of connection: packets whose
// remaining length is encoded in more than one byte used to reach the parser
// with part of that length missing, which shifted the message contents.
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "litemqtt/mqtt_helper.hpp"

namespace {

using namespace std::chrono;
using asio::ip::tcp;

bool wait_for(const std::function<bool()>& pred, int timeout_ms) {
    const auto deadline = steady_clock::now() + milliseconds(timeout_ms);
    while (steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(milliseconds(5));
    }
    return pred();
}

/// Serializes a QoS0 PUBLISH so that the remaining length uses as many bytes as
/// the payload requires (1 byte below 128, 2 below 16384, 3 above).
std::vector<uint8_t> make_publish(const std::string& topic, const std::string& payload) {
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

/// Accepts one client, answers CONNECT, pushes the given publishes and keeps the
/// link open until told to stop.
class loopback_broker {
public:
    loopback_broker() : acc_(io_, tcp::endpoint(asio::ip::tcp::v4(), 0)) {
        port_ = acc_.local_endpoint().port();
        thread_ = std::thread([this]() { serve(); });
    }

    ~loopback_broker() { stop(); }

    uint16_t port() const { return port_; }

    void stop() {
        io_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }

private:
    void serve() {
        tcp::socket sock(io_);
        asio::error_code ec;
        acc_.accept(sock, ec);
        if (ec) {
            return;
        }
        std::vector<char> buf(4096);
        sock.read_some(asio::buffer(buf), ec);  // CONNECT
        if (ec) {
            return;
        }
        const uint8_t connack[4] = {0x20, 0x02, 0x00, 0x00};
        asio::write(sock, asio::buffer(connack, 4), ec);

        for (const auto& pkt : publishes_) {
            asio::write(sock, asio::buffer(pkt), ec);
            if (ec) {
                return;
            }
        }

        // Keep the link open long enough for the client to read everything.
        std::this_thread::sleep_for(milliseconds(500));
        sock.close(ec);
    }

public:
    std::vector<std::vector<uint8_t>> publishes_;

private:
    asio::io_context io_;
    tcp::acceptor acc_;
    std::thread thread_;
    uint16_t port_ = 0;
};

struct received_message {
    std::string topic;
    std::string payload;
};

}  // namespace

TEST(ConnectionFraming, ParsesOneTwoAndThreeByteRemainingLengths) {
    // 8 bytes -> 1 length byte, 300 bytes -> 2, 100 KiB -> 3.
    std::vector<std::size_t> payload_sizes = {8, 300, 100 * 1024};
    loopback_broker broker;
    for (std::size_t size : payload_sizes) {
        broker.publishes_.push_back(make_publish("big/topic", std::string(size, 'B')));
    }

    auto helper = std::make_shared<litemqtt::mqtt_helper>("127.0.0.1", broker.port(), "framing");
    helper->set_log_handler([](const litemqtt::log_record&) {});
    helper->set_reconnect_config(0, 10);

    std::mutex mutex;
    std::vector<received_message> messages;
    helper->on_message([&mutex, &messages](std::string topic, std::string payload, uint8_t,
                                          uint16_t) {
        std::lock_guard<std::mutex> lock(mutex);
        messages.push_back(received_message{std::move(topic), std::move(payload)});
    });

    helper->start();
    wait_for([&mutex, &messages]() {
        std::lock_guard<std::mutex> lock(mutex);
        return messages.size() >= 3;
    }, 3000);
    helper->stop();

    std::vector<received_message> got;
    {
        std::lock_guard<std::mutex> lock(mutex);
        got = messages;
    }

    ASSERT_EQ(got.size(), payload_sizes.size()) << "not every message was delivered";
    for (std::size_t i = 0; i < payload_sizes.size(); ++i) {
        EXPECT_EQ(got[i].topic, "big/topic") << "message " << i;
        EXPECT_EQ(got[i].payload.size(), payload_sizes[i]) << "message " << i;
        EXPECT_EQ(got[i].payload, std::string(payload_sizes[i], 'B')) << "message " << i;
    }
}

// A write that can never make progress must be reported back, not parked in the
// queue: the handler usually holds its owner (mqtt_client holds the connection),
// so a stranded item is both a silent lost callback and a reference cycle.
TEST(ConnectionWriteQueue, FailsWritesThatCannotBeSent) {
    asio::io_context io;
    auto conn = std::make_shared<litemqtt::connection>(io);

    std::atomic<int> calls{0};
    std::atomic<int> errors{0};
    auto handler = [&calls, &errors](const asio::error_code& ec) {
        ++calls;
        if (ec) {
            ++errors;
        }
    };

    // Never connected: there is no socket, so nothing can drain the queue.
    conn->async_write_packet({0x10, 0x00}, handler);
    conn->async_write_packet({0x10, 0x00}, handler);
    for (int i = 0; i < 200 && calls.load() < 2; ++i) {
        io.poll();
        std::this_thread::sleep_for(milliseconds(5));
    }
    EXPECT_EQ(calls.load(), 2) << "writes on an unusable socket were never reported";
    EXPECT_EQ(errors.load(), 2) << "the failures were reported as success";

    // ... and so is anything written after close().
    calls.store(0);
    errors.store(0);
    conn->close();
    io.restart();  // poll() marks the context stopped once it runs out of work
    conn->async_write_packet({0x10, 0x00}, handler);
    for (int i = 0; i < 200 && calls.load() < 1; ++i) {
        io.poll();
        std::this_thread::sleep_for(milliseconds(5));
    }
    EXPECT_EQ(calls.load(), 1) << "a write queued after close() was dropped";
    EXPECT_EQ(errors.load(), 1);
}

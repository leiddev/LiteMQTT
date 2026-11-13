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
#include "loopback_broker.hpp"

namespace {

using namespace std::chrono;
using litemqtt_test::loopback_broker;
using litemqtt_test::make_raw_publish;
using litemqtt_test::wait_for;

struct received_message {
    std::string topic;
    std::string payload;
};

}  // namespace

TEST(ConnectionFraming, ParsesOneTwoAndThreeByteRemainingLengths) {
    // 8 bytes -> 1 length byte, 300 bytes -> 2, 100 KiB -> 3.
    std::vector<std::size_t> payload_sizes = {8, 300, 100 * 1024};

    std::vector<std::vector<uint8_t>> pushes;
    for (std::size_t size : payload_sizes) {
        pushes.push_back(make_raw_publish("big/topic", std::string(size, 'B')));
    }

    // The peer only has to stay open: the frames are pushed from this thread, so
    // any inbound frame (the SUBSCRIBE) is answered by nothing at all.
    loopback_broker broker;

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
    ASSERT_TRUE(wait_for([&broker]() { return broker.client_connected(); }, 2000));
    for (const auto& pkt : pushes) {
        broker.reply(pkt);
    }
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


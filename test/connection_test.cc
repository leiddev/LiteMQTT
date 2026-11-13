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

// --- inbound framing errors -----------------------------------------------

namespace {

/// Drives one connection against a peer that sends @p bytes and reports the
/// first read result.  Returns true when a result arrived before the timeout.
struct read_result {
    bool got = false;
    bool failed = false;
    std::string message;
    std::vector<uint8_t> packet;
};

read_result read_one(const std::vector<uint8_t>& bytes, std::size_t max_packet = 0) {
    asio::io_context io;
    auto conn = std::make_shared<litemqtt::connection>(io);
    if (max_packet != 0) {
        conn->set_max_packet_size(max_packet);
    }

    // A peer on loopback that accepts and writes the bytes, then holds the link
    // open until the client closes it (join happens after conn->close()).
    asio::ip::tcp::acceptor acc(io, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), 0));
    const uint16_t port = acc.local_endpoint().port();
    asio::ip::tcp::socket peer(io);
    std::thread writer([&]() {
        asio::error_code ec;
        acc.accept(peer, ec);
        if (ec) {
            return;
        }
        asio::write(peer, asio::buffer(bytes), ec);
        char drain[64];
        while (!ec) {
            peer.read_some(asio::buffer(drain), ec);
        }
        peer.close(ec);
    });

    read_result result;
    conn->async_connect("127.0.0.1", port, [&](const asio::error_code& ec) {
        if (ec) {
            result.got = true;
            result.failed = true;
            result.message = ec.message();
            return;
        }
        conn->mark_connected();
        conn->async_read_packet([&](const asio::error_code& rec, const std::vector<uint8_t>& pkt) {
            result.got = true;
            result.failed = static_cast<bool>(rec);
            result.message = rec.message();
            result.packet = pkt;
        });
    });

    for (int i = 0; i < 400 && !result.got; ++i) {
        io.run_one();
        if (io.stopped()) {
            io.restart();
        }
    }

    conn->close();
    io.restart();
    io.run();  // let the close handlers drain
    writer.join();
    return result;
}

}  // namespace

TEST(ConnectionFramingErrors, ZeroLengthPacketIsDelivered) {
    const auto r = read_one({0xC0, 0x00});
    ASSERT_TRUE(r.got) << "no read result";
    EXPECT_FALSE(r.failed) << r.message;
    EXPECT_EQ(r.packet, (std::vector<uint8_t>{0xC0, 0x00}));
}

TEST(ConnectionFramingErrors, ReservedPacketTypeZeroIsRejected) {
    const auto r = read_one({0x00, 0x00});
    ASSERT_TRUE(r.got) << "no read result";
    EXPECT_TRUE(r.failed) << "packet type 0 is reserved and must be rejected";
}

TEST(ConnectionFramingErrors, ReservedPacketTypeFifteenIsRejected) {
    const auto r = read_one({0xF0, 0x00});
    ASSERT_TRUE(r.got) << "no read result";
    EXPECT_TRUE(r.failed) << "packet type 15 is reserved and must be rejected";
}

TEST(ConnectionFramingErrors, RemainingLengthOverFourBytesIsRejected) {
    // Five length bytes, all continuation: not a legal encoding.
    const auto r = read_one({0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F});
    ASSERT_TRUE(r.got) << "no read result";
    EXPECT_TRUE(r.failed) << "a five-byte remaining length must be rejected";
}

// The length field is chosen by the peer, and the body buffer is allocated at
// its full size.  Without a cap a four-byte length of 268435455 allocates
// 256 MiB before any payload arrives.
TEST(ConnectionFramingErrors, OversizedPacketIsRejectedBeforeAllocating) {
    // PINGREQ type, but a declared body far above the default 1 MiB cap.
    const auto r = read_one({0xC0, 0xFF, 0xFF, 0x7F});
    ASSERT_TRUE(r.got) << "no read result";
    EXPECT_TRUE(r.failed) << "an oversized remaining length must be rejected";
}

TEST(ConnectionFramingErrors, PacketBelowTheCapIsAccepted) {
    // 8-byte body, comfortably under the default cap.
    std::vector<uint8_t> frame = {0x30, 0x08};
    frame.insert(frame.end(), 8, 'x');
    const auto r = read_one(frame);
    ASSERT_TRUE(r.got) << "no read result";
    EXPECT_FALSE(r.failed) << r.message;
    EXPECT_EQ(r.packet.size(), frame.size());
}

TEST(ConnectionFramingErrors, CapIsConfigurable) {
    std::vector<uint8_t> frame = {0x30, 0x08};
    frame.insert(frame.end(), 8, 'x');

    // A 4-byte cap rejects what the default accepts...
    const auto rejected = read_one(frame, 4);
    ASSERT_TRUE(rejected.got);
    EXPECT_TRUE(rejected.failed) << "a body above the configured cap must be rejected";

    // ... and a generous cap accepts it.
    const auto accepted = read_one(frame, 1024);
    ASSERT_TRUE(accepted.got);
    EXPECT_FALSE(accepted.failed) << accepted.message;
}


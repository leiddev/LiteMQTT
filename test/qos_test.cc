#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "litemqtt/client.hpp"
#include "litemqtt/packets.hpp"
#include "loopback_broker.hpp"

namespace {

using namespace std::chrono;
using litemqtt::mqtt_client;
using litemqtt_test::connack_bytes;
using litemqtt_test::frame_packet_id;
using litemqtt_test::publish_dup;
using litemqtt_test::publish_packet_id;
using litemqtt_test::loopback_broker;
using litemqtt_test::wait_for;

/// Runs mqtt_client on its own io thread, connected to @p broker, so the tests
/// can drive the real socket path instead of the handle_*() entry points.
class client_harness {
public:
    explicit client_harness(loopback_broker& broker) : guard_(io_) {
        client_ = std::make_shared<mqtt_client>(io_);
        client_->set_retry_interval_for_test(milliseconds(60));
        client_->set_connect_timeout(2000);
        client_->set_log_handler([](const litemqtt::log_record&) {});
        thread_ = std::thread([this]() { io_.run(); });
        broker_ = &broker;
    }

    ~client_harness() {
        client_->close();
        io_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    bool connect_and_wait() {
        connected_ = false;
        client_->async_connect("127.0.0.1", broker_->port(),
                               [this](bool ok, uint8_t) { connected_ = ok; });
        return wait_for([this]() { return connected_.load(); }, 3000);
    }

    mqtt_client& client() { return *client_; }

private:
    asio::io_context io_;
    asio::io_context::work guard_;
    std::shared_ptr<mqtt_client> client_;
    loopback_broker* broker_ = nullptr;
    std::thread thread_;
    std::atomic<bool> connected_{false};
};

/// Every PUBLISH frame the peer received, in order.
std::vector<std::vector<uint8_t>> publishes_received(const loopback_broker& broker) {
    std::vector<std::vector<uint8_t>> out;
    for (const auto& frame : broker.received()) {
        if (!frame.empty() && (frame[0] >> 4) == 3) {
            out.push_back(frame);
        }
    }
    return out;
}

std::vector<std::vector<uint8_t>> frames_of_type(const loopback_broker& broker, uint8_t type) {
    std::vector<std::vector<uint8_t>> out;
    for (const auto& frame : broker.received()) {
        if (!frame.empty() && (frame[0] >> 4) == type) {
            out.push_back(frame);
        }
    }
    return out;
}

}  // namespace

// An unacknowledged QoS 1 publish must be retransmitted on the retry interval,
// with DUP set and the *same* packet id -- that is what lets the broker drop the
// duplicate instead of delivering it twice.
TEST(ClientQos1, RetransmitsWithDupAndSamePacketId) {
    loopback_broker broker;
    client_harness harness(broker);
    ASSERT_TRUE(harness.connect_and_wait());

    std::atomic<int> callbacks{0};
    std::atomic<bool> last_success{true};
    harness.client().async_publish("t/retry", "payload", 1,
        [&callbacks, &last_success](bool ok, std::string, uint8_t, uint16_t) {
            ++callbacks;
            last_success = ok;
        });

    ASSERT_TRUE(wait_for([&broker]() { return publishes_received(broker).size() >= 2; }, 3000))
        << "no retransmission happened";

    auto sent = publishes_received(broker);
    ASSERT_GE(sent.size(), 2u);
    EXPECT_FALSE(publish_dup(sent[0])) << "the first send must not set DUP";
    EXPECT_TRUE(publish_dup(sent[1])) << "the retransmission must set DUP";
    EXPECT_EQ(publish_packet_id(sent[0]), publish_packet_id(sent[1]))
        << "a retransmission must reuse the packet id";
}

// Nothing in the spec bounds the retries, but a client that never gives up on a
// dead broker accumulates pending state forever.  After the configured number of
// attempts the publish is failed *and* the session is closed so the reconnect
// policy can take over.
TEST(ClientQos1, GivesUpAfterMaxRetransmits) {
    loopback_broker broker;
    client_harness harness(broker);
    ASSERT_TRUE(harness.connect_and_wait());

    std::atomic<int> callbacks{0};
    std::atomic<bool> last_success{true};
    std::atomic<int> closes{0};
    harness.client().on_close([&closes]() { ++closes; });
    harness.client().async_publish("t/giveup", "payload", 1,
        [&callbacks, &last_success](bool ok, std::string, uint8_t, uint16_t) {
            ++callbacks;
            last_success = ok;
        });

    ASSERT_TRUE(wait_for([&callbacks]() { return callbacks.load() >= 1; }, 5000))
        << "the publish callback never ran";
    EXPECT_EQ(callbacks.load(), 1) << "exactly one terminal callback expected";
    EXPECT_FALSE(last_success.load());
    EXPECT_EQ(harness.client().debug_pending_publish_count(), 0u);
    EXPECT_GE(closes.load(), 1) << "giving up must close the session";

    // No further retransmissions once the message was failed.
    const std::size_t after = publishes_received(broker).size();
    std::this_thread::sleep_for(milliseconds(200));
    EXPECT_EQ(publishes_received(broker).size(), after);
}

// The common case: a PUBACK arrives, so the retry timer must be cancelled and
// the callback fired exactly once.
TEST(ClientQos1, AckStopsRetransmission) {
    loopback_broker broker;
    broker.set_frame_handler([&broker](const std::vector<uint8_t>& frame) {
        if ((frame[0] >> 4) == 3 && litemqtt_test::publish_qos(frame) == 1) {
            litemqtt::puback_packet ack;
            ack.packet_id = publish_packet_id(frame);
            broker.reply(ack.serialize());
        }
    });

    client_harness harness(broker);
    ASSERT_TRUE(harness.connect_and_wait());

    std::atomic<int> callbacks{0};
    std::atomic<bool> last_success{false};
    harness.client().async_publish("t/ack", "payload", 1,
        [&callbacks, &last_success](bool ok, std::string, uint8_t, uint16_t) {
            ++callbacks;
            last_success = ok;
        });

    ASSERT_TRUE(wait_for([&callbacks]() { return callbacks.load() >= 1; }, 3000));
    EXPECT_TRUE(last_success.load());
    EXPECT_EQ(harness.client().debug_pending_publish_count(), 0u);

    // Wait well past the retry interval: nothing may be sent again.
    const std::size_t after = publishes_received(broker).size();
    std::this_thread::sleep_for(milliseconds(250));
    EXPECT_EQ(publishes_received(broker).size(), after)
        << "an acknowledged publish was retransmitted";
    EXPECT_EQ(callbacks.load(), 1);
}

// A duplicated PUBACK for an id that is no longer pending must be ignored
// rather than fire the callback twice.
TEST(ClientQos1, DuplicateAckIsIgnored) {
    loopback_broker broker;
    broker.set_frame_handler([&broker](const std::vector<uint8_t>& frame) {
        if ((frame[0] >> 4) == 3) {
            litemqtt::puback_packet ack;
            ack.packet_id = publish_packet_id(frame);
            broker.reply(ack.serialize());
            broker.reply(ack.serialize());
        }
    });

    client_harness harness(broker);
    ASSERT_TRUE(harness.connect_and_wait());

    std::atomic<int> callbacks{0};
    harness.client().async_publish("t/dup", "payload", 1,
        [&callbacks](bool, std::string, uint8_t, uint16_t) { ++callbacks; });

    ASSERT_TRUE(wait_for([&callbacks]() { return callbacks.load() >= 1; }, 3000));
    std::this_thread::sleep_for(milliseconds(200));
    EXPECT_EQ(callbacks.load(), 1) << "a duplicate PUBACK fired the callback again";
}

// QoS 2: PUBLISH -> PUBREC -> PUBREL -> PUBCOMP.  The message is only reported
// done at PUBCOMP, and the PUBREL must carry the same packet id.
TEST(ClientQos2, FullHandshake) {
    loopback_broker broker;
    broker.set_frame_handler([&broker](const std::vector<uint8_t>& frame) {
        const uint8_t type = frame[0] >> 4;
        if (type == 3) {
            litemqtt::pubrec_packet rec;
            rec.packet_id = publish_packet_id(frame);
            broker.reply(rec.serialize());
        } else if (type == 6) {
            litemqtt::pubcomp_packet comp;
            comp.packet_id = frame_packet_id(frame);
            broker.reply(comp.serialize());
        }
    });

    client_harness harness(broker);
    ASSERT_TRUE(harness.connect_and_wait());

    std::atomic<int> callbacks{0};
    std::atomic<bool> last_success{false};
    harness.client().async_publish("t/q2", "payload", 2,
        [&callbacks, &last_success](bool ok, std::string, uint8_t, uint16_t) {
            ++callbacks;
            last_success = ok;
        });

    ASSERT_TRUE(wait_for([&callbacks]() { return callbacks.load() >= 1; }, 3000))
        << "the QoS 2 handshake never completed";
    EXPECT_TRUE(last_success.load());

    auto rel = frames_of_type(broker, 6);
    ASSERT_EQ(rel.size(), 1u) << "expected exactly one PUBREL";
    auto pub = publishes_received(broker);
    ASSERT_FALSE(pub.empty());
    EXPECT_EQ(frame_packet_id(rel[0]), publish_packet_id(pub[0]))
        << "PUBREL must reuse the PUBLISH packet id";
    EXPECT_EQ(rel[0][0] & 0x0F, 0x02) << "PUBREL fixed header flags must be 0x02";
    EXPECT_EQ(harness.client().debug_pending_publish_count(), 0u);
}

// If PUBCOMP never arrives the PUBREL is retransmitted, and the session is
// closed once the retries are exhausted.
TEST(ClientQos2, PubrelRetransmitsAndGivesUp) {
    loopback_broker broker;
    broker.set_frame_handler([&broker](const std::vector<uint8_t>& frame) {
        if ((frame[0] >> 4) == 3) {
            // PUBREC only: never complete the handshake.
            litemqtt::pubrec_packet rec;
            rec.packet_id = publish_packet_id(frame);
            broker.reply(rec.serialize());
        }
    });

    client_harness harness(broker);
    ASSERT_TRUE(harness.connect_and_wait());

    std::atomic<int> closes{0};
    harness.client().on_close([&closes]() { ++closes; });
    harness.client().async_publish("t/q2retry", "payload", 2);

    ASSERT_TRUE(wait_for([&broker]() { return frames_of_type(broker, 6).size() >= 2; }, 3000))
        << "PUBREL was never retransmitted";
    ASSERT_TRUE(wait_for([&closes]() { return closes.load() >= 1; }, 3000))
        << "the session must be closed once the PUBREL retries are exhausted";

    auto rel = frames_of_type(broker, 6);
    // The first PUBREL plus at most three retransmissions (the default limit).
    EXPECT_LE(rel.size(), 4u) << "more PUBREL retries than the configured limit";
}

// A duplicate PUBREC for an id that already has a PUBREL in flight must not
// start a second flow.
TEST(ClientQos2, DuplicatePubrecDoesNotRestart) {
    loopback_broker broker;
    broker.set_frame_handler([&broker](const std::vector<uint8_t>& frame) {
        if ((frame[0] >> 4) == 3) {
            litemqtt::pubrec_packet rec;
            rec.packet_id = publish_packet_id(frame);
            broker.reply(rec.serialize());
            broker.reply(rec.serialize());
        }
    });

    client_harness harness(broker);
    ASSERT_TRUE(harness.connect_and_wait());

    std::atomic<int> callbacks{0};
    harness.client().async_publish("t/q2dup", "payload", 2,
        [&callbacks](bool, std::string, uint8_t, uint16_t) { ++callbacks; });

    ASSERT_TRUE(wait_for([&broker]() { return frames_of_type(broker, 6).size() >= 1; }, 3000));
    // Well inside the 60 ms retry window, so any extra PUBREL here can only come
    // from the duplicate PUBREC restarting the flow.
    std::this_thread::sleep_for(milliseconds(20));
    EXPECT_EQ(frames_of_type(broker, 6).size(), 1u)
        << "a duplicate PUBREC produced a second PUBREL";
    EXPECT_EQ(callbacks.load(), 0) << "nothing is done until PUBCOMP arrives";
}

// A QoS 0 publish with no session must fail through the callback, not vanish.
TEST(ClientPublish, FailsWithoutConnection) {
    asio::io_context io;
    auto client = std::make_shared<mqtt_client>(io);
    client->set_log_handler([](const litemqtt::log_record&) {});

    std::atomic<int> callbacks{0};
    std::atomic<bool> last_success{true};
    std::atomic<uint16_t> last_id{1};
    client->async_publish("t/nc", "payload", 0,
        [&callbacks, &last_success, &last_id](bool ok, std::string, uint8_t, uint16_t id) {
            ++callbacks;
            last_success = ok;
            last_id = id;
        });
    io.run();

    EXPECT_EQ(callbacks.load(), 1);
    EXPECT_FALSE(last_success.load());
    EXPECT_EQ(last_id.load(), 0) << "a failed publish reports packet id 0";
}

// An out-of-range QoS is a programming error: reported through the callback
// whatever the connection state is.
TEST(ClientPublish, RejectsInvalidQos) {
    asio::io_context io;
    auto client = std::make_shared<mqtt_client>(io);
    client->set_log_handler([](const litemqtt::log_record&) {});

    for (uint8_t qos : {3, 4, 255}) {
        std::atomic<int> callbacks{0};
        std::atomic<bool> last_success{true};
        client->async_publish("t/badqos", "payload", qos,
            [&callbacks, &last_success](bool ok, std::string, uint8_t, uint16_t) {
                ++callbacks;
                last_success = ok;
            });
        io.restart();
        io.run();
        EXPECT_EQ(callbacks.load(), 1) << "qos " << static_cast<int>(qos);
        EXPECT_FALSE(last_success.load()) << "qos " << static_cast<int>(qos);
    }
}

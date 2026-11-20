#include <gtest/gtest.h>
#include <memory>
#include <vector>

#include <asio.hpp>

#include "litemqtt/buffer_writer.hpp"
#include "litemqtt/client.hpp"
#include "litemqtt/packets.hpp"
#include "litemqtt/packet_type.hpp"
#include "loopback_broker.hpp"

namespace {

using namespace litemqtt;

namespace {

std::vector<uint8_t> build_puback_bytes(uint16_t packet_id) {
    puback_packet pkt;
    pkt.packet_id = packet_id;
    return pkt.serialize();
}

std::vector<uint8_t> build_suback_bytes(uint16_t packet_id,
                                        const std::vector<uint8_t>& return_codes) {
    suback_packet pkt;
    pkt.packet_id = packet_id;
    pkt.return_codes = return_codes;
    return pkt.serialize();
}

std::vector<uint8_t> build_publish_bytes(uint16_t packet_id,
                                         const std::string& topic,
                                         const std::string& payload,
                                         uint8_t qos,
                                         bool dup) {
    publish_packet pkt;
    pkt.packet_id = packet_id;
    pkt.qos = qos;
    pkt.dup = dup;
    pkt.topic_name = topic;
    pkt.payload = payload;
    return pkt.serialize();
}

std::shared_ptr<mqtt_client> make_test_client(asio::io_context& io) {
    return std::make_shared<mqtt_client>(io);
}

}  // namespace

TEST(MqttClientTest, AcquirePacketIdSkipsZeroAndPending) {
    asio::io_context io;
    auto client = make_test_client(io);

    uint16_t first = client->acquire_packet_id_for_test();
    EXPECT_NE(first, 0);

    uint16_t blocked = 42;
    client->debug_insert_pending_publish(blocked);

    uint16_t a = client->acquire_packet_id_for_test();
    uint16_t b = client->acquire_packet_id_for_test();
    EXPECT_NE(a, blocked);
    EXPECT_NE(b, blocked);
    EXPECT_NE(a, 0);
    EXPECT_NE(b, 0);

    client->debug_clear_pending_publishes();
}

TEST(MqttClientTest, PubackClearsPendingAndFiresCallback) {
    asio::io_context io;
    auto client = make_test_client(io);

    bool fired = false;
    bool got_success = false;
    uint16_t got_id = 0;
    uint16_t pid = 7;

    client->debug_insert_pending_publish_with_cb(pid,
        [&](bool success, std::string /*topic*/, uint8_t /*qos*/, uint16_t id) {
            fired = true;
            got_success = success;
            got_id = id;
        });

    client->handle_puback(build_puback_bytes(pid));

    EXPECT_TRUE(fired);
    EXPECT_TRUE(got_success);
    EXPECT_EQ(got_id, pid);
    EXPECT_EQ(client->debug_pending_publish_count(), 0u);
}

TEST(MqttClientTest, SubackPerFilter) {
    asio::io_context io;
    auto client = make_test_client(io);

    uint16_t pid = 9;
    std::vector<std::tuple<bool, std::string, uint8_t>> calls;
    client->debug_insert_pending_subscribe_with_cb(pid,
        {{"a/topic", 1}, {"b/topic", 0}},
        [&](bool success, std::string topic, uint8_t qos) {
            calls.emplace_back(success, topic, qos);
        });

    client->handle_suback(build_suback_bytes(pid, {0x01, 0x00}));

    ASSERT_EQ(calls.size(), 2u);
    EXPECT_TRUE(std::get<0>(calls[0]));
    EXPECT_EQ(std::get<1>(calls[0]), "a/topic");
    EXPECT_EQ(std::get<2>(calls[0]), 1);
    EXPECT_TRUE(std::get<0>(calls[1]));
    EXPECT_EQ(std::get<1>(calls[1]), "b/topic");
    EXPECT_EQ(std::get<2>(calls[1]), 0);
    EXPECT_EQ(client->debug_pending_subscribe_count(), 0u);
}

TEST(MqttClientTest, SubackFailureCodeMarksTopicFailed) {
    asio::io_context io;
    auto client = make_test_client(io);

    uint16_t pid = 11;
    std::vector<std::tuple<bool, std::string, uint8_t>> calls;
    client->debug_insert_pending_subscribe_with_cb(pid,
        {{"good", 1}, {"bad", 1}},
        [&](bool success, std::string topic, uint8_t qos) {
            calls.emplace_back(success, topic, qos);
        });

    client->handle_suback(build_suback_bytes(pid, {0x01, 0x80}));

    ASSERT_EQ(calls.size(), 2u);
    EXPECT_TRUE(std::get<0>(calls[0]));
    EXPECT_FALSE(std::get<0>(calls[1]));
    EXPECT_EQ(std::get<2>(calls[1]), 0);
}

TEST(MqttClientTest, InboundPublishQos1IsAtLeastOnce) {
    asio::io_context io;
    auto client = make_test_client(io);

    std::vector<std::tuple<std::string, uint8_t, uint16_t>> delivered;
    client->on_message(
        [&](std::string topic, std::string, uint8_t qos, uint16_t id) {
            delivered.emplace_back(topic, qos, id);
        });

    // QoS 1 is "at least once" (MQTT 3.1.1 section 4.3.2): every PUBLISH is
    // acknowledged and the broker may retransmit it with DUP=1.  The client
    // deliberately does not deduplicate locally ("trust broker QoS handshake",
    // see 88d09da), so a retransmission is delivered again.
    client->handle_publish(build_publish_bytes(3, "t/a", "payload", 1, false));
    ASSERT_EQ(delivered.size(), 1u);
    EXPECT_EQ(std::get<0>(delivered[0]), "t/a");
    EXPECT_EQ(std::get<1>(delivered[0]), 1);
    EXPECT_EQ(std::get<2>(delivered[0]), 3);

    // DUP retransmission of the same packet id: delivered again.
    client->handle_publish(build_publish_bytes(3, "t/a", "payload", 1, true));
    EXPECT_EQ(delivered.size(), 2u);

    // A new message (different packet id) is independent of the previous one.
    client->handle_publish(build_publish_bytes(4, "t/a", "payload", 1, false));
    ASSERT_EQ(delivered.size(), 3u);
    EXPECT_EQ(std::get<2>(delivered[2]), 4);
}

TEST(MqttClientTest, InboundPublishQos2DeliversOnPublishOnly) {
    asio::io_context io;
    auto client = make_test_client(io);

    int seen = 0;
    uint8_t seen_qos = 255;
    uint16_t seen_id = 0;
    client->on_message([&](std::string, std::string, uint8_t qos, uint16_t id) {
        ++seen;
        seen_qos = qos;
        seen_id = id;
    });

    // A QoS 2 message is delivered when the PUBLISH arrives (PUBREC is replied
    // right away); the following PUBREL only completes the handshake with
    // PUBCOMP and must not dispatch the message a second time.
    client->handle_publish(build_publish_bytes(21, "t/b", "p2", 2, false));
    EXPECT_EQ(seen, 1);
    EXPECT_EQ(seen_qos, 2);
    EXPECT_EQ(seen_id, 21);

    pubrel_packet pubrel;
    pubrel.packet_id = 21;
    client->handle_pubrel(pubrel.serialize());
    EXPECT_EQ(seen, 1);
}

TEST(MqttClientTest, PublishPacketDupFlagOnWire) {
    publish_packet pkt;
    pkt.packet_id = 5;
    pkt.qos = 1;
    pkt.dup = true;
    pkt.topic_name = "x";
    pkt.payload = "y";
    auto data = pkt.serialize();
    EXPECT_EQ(data[0] & 0x08, 0x08);
}

TEST(MqttClientTest, PublishPacketDupClearedByDefault) {
    publish_packet pkt;
    pkt.packet_id = 6;
    pkt.qos = 1;
    pkt.topic_name = "x";
    pkt.payload = "y";
    auto data = pkt.serialize();
    EXPECT_EQ(data[0] & 0x08, 0x00);
}

// --- CONNACK handling ------------------------------------------------------

TEST(MqttClientConnack, RefusedSessionReportsTheCodeAndStaysDisconnected) {
    asio::io_context io;
    auto client = make_test_client(io);
    client->set_log_handler([](const litemqtt::log_record&) {});

    bool connect_called = false;
    bool connect_ok = true;
    uint8_t connect_rc = 0;
    int closes = 0;
    client->on_connect([&](bool ok, uint8_t rc) {
        connect_called = true;
        connect_ok = ok;
        connect_rc = rc;
    });
    int disconnects = 0;
    client->on_close([&closes]() { ++closes; });
    client->on_disconnected([&disconnects]() { ++disconnects; });

    connack_packet refusal;
    refusal.return_code = 5;  // not authorized
    client->handle_packet(refusal.serialize());

    EXPECT_TRUE(connect_called);
    EXPECT_FALSE(connect_ok);
    EXPECT_EQ(connect_rc, 5);
    EXPECT_EQ(client->state(), connection_state::disconnected);
    // The refusal is reported through the connect callback only; the close
    // notification is suppressed so the reconnect policy is driven from one
    // place instead of two.
    EXPECT_EQ(closes, 0) << "a refused CONNACK must not also fire on_close";
    EXPECT_EQ(disconnects, 0) << "nothing was established, so nothing disconnected";
}

// The other half of the split: once the session is up, its teardown belongs to
// on_disconnected() and on_close() stays quiet.
TEST(MqttClientConnack, ClosingAnEstablishedSessionReportsOnDisconnected) {
    asio::io_context io;
    auto client = make_test_client(io);
    client->set_log_handler([](const litemqtt::log_record&) {});

    int closes = 0;
    int disconnects = 0;
    client->on_close([&closes]() { ++closes; });
    client->on_disconnected([&disconnects]() { ++disconnects; });

    connack_packet accept;
    accept.return_code = 0;
    client->handle_packet(accept.serialize());
    ASSERT_EQ(client->state(), connection_state::connected);

    // A protocol error on the established session: malformed inbound PUBLISH.
    client->handle_publish({0x36, 0x04, 0x00, 0x01, 'a', 0x00, 0x01});

    EXPECT_EQ(disconnects, 1);
    EXPECT_EQ(closes, 0) << "an established session must not report on_close as well";
    EXPECT_EQ(client->state(), connection_state::disconnected);
}

TEST(MqttClientConnack, AcceptedSessionMarksConnected) {
    asio::io_context io;
    auto client = make_test_client(io);
    client->set_log_handler([](const litemqtt::log_record&) {});

    bool connect_ok = false;
    uint8_t connect_rc = 255;
    client->on_connect([&](bool ok, uint8_t rc) {
        connect_ok = ok;
        connect_rc = rc;
    });

    connack_packet accept;
    accept.return_code = 0;
    client->handle_packet(accept.serialize());

    EXPECT_TRUE(connect_ok);
    EXPECT_EQ(connect_rc, 0);
    EXPECT_EQ(client->state(), connection_state::connected);
}

TEST(MqttClientConnack, MalformedConnackClosesTheSession) {
    asio::io_context io;
    auto client = make_test_client(io);
    client->set_log_handler([](const litemqtt::log_record&) {});

    int closes = 0;
    client->on_close([&closes]() { ++closes; });

    // Declares a 2-byte body but carries none of it.
    client->handle_packet({0x20, 0x02});

    EXPECT_EQ(closes, 1) << "a malformed CONNACK must close the session";
}

// --- SUBACK handling -------------------------------------------------------

// A SUBACK that answers fewer filters than the SUBSCRIBE asked for is a
// protocol error.  It used to be missed when the caller passed no callback,
// which is exactly what mqtt_helper does when it restores subscriptions after a
// reconnect.
TEST(MqttClientSuback, ShortReturnCodesWithNullCallbackStillCloses) {
    asio::io_context io;
    auto client = make_test_client(io);
    client->set_log_handler([](const litemqtt::log_record&) {});

    int closes = 0;
    client->on_close([&closes]() { ++closes; });

    uint16_t pid = 31;
    client->debug_insert_pending_subscribe(pid, {{"a", 1}, {"b", 1}, {"c", 1}});
    client->handle_suback(build_suback_bytes(pid, {0x01, 0x01}));

    EXPECT_EQ(closes, 1) << "the count check must not depend on the callback";
    EXPECT_EQ(client->debug_pending_subscribe_count(), 0u);
}

TEST(MqttClientSuback, ShortReturnCodesWithCallbackCloses) {
    asio::io_context io;
    auto client = make_test_client(io);
    client->set_log_handler([](const litemqtt::log_record&) {});

    int closes = 0;
    int callbacks = 0;
    client->on_close([&closes]() { ++closes; });

    uint16_t pid = 32;
    client->debug_insert_pending_subscribe_with_cb(
        pid, {{"a", 1}, {"b", 1}}, [&callbacks](bool, std::string, uint8_t) { ++callbacks; });
    client->handle_suback(build_suback_bytes(pid, {0x01}));

    EXPECT_EQ(closes, 1);
    EXPECT_EQ(callbacks, 0) << "a failed SUBACK must not report per-filter success";
}

TEST(MqttClientSuback, UnknownPacketIdIsIgnored) {
    asio::io_context io;
    auto client = make_test_client(io);
    client->set_log_handler([](const litemqtt::log_record&) {});

    int closes = 0;
    client->on_close([&closes]() { ++closes; });
    client->handle_suback(build_suback_bytes(99, {0x00}));
    EXPECT_EQ(closes, 0);
}

// --- subscribe / publish guards --------------------------------------------

// async_publish refuses to run without a session; async_subscribe used to queue
// the SUBSCRIBE instead, leaving a pending entry behind on a failed connect.
TEST(MqttClientGuards, SubscribeWithoutSessionIsRejected) {
    asio::io_context io;
    auto client = make_test_client(io);
    client->set_log_handler([](const litemqtt::log_record&) {});

    bool called = false;
    bool success = true;
    client->async_subscribe("t", 1, [&](bool ok, std::string, uint8_t) {
        called = true;
        success = ok;
    });
    io.run();

    EXPECT_TRUE(called);
    EXPECT_FALSE(success);
    EXPECT_EQ(client->debug_pending_subscribe_count(), 0u);
}

TEST(MqttClientGuards, SubscribeRejectsInvalidQos) {
    asio::io_context io;
    auto client = make_test_client(io);
    client->set_log_handler([](const litemqtt::log_record&) {});

    for (uint8_t qos : {3, 4, 255}) {
        bool called = false;
        bool success = true;
        client->async_subscribe("t", qos, [&](bool ok, std::string, uint8_t) {
            called = true;
            success = ok;
        });
        io.restart();
        io.run();
        EXPECT_TRUE(called) << "qos " << static_cast<int>(qos);
        EXPECT_FALSE(success) << "qos " << static_cast<int>(qos);
    }
}

// --- inbound packet size cap ----------------------------------------------

// The cap lives on the connection; the client has to forward it, otherwise a
// caller who only holds an mqtt_client has no way to raise it.
TEST(MqttClientPacketSize, DefaultsToOneMiB) {
    asio::io_context io;
    auto client = make_test_client(io);
    EXPECT_EQ(client->max_packet_size(), connection::kDefaultMaxPacketSize);
    EXPECT_EQ(client->max_packet_size(), 1024u * 1024u);
}

TEST(MqttClientPacketSize, SetterReachesTheConnection) {
    asio::io_context io;
    auto client = make_test_client(io);

    client->set_max_packet_size(4u * 1024u * 1024u);
    EXPECT_EQ(client->max_packet_size(), 4u * 1024u * 1024u);

    // 0 is the documented "no check" value and must survive unchanged.
    client->set_max_packet_size(0);
    EXPECT_EQ(client->max_packet_size(), 0u);
}

// The write-queue bound has to be reachable from the client for the same reason
// the packet cap is: conn_ is private.
TEST(MqttClientWriteQueue, DefaultsAndForwards) {
    asio::io_context io;
    auto client = make_test_client(io);

    EXPECT_EQ(client->max_write_queue(), connection::kDefaultMaxWriteQueue);

    client->set_max_write_queue(64);
    EXPECT_EQ(client->max_write_queue(), 64u);

    client->set_max_write_queue(0);
    EXPECT_EQ(client->max_write_queue(), 0u);
}

// --- packet id allocation --------------------------------------------------

TEST(MqttClientPacketId, NeverReturnsZero) {
    asio::io_context io;
    auto client = make_test_client(io);
    client->debug_set_next_packet_id(0xFFFE);
    EXPECT_NE(client->acquire_packet_id_for_test(), 0);
    EXPECT_NE(client->acquire_packet_id_for_test(), 0);  // wraps past 0
}

// Exhaustion is the one path that closes the session from inside the allocator.
TEST(MqttClientPacketId, ExhaustionClosesTheSession) {
    asio::io_context io;
    auto client = make_test_client(io);
    client->set_log_handler([](const litemqtt::log_record&) {});

    // Take one id, then occupy every usable one (1..65535) so the next scan
    // finds nothing free.
    const uint16_t first = 5;
    client->debug_set_next_packet_id(first);
    const uint16_t taken = client->acquire_packet_id_for_test();
    ASSERT_EQ(taken, first);
    for (uint32_t id = 1; id <= 0xFFFF; ++id) {
        client->debug_insert_pending_publish(static_cast<uint16_t>(id));
    }

    int closes = 0;
    client->on_close([&closes]() { ++closes; });
    EXPECT_EQ(client->acquire_packet_id_for_test(), 0) << "should report exhaustion";
    EXPECT_EQ(closes, 1) << "exhaustion must close the session exactly once";
}

// --- inbound PUBLISH -------------------------------------------------------

TEST(MqttClientInbound, InvalidQosIsAProtocolError) {
    asio::io_context io;
    auto client = make_test_client(io);
    client->set_log_handler([](const litemqtt::log_record&) {});

    int messages = 0;
    int closes = 0;
    client->on_message([&messages](std::string, std::string, uint8_t, uint16_t) { ++messages; });
    client->on_close([&closes]() { ++closes; });

    // QoS bits set to 3, which is reserved.
    client->handle_publish({0x36, 0x04, 0x00, 0x01, 'a', 0x00, 0x01});

    EXPECT_EQ(messages, 0) << "a reserved QoS must not reach the message callback";
    EXPECT_EQ(closes, 1);
}

TEST(MqttClientInbound, TruncatedPublishIsAProtocolError) {
    asio::io_context io;
    auto client = make_test_client(io);
    client->set_log_handler([](const litemqtt::log_record&) {});

    int messages = 0;
    int closes = 0;
    client->on_message([&messages](std::string, std::string, uint8_t, uint16_t) { ++messages; });
    client->on_close([&closes]() { ++closes; });

    // Declares a 10-byte body, carries 3.
    client->handle_publish({0x30, 0x0A, 0x00, 0x01, 'a', 'b', 'c'});

    EXPECT_EQ(messages, 0);
    EXPECT_EQ(closes, 1);
}

// Keep the client on the test thread so callbacks and assertions need no locks.
class MqttClientUnsubscribe : public ::testing::Test {
protected:
    asio::io_context io;
    litemqtt_test::loopback_broker broker;
    std::shared_ptr<mqtt_client> client = make_test_client(io);
    int disconnects = 0;

    bool pump_until(const std::function<bool()>& done) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (!done() && std::chrono::steady_clock::now() < deadline) {
            io.restart();
            io.run_for(std::chrono::milliseconds(5));
        }
        return done();
    }

    void SetUp() override {
        client->set_keep_alive(0);
        client->set_log_handler([](const log_record&) {});
        client->on_disconnected([this]() { ++disconnects; });
        client->async_connect("127.0.0.1", broker.port());
        ASSERT_TRUE(pump_until([this]() { return client->state() == connection_state::connected; }));
    }

    void TearDown() override {
        client->on_disconnected(nullptr);
        client->close();
        io.restart();
        io.poll();
        broker.stop();
    }

    std::vector<std::vector<uint8_t>> frames(uint8_t header) {
        std::vector<std::vector<uint8_t>> result;
        for (const auto& frame : broker.received()) {
            if (frame[0] == header) result.push_back(frame);
        }
        return result;
    }

    void ack(uint16_t pid) {
        unsuback_packet pkt;
        pkt.packet_id = pid;
        broker.reply(pkt.serialize());
    }
};

TEST_F(MqttClientUnsubscribe, WireFormatWaitsForAckAndIgnoresDuplicates) {
    int calls = 0;
    client->debug_set_next_packet_id(42);
    client->async_unsubscribe("a/+", [&](bool ok, std::string topic) {
        ++calls;
        EXPECT_TRUE(ok);
        EXPECT_EQ(topic, "a/+");
        // The entry must already be gone, even during the callback.
        client->debug_set_next_packet_id(42);
        EXPECT_EQ(client->acquire_packet_id_for_test(), 42);
        client->handle_packet({0xB0, 2, 0, 42});
    });
    EXPECT_EQ(calls, 0);
    ASSERT_TRUE(pump_until([&]() { return frames(0xA2).size() == 1; }));
    EXPECT_EQ(frames(0xA2)[0], (std::vector<uint8_t>{0xA2, 7, 0, 42, 0, 3, 'a', '/', '+'}));
    EXPECT_EQ(calls, 0);
    // An unrelated ACK must not complete the request either.
    ack(43);
    broker.reply(litemqtt_test::make_raw_publish("barrier", "before"));
    int barriers = 0;
    client->on_message([&](std::string, std::string, uint8_t, uint16_t) { ++barriers; });
    ASSERT_TRUE(pump_until([&]() { return barriers == 1; }));
    EXPECT_EQ(calls, 0);
    ack(42);
    ack(42);
    broker.reply(litemqtt_test::make_raw_publish("barrier", "after"));
    ASSERT_TRUE(pump_until([&]() { return barriers == 2; }));
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(disconnects, 0);
}

TEST_F(MqttClientUnsubscribe, DefaultCallbackAndMaximumTopicLength) {
    const std::string topic(65535, 'a');
    int defaults = 0;
    int explicit_calls = 0;
    client->on_unsubscribe([&](bool ok, std::string returned) {
        ++defaults;
        EXPECT_TRUE(ok);
        EXPECT_EQ(returned, topic);
    });
    client->async_unsubscribe(topic);
    client->async_unsubscribe("explicit", [&](bool ok, std::string returned) {
        ++explicit_calls;
        EXPECT_TRUE(ok);
        EXPECT_EQ(returned, "explicit");
    });
    ASSERT_TRUE(pump_until([&]() { return frames(0xA2).size() == 2; }));
    auto sent = frames(0xA2);
    buffer_reader reader(sent[0]);
    uint8_t header = 0;
    std::size_t length = 0;
    uint16_t pid = 0;
    std::string filter;
    ASSERT_TRUE(reader.read_uint8(header));
    ASSERT_TRUE(reader.read_remaining_length(length));
    ASSERT_TRUE(reader.read_uint16(pid));
    ASSERT_TRUE(reader.read_string(filter));
    EXPECT_EQ(length, 65539u);
    EXPECT_EQ(filter, topic);
    EXPECT_EQ(reader.remaining(), 0u);
    EXPECT_EQ(defaults, 0);
    EXPECT_EQ(explicit_calls, 0);
    ack(pid);
    ack(litemqtt_test::frame_packet_id(sent[1]));
    ASSERT_TRUE(pump_until([&]() { return defaults == 1 && explicit_calls == 1; }));
    client->on_unsubscribe(nullptr);
}

TEST_F(MqttClientUnsubscribe, PeerDisconnectFailsAllPendingOnceBeforeReentry) {
    int calls = 0;
    for (int i = 0; i < 2; ++i) {
        client->async_unsubscribe("pending", [&](bool ok, std::string topic) {
            ++calls;
            EXPECT_FALSE(ok);
            EXPECT_EQ(topic, "pending");
            // Neither pending request may be completed by a reentrant ACK.
            client->handle_packet({0xB0, 2, 0, 1});
            client->handle_packet({0xB0, 2, 0, 2});
        });
    }
    ASSERT_TRUE(pump_until([&]() { return frames(0xA2).size() == 2; }));
    EXPECT_EQ(calls, 0);
    broker.close_client_connection();
    ASSERT_TRUE(pump_until([&]() { return disconnects == 1; }));
    EXPECT_EQ(calls, 2);
    client->close();
    io.poll();
    EXPECT_EQ(calls, 2);
}

TEST_F(MqttClientUnsubscribe, CloseWithStoppedIoFailsPendingSynchronously) {
    int calls = 0;
    client->async_unsubscribe("pending", [&](bool ok, std::string) {
        ++calls;
        EXPECT_FALSE(ok);
        client->close();
    });
    ASSERT_TRUE(pump_until([&]() { return frames(0xA2).size() == 1; }));
    io.stop();
    client->close();
    EXPECT_EQ(calls, 1);
    io.restart();
    io.poll();
    EXPECT_EQ(calls, 1);
}

TEST_F(MqttClientUnsubscribe, GracefulDisconnectFailsPendingBeforeOnClosedStopsIo) {
    int calls = 0;
    bool closed = false;
    client->async_unsubscribe("pending", [&](bool ok, std::string) {
        ++calls;
        EXPECT_FALSE(ok);
    });
    ASSERT_TRUE(pump_until([&]() { return frames(0xA2).size() == 1; }));
    client->async_disconnect([&]() {
        EXPECT_EQ(calls, 1);
        closed = true;
        io.stop();
    });
    ASSERT_TRUE(pump_until([&]() { return closed; }));
    EXPECT_EQ(calls, 1);
    io.restart();
    io.poll();
    EXPECT_EQ(calls, 1);
}

TEST_F(MqttClientUnsubscribe, PacketIdsAreSharedWithSubscribePublishAndPubrel) {
    client->debug_set_next_packet_id(1);
    client->async_publish("p", "v", 2);
    ASSERT_TRUE(pump_until([&]() { return frames(0x34).size() == 1; }));
    EXPECT_EQ(litemqtt_test::publish_packet_id(frames(0x34)[0]), 1);
    broker.reply({0x50, 2, 0, 1});
    ASSERT_TRUE(pump_until([&]() { return frames(0x62).size() == 1; }));
    client->debug_set_next_packet_id(1);
    client->async_subscribe("s");
    ASSERT_TRUE(pump_until([&]() { return frames(0x82).size() == 1; }));
    EXPECT_EQ(litemqtt_test::frame_packet_id(frames(0x82)[0]), 2);
    client->debug_set_next_packet_id(1);
    client->async_publish("p", "v", 1);
    ASSERT_TRUE(pump_until([&]() { return frames(0x32).size() == 1; }));
    EXPECT_EQ(litemqtt_test::publish_packet_id(frames(0x32)[0]), 3);
    client->debug_set_next_packet_id(1);
    client->async_unsubscribe("u");
    ASSERT_TRUE(pump_until([&]() { return frames(0xA2).size() == 1; }));
    EXPECT_EQ(litemqtt_test::frame_packet_id(frames(0xA2)[0]), 4);
    client->debug_set_next_packet_id(4);
    client->async_subscribe("s2");
    ASSERT_TRUE(pump_until([&]() { return frames(0x82).size() == 2; }));
    EXPECT_EQ(litemqtt_test::frame_packet_id(frames(0x82)[1]), 5);
    client->debug_set_next_packet_id(4);
    client->async_publish("p2", "v", 1);
    ASSERT_TRUE(pump_until([&]() { return frames(0x32).size() == 2; }));
    EXPECT_EQ(litemqtt_test::publish_packet_id(frames(0x32)[1]), 6);
}

TEST_F(MqttClientUnsubscribe, InvalidTopicsFailWithoutSendingOrClosing) {
    int calls = 0;
    for (const auto& topic : {std::string(), std::string(65536, 'a')}) {
        client->async_unsubscribe(topic, [&, topic](bool ok, std::string returned) {
            ++calls;
            EXPECT_FALSE(ok);
            EXPECT_EQ(returned, topic);
        });
    }
    EXPECT_EQ(calls, 0);
    ASSERT_TRUE(pump_until([&]() { return calls == 2; }));
    EXPECT_TRUE(frames(0xA2).empty());
    EXPECT_EQ(client->state(), connection_state::connected);
}

class MqttClientMalformedUnsuback : public MqttClientUnsubscribe,
                                   public ::testing::WithParamInterface<std::vector<uint8_t>> {};

TEST_P(MqttClientMalformedUnsuback, ClosesAndFailsPendingOnce) {
    int calls = 0;
    client->async_unsubscribe("pending", [&](bool ok, std::string) {
        ++calls;
        EXPECT_FALSE(ok);
    });
    ASSERT_TRUE(pump_until([&]() { return frames(0xA2).size() == 1; }));
    broker.reply(GetParam());
    ASSERT_TRUE(pump_until([&]() { return disconnects == 1; }));
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(client->state(), connection_state::disconnected);
}

INSTANTIATE_TEST_SUITE_P(InvalidFrames, MqttClientMalformedUnsuback, ::testing::Values(
    std::vector<uint8_t>{0xB1, 2, 0, 1},
    std::vector<uint8_t>{0xB0, 1, 1},
    std::vector<uint8_t>{0xB0, 3, 0, 1, 0},
    std::vector<uint8_t>{0xB0, 2, 0, 0}));

TEST(MqttClientUnsubscribeGuards, NoSessionFailsAsynchronouslyUsingDefaultCallback) {
    asio::io_context io;
    auto client = make_test_client(io);
    int calls = 0;
    client->on_unsubscribe([&](bool ok, std::string topic) {
        ++calls;
        EXPECT_FALSE(ok);
        EXPECT_EQ(topic, "offline");
    });
    client->async_unsubscribe("offline");
    EXPECT_EQ(calls, 0);
    io.run();
    EXPECT_EQ(calls, 1);
}

TEST(MqttClientUnsubscribeGuards, WriteFailureErasesBeforeCallbackAndFailsOnce) {
    asio::io_context io;
    auto client = make_test_client(io);
    // An accepted CONNACK without a socket deterministically exercises write failure.
    client->handle_packet({0x20, 2, 0, 0});
    int calls = 0;
    client->async_unsubscribe("write-failure", [&](bool ok, std::string topic) {
        ++calls;
        EXPECT_FALSE(ok);
        EXPECT_EQ(topic, "write-failure");
        client->debug_set_next_packet_id(1);
        EXPECT_EQ(client->acquire_packet_id_for_test(), 1);
        client->handle_packet({0xB0, 2, 0, 1});
    });
    io.run();
    EXPECT_EQ(calls, 1);
    client->handle_packet({0xB1, 2, 0, 1});
    EXPECT_EQ(calls, 1);
}

}  // namespace

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

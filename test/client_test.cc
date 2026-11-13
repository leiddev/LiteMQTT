#include <gtest/gtest.h>
#include <memory>
#include <vector>

#include <asio.hpp>

#include "litemqtt/buffer_writer.hpp"
#include "litemqtt/client.hpp"
#include "litemqtt/packets.hpp"
#include "litemqtt/packet_type.hpp"

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
    client->on_close([&closes]() { ++closes; });

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

}  // namespace

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

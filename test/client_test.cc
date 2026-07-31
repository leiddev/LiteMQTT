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

TEST(MqttClientTest, InboundPublishDedup) {
    asio::io_context io;
    auto client = make_test_client(io);

    int seen = 0;
    client->on_message([&](std::string, std::string, uint8_t, uint16_t) {
        ++seen;
    });

    client->handle_publish(build_publish_bytes(3, "t/a", "payload", 1, false));
    EXPECT_EQ(seen, 1);

    client->handle_publish(build_publish_bytes(3, "t/a", "payload", 1, true));
    EXPECT_EQ(seen, 1);

    client->handle_publish(build_publish_bytes(3, "t/a", "payload", 1, false));
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

}  // namespace

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

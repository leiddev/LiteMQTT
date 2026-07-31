#include <gtest/gtest.h>
#include <vector>
#include <utility>

#include "litemqtt/buffer_writer.hpp"
#include "litemqtt/buffer_reader.hpp"
#include "litemqtt/packets.hpp"
#include "litemqtt/packet_type.hpp"

namespace {

using namespace litemqtt;

TEST(BufferWriterTest, WriteUint8) {
    std::vector<uint8_t> buf;
    buffer_writer writer(buf);
    writer.write_uint8(0x42);
    EXPECT_EQ(buf.size(), 1);
    EXPECT_EQ(buf[0], 0x42);
}

TEST(BufferWriterTest, WriteUint16) {
    std::vector<uint8_t> buf;
    buffer_writer writer(buf);
    writer.write_uint16(0x1234);
    EXPECT_EQ(buf.size(), 2);
    EXPECT_EQ(buf[0], 0x12);
    EXPECT_EQ(buf[1], 0x34);
}

TEST(BufferWriterTest, WriteUint32) {
    std::vector<uint8_t> buf;
    buffer_writer writer(buf);
    writer.write_uint32(0x12345678);
    EXPECT_EQ(buf.size(), 4);
    EXPECT_EQ(buf[0], 0x12);
    EXPECT_EQ(buf[1], 0x34);
    EXPECT_EQ(buf[2], 0x56);
    EXPECT_EQ(buf[3], 0x78);
}

TEST(BufferWriterTest, WriteString) {
    std::vector<uint8_t> buf;
    buffer_writer writer(buf);
    writer.write_string("MQTT");
    EXPECT_EQ(buf.size(), 6);
    EXPECT_EQ(buf[0], 0x00);
    EXPECT_EQ(buf[1], 0x04);
    EXPECT_EQ(buf[2], 'M');
    EXPECT_EQ(buf[3], 'Q');
    EXPECT_EQ(buf[4], 'T');
    EXPECT_EQ(buf[5], 'T');
}

TEST(BufferWriterTest, WriteEmptyString) {
    std::vector<uint8_t> buf;
    buffer_writer writer(buf);
    writer.write_string("");
    EXPECT_EQ(buf.size(), 2);
    EXPECT_EQ(buf[0], 0x00);
    EXPECT_EQ(buf[1], 0x00);
}

TEST(BufferWriterTest, RemainingLengthBoundary) {
    std::vector<uint8_t> buf;
    std::vector<uint8_t> expected;

    buf.clear();
    expected = {0x00};
    buffer_writer(buf).write_remaining_length(0);
    EXPECT_EQ(buf, expected);

    buf.clear();
    expected = {0x7F};
    buffer_writer(buf).write_remaining_length(127);
    EXPECT_EQ(buf, expected);

    buf.clear();
    expected = {0x80, 0x01};
    buffer_writer(buf).write_remaining_length(128);
    EXPECT_EQ(buf, expected);

    buf.clear();
    expected = {0xFF, 0x7F};
    buffer_writer(buf).write_remaining_length(16383);
    EXPECT_EQ(buf, expected);

    buf.clear();
    expected = {0x80, 0x80, 0x01};
    buffer_writer(buf).write_remaining_length(16384);
    EXPECT_EQ(buf, expected);

    buf.clear();
    expected = {0x80, 0x80, 0x80, 0x01};
    buffer_writer(buf).write_remaining_length(2097152);
    EXPECT_EQ(buf, expected);

    buf.clear();
    expected = {0xFF, 0xFF, 0xFF, 0x7F};
    buffer_writer(buf).write_remaining_length(268435455);
    EXPECT_EQ(buf, expected);
}

TEST(BufferReaderTest, ReadUint8) {
    std::vector<uint8_t> buf = {0x42, 0x00};
    buffer_reader reader(buf);
    uint8_t val = 0;
    EXPECT_TRUE(reader.read_uint8(val));
    EXPECT_EQ(val, 0x42);
    EXPECT_TRUE(reader.read_uint8(val));
    EXPECT_EQ(val, 0x00);
    EXPECT_FALSE(reader.read_uint8(val));
}

TEST(BufferReaderTest, ReadUint16) {
    std::vector<uint8_t> buf = {0x12, 0x34};
    buffer_reader reader(buf);
    uint16_t val = 0;
    EXPECT_TRUE(reader.read_uint16(val));
    EXPECT_EQ(val, 0x1234);
    EXPECT_FALSE(reader.read_uint16(val));
}

TEST(BufferReaderTest, ReadUint32) {
    std::vector<uint8_t> buf = {0x12, 0x34, 0x56, 0x78};
    buffer_reader reader(buf);
    uint32_t val = 0;
    EXPECT_TRUE(reader.read_uint32(val));
    EXPECT_EQ(val, 0x12345678);
    EXPECT_FALSE(reader.read_uint32(val));
}

TEST(BufferReaderTest, ReadString) {
    std::vector<uint8_t> buf = {0x00, 0x04, 'M', 'Q', 'T', 'T'};
    buffer_reader reader(buf);
    std::string str;
    EXPECT_TRUE(reader.read_string(str));
    EXPECT_EQ(str, "MQTT");
    EXPECT_TRUE(reader.empty());
}

TEST(BufferReaderTest, ReadRemainingLength) {
    std::vector<uint8_t> buf1 = {0x00};
    {
        std::size_t v = 0;
        EXPECT_TRUE(buffer_reader(buf1).read_remaining_length(v));
        EXPECT_EQ(v, 0);
    }

    std::vector<uint8_t> buf2 = {0x7F};
    {
        std::size_t v = 0;
        EXPECT_TRUE(buffer_reader(buf2).read_remaining_length(v));
        EXPECT_EQ(v, 127);
    }

    std::vector<uint8_t> buf3 = {0x80, 0x01};
    std::size_t val3 = 0;
    buffer_reader reader3(buf3);
    EXPECT_TRUE(reader3.read_remaining_length(val3));
    EXPECT_EQ(val3, 128);
}

TEST(PacketTypeTest, ToString) {
    EXPECT_STREQ(to_string(packet_type::connect), "CONNECT");
    EXPECT_STREQ(to_string(packet_type::connack), "CONNACK");
    EXPECT_STREQ(to_string(packet_type::publish), "PUBLISH");
    EXPECT_STREQ(to_string(packet_type::subscribe), "SUBSCRIBE");
    EXPECT_STREQ(to_string(packet_type::pingreq), "PINGREQ");
    EXPECT_STREQ(to_string(packet_type::pingresp), "PINGRESP");
    EXPECT_STREQ(to_string(packet_type::disconnect), "DISCONNECT");
}

TEST(PacketTypeTest, ValidPacketType) {
    EXPECT_TRUE(is_valid_packet_type(1));
    EXPECT_TRUE(is_valid_packet_type(14));
    EXPECT_FALSE(is_valid_packet_type(0));
    EXPECT_FALSE(is_valid_packet_type(15));
    EXPECT_FALSE(is_valid_packet_type(16));
}

TEST(ConnectPacketTest, Serialize) {
    connect_packet pkt;
    pkt.client_id = "test_client";
    pkt.keep_alive_seconds = 60;
    pkt.clean_session = true;

    std::vector<uint8_t> data = pkt.serialize();

    EXPECT_GE(data.size(), 14);
    EXPECT_EQ(data[0], 0x10);

    std::size_t remaining_len = 0;
    buffer_reader reader(data);
    reader.read_uint8(data[0]);
    reader.read_remaining_length(remaining_len);
    EXPECT_EQ(remaining_len, data.size() - 2);
}

TEST(ConnectPacketTest, Parse) {
    connect_packet pkt;
    pkt.client_id = "test_client";
    pkt.keep_alive_seconds = 60;
    pkt.clean_session = true;

    std::vector<uint8_t> data = pkt.serialize();
    connect_packet parsed = connect_packet::parse(data);

    EXPECT_EQ(parsed.client_id, "test_client");
    EXPECT_EQ(parsed.keep_alive_seconds, 60);
    EXPECT_TRUE(parsed.clean_session);
}

TEST(ConnackPacketTest, SerializeAndParse) {
    connack_packet pkt;
    pkt.session_present = 0;
    pkt.return_code = 0;

    std::vector<uint8_t> data = pkt.serialize();
    EXPECT_EQ(data.size(), 4);
    EXPECT_EQ(data[0], 0x20);
    EXPECT_EQ(data[1], 0x02);
    EXPECT_EQ(data[2], 0x00);
    EXPECT_EQ(data[3], 0x00);

    connack_packet parsed = connack_packet::parse(data);
    EXPECT_EQ(parsed.session_present, 0);
    EXPECT_EQ(parsed.return_code, 0);
}

TEST(PublishPacketTest, SerializeAndParse) {
    publish_packet pkt;
    pkt.packet_id = 1;
    pkt.qos = 1;
    pkt.topic_name = "test/topic";
    pkt.payload = "Hello, MQTT!";

    std::vector<uint8_t> data = pkt.serialize();
    EXPECT_GE(data.size(), 16);

    publish_packet parsed = publish_packet::parse(data);
    EXPECT_EQ(parsed.packet_id, 1);
    EXPECT_EQ(parsed.qos, 1);
    EXPECT_EQ(parsed.topic_name, "test/topic");
    EXPECT_EQ(parsed.payload, "Hello, MQTT!");
}

TEST(PublishPacketTest, QoS0NoPacketId) {
    publish_packet pkt;
    pkt.qos = 0;
    pkt.topic_name = "test/topic";
    pkt.payload = "Hello";

    std::vector<uint8_t> data = pkt.serialize();
    EXPECT_GE(data.size(), 7);

    publish_packet parsed = publish_packet::parse(data);
    EXPECT_EQ(parsed.qos, 0);
    EXPECT_EQ(parsed.topic_name, "test/topic");
    EXPECT_EQ(parsed.payload, "Hello");
}

TEST(PublishPacketTest, DupBitRoundtrip) {
    publish_packet pkt;
    pkt.packet_id = 7;
    pkt.qos = 1;
    pkt.dup = true;
    pkt.topic_name = "test/topic";
    pkt.payload = "retry";

    std::vector<uint8_t> data = pkt.serialize();
    EXPECT_NE(data.empty(), true);

    publish_packet parsed = publish_packet::parse(data);
    EXPECT_TRUE(parsed.dup);
    EXPECT_EQ(parsed.qos, 1);
    EXPECT_EQ(parsed.packet_id, 7);
}

TEST(PublishPacketTest, DupBitClearedByDefault) {
    publish_packet pkt;
    pkt.packet_id = 1;
    pkt.qos = 1;
    pkt.topic_name = "test/topic";
    pkt.payload = "first";

    std::vector<uint8_t> data = pkt.serialize();
    publish_packet parsed = publish_packet::parse(data);
    EXPECT_FALSE(parsed.dup);
}

TEST(SubscribePacketTest, Serialize) {
    subscribe_packet pkt;
    pkt.packet_id = 1;
    pkt.topic_filters.push_back(std::make_pair("test/topic", 0));

    std::vector<uint8_t> data = pkt.serialize();
    EXPECT_GE(data.size(), 5);
    EXPECT_EQ(data[0], 0x82);
}

TEST(PingreqPacketTest, SerializeAndParse) {
    pingreq_packet pkt;

    std::vector<uint8_t> data = pkt.serialize();
    EXPECT_EQ(data.size(), 2);
    EXPECT_EQ(data[0], 0xC0);
    EXPECT_EQ(data[1], 0x00);

    pingreq_packet parsed = pingreq_packet::parse(data);
}

TEST(PingrespPacketTest, SerializeAndParse) {
    pingresp_packet pkt;

    std::vector<uint8_t> data = pkt.serialize();
    EXPECT_EQ(data.size(), 2);
    EXPECT_EQ(data[0], 0xD0);
    EXPECT_EQ(data[1], 0x00);

    pingresp_packet parsed = pingresp_packet::parse(data);
}

TEST(DisconnectPacketTest, SerializeAndParse) {
    disconnect_packet pkt;

    std::vector<uint8_t> data = pkt.serialize();
    EXPECT_EQ(data.size(), 2);
    EXPECT_EQ(data[0], 0xE0);
    EXPECT_EQ(data[1], 0x00);

    disconnect_packet parsed = disconnect_packet::parse(data);
}

}  // namespace

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

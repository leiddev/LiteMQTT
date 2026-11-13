// Parser robustness: a truncated, padded or wrongly typed packet must be
// rejected rather than yield a half-filled struct.
//
// Before this was enforced, every parse() ignored the return value of the
// buffer_reader calls and never compared the declared remaining length against
// the bytes actually present.  A two-byte CONNACK therefore "parsed" into
// return_code 0, which the client read as a successful connect.
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "litemqtt/packets.hpp"
#include "litemqtt/packet_type.hpp"

namespace {

using namespace litemqtt;

std::vector<uint8_t> bytes(std::initializer_list<int> values) {
    std::vector<uint8_t> out;
    for (int v : values) {
        out.push_back(static_cast<uint8_t>(v));
    }
    return out;
}

/// Serializes @p pkt, then hands it to parse() and reports whether that
/// succeeded.  A well-formed packet must round-trip.
template <typename Packet>
bool round_trips(const Packet& pkt) {
    Packet parsed;
    return Packet::parse(pkt.serialize(), parsed);
}

}  // namespace

// --- CONNACK ---------------------------------------------------------------

TEST(ParseTruncated, ConnackEmpty) {
    connack_packet pkt;
    EXPECT_FALSE(connack_packet::parse({}, pkt));
}

TEST(ParseTruncated, ConnackFixedHeaderOnly) {
    connack_packet pkt;
    EXPECT_FALSE(connack_packet::parse(bytes({0x20}), pkt));
    EXPECT_FALSE(connack_packet::parse(bytes({0x20, 0x02}), pkt));
}

TEST(ParseTruncated, ConnackBodyShorterThanDeclared) {
    connack_packet pkt;
    // Declares a 5-byte body, carries 2.
    EXPECT_FALSE(connack_packet::parse(bytes({0x20, 0x05, 0x00, 0x00}), pkt));
}

TEST(ParseTruncated, ConnackBodyLongerThanDeclared) {
    connack_packet pkt;
    // Declares 2, carries 3.
    EXPECT_FALSE(connack_packet::parse(bytes({0x20, 0x02, 0x00, 0x00, 0x00}), pkt));
}

TEST(ParseTruncated, ConnackAcceptsAWellFormedPacket) {
    connack_packet in;
    in.session_present = 1;
    in.return_code = 5;
    connack_packet out;
    ASSERT_TRUE(connack_packet::parse(in.serialize(), out));
    EXPECT_EQ(out.session_present, 1);
    EXPECT_EQ(out.return_code, 5);
}

TEST(ParseTruncated, WrongPacketTypeIsRejected) {
    // A PUBLISH body fed to the CONNACK parser.
    connack_packet pkt;
    EXPECT_FALSE(connack_packet::parse(bytes({0x30, 0x02, 0x00, 0x00}), pkt));
}

// --- PUBLISH ---------------------------------------------------------------

TEST(ParseTruncated, PublishNoTopicLength) {
    publish_packet pkt;
    EXPECT_FALSE(publish_packet::parse(bytes({0x30, 0x01, 0x00}), pkt));
}

TEST(ParseTruncated, PublishTopicLengthOverrunsBuffer) {
    publish_packet pkt;
    // Topic claims 255 bytes, one is present.
    EXPECT_FALSE(publish_packet::parse(bytes({0x30, 0x05, 0x00, 0xFF, 'a'}), pkt));
}

TEST(ParseTruncated, PublishQos1WithoutPacketId) {
    publish_packet pkt;
    // QoS 1, topic "a", then nothing where the packet id should be.
    EXPECT_FALSE(publish_packet::parse(bytes({0x32, 0x03, 0x00, 0x01, 'a'}), pkt));
}

TEST(ParseTruncated, PublishPayloadTruncated) {
    publish_packet pkt;
    // Declares a 10-byte body, carries 5.
    std::vector<uint8_t> data = {0x30, 0x0A, 0x00, 0x01, 'a', 'b', 'c'};
    EXPECT_FALSE(publish_packet::parse(data, pkt));
}

TEST(ParseTruncated, PublishQos3IsRejected) {
    publish_packet pkt;
    // QoS 3 is reserved and cannot be encoded legally.
    EXPECT_FALSE(publish_packet::parse(bytes({0x36, 0x04, 0x00, 0x01, 'a', 0x00, 0x01}), pkt));
}

TEST(ParseTruncated, PublishAcceptsAWellFormedPacket) {
    publish_packet in;
    in.qos = 1;
    in.packet_id = 7;
    in.topic_name = "a/b";
    in.payload = "hello";
    publish_packet out;
    ASSERT_TRUE(publish_packet::parse(in.serialize(), out));
    EXPECT_EQ(out.qos, 1);
    EXPECT_EQ(out.packet_id, 7);
    EXPECT_EQ(out.topic_name, "a/b");
    EXPECT_EQ(out.payload, "hello");
}

TEST(ParseTruncated, PublishQos0WithEmptyPayload) {
    publish_packet in;
    in.qos = 0;
    in.topic_name = "t";
    publish_packet out;
    ASSERT_TRUE(publish_packet::parse(in.serialize(), out));
    EXPECT_EQ(out.topic_name, "t");
    EXPECT_TRUE(out.payload.empty());
}

// --- SUBACK ----------------------------------------------------------------

TEST(ParseTruncated, SubackNoPacketId) {
    suback_packet pkt;
    EXPECT_FALSE(suback_packet::parse(bytes({0x90, 0x01, 0x00}), pkt));
}

TEST(ParseTruncated, SubackEmptyReturnCodesIsAccepted) {
    // Legal on the wire; whether it is useful is the caller's problem.
    suback_packet pkt;
    EXPECT_TRUE(suback_packet::parse(bytes({0x90, 0x02, 0x00, 0x01}), pkt));
    EXPECT_TRUE(pkt.return_codes.empty());
    EXPECT_EQ(pkt.packet_id, 1);
}

TEST(ParseTruncated, SubackAcceptsAWellFormedPacket) {
    suback_packet in;
    in.packet_id = 9;
    in.return_codes = {0x00, 0x01, 0x80};
    suback_packet out;
    ASSERT_TRUE(suback_packet::parse(in.serialize(), out));
    EXPECT_EQ(out.packet_id, 9);
    EXPECT_EQ(out.return_codes, in.return_codes);
}

// --- the fixed-size acks ---------------------------------------------------

TEST(ParseTruncated, PubackShort) {
    puback_packet pkt;
    EXPECT_FALSE(puback_packet::parse(bytes({0x40, 0x02, 0x00}), pkt));
    EXPECT_FALSE(puback_packet::parse(bytes({0x40}), pkt));
}

TEST(ParseTruncated, PubrecPubrelPubcompShort) {
    pubrec_packet rec;
    pubrel_packet rel;
    pubcomp_packet comp;
    EXPECT_FALSE(pubrec_packet::parse(bytes({0x50, 0x02, 0x00}), rec));
    EXPECT_FALSE(pubrel_packet::parse(bytes({0x62, 0x02, 0x00}), rel));
    EXPECT_FALSE(pubcomp_packet::parse(bytes({0x70, 0x02, 0x00}), comp));
}

TEST(ParseTruncated, FixedSizeAcksRoundTrip) {
    puback_packet ack;
    ack.packet_id = 4242;
    puback_packet ack_out;
    ASSERT_TRUE(puback_packet::parse(ack.serialize(), ack_out));
    EXPECT_EQ(ack_out.packet_id, 4242);

    pubrec_packet rec;
    rec.packet_id = 4243;
    pubrec_packet rec_out;
    ASSERT_TRUE(pubrec_packet::parse(rec.serialize(), rec_out));
    EXPECT_EQ(rec_out.packet_id, 4243);
}

TEST(ParseTruncated, UnsubackRoundTrips) {
    unsuback_packet in;
    in.packet_id = 12;
    unsuback_packet out;
    ASSERT_TRUE(unsuback_packet::parse(in.serialize(), out));
    EXPECT_EQ(out.packet_id, 12);
}

// --- CONNECT ---------------------------------------------------------------

TEST(ParseTruncated, ConnectEmpty) {
    connect_packet pkt;
    EXPECT_FALSE(connect_packet::parse({}, pkt));
}

TEST(ParseTruncated, ConnectTruncatedVariableHeader) {
    // Fixed header plus a cut-off protocol name.
    connect_packet pkt;
    EXPECT_FALSE(connect_packet::parse(bytes({0x10, 0x04, 0x00, 0x04, 'M', 'Q'}), pkt));
}

TEST(ParseTruncated, ConnectWrongProtocolName) {
    connect_packet in;
    in.client_id = "c";
    std::vector<uint8_t> data = in.serialize();
    data[2] = 'X';  // first byte of the protocol name
    connect_packet out;
    EXPECT_FALSE(connect_packet::parse(data, out));
}

TEST(ParseTruncated, ConnectWrongProtocolLevel) {
    connect_packet in;
    in.client_id = "c";
    std::vector<uint8_t> data = in.serialize();
    data[6] = 3;  // protocol level, MQTT 3.1 rather than 3.1.1
    connect_packet out;
    EXPECT_FALSE(connect_packet::parse(data, out));
}

TEST(ParseTruncated, ConnectPasswordWithoutUsernameIsRejected) {
    // MQTT 3.1.1 section 3.1.2.9: password flag set requires the username flag.
    // serialize() cannot produce this combination, so the bytes are built by
    // hand: CONNECT, body 16, "MQTT", level 4, flags 0x42 (clean + password
    // only), keepalive 0, client id "c", password "p".
    std::vector<uint8_t> data = bytes({0x10, 0x10, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04,
                                       0x42, 0x00, 0x00, 0x00, 0x01, 'c', 0x00, 0x01, 'p'});
    connect_packet pkt;
    EXPECT_FALSE(connect_packet::parse(data, pkt));
}

TEST(ParseTruncated, ConnectWithUsernameAndPasswordRoundTrips) {
    connect_packet in;
    in.client_id = "client";
    in.keep_alive_seconds = 30;
    in.clean_session = true;
    in.username = "user";
    in.password = "pass";
    connect_packet out;
    ASSERT_TRUE(connect_packet::parse(in.serialize(), out));
    EXPECT_EQ(out.client_id, "client");
    EXPECT_EQ(out.keep_alive_seconds, 30);
    EXPECT_TRUE(out.clean_session);
    EXPECT_EQ(out.username, "user");
    EXPECT_EQ(out.password, "pass");
}

TEST(ParseTruncated, ConnectWithUsernameOnlyLeavesPasswordEmpty) {
    connect_packet in;
    in.client_id = "client";
    in.username = "user";
    connect_packet out;
    ASSERT_TRUE(connect_packet::parse(in.serialize(), out));
    EXPECT_EQ(out.username, "user");
    EXPECT_TRUE(out.password.empty());
}

TEST(ParseTruncated, ConnectMinimalRoundTrips) {
    connect_packet in;
    in.client_id = "";
    connect_packet out;
    ASSERT_TRUE(connect_packet::parse(in.serialize(), out));
    EXPECT_TRUE(out.client_id.empty());
    EXPECT_FALSE(out.clean_session);
}

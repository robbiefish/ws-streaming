// Tests for the WebSocket Streaming Protocol packet header.
//
// The header is a little-endian word on either transport: the direct TCP protocol differs from
// the WebSocket protocol only in that packets are not wrapped in WebSocket frames.

#include <cstddef>
#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

#include <ws-streaming/detail/streaming_protocol.hpp>

using namespace testing;
using namespace wss::detail::streaming_protocol;

namespace
{
    // Builds a packet with a generated header and a payload of the given size, so that
    // decode_header() sees a complete packet rather than a truncated one.
    std::vector<std::uint8_t> make_packet(unsigned signo, unsigned type, std::size_t payload_size)
    {
        std::vector<std::uint8_t> packet(MAX_HEADER_SIZE + payload_size);

        std::size_t header_size = generate_header(packet.data(), signo, type, payload_size);
        packet.resize(header_size + payload_size);

        return packet;
    }

    decoded_header round_trip(unsigned signo, unsigned type, std::size_t payload_size)
    {
        auto packet = make_packet(signo, type, payload_size);
        return decode_header(packet.data(), packet.size());
    }

    // The header bytes a packet begins with, which is what the wire format tests compare.
    std::vector<std::uint8_t> header_bytes(unsigned signo, unsigned type, std::size_t payload_size)
    {
        std::vector<std::uint8_t> header(MAX_HEADER_SIZE);
        header.resize(generate_header(header.data(), signo, type, payload_size));
        return header;
    }
}

TEST(StreamingProtocolHeader, RoundTripsAPacketWithAnInlineSize)
{
    auto header = round_trip(7, packet_type::DATA, 24);

    EXPECT_EQ(header.header_size, sizeof(std::uint32_t));
    EXPECT_EQ(header.signo, 7u);
    EXPECT_EQ(header.type, packet_type::DATA);
    EXPECT_EQ(header.payload_size, 24u);
}

TEST(StreamingProtocolHeader, RoundTripsAPacketWithAnExtendedSize)
{
    auto header = round_trip(7, packet_type::DATA, 800);

    EXPECT_EQ(header.header_size, 2 * sizeof(std::uint32_t));
    EXPECT_EQ(header.signo, 7u);
    EXPECT_EQ(header.type, packet_type::DATA);
    EXPECT_EQ(header.payload_size, 800u);
}

TEST(StreamingProtocolHeader, RoundTripsMetadataPackets)
{
    auto header = round_trip(0, packet_type::METADATA, 300);

    EXPECT_EQ(header.signo, 0u);
    EXPECT_EQ(header.type, packet_type::METADATA);
    EXPECT_EQ(header.payload_size, 300u);
}

// 255 is the largest payload the inline size field can hold, and 256 is the first that needs the
// additional length word. Both sides have to agree on where that step is.
TEST(StreamingProtocolHeader, RoundTripsAcrossTheInlineSizeBoundary)
{
    for (std::size_t payload_size : {std::size_t{0}, std::size_t{1}, std::size_t{254},
        std::size_t{255}, std::size_t{256}, std::size_t{257}})
    {
        auto header = round_trip(1, packet_type::DATA, payload_size);

        EXPECT_EQ(header.payload_size, payload_size) << "payload_size = " << payload_size;
        EXPECT_EQ(header.signo, 1u) << "payload_size = " << payload_size;
        EXPECT_EQ(header.type, packet_type::DATA) << "payload_size = " << payload_size;
    }
}

// The signal number occupies the low 20 bits of the header word, so it must survive right up to
// its maximum without spilling into the size or type fields.
TEST(StreamingProtocolHeader, RoundTripsTheLargestSignalNumber)
{
    constexpr unsigned MAX_SIGNO = 0xFFFFFu;

    auto header = round_trip(MAX_SIGNO, packet_type::DATA, 24);

    EXPECT_EQ(header.signo, MAX_SIGNO);
    EXPECT_EQ(header.type, packet_type::DATA);
    EXPECT_EQ(header.payload_size, 24u);
}

// The reference implementation emits the header word in host order on a little-endian machine.
TEST(StreamingProtocolHeader, MatchesTheReferenceWireFormat)
{
    EXPECT_EQ(
        header_bytes(7, packet_type::DATA, 24),
        (std::vector<std::uint8_t>{0x07, 0x00, 0x80, 0x11}));
}

TEST(StreamingProtocolHeader, MatchesTheReferenceWireFormatWithAnExtendedSize)
{
    EXPECT_EQ(
        header_bytes(7, packet_type::DATA, 800),
        (std::vector<std::uint8_t>{0x07, 0x00, 0x00, 0x10, 0x20, 0x03, 0x00, 0x00}));
}

TEST(StreamingProtocolHeader, ReportsTruncatedHeadersAsIncomplete)
{
    auto packet = make_packet(7, packet_type::DATA, 800);

    // One byte short of the first word, of the additional length word, and of the payload.
    for (std::size_t size : {std::size_t{3}, std::size_t{7}, packet.size() - 1})
        EXPECT_EQ(decode_header(packet.data(), size).header_size, 0u) << "size = " << size;
}

TEST(StreamingProtocolHeader, ReportsACompletePacketAsComplete)
{
    auto packet = make_packet(7, packet_type::DATA, 800);

    EXPECT_NE(decode_header(packet.data(), packet.size()).header_size, 0u);

    // Trailing data belonging to the next packet must not disturb the decoding of this one.
    EXPECT_EQ(
        decode_header(packet.data(), packet.size() + 100).payload_size,
        800u);
}

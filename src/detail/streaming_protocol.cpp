#include <cstddef>
#include <cstdint>

#include <ws-streaming/detail/streaming_protocol.hpp>

wss::detail::streaming_protocol::decoded_header
wss::detail::streaming_protocol::decode_header(
    const std::uint8_t *data,
    std::size_t size) noexcept
{
    decoded_header header { };
    const std::uint8_t *data_begin = data;

    if (size < sizeof(std::uint32_t))
        return header;

    // The header is a little-endian 32-bit word carrying the signal number in its low 20 bits,
    // the payload size in the next 8 and the packet type above those. The byte order does not
    // depend on the transport: the reference implementation writes the word in host order and
    // both ends of this protocol are little-endian, which is what generate_header() emits.
    header.type = data[3] >> 4;
    header.signo = ((data[2] & 0xFu) << 16) | (data[1] << 8) | data[0];
    header.payload_size = ((data[3] & 0xFu) << 4) | (data[2] >> 4);

    data += sizeof(std::uint32_t);
    size -= sizeof(std::uint32_t);

    // A payload too large for the size field leaves that field zero and is carried in a second
    // word instead.
    if (header.payload_size == 0)
    {
        if (size < sizeof(std::uint32_t))
            return header;

        header.payload_size = (data[3] << 24) | (data[2] << 16) | (data[1] << 8) | data[0];

        data += sizeof(std::uint32_t);
        size -= sizeof(std::uint32_t);
    }

    if (size >= header.payload_size)
        header.header_size = data - data_begin;

    return header;
}
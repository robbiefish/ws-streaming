#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/post.hpp>
#include <boost/system/error_code.hpp>

#include <nlohmann/json.hpp>

#include <ws-streaming/detail/base_peer.hpp>
#include <ws-streaming/detail/streaming_protocol.hpp>
#include <ws-streaming/detail/websocket_protocol.hpp>

wss::detail::base_peer::base_peer(bool use_tcp_protocol, std::size_t rx_buffer_size)
    : _use_tcp_protocol(use_tcp_protocol)
    , _rx_buffer(std::min(INITIAL_RX_BUFFER_SIZE, rx_buffer_size))
    , _rx_buffer_max(rx_buffer_size)
{
}

bool wss::detail::base_peer::grow_rx_buffer()
{
    if (_rx_buffer.size() >= _rx_buffer_max)
        return false;

    // Doubling reaches the maximum in a handful of steps, and the buffer is never shrunk again,
    // so a connection reallocates only a few times however long it lives.
    _rx_buffer.resize(std::min(_rx_buffer.size() * 2, _rx_buffer_max));

    return true;
}

void wss::detail::base_peer::run()
{
    do_read();
}

void wss::detail::base_peer::run(const void *data, std::size_t size)
{
    // Make room for the handed-over data, which arrives before any read has happened.
    while (size > _rx_buffer.size() && grow_rx_buffer())
    {
        // do nothing
    }

    // The data cannot be stored, so there is nothing to process: returning here is what keeps the
    // copy below from running past the end of the buffer.
    if (size > _rx_buffer.size())
    {
        boost::asio::post(socket().get_executor(),
                          [self_weak = weak_from_this()]()
                          {
                              if (auto self = self_weak.lock())
                                  self->close(boost::asio::error::no_buffer_space);
                          });

        return;
    }

    std::memcpy(_rx_buffer.data(), data, size);
    _rx_buffer_bytes = size;

    boost::asio::post(socket().get_executor(),
                      [self_weak = weak_from_this()]()
                      {
                          if (auto self = self_weak.lock())
                              self->process_buffer();
                      });
}

void wss::detail::base_peer::stop()
{
    boost::system::error_code ec;
    socket().close(ec);
}

void wss::detail::base_peer::process_buffer()
{
    if (_use_tcp_protocol)
        process_buffer_tcp();
    else
        process_buffer_ws();
}

void wss::detail::base_peer::process_buffer_tcp()
{
    std::size_t offset = 0;

    // Process as many streaming protocol packets as possible.
    while (true)
    {
        std::size_t bytes_consumed = process_packet(
            _rx_buffer.data() + offset,
            _rx_buffer_bytes - offset);

        // If there's not enough data to form a complete packet, we can't process any more.
        if (!bytes_consumed)
            break;

        // Consume the handled packet by leaving it behind: nothing is moved while the loop runs.
        offset += bytes_consumed;
    }

    // The loop only stops once what remains is an incomplete packet, so at most one packet is
    // left. Slide it to the front so that the next read has the rest of the buffer to fill.
    // (Can't use std::memcpy() for this because the ranges overlap.)
    if (offset)
    {
        _rx_buffer_bytes -= offset;

        std::memmove(
            _rx_buffer.data(),
            _rx_buffer.data() + offset,
            _rx_buffer_bytes);
    }

    // Nothing could be parsed and the buffer is full, so the frame being received needs more room
    // than the buffer currently has. Grow it, or give up once it has reached its maximum size:
    // that means the remote peer is sending a frame we have no way of holding.
    if (_rx_buffer_bytes == _rx_buffer.size() && !grow_rx_buffer())
        return close(boost::asio::error::no_buffer_space);

    do_read();
}

void wss::detail::base_peer::process_buffer_ws()
{
    std::size_t offset = 0;

    // Process as many WebSocket frames as possible.
    while (true)
    {
        // Try to decode the WebSocket header.
        auto header = detail::websocket_protocol::decode_header(
            _rx_buffer.data() + offset,
            _rx_buffer_bytes - offset);

        // If there's not enough data to form a complete frame, we can't process any more.
        if (!header.header_size)
            break;

        boost::system::error_code process_ec;
        process_websocket_frame(
            header,
            _rx_buffer.data() + offset + header.header_size,
            header.payload_size,
            process_ec);

        if (process_ec)
            return close(process_ec);

        // Consume the handled frame by leaving it behind: nothing is moved while the loop runs.
        offset += header.header_size + header.payload_size;
    }

    // The loop only stops once what remains is an incomplete frame, so at most one frame is left.
    // Slide it to the front so that the next read has the rest of the buffer to fill.
    // (Can't use std::memcpy() for this because the ranges overlap.)
    if (offset)
    {
        _rx_buffer_bytes -= offset;

        std::memmove(
            _rx_buffer.data(),
            _rx_buffer.data() + offset,
            _rx_buffer_bytes);
    }

    // Nothing could be parsed and the buffer is full, so the frame being received needs more room
    // than the buffer currently has. Grow it, or give up once it has reached its maximum size:
    // that means the remote peer is sending a frame we have no way of holding.
    if (_rx_buffer_bytes == _rx_buffer.size() && !grow_rx_buffer())
        return close(boost::asio::error::no_buffer_space);

    do_read();
}

void wss::detail::base_peer::process_websocket_frame(
    const detail::websocket_protocol::decoded_header& header,
    std::uint8_t *data,
    std::size_t size,
    boost::system::error_code& ec)
{
    if (!(header.flags & detail::websocket_protocol::flags::FIN))
    {
        ec = boost::asio::error::operation_not_supported;
        return;
    }

    if (header.is_masked)
    {
        for (std::size_t i = 0; i < size; ++i)
            data[i] ^= header.masking_key[i % 4];
    }

    // We have a valid and complete WebSocket frame.
    switch (header.opcode)
    {
        // React to close frames by sending our own close frame and then signaling the caller to disconnect.
        case detail::websocket_protocol::opcodes::CLOSE:
        {
            send_control_frame(
                detail::websocket_protocol::opcodes::CLOSE,
                boost::asio::const_buffer(),
                0,
                true);
            break;
        }

        case detail::websocket_protocol::opcodes::PING:
        {
            send_control_frame(
                detail::websocket_protocol::opcodes::PONG,
                boost::asio::const_buffer(data, size),
                size,
                false);
            break;
        }

        case detail::websocket_protocol::opcodes::TEXT:
            break;

        case detail::websocket_protocol::opcodes::BINARY:
        {
            std::size_t offset = 0;
            std::size_t bytes_consumed = 0;
            do
            {
                bytes_consumed = process_packet(data + offset, size - offset);
                offset += bytes_consumed;
            } while (bytes_consumed != 0);
            break;
        }

        // React to any other frames by ignoring them.
        default:
            break;
    }
}

std::size_t wss::detail::base_peer::process_packet(
    const std::uint8_t *data,
    std::size_t size)
{
    // Try to decode the WebSocket Streaming Protocol packet.
    auto header = detail::streaming_protocol::decode_header(data, size);
    if (!header.header_size)
        return 0;

    switch (header.type)
    {
        case detail::streaming_protocol::packet_type::DATA:
            process_data_packet(
                header.signo,
                data + header.header_size,
                header.payload_size);
            break;

        case detail::streaming_protocol::packet_type::METADATA:
            process_metadata_packet(
                header.signo,
                data + header.header_size,
                header.payload_size);
            break;

        default:
            break;
    }

    return header.header_size + header.payload_size;
}

void wss::detail::base_peer::process_data_packet(
    unsigned signo,
    const std::uint8_t *data,
    std::size_t size)
{
    on_data_received(signo, data, size);
}

void wss::detail::base_peer::process_metadata_packet(
    unsigned signo,
    const std::uint8_t *data,
    std::size_t size)
{
    if (size < sizeof(std::uint32_t))
        return;

    std::uint32_t encoding =
        _use_tcp_protocol
            ? (data[3] | (data[2] << 8) | (data[1] << 16) | (data[0] << 24))
            : (data[0] | (data[1] << 8) | (data[2] << 16) | (data[3] << 24));

    nlohmann::json metadata;

    try
    {
        switch (encoding)
        {
            case detail::streaming_protocol::metadata_encoding::JSON:
                metadata = nlohmann::json::parse(
                    data + sizeof(encoding),
                    data + size);
                break;

            case detail::streaming_protocol::metadata_encoding::MSGPACK:
                metadata = nlohmann::json::from_msgpack(
                    data + sizeof(encoding),
                    data + size);
                break;

            default:
                break;
        }
    }

    catch (const nlohmann::json::exception&)
    {
    }

    if (metadata.is_object()
            && metadata.contains("method")
            && metadata["method"].is_string())
        on_metadata_received(
            signo,
            metadata["method"],
            metadata.contains("params")
                ? metadata["params"]
                : nlohmann::json{nullptr});
}

void wss::detail::base_peer::close(
    const boost::system::error_code& ec)
{
    if (_is_closed)
        return;

    _is_closed = true;

    shutdown_and_close();

    on_closed(ec);
}

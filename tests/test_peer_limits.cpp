// Tests for the receive-buffer limits of wss::detail::peer.
//
// The peer's receive buffer starts small and grows on demand up to the maximum given to the
// constructor. Two things follow from that and are covered here: a frame larger than the initial
// size must still be received, because the buffer grows to hold it, and a frame larger than the
// maximum must disconnect the peer with no_buffer_space, because it never can be held. The
// handover path, which fills the buffer before any read has happened, has the same two outcomes.
//
// Each test drives one real peer over a loopback socket. The far end is a plain socket that the
// test writes hand-built bytes to, rather than a second peer, so that a frame can claim a payload
// size the test has no intention of ever sending. Everything runs on one single-threaded
// io_context on the test's own thread, which is the implicit strand peer requires and makes the
// ordering of reads, writes and timers deterministic.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/write.hpp>
#include <boost/system/error_code.hpp>

#include <ws-streaming/detail/peer.hpp>
#include <ws-streaming/detail/streaming_protocol.hpp>
#include <ws-streaming/detail/websocket_protocol.hpp>

using namespace testing;
using namespace wss::detail;
using namespace std::chrono_literals;

namespace
{
    // The signal number used for all test packets. Any nonzero value works.
    constexpr unsigned SIGNO = 7;

    // The size peer's receive buffer is allocated at before it grows. It is a private constant of
    // the class, repeated here so that the tests can straddle it: the payload sizes below are
    // chosen to sit above it, so that receiving them at all requires the buffer to have grown.
    constexpr std::size_t INITIAL_RX_BUFFER_SIZE = 64 * 1024;

    // The receive buffer maximum used by most tests, two growth steps above the initial size.
    constexpr std::size_t RX_BUFFER_MAX = 128 * 1024;

    // A payload that fits within RX_BUFFER_MAX but not within INITIAL_RX_BUFFER_SIZE.
    constexpr std::size_t LARGE_PAYLOAD = 100 * 1024;

    // The payload size claimed by frames that are meant to be rejected, and the number of bytes of
    // it the test actually sends. Only enough to fill the buffer to its maximum is needed: the
    // peer gives up as soon as it holds a full buffer it cannot parse.
    constexpr std::size_t OVERSIZED_PAYLOAD = 1024 * 1024;
    constexpr std::size_t OVERSIZED_PREFIX = 192 * 1024;

    // How long a test waits for the peer to reach its expected state before giving up.
    constexpr auto WATCHDOG = 5s;

    std::vector<std::uint8_t> pattern(std::size_t size)
    {
        std::vector<std::uint8_t> result(size);
        for (std::size_t i = 0; i < size; ++i)
            result[i] = static_cast<std::uint8_t>(i * 31 + 7);
        return result;
    }

    void append_packet_header(
        std::vector<std::uint8_t>& out,
        unsigned signo,
        unsigned type,
        std::size_t payload_size)
    {
        // Payloads of 256 bytes and up do not fit the header's inline size field, and are instead
        // carried in a second word with the inline field left at zero.
        bool extended = payload_size >= 256;

        auto append_word = [&out](std::uint32_t word)
        {
            for (unsigned i = 0; i < 4; ++i)
                out.push_back(static_cast<std::uint8_t>(word >> (8 * i)));
        };

        append_word(
            signo
                | (extended ? 0u : static_cast<std::uint32_t>(payload_size) << 20)
                | (type << 28));

        if (extended)
            append_word(static_cast<std::uint32_t>(payload_size));
    }

    std::vector<std::uint8_t> make_packet(
        unsigned signo,
        const std::vector<std::uint8_t>& payload)
    {
        std::vector<std::uint8_t> packet;

        append_packet_header(
            packet,
            signo,
            streaming_protocol::packet_type::DATA,
            payload.size());

        packet.insert(packet.end(), payload.begin(), payload.end());

        return packet;
    }

    // Builds the beginning of a packet whose header claims a payload of claimed_payload_size but
    // which is followed by only actual_payload_size bytes. The peer can never complete such a
    // packet, which is the point: it keeps buffering until it runs out of room.
    std::vector<std::uint8_t> make_truncated_packet(
        unsigned signo,
        std::size_t claimed_payload_size,
        std::size_t actual_payload_size)
    {
        std::vector<std::uint8_t> packet;

        append_packet_header(
            packet,
            signo,
            streaming_protocol::packet_type::DATA,
            claimed_payload_size);

        auto payload = pattern(actual_payload_size);
        packet.insert(packet.end(), payload.begin(), payload.end());

        return packet;
    }

    // Wraps data in an unmasked binary WebSocket frame. Masking is optional as far as this
    // implementation is concerned, and leaving it off keeps the payload readable in a hex dump.
    std::vector<std::uint8_t> make_ws_frame(const std::vector<std::uint8_t>& payload)
    {
        std::uint8_t header[websocket_protocol::MAX_HEADER_SIZE];

        std::size_t header_size = websocket_protocol::generate_header(
            header,
            websocket_protocol::opcodes::BINARY,
            websocket_protocol::flags::FIN,
            payload.size());

        std::vector<std::uint8_t> frame{header, header + header_size};
        frame.insert(frame.end(), payload.begin(), payload.end());

        return frame;
    }

    // The WebSocket counterpart of make_truncated_packet(): a frame header claiming a payload the
    // test has no intention of finishing.
    std::vector<std::uint8_t> make_truncated_ws_frame(
        std::size_t claimed_payload_size,
        std::size_t actual_payload_size)
    {
        std::uint8_t header[websocket_protocol::MAX_HEADER_SIZE];

        std::size_t header_size = websocket_protocol::generate_header(
            header,
            websocket_protocol::opcodes::BINARY,
            websocket_protocol::flags::FIN,
            claimed_payload_size);

        std::vector<std::uint8_t> frame{header, header + header_size};

        auto payload = pattern(actual_payload_size);
        frame.insert(frame.end(), payload.begin(), payload.end());

        return frame;
    }

    struct received_packet
    {
        unsigned signo;
        std::vector<std::uint8_t> payload;
    };

    class PeerBufferLimits : public Test
    {
        protected:

            static constexpr bool TCP = true;
            static constexpr bool WS = false;

            // Creates a connected socket pair on the loopback interface and puts a peer on one
            // end of it. The peer is not started: tests call run() themselves, because the
            // handover tests need the other overload.
            void connect(bool use_tcp_protocol, std::size_t rx_buffer_size = RX_BUFFER_MAX)
            {
                boost::asio::ip::tcp::acceptor acceptor{
                    ioc,
                    boost::asio::ip::tcp::endpoint{
                        boost::asio::ip::make_address("127.0.0.1"),
                        0}};

                boost::asio::ip::tcp::socket subject_socket{ioc};

                remote.connect(acceptor.local_endpoint());
                acceptor.accept(subject_socket);

                subject = std::make_shared<peer>(
                    std::move(subject_socket),
                    false,
                    use_tcp_protocol,
                    rx_buffer_size,
                    1024 * 1024);

                subject->on_data_received.connect(
                    [this](unsigned signo, const std::uint8_t *data, std::size_t size)
                    {
                        received.push_back(received_packet{signo, {data, data + size}});

                        if (stop_after && received.size() >= stop_after)
                            subject->stop();
                    });

                subject->on_closed.connect(
                    [this](const boost::system::error_code& ec)
                    {
                        close_ec = ec;
                        finish();
                    });
            }

            // Queues data to be written to the peer from the far end. The bytes are kept alive in
            // a deque, whose references remain valid as further writes are queued behind them.
            void send(std::vector<std::uint8_t> bytes)
            {
                pending_writes.push_back(std::move(bytes));

                boost::asio::async_write(
                    remote,
                    boost::asio::buffer(pending_writes.back()),
                    [](const boost::system::error_code&, std::size_t)
                    {
                        // Writes are expected to fail once the peer rejects a frame and closes,
                        // so there is nothing to check here.
                    });
            }

            // Queues data to be written after a delay. Used to guarantee that the peer performs a
            // separate read for each part of a split frame: the delay is orders of magnitude
            // longer than the loopback round trip, so the first part is always consumed first.
            void send_after(std::chrono::steady_clock::duration delay, std::vector<std::uint8_t> bytes)
            {
                delayed_write.expires_after(delay);
                delayed_write.async_wait(
                    [this, bytes = std::move(bytes)](const boost::system::error_code& ec) mutable
                    {
                        if (!ec)
                            send(std::move(bytes));
                    });
            }

            // Runs the execution context until the peer closes, or until the watchdog gives up.
            void run_until_closed()
            {
                watchdog.expires_after(WATCHDOG);
                watchdog.async_wait(
                    [this](const boost::system::error_code& ec)
                    {
                        if (ec == boost::asio::error::operation_aborted)
                            return;

                        timed_out = true;

                        if (subject)
                            subject->stop();

                        finish();
                    });

                ioc.run();
            }

            boost::asio::io_context ioc{1};
            boost::asio::ip::tcp::socket remote{ioc};
            boost::asio::steady_timer watchdog{ioc};
            boost::asio::steady_timer delayed_write{ioc};

            std::shared_ptr<peer> subject;

            std::deque<std::vector<std::uint8_t>> pending_writes;
            std::vector<received_packet> received;
            std::optional<boost::system::error_code> close_ec;

            // The number of packets to receive before stopping the peer, or zero to keep running
            // until the peer closes on its own.
            std::size_t stop_after = 0;

            bool timed_out = false;

        private:

            // Releases everything the execution context is still waiting on, so that run()
            // returns instead of blocking on a write nobody will ever read.
            void finish()
            {
                boost::system::error_code ec;

                watchdog.cancel();
                delayed_write.cancel();
                remote.close(ec);
            }
    };
}

TEST_F(PeerBufferLimits, TcpFrameLargerThanTheInitialBufferIsReceived)
{
    connect(TCP);
    stop_after = 1;
    subject->run();

    auto payload = pattern(LARGE_PAYLOAD);
    send(make_packet(SIGNO, payload));

    run_until_closed();

    ASSERT_FALSE(timed_out);
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0].signo, SIGNO);
    EXPECT_EQ(received[0].payload, payload);
    EXPECT_GT(LARGE_PAYLOAD, INITIAL_RX_BUFFER_SIZE);
}

TEST_F(PeerBufferLimits, TcpFrameLargerThanTheMaximumClosesTheConnection)
{
    connect(TCP);
    subject->run();

    send(make_truncated_packet(SIGNO, OVERSIZED_PAYLOAD, OVERSIZED_PREFIX));

    run_until_closed();

    ASSERT_FALSE(timed_out);
    EXPECT_TRUE(received.empty());
    ASSERT_TRUE(close_ec.has_value());
    EXPECT_EQ(*close_ec, boost::asio::error::no_buffer_space);
}

TEST_F(PeerBufferLimits, TcpFrameSplitAcrossReadsIsReassembled)
{
    connect(TCP);
    stop_after = 1;
    subject->run();

    auto payload = pattern(LARGE_PAYLOAD);
    auto packet = make_packet(SIGNO, payload);

    // The split point sits below the initial buffer size, so the first part is buffered as an
    // incomplete packet and the buffer still has to grow to fit the rest.
    auto split = packet.begin() + 40 * 1024;

    send(std::vector<std::uint8_t>{packet.begin(), split});
    send_after(50ms, std::vector<std::uint8_t>{split, packet.end()});

    run_until_closed();

    ASSERT_FALSE(timed_out);
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0].payload, payload);
}

TEST_F(PeerBufferLimits, TcpPacketsArrivingInOneReadAreAllReceived)
{
    connect(TCP);
    stop_after = 3;
    subject->run();

    std::vector<std::uint8_t> batch;
    for (unsigned i = 0; i < 3; ++i)
    {
        auto packet = make_packet(SIGNO + i, pattern(300));
        batch.insert(batch.end(), packet.begin(), packet.end());
    }

    send(std::move(batch));

    run_until_closed();

    ASSERT_FALSE(timed_out);
    ASSERT_EQ(received.size(), 3u);
    for (unsigned i = 0; i < 3; ++i)
    {
        EXPECT_EQ(received[i].signo, SIGNO + i);
        EXPECT_EQ(received[i].payload, pattern(300));
    }
}

TEST_F(PeerBufferLimits, WsFrameLargerThanTheInitialBufferIsReceived)
{
    connect(WS);
    stop_after = 1;
    subject->run();

    auto payload = pattern(LARGE_PAYLOAD);
    send(make_ws_frame(make_packet(SIGNO, payload)));

    run_until_closed();

    ASSERT_FALSE(timed_out);
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0].signo, SIGNO);
    EXPECT_EQ(received[0].payload, payload);
}

TEST_F(PeerBufferLimits, WsFrameLargerThanTheMaximumClosesTheConnection)
{
    connect(WS);
    subject->run();

    send(make_truncated_ws_frame(OVERSIZED_PAYLOAD, OVERSIZED_PREFIX));

    run_until_closed();

    ASSERT_FALSE(timed_out);
    EXPECT_TRUE(received.empty());
    ASSERT_TRUE(close_ec.has_value());
    EXPECT_EQ(*close_ec, boost::asio::error::no_buffer_space);
}

TEST_F(PeerBufferLimits, WsFramesArrivingInOneReadAreAllReceived)
{
    connect(WS);
    stop_after = 3;
    subject->run();

    std::vector<std::uint8_t> batch;
    for (unsigned i = 0; i < 3; ++i)
    {
        auto frame = make_ws_frame(make_packet(SIGNO + i, pattern(300)));
        batch.insert(batch.end(), frame.begin(), frame.end());
    }

    send(std::move(batch));

    run_until_closed();

    ASSERT_FALSE(timed_out);
    ASSERT_EQ(received.size(), 3u);
    for (unsigned i = 0; i < 3; ++i)
        EXPECT_EQ(received[i].signo, SIGNO + i);
}

// A WebSocket frame is a transport container, not a packet boundary: a sender is free to put
// several streaming protocol packets into one frame, and every one of them has to be delivered.
TEST_F(PeerBufferLimits, WsFrameCarryingSeveralPacketsDeliversAll)
{
    connect(WS);
    stop_after = 2;
    subject->run();

    std::vector<std::uint8_t> payload;
    for (unsigned i = 0; i < 2; ++i)
    {
        auto packet = make_packet(SIGNO + i, pattern(300));
        payload.insert(payload.end(), packet.begin(), packet.end());
    }

    send(make_ws_frame(payload));

    run_until_closed();

    ASSERT_FALSE(timed_out);
    ASSERT_EQ(received.size(), 2u);
    for (unsigned i = 0; i < 2; ++i)
    {
        EXPECT_EQ(received[i].signo, SIGNO + i);
        EXPECT_EQ(received[i].payload, pattern(300));
    }
}

// Packets below 256 bytes carry their size inline in a one-word header, larger ones need a second
// word for it. Mixing both in one frame is what tells a correct walk over the frame from one that
// assumes every header is the same length.
TEST_F(PeerBufferLimits, WsFrameCarryingPacketsWithDifferentHeaderSizesDeliversAll)
{
    constexpr std::size_t SIZES[] = {100, 300, 50};

    connect(WS);
    stop_after = 3;
    subject->run();

    std::vector<std::uint8_t> payload;
    for (unsigned i = 0; i < 3; ++i)
    {
        auto packet = make_packet(SIGNO + i, pattern(SIZES[i]));
        payload.insert(payload.end(), packet.begin(), packet.end());
    }

    send(make_ws_frame(payload));

    run_until_closed();

    ASSERT_FALSE(timed_out);
    ASSERT_EQ(received.size(), 3u);
    for (unsigned i = 0; i < 3; ++i)
    {
        EXPECT_EQ(received[i].signo, SIGNO + i);
        EXPECT_EQ(received[i].payload, pattern(SIZES[i]));
    }
}

// An empty binary frame carries no packet at all. Walking it must simply come up empty rather
// than spin, so the packet in the frame behind it still has to arrive.
TEST_F(PeerBufferLimits, EmptyWsFrameIsHarmless)
{
    connect(WS);
    stop_after = 1;
    subject->run();

    std::vector<std::uint8_t> batch = make_ws_frame({});
    auto frame = make_ws_frame(make_packet(SIGNO, pattern(300)));
    batch.insert(batch.end(), frame.begin(), frame.end());

    send(std::move(batch));

    run_until_closed();

    ASSERT_FALSE(timed_out);
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0].signo, SIGNO);
    EXPECT_EQ(received[0].payload, pattern(300));
}

// The handed-over data arrives before any read has happened, so the buffer has to be grown to
// hold it rather than filled by degrees.
TEST_F(PeerBufferLimits, HandedOverDataLargerThanTheInitialBufferIsProcessed)
{
    connect(TCP);
    stop_after = 1;

    auto payload = pattern(LARGE_PAYLOAD);
    auto packet = make_packet(SIGNO, payload);

    subject->run(packet.data(), packet.size());

    run_until_closed();

    ASSERT_FALSE(timed_out);
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0].payload, payload);
}

// Data that cannot be stored must be refused outright. Copying it into a buffer too small to hold
// it would run past the end, so this test is worth running under a sanitizer.
TEST_F(PeerBufferLimits, HandedOverDataLargerThanTheMaximumClosesTheConnection)
{
    connect(TCP);

    auto packet = make_truncated_packet(SIGNO, OVERSIZED_PAYLOAD, OVERSIZED_PREFIX);
    ASSERT_GT(packet.size(), RX_BUFFER_MAX);

    subject->run(packet.data(), packet.size());

    run_until_closed();

    ASSERT_FALSE(timed_out);
    EXPECT_TRUE(received.empty());
    ASSERT_TRUE(close_ec.has_value());
    EXPECT_EQ(*close_ec, boost::asio::error::no_buffer_space);
}

// A maximum below the size the buffer would otherwise start at has to be honoured as given, with
// no growth available at all.
TEST_F(PeerBufferLimits, MaximumSmallerThanTheInitialBufferIsHonoured)
{
    constexpr std::size_t SMALL_MAX = 16 * 1024;

    connect(TCP, SMALL_MAX);
    subject->run();

    send(make_truncated_packet(SIGNO, OVERSIZED_PAYLOAD, 2 * SMALL_MAX));

    run_until_closed();

    ASSERT_FALSE(timed_out);
    EXPECT_TRUE(received.empty());
    ASSERT_TRUE(close_ec.has_value());
    EXPECT_EQ(*close_ec, boost::asio::error::no_buffer_space);
}

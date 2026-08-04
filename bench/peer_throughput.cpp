// A throughput benchmark for wss::detail::peer.
//
// This program connects two peer objects over a loopback TCP socket, each running on its own
// single-threaded Boost.Asio execution context and thread. The transmitting peer sends data
// packets of a fixed size as fast as the configured flow-control limits allow; the receiving peer
// counts the payload bytes it decodes. The benchmark deliberately exercises the peer's user-space
// buffering: passing --sndbuf and --rcvbuf shrinks the kernel socket buffers so that outgoing data
// backs up into peer's _tx_buffer, and --throttle applies a processing cost per received byte so
// that the receiver cannot keep up.
//
// One process runs exactly one scenario, so that the reported peak RSS belongs to that scenario
// alone. See run.sh, which runs the full matrix and collects the CSV rows.

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <sys/resource.h>

#include <boost/asio/buffer.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/socket_base.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/system/error_code.hpp>

#include <ws-streaming/detail/peer.hpp>

using namespace std::chrono;
using namespace std::chrono_literals;

namespace
{
    // The signal number used for all benchmark data packets. Any nonzero value works.
    constexpr unsigned SIGNO = 1;

    // The number of packets the transmit pump submits before yielding to the execution context.
    constexpr int PUMP_BATCH = 32;

    struct options
    {
        bool use_tcp_protocol = false;
        std::size_t payload = 64 * 1024;
        double seconds = 5.0;
        double warmup = 0.5;
        std::size_t rx_buffer = 0;          // 0 = derive from the payload size
        std::size_t tx_buffer = 32 * 1024 * 1024;
        std::size_t sndbuf = 0;             // 0 = leave whatever peer configured
        std::size_t rcvbuf = 0;             // 0 = leave the system default
        std::size_t inflight_cap = 0;       // 0 = derive from the payload size
        double throttle_mbps = 0;           // 0 = receiver consumes as fast as it can
        std::string label;
        bool csv = false;
    };

    struct results
    {
        double elapsed = 0;
        std::uint64_t rx_bytes = 0;
        std::uint64_t rx_packets = 0;
        std::uint64_t tx_bytes = 0;
        std::uint64_t tx_packets = 0;
        double cpu_user = 0;
        double cpu_sys = 0;
        long maxrss_kb = 0;
        bool closed_early = false;
        std::string close_reason;
    };

    std::size_t parse_size(const std::string& text)
    {
        std::size_t index = 0;
        auto value = static_cast<std::size_t>(std::stoull(text, &index));

        if (index < text.size())
            switch (std::tolower(static_cast<unsigned char>(text[index])))
            {
                case 'k': value *= 1024; break;
                case 'm': value *= 1024 * 1024; break;
                case 'g': value *= 1024 * 1024 * 1024; break;
                default: break;
            }

        return value;
    }

    void print_usage()
    {
        std::cerr <<
            "usage: peer-throughput [options]\n"
            "  --transport tcp|ws    protocol variant (default ws)\n"
            "  --payload SIZE        data packet payload size, e.g. 64, 1k, 1M (default 64k)\n"
            "  --seconds S           measurement window after warmup (default 5)\n"
            "  --warmup S            warmup period excluded from the measurement (default 0.5)\n"
            "  --rx-buffer SIZE      peer receive buffer (default max(1M, 2*payload+64k))\n"
            "  --tx-buffer SIZE      peer transmit buffer (default 32M)\n"
            "  --sndbuf SIZE         override SO_SNDBUF on the sending socket after construction\n"
            "  --rcvbuf SIZE         set SO_RCVBUF on the receiving socket\n"
            "  --inflight SIZE       unacknowledged payload cap (default max(4M, 16*payload))\n"
            "  --throttle MBPS       receiver processing rate limit in MB/s (default unlimited)\n"
            "  --label TEXT          free-form label copied into the CSV row\n"
            "  --csv                 print a CSV row instead of a human-readable summary\n";
    }

    bool parse_options(int argc, char *argv[], options& opts)
    {
        for (int i = 1; i < argc; ++i)
        {
            std::string arg = argv[i];
            auto next = [&]() -> std::string
            {
                if (i + 1 >= argc)
                    throw std::runtime_error("missing value for " + arg);
                return argv[++i];
            };

            if (arg == "--transport")
            {
                auto value = next();
                if (value == "tcp")
                    opts.use_tcp_protocol = true;
                else if (value == "ws")
                    opts.use_tcp_protocol = false;
                else
                    throw std::runtime_error("--transport must be tcp or ws");
            }
            else if (arg == "--payload")       opts.payload = parse_size(next());
            else if (arg == "--seconds")       opts.seconds = std::stod(next());
            else if (arg == "--warmup")        opts.warmup = std::stod(next());
            else if (arg == "--rx-buffer")     opts.rx_buffer = parse_size(next());
            else if (arg == "--tx-buffer")     opts.tx_buffer = parse_size(next());
            else if (arg == "--sndbuf")        opts.sndbuf = parse_size(next());
            else if (arg == "--rcvbuf")        opts.rcvbuf = parse_size(next());
            else if (arg == "--inflight")      opts.inflight_cap = parse_size(next());
            else if (arg == "--throttle")      opts.throttle_mbps = std::stod(next());
            else if (arg == "--label")         opts.label = next();
            else if (arg == "--csv")           opts.csv = true;
            else if (arg == "--help" || arg == "-h")
            {
                print_usage();
                return false;
            }
            else
                throw std::runtime_error("unrecognized option: " + arg);
        }

        if (!opts.rx_buffer)
            opts.rx_buffer = std::max<std::size_t>(1024 * 1024, 2 * opts.payload + 64 * 1024);

        if (!opts.inflight_cap)
            opts.inflight_cap = std::max<std::size_t>(4 * 1024 * 1024, 16 * opts.payload);

        return true;
    }

    // Spins (with a coarse sleep for long waits) until the specified time point. Used to simulate
    // a receiver whose per-byte processing cost limits how fast it can drain the socket.
    void wait_until(steady_clock::time_point when)
    {
        for (;;)
        {
            auto now = steady_clock::now();
            if (now >= when)
                return;

            auto remaining = when - now;
            if (remaining > 200us)
                std::this_thread::sleep_for(remaining - 100us);
            else
                std::this_thread::yield();
        }
    }

    void snapshot_cpu(double& user, double& sys, long& maxrss_kb)
    {
        struct rusage usage { };
        getrusage(RUSAGE_SELF, &usage);

        user = usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6;
        sys = usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1e6;
        maxrss_kb = usage.ru_maxrss;
    }
}

int main(int argc, char *argv[])
{
    options opts;

    try
    {
        if (!parse_options(argc, argv, opts))
            return 0;
    }

    catch (const std::exception& ex)
    {
        std::cerr << "error: " << ex.what() << std::endl;
        print_usage();
        return 2;
    }

    // Establish a loopback connection. The two sockets are constructed on the execution contexts
    // their peers will run on, so no cross-context dispatching occurs during the measurement.
    boost::asio::io_context ioc_tx{1};
    boost::asio::io_context ioc_rx{1};

    boost::asio::ip::tcp::socket tx_socket{ioc_tx};
    boost::asio::ip::tcp::socket rx_socket{ioc_rx};

    try
    {
        boost::asio::ip::tcp::acceptor acceptor{ioc_rx,
            boost::asio::ip::tcp::endpoint{
                boost::asio::ip::make_address("127.0.0.1"), 0}};

        tx_socket.connect(acceptor.local_endpoint());
        acceptor.accept(rx_socket);
    }

    catch (const std::exception& ex)
    {
        std::cerr << "error: could not set up the loopback connection: " << ex.what() << std::endl;
        return 1;
    }

    // Shrink the receive buffer before the peer starts reading, so that back-pressure reaches the
    // sender quickly instead of being absorbed by the kernel.
    if (opts.rcvbuf)
    {
        boost::system::error_code ec;
        rx_socket.set_option(
            boost::asio::socket_base::receive_buffer_size{static_cast<int>(opts.rcvbuf)}, ec);
    }

    auto tx_peer = std::make_shared<wss::detail::peer>(
        std::move(tx_socket), true, opts.use_tcp_protocol, opts.rx_buffer, opts.tx_buffer);

    auto rx_peer = std::make_shared<wss::detail::peer>(
        std::move(rx_socket), false, opts.use_tcp_protocol, opts.rx_buffer, opts.tx_buffer);

    // The peer constructor sets SO_SNDBUF to the transmit buffer size, which would let the kernel
    // absorb everything and hide the user-space transmit buffer entirely. Override it afterwards
    // to force data through peer's own buffering.
    if (opts.sndbuf)
    {
        boost::system::error_code ec;
        tx_peer->socket().set_option(
            boost::asio::socket_base::send_buffer_size{static_cast<int>(opts.sndbuf)}, ec);
    }

    std::atomic<std::uint64_t> rx_bytes{0};
    std::atomic<std::uint64_t> rx_packets{0};
    std::atomic<std::uint64_t> tx_bytes{0};
    std::atomic<std::uint64_t> tx_packets{0};
    std::atomic<bool> closed{false};

    std::string close_reason;
    std::promise<results> promise;
    auto future = promise.get_future();

    // Receiving side: count payload bytes, optionally simulating a per-byte processing cost.
    const double throttle_bytes_per_sec = opts.throttle_mbps * 1024 * 1024;
    auto throttle_clock = steady_clock::now();

    rx_peer->on_data_received.connect(
        [&](unsigned, const std::uint8_t *, std::size_t size)
        {
            if (throttle_bytes_per_sec > 0)
            {
                auto now = steady_clock::now();
                if (throttle_clock < now)
                    throttle_clock = now;

                throttle_clock += duration_cast<steady_clock::duration>(
                    duration<double>{size / throttle_bytes_per_sec});

                wait_until(throttle_clock);
            }

            rx_packets.fetch_add(1, std::memory_order_relaxed);
            rx_bytes.fetch_add(size, std::memory_order_release);
        });

    // The close reason is written from whichever execution context closed first and read by the
    // pump once it observes the flag, so it needs its own mutex: the flag alone would let the
    // pump read a half-written string.
    std::mutex close_mutex;

    auto note_close = [&](const char *side, const boost::system::error_code& ec)
    {
        std::lock_guard<std::mutex> lock{close_mutex};

        if (close_reason.empty())
            close_reason = std::string{side} + ": " + ec.message()
                + " (value " + std::to_string(ec.value()) + ')';

        closed.store(true, std::memory_order_release);
    };

    tx_peer->on_closed.connect([&](const boost::system::error_code& ec) { note_close("tx", ec); });
    rx_peer->on_closed.connect([&](const boost::system::error_code& ec) { note_close("rx", ec); });

    tx_peer->run();
    rx_peer->run();

    std::vector<std::uint8_t> payload(opts.payload);
    for (std::size_t i = 0; i < payload.size(); ++i)
        payload[i] = static_cast<std::uint8_t>(i);

    // Transmit pump. It runs on the sending execution context, submitting packets until the
    // unacknowledged payload reaches the in-flight cap, then yielding so that the reactor can run
    // the socket's write-readiness handler.
    boost::asio::steady_timer pump_timer{ioc_tx};

    auto start = steady_clock::now();
    auto measure_from = start + duration_cast<steady_clock::duration>(duration<double>{opts.warmup});
    auto measure_to = measure_from + duration_cast<steady_clock::duration>(duration<double>{opts.seconds});

    bool measuring = false;
    results measured;
    std::uint64_t base_rx_bytes = 0;
    std::uint64_t base_rx_packets = 0;
    std::uint64_t base_tx_bytes = 0;
    std::uint64_t base_tx_packets = 0;
    double base_user = 0, base_sys = 0;
    long base_maxrss = 0;
    steady_clock::time_point measure_started;

    std::function<void()> pump = [&]()
    {
        if (closed.load(std::memory_order_acquire))
        {
            std::lock_guard<std::mutex> lock{close_mutex};
            measured.closed_early = true;
            measured.close_reason = close_reason;
        }

        auto now = steady_clock::now();

        if (!measuring && now >= measure_from)
        {
            measuring = true;
            measure_started = now;
            base_rx_bytes = rx_bytes.load(std::memory_order_acquire);
            base_rx_packets = rx_packets.load(std::memory_order_relaxed);
            base_tx_bytes = tx_bytes.load(std::memory_order_relaxed);
            base_tx_packets = tx_packets.load(std::memory_order_relaxed);
            snapshot_cpu(base_user, base_sys, base_maxrss);
        }

        if (measured.closed_early || (measuring && now >= measure_to))
        {
            double user = 0, sys = 0;
            long maxrss = 0;
            snapshot_cpu(user, sys, maxrss);

            measured.elapsed = duration<double>{now - measure_started}.count();
            measured.rx_bytes = rx_bytes.load(std::memory_order_acquire) - base_rx_bytes;
            measured.rx_packets = rx_packets.load(std::memory_order_relaxed) - base_rx_packets;
            measured.tx_bytes = tx_bytes.load(std::memory_order_relaxed) - base_tx_bytes;
            measured.tx_packets = tx_packets.load(std::memory_order_relaxed) - base_tx_packets;
            measured.cpu_user = user - base_user;
            measured.cpu_sys = sys - base_sys;
            measured.maxrss_kb = maxrss;

            promise.set_value(measured);
            return;
        }

        int submitted = 0;

        for (int i = 0; i < PUMP_BATCH; ++i)
        {
            auto sent = tx_bytes.load(std::memory_order_relaxed);
            auto received = rx_bytes.load(std::memory_order_acquire);

            if (sent - received >= opts.inflight_cap)
                break;

            tx_peer->send_data(SIGNO, boost::asio::const_buffer{payload.data(), payload.size()});

            tx_bytes.store(sent + payload.size(), std::memory_order_relaxed);
            tx_packets.fetch_add(1, std::memory_order_relaxed);
            ++submitted;

            if (closed.load(std::memory_order_relaxed))
                break;
        }

        // When the in-flight cap is saturated there is nothing useful to do until the receiver
        // drains; sleep briefly rather than spinning, so the CPU numbers stay meaningful.
        if (!submitted)
        {
            pump_timer.expires_after(100us);
            pump_timer.async_wait([&](const boost::system::error_code&) { pump(); });
        }

        else
            boost::asio::post(ioc_tx, [&]() { pump(); });
    };

    auto tx_guard = boost::asio::make_work_guard(ioc_tx);
    auto rx_guard = boost::asio::make_work_guard(ioc_rx);

    std::thread rx_thread{[&]() { ioc_rx.run(); }};
    std::thread tx_thread{[&]() { ioc_tx.run(); }};

    boost::asio::post(ioc_tx, [&]() { pump(); });

    auto result = future.get();

    boost::asio::post(ioc_tx, [&]() { tx_peer->stop(); });
    boost::asio::post(ioc_rx, [&]() { rx_peer->stop(); });

    tx_guard.reset();
    rx_guard.reset();
    ioc_tx.stop();
    ioc_rx.stop();
    tx_thread.join();
    rx_thread.join();

    const char *transport = opts.use_tcp_protocol ? "tcp" : "ws";
    double mb = result.rx_bytes / (1024.0 * 1024.0);
    double mbps = result.elapsed > 0 ? mb / result.elapsed : 0;
    double pps = result.elapsed > 0 ? result.rx_packets / result.elapsed : 0;
    double cpu_total = result.cpu_user + result.cpu_sys;
    double cpu_per_mb = mb > 0 ? cpu_total / mb : 0;

    if (opts.csv)
    {
        std::cout
            << opts.label << ','
            << transport << ','
            << opts.payload << ','
            << opts.tx_buffer << ','
            << opts.rx_buffer << ','
            << opts.sndbuf << ','
            << opts.rcvbuf << ','
            << opts.throttle_mbps << ','
            << result.elapsed << ','
            << result.rx_bytes << ','
            << result.rx_packets << ','
            << mbps << ','
            << pps << ','
            << result.cpu_user << ','
            << result.cpu_sys << ','
            << cpu_per_mb << ','
            << result.maxrss_kb << ','
            << (result.closed_early ? 1 : 0)
            << std::endl;
    }

    else
    {
        std::cout
            << "transport      " << transport << '\n'
            << "payload        " << opts.payload << " B\n"
            << "tx buffer      " << opts.tx_buffer << " B\n"
            << "rx buffer      " << opts.rx_buffer << " B\n"
            << "SO_SNDBUF      " << (opts.sndbuf ? std::to_string(opts.sndbuf) : "default") << '\n'
            << "SO_RCVBUF      " << (opts.rcvbuf ? std::to_string(opts.rcvbuf) : "default") << '\n'
            << "throttle       " << (opts.throttle_mbps ? std::to_string(opts.throttle_mbps) + " MB/s" : "none") << '\n'
            << "elapsed        " << result.elapsed << " s\n"
            << "received       " << mb << " MB in " << result.rx_packets << " packets\n"
            << "throughput     " << mbps << " MB/s, " << pps << " packets/s\n"
            << "cpu            " << result.cpu_user << " s user, " << result.cpu_sys << " s sys ("
                                 << cpu_per_mb * 1000 << " ms/MB)\n"
            << "peak rss       " << result.maxrss_kb << " kB\n";

        if (result.closed_early)
            std::cout << "CLOSED EARLY   " << result.close_reason << '\n';
    }

    if (result.closed_early)
    {
        std::cerr << "warning: the connection closed before the measurement window ended ("
            << result.close_reason << ')' << std::endl;
        return 1;
    }

    // Nothing decoded at all means the two peers did not agree on the framing
    if (result.rx_bytes == 0)
    {
        std::cerr << "warning: no payload was decoded; the peers may not agree on the framing"
            << std::endl;
        return 1;
    }

    return 0;
}

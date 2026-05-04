// Mimics the device's web page. That page never sends an
// unsubscribe, it just closes the WebSocket, and "Start" opens a fresh one
// and it never seems to misbehave. So the question this script answers is: if we behave like
// the device and just close-and-reopen instead of unsubscribe/resubscribe, does the bug show up?
//
// Each cycle: open WS, wait for the signal, subscribe, read for a while, close. Repeat.
//
// Usage: client-reconnect-test <url> <signal-id>
//                              [read_seconds=5] [wait_seconds=1] [cycles=10]

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <boost/asio.hpp>

#include <ws-streaming/ws-streaming.hpp>

namespace
{
    enum class phase
    {
        connecting,
        waiting_for_signal,
        receiving,
        between,        // closed, idling before the next cycle
        finished,
    };

    struct state
    {
        phase current = phase::connecting;
        std::size_t cycle_index = 0;          // 1-based once a cycle starts
        std::size_t current_cycle_blocks = 0;
        std::vector<std::size_t> per_cycle;
        bool shutting_down = false;
        wss::connection_ptr active;           // current cycle's connection, if any
    };
}

int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        std::cerr << "Usage: " << argv[0]
                  << " <url> <signal-id> [read_seconds=5] [wait_seconds=1] [cycles=10]\n";
        return 2;
    }

    std::string url = argv[1];
    std::string target_id = argv[2];
    auto read_seconds = std::chrono::seconds{(argc >= 4) ? std::atoi(argv[3]) : 5};
    auto wait_seconds = std::chrono::seconds{(argc >= 5) ? std::atoi(argv[4]) : 1};
    std::size_t total_cycles = (argc >= 6) ? std::stoul(argv[5]) : 10;
    if (total_cycles == 0) total_cycles = 1;

    boost::asio::io_context ioc{1};
    wss::client client{ioc.get_executor()};
    auto st = std::make_shared<state>();

    // Ctrl+C handler. Bound to the io_context, not a connection's executor, so it survives
    // across reconnect cycles.
    auto signal_handler = std::make_shared<boost::asio::signal_set>(ioc.get_executor(), SIGINT);

    // Recreated each cycle -- each connection has its own executor, but we keep these on the
    // io_context so the timers don't get tangled up with the connection's lifetime.
    auto read_timer = std::make_shared<boost::asio::steady_timer>(ioc.get_executor());
    auto wait_timer = std::make_shared<boost::asio::steady_timer>(ioc.get_executor());

    // shared_ptr<function> so the SIGINT path and the per-cycle code can both reach it,
    // and so start_cycle can re-arm itself.
    auto print_results_and_stop = std::make_shared<std::function<void()>>();
    auto start_cycle = std::make_shared<std::function<void()>>();

    *print_results_and_stop = [st, read_timer, wait_timer, signal_handler, total_cycles]()
    {
        if (st->current == phase::finished) return;
        st->shutting_down = true;
        st->current = phase::finished;
        read_timer->cancel();
        wait_timer->cancel();
        if (st->active)
        {
            st->active->close();
            st->active.reset();
        }

        std::cout << "\nResults:\n";
        bool bug_seen = false;
        for (std::size_t i = 0; i < st->per_cycle.size(); ++i)
        {
            std::cout << "  cycle " << std::setw(3) << (i + 1)
                      << ": " << std::setw(4) << st->per_cycle[i] << " block(s)";
            if (st->per_cycle[i] == 0)
            {
                std::cout << "   <-- no data, bug reproduced";
                bug_seen = true;
            }
            std::cout << '\n';
        }
        if (st->per_cycle.size() < total_cycles)
            std::cout << "Stopped after cycle " << st->per_cycle.size()
                      << " of " << total_cycles << ".\n";
        std::cout << (bug_seen
                ? "Bug reproduced: even close-and-reopen stopped delivering data.\n"
                : "No bug seen: every reconnect cycle delivered data.\n");

        signal_handler->cancel();
    };

    signal_handler->async_wait(
        [print_results_and_stop](const boost::system::error_code& ec, int)
        {
            if (!ec)
            {
                std::cout << "\nInterrupted.\n";
                (*print_results_and_stop)();
            }
        });

    *start_cycle = [&client, &url, st, target_id, read_seconds, wait_seconds, total_cycles,
                    read_timer, wait_timer, start_cycle, print_results_and_stop]()
    {
        if (st->shutting_down) return;

        ++st->cycle_index;
        st->current_cycle_blocks = 0;
        st->current = phase::connecting;

        std::cout << "\nCycle " << st->cycle_index << "/" << total_cycles
                  << ": opening WebSocket to " << url << " ...\n";

        client.async_connect(url,
            [st, target_id, read_seconds, wait_seconds, total_cycles,
             read_timer, wait_timer, start_cycle, print_results_and_stop]
            (const boost::system::error_code& ec, wss::connection_ptr connection)
            {
                if (st->shutting_down) return;

                if (ec)
                {
                    std::cout << "Cycle " << st->cycle_index
                              << ": connect failed: " << ec.message() << '\n';
                    st->per_cycle.push_back(0);
                    (*print_results_and_stop)();
                    return;
                }

                st->active = connection;
                st->current = phase::waiting_for_signal;
                std::cout << "Cycle " << st->cycle_index
                          << ": connected, waiting for '" << target_id << "'.\n";

                connection->on_disconnected.connect(
                    [st](const boost::system::error_code& ec)
                    {
                        if (st->shutting_down) return;
                        std::cout << "Cycle " << st->cycle_index
                                  << ": connection closed (" << ec.message() << ")\n";
                    });

                connection->on_available.connect(
                    [st, target_id, read_seconds, wait_seconds, total_cycles,
                     read_timer, wait_timer, start_cycle, print_results_and_stop, connection]
                    (wss::remote_signal_ptr signal)
                    {
                        if (st->shutting_down) return;
                        if (signal->id() != target_id) return;
                        if (st->current != phase::waiting_for_signal) return;

                        std::cout << "Cycle " << st->cycle_index
                                  << ": subscribed, reading "
                                  << read_seconds.count() << "s.\n";

                        signal->on_data_received.connect(
                            [st](std::int64_t, std::size_t, const void *, std::size_t)
                            {
                                if (st->current == phase::receiving)
                                    ++st->current_cycle_blocks;
                            });

                        st->current = phase::receiving;
                        signal->subscribe();

                        read_timer->expires_after(read_seconds);
                        read_timer->async_wait(
                            [st, wait_seconds, total_cycles, wait_timer,
                             start_cycle, print_results_and_stop, connection]
                            (const boost::system::error_code& ec)
                            {
                                if (ec) return;
                                if (st->shutting_down) return;
                                if (st->current != phase::receiving) return;

                                std::size_t got = st->current_cycle_blocks;
                                st->per_cycle.push_back(got);
                                std::cout << "Cycle " << st->cycle_index
                                          << ": " << got << " block(s); closing.\n";

                                // Web-page-style "stop streaming": just close the socket.
                                // Deferring the close to the next tick lets the read
                                // completion unwind first; otherwise on_disconnected
                                // fires with EBADF instead of operation_aborted.
                                boost::asio::post(connection->executor(),
                                    [connection]{ connection->close(); });
                                st->active.reset();

                                if (got == 0 || st->cycle_index >= total_cycles)
                                {
                                    (*print_results_and_stop)();
                                    return;
                                }

                                st->current = phase::between;
                                std::cout << "Cycle " << st->cycle_index
                                          << ": idle " << wait_seconds.count()
                                          << "s before next.\n";
                                wait_timer->expires_after(wait_seconds);
                                wait_timer->async_wait(
                                    [st, start_cycle](const boost::system::error_code& ec)
                                    {
                                        if (ec) return;
                                        if (st->shutting_down) return;
                                        (*start_cycle)();
                                    });
                            });
                    });
            });
    };

    (*start_cycle)();
    ioc.run();
    return 0;
}

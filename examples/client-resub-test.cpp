// Pokes at a device bug: after a `subscribe -> unsubscribe -> wait -> subscribe` sequence,
// the device sometimes stops sending data on the resubscribe even though the WebSocket itself
// stays up and the device responds with a 200 OK. The interesting knobs are the wait length
// and how many cycles you run, so both are configurable.
//
// Usage: client-resub-test <url> <signal-id>
//                          [wait_seconds=10] [pre_blocks=3]
//                          [observe_seconds=5] [cycles=1]

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
        warmup,      // first subscribe, gathering pre_blocks to confirm data is flowing
        idle,        // unsubscribed, waiting wait_seconds
        observing,   // resubscribed, counting blocks for observe_seconds
        finished,    // results printed, connection going away
    };

    const char *phase_name(phase p)
    {
        switch (p)
        {
            case phase::warmup:    return "warmup";
            case phase::idle:      return "idle (unsubscribed)";
            case phase::observing: return "observing";
            case phase::finished:  return "finished";
        }
        return "?";
    }

    struct state
    {
        phase current = phase::warmup;
        std::size_t warmup_blocks = 0;
        std::size_t cycle_index = 0;          // 1-based once a cycle starts
        std::size_t current_cycle_blocks = 0;
        std::vector<std::size_t> per_cycle;   // per_cycle[i] = blocks observed in cycle i+1
        bool shutting_down = false;
    };
}

int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        std::cerr << "Usage: " << argv[0]
                  << " <url> <signal-id> [wait_seconds=10] [pre_blocks=3]"
                     " [observe_seconds=5] [cycles=1]\n";
        return 2;
    }

    std::string url = argv[1];
    std::string target_id = argv[2];
    auto wait_seconds    = std::chrono::seconds{(argc >= 4) ? std::atoi(argv[3]) : 10};
    std::size_t pre_blocks_target = (argc >= 5) ? std::stoul(argv[4]) : 3;
    auto observe_seconds = std::chrono::seconds{(argc >= 6) ? std::atoi(argv[5]) : 5};
    std::size_t total_cycles = (argc >= 7) ? std::stoul(argv[6]) : 1;
    if (total_cycles == 0) total_cycles = 1;

    boost::asio::io_context ioc{1};
    wss::client client{ioc.get_executor()};
    auto st = std::make_shared<state>();

    std::cout << "Connecting to " << url << " ...\n";

    client.async_connect(url,
        [&, st, target_id, wait_seconds, pre_blocks_target, observe_seconds, total_cycles]
        (const boost::system::error_code& ec, wss::connection_ptr connection)
        {
            if (ec)
            {
                std::cerr << "Connection failed: " << ec.message() << '\n';
                return;
            }
            std::cout << "Connected. Waiting for signal '" << target_id << "'.\n";

            // Ctrl+C tears the connection down cleanly.
            auto signal_handler = std::make_shared<boost::asio::signal_set>(
                connection->executor(), SIGINT);
            signal_handler->async_wait(
                [connection, st](const boost::system::error_code& ec, int)
                {
                    if (!ec) { st->shutting_down = true; connection->close(); }
                });
            // Without canceling the signal_set its pending wait keeps the io_context alive.
            connection->on_disconnected.connect(
                [signal_handler](const boost::system::error_code& ec)
                {
                    std::cout << "Connection closed: " << ec.message() << '\n';
                    signal_handler->cancel();
                });

            auto wait_timer = std::make_shared<boost::asio::steady_timer>(connection->executor());
            auto observe_timer = std::make_shared<boost::asio::steady_timer>(connection->executor());

            connection->on_available.connect(
                [st, target_id, wait_timer, observe_timer, wait_seconds, pre_blocks_target,
                 observe_seconds, total_cycles, connection]
                (wss::remote_signal_ptr signal)
                {
                    if (signal->id() != target_id)
                        return;

                    auto target = signal;  // keep alive for inner lambdas

                    auto finish = [st, wait_timer, observe_timer, total_cycles, connection]()
                    {
                        if (st->current == phase::finished) return;
                        st->current = phase::finished;
                        wait_timer->cancel();
                        observe_timer->cancel();

                        std::cout << "\nResults:\n";
                        std::cout << "  warmup blocks: " << st->warmup_blocks << '\n';
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
                                ? "Bug reproduced: data stopped arriving after a resubscribe.\n"
                                : "No bug seen: data resumed after every resubscribe.\n");

                        // Defer the close so the on_disconnected handler reports
                        // operation_aborted instead of "Bad file descriptor" -- otherwise
                        // we close the socket while the read loop is still on the stack.
                        st->shutting_down = true;
                        boost::asio::post(connection->executor(),
                            [connection]{ connection->close(); });
                    };

                    // shared_ptr<function> so start_cycle can re-arm itself for the next cycle.
                    auto start_cycle = std::make_shared<std::function<void()>>();

                    *start_cycle = [st, target, wait_timer, observe_timer, wait_seconds,
                                    observe_seconds, total_cycles, finish, start_cycle]()
                    {
                        if (st->current == phase::finished) return;

                        ++st->cycle_index;
                        st->current_cycle_blocks = 0;
                        st->current = phase::idle;

                        std::cout << "\nCycle " << st->cycle_index << "/" << total_cycles
                                  << ": unsubscribe, wait " << wait_seconds.count() << "s.\n";
                        target->unsubscribe();

                        wait_timer->expires_after(wait_seconds);
                        wait_timer->async_wait(
                            [st, target, observe_timer, observe_seconds, total_cycles,
                             finish, start_cycle]
                            (const boost::system::error_code& ec)
                            {
                                if (ec) return;
                                if (st->current != phase::idle) return;

                                std::cout << "Cycle " << st->cycle_index
                                          << ": subscribe again, watch "
                                          << observe_seconds.count() << "s for data.\n";
                                st->current = phase::observing;
                                target->subscribe();

                                observe_timer->expires_after(observe_seconds);
                                observe_timer->async_wait(
                                    [st, total_cycles, finish, start_cycle]
                                    (const boost::system::error_code& ec)
                                    {
                                        if (ec) return;
                                        if (st->current != phase::observing) return;

                                        std::size_t got = st->current_cycle_blocks;
                                        st->per_cycle.push_back(got);
                                        std::cout << "Cycle " << st->cycle_index
                                                  << ": " << got
                                                  << " block(s) in window.\n";

                                        // Stop early on a zero-block cycle (the bug we're after).
                                        if (got == 0 || st->cycle_index >= total_cycles)
                                            finish();
                                        else
                                            (*start_cycle)();
                                    });
                            });
                    };

                    target->on_data_received.connect(
                        [st, target, pre_blocks_target, start_cycle]
                        (std::int64_t /*domain*/, std::size_t sample_count,
                         const void * /*data*/, std::size_t size)
                        {
                            switch (st->current)
                            {
                                case phase::warmup:
                                    ++st->warmup_blocks;
                                    std::cout << "Warmup: block " << st->warmup_blocks
                                              << "/" << pre_blocks_target
                                              << " (" << sample_count << " samples, "
                                              << size << " bytes)\n";
                                    if (st->warmup_blocks >= pre_blocks_target)
                                        (*start_cycle)();
                                    return;

                                case phase::idle:
                                    // Stray sample after we asked to unsubscribe -- worth flagging.
                                    std::cout << "Got data while unsubscribed ("
                                              << sample_count << " samples)\n";
                                    return;

                                case phase::observing:
                                    ++st->current_cycle_blocks;
                                    std::cout << "Cycle " << st->cycle_index
                                              << ": block " << st->current_cycle_blocks
                                              << " (" << sample_count << " samples, "
                                              << size << " bytes)\n";
                                    return;

                                case phase::finished:
                                    return;
                            }
                        });

                    target->subscribe();
                });

            // The library raises on_unavailable for every signal during any teardown, including
            // ours, so suppress the noise during shutdown and only print when the device really
            // retracted the signal mid-test.
            connection->on_unavailable.connect(
                [target_id, st](wss::remote_signal_ptr signal)
                {
                    if (st->shutting_down) return;
                    if (signal->id() == target_id)
                        std::cout << "Device removed signal during phase: "
                                  << phase_name(st->current) << '\n';
                });
        });

    ioc.run();
    return 0;
}

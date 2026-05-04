// Reproduces the flaky behavior we see, where every signal is
// subscribed and unsubscribed individually rather than batched. This is the lifecycle that
// triggers the bug; the device uses one batched subscribe and never unsubscribes,
// and that path doesn't misbehave.
//
// The script: discover all signals, subscribe to each one individually, read for a while,
// unsubscribe each individually, idle, then resubscribe each one with a configurable delay
// between calls. The per-signal table at the end shows whether each signal came back.
//
// Usage: client-resub-all-test <url>
//                              [discovery_seconds=0] [receive_seconds=0]
//                              [soak_seconds=10] [wait_between_seconds=0]
//                              [observe_seconds=10]
//
// Example: client-resub-all-test ws://192.168.1.1:80/stream 0 0 10 0 10
//
// Ctrl+C to abort early -- the partial results so far still get printed.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <boost/asio.hpp>

#include <ws-streaming/ws-streaming.hpp>

namespace
{
    enum class phase
    {
        discovery,            // waiting for the device to advertise signals
        receiving,            // subscribed (one RPC per signal); counting blocks
        soaking,              // unsubscribed; idling soak_seconds
        resubscribing,        // sending individual subscribes, one per wait_between
        observing,            // last resubscribe sent; counting until observe_timer fires
        finished,
    };

    using clock = std::chrono::steady_clock;

    struct state
    {
        phase current = phase::discovery;
        std::map<std::string, wss::remote_signal_ptr> signals;
        std::vector<std::string> ids_in_advertise_order;

        std::map<std::string, std::size_t>      blocks_before;     // during the receiving phase
        std::map<std::string, std::size_t>      blocks_after;      // resubscribing + observing
        std::map<std::string, clock::time_point> resubscribed_at;  // when subscribe() was called
        std::map<std::string, clock::time_point> first_block_after;// when the first block arrived after that

        std::size_t resub_index = 0;
        bool shutting_down = false;
        wss::connection_ptr connection;
    };
}

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0]
                  << " <url> [discovery_seconds=0] [receive_seconds=0] [soak_seconds=10]"
                     " [wait_between_seconds=0] [observe_seconds=10]\n";
        return 2;
    }

    std::string url = argv[1];
    auto discovery_seconds    = std::chrono::seconds{(argc >= 3) ? std::atoi(argv[2]) : 0};
    auto receive_seconds      = std::chrono::seconds{(argc >= 4) ? std::atoi(argv[3]) : 0};
    auto soak_seconds         = std::chrono::seconds{(argc >= 5) ? std::atoi(argv[4]) : 10};
    auto wait_between_seconds = std::chrono::seconds{(argc >= 6) ? std::atoi(argv[5]) : 0};
    auto observe_seconds      = std::chrono::seconds{(argc >= 7) ? std::atoi(argv[6]) : 10};

    boost::asio::io_context ioc{1};
    wss::client client{ioc.get_executor()};
    auto st = std::make_shared<state>();

    std::cout << "Connecting to " << url << " ...\n";

    auto signal_handler = std::make_shared<boost::asio::signal_set>(ioc.get_executor(), SIGINT);
    auto discovery_timer = std::make_shared<boost::asio::steady_timer>(ioc.get_executor());
    auto receive_timer   = std::make_shared<boost::asio::steady_timer>(ioc.get_executor());
    auto soak_timer      = std::make_shared<boost::asio::steady_timer>(ioc.get_executor());
    auto resub_timer     = std::make_shared<boost::asio::steady_timer>(ioc.get_executor());
    auto observe_timer   = std::make_shared<boost::asio::steady_timer>(ioc.get_executor());

    // shared_ptr<function> so the SIGINT handler and the resub loop can both reach them.
    auto print_results = std::make_shared<std::function<void()>>();
    auto do_resub_step = std::make_shared<std::function<void()>>();

    *print_results = [st, signal_handler,
                      discovery_timer, receive_timer, soak_timer, resub_timer, observe_timer]()
    {
        if (st->current == phase::finished) return;
        st->shutting_down = true;
        st->current = phase::finished;
        discovery_timer->cancel();
        receive_timer->cancel();
        soak_timer->cancel();
        resub_timer->cancel();
        observe_timer->cancel();

        std::cout << "\nResults:\n";

        if (st->ids_in_advertise_order.empty())
        {
            std::cout << "  no signals were advertised.\n";
        }
        else
        {
            std::cout << std::left
                      << "  " << std::setw(16) << "signal"
                      << std::setw(12) << "before"
                      << std::setw(12) << "after"
                      << "first block after resubscribe\n";

            std::size_t failures = 0;
            for (const auto& id : st->ids_in_advertise_order)
            {
                std::size_t before = st->blocks_before.count(id) ? st->blocks_before[id] : 0;
                std::size_t after  = st->blocks_after.count(id)  ? st->blocks_after[id]  : 0;

                std::string note;
                if (st->first_block_after.count(id) && st->resubscribed_at.count(id))
                {
                    auto delay_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        st->first_block_after[id] - st->resubscribed_at[id]).count();
                    note = std::to_string(delay_ms) + " ms";
                }
                else if (!st->resubscribed_at.count(id))
                {
                    note = "(never resubscribed -- aborted early)";
                }
                else
                {
                    ++failures;
                    note = "<-- no data, bug reproduced";
                }

                std::cout << "  " << std::setw(16) << id
                          << std::setw(12) << before
                          << std::setw(12) << after
                          << note << '\n';
            }

            std::cout << '\n';
            if (failures > 0)
                std::cout << "Bug reproduced: " << failures << " of "
                          << st->ids_in_advertise_order.size()
                          << " signals delivered no data after individual resubscribe.\n";
            else
                std::cout << "No bug seen: every signal delivered data after resubscribe.\n";
        }

        if (st->connection)
        {
            // Defer so the read loop unwinds first (otherwise on_disconnected reports EBADF).
            auto c = st->connection;
            st->connection.reset();
            boost::asio::post(c->executor(), [c]{ c->close(); });
        }
        signal_handler->cancel();
    };

    signal_handler->async_wait(
        [print_results](const boost::system::error_code& ec, int)
        {
            if (!ec)
            {
                std::cout << "\nInterrupted.\n";
                (*print_results)();
            }
        });

    // The resubscribe loop: subscribe to one signal, arm the timer, come back and do the next.
    // When we run out of signals, switch to observing for observe_seconds before printing.
    *do_resub_step = [st, wait_between_seconds, observe_seconds,
                      resub_timer, observe_timer, print_results, do_resub_step]()
    {
        if (st->shutting_down) return;
        if (st->current != phase::resubscribing) return;

        if (st->resub_index >= st->ids_in_advertise_order.size())
        {
            std::cout << "All " << st->ids_in_advertise_order.size()
                      << " signal(s) resubscribed; observing "
                      << observe_seconds.count() << "s.\n";
            st->current = phase::observing;

            observe_timer->expires_after(observe_seconds);
            observe_timer->async_wait(
                [print_results](const boost::system::error_code& ec)
                {
                    if (!ec) (*print_results)();
                });
            return;
        }

        const auto& id = st->ids_in_advertise_order[st->resub_index];
        auto it = st->signals.find(id);
        if (it != st->signals.end())
        {
            std::cout << "Subscribing to '" << id << "' ("
                      << (st->resub_index + 1) << "/"
                      << st->ids_in_advertise_order.size() << ")\n";
            st->resubscribed_at[id] = clock::now();
            it->second->subscribe();
        }
        ++st->resub_index;

        resub_timer->expires_after(wait_between_seconds);
        resub_timer->async_wait(
            [do_resub_step](const boost::system::error_code& ec)
            {
                if (!ec) (*do_resub_step)();
            });
    };

    client.async_connect(url,
        [&, st, signal_handler, discovery_timer, receive_timer, soak_timer, observe_timer,
         do_resub_step, print_results,
         discovery_seconds, receive_seconds, soak_seconds]
        (const boost::system::error_code& ec, wss::connection_ptr connection)
        {
            if (ec)
            {
                std::cerr << "Connection failed: " << ec.message() << '\n';
                signal_handler->cancel();
                return;
            }

            std::cout << "Connected. Discovering signals for "
                      << discovery_seconds.count() << "s ...\n";
            st->connection = connection;

            connection->on_disconnected.connect(
                [st](const boost::system::error_code& ec)
                {
                    if (st->shutting_down) return;
                    std::cout << "Connection dropped: " << ec.message() << '\n';
                });

            connection->on_available.connect(
                [st](wss::remote_signal_ptr signal)
                {
                    if (st->shutting_down) return;
                    if (st->signals.count(signal->id())) return;

                    std::string id = signal->id();
                    st->signals[id] = signal;
                    st->ids_in_advertise_order.push_back(id);
                    std::cout << "  " << id << '\n';

                    // Hook the data callback now so we don't miss any blocks once the
                    // device starts pushing samples in response to our subscribes.
                    signal->on_data_received.connect(
                        [st, id](std::int64_t, std::size_t, const void *, std::size_t)
                        {
                            if (st->current == phase::receiving)
                            {
                                ++st->blocks_before[id];
                            }
                            else if (st->current == phase::resubscribing
                                  || st->current == phase::observing)
                            {
                                if (!st->blocks_after.count(id))
                                    st->first_block_after[id] = clock::now();
                                ++st->blocks_after[id];
                            }
                        });
                });

            // Once discovery finishes, fire off all the individual subscribes.
            discovery_timer->expires_after(discovery_seconds);
            discovery_timer->async_wait(
                [st, connection, receive_timer, soak_timer, observe_timer,
                 do_resub_step, print_results,
                 receive_seconds, soak_seconds]
                (const boost::system::error_code& ec)
                {
                    if (ec || st->shutting_down) return;
                    if (st->ids_in_advertise_order.empty())
                    {
                        std::cout << "No signals advertised; giving up.\n";
                        (*print_results)();
                        return;
                    }

                    std::cout << "Discovered " << st->ids_in_advertise_order.size()
                              << " signal(s). Subscribing to each individually.\n";
                    st->current = phase::receiving;
                    for (const auto& id : st->ids_in_advertise_order)
                    {
                        auto it = st->signals.find(id);
                        if (it != st->signals.end())
                            it->second->subscribe();
                    }

                    // After receive_seconds, unsubscribe each one and start the soak.
                    receive_timer->expires_after(receive_seconds);
                    receive_timer->async_wait(
                        [st, connection, soak_timer, do_resub_step, print_results, soak_seconds]
                        (const boost::system::error_code& ec)
                        {
                            if (ec || st->shutting_down) return;

                            std::size_t total_before = 0;
                            for (const auto& [id, n] : st->blocks_before) total_before += n;
                            std::cout << "Got " << total_before
                                      << " block(s) across " << st->blocks_before.size()
                                      << " signal(s). Unsubscribing each, soaking "
                                      << soak_seconds.count() << "s.\n";

                            st->current = phase::soaking;
                            for (const auto& id : st->ids_in_advertise_order)
                            {
                                auto it = st->signals.find(id);
                                if (it != st->signals.end())
                                    it->second->unsubscribe();
                            }

                            soak_timer->expires_after(soak_seconds);
                            soak_timer->async_wait(
                                [st, do_resub_step](const boost::system::error_code& ec)
                                {
                                    if (ec || st->shutting_down) return;

                                    std::cout << "Soak done. Resubscribing one at a time.\n";
                                    st->current = phase::resubscribing;
                                    st->resub_index = 0;
                                    (*do_resub_step)();
                                });
                        });
                });
        });

    ioc.run();
    return 0;
}

// Behavioural probes for the LiteMQTT reconnect path.
//
// Every scenario starts a tiny broker on loopback (or deliberately none at all),
// pushes an mqtt_helper through one failure mode and prints the numbers that
// make the behaviour checkable.  They cover what unit tests cannot observe:
// fd/handle leaks across churn, stop() latency while the backoff timer is armed,
// write serialization under load, and whether a rejected session is closed.
//
// Build:
//   g++ -std=c++14 -Wall -Wextra -pthread -I inc -I 3rdparty/asio/include
//       -DASIO_STANDALONE tools/reconnect_probe.cc -o reconnect_probe
//   (or: cmake --build build --target reconnect_probe)
//
// Run:
//   ./reconnect_probe --help        list the scenarios
//   ./reconnect_probe framing       run one scenario
//   ./reconnect_probe all           run everything, print a PASS/FAIL summary
//
// Each scenario listens on loopback port (18840 + slot); export
// LITEMQTT_PROBE_PORT_BASE to move the whole block when those are taken.
// Exit code 0 means "the observed behaviour is the expected one".
#include <asio.hpp>

#if defined(__linux__)
#include <dirent.h>
#include <execinfo.h>
#include <unistd.h>
#endif
#if defined(_WIN32)
#include <process.h>
#endif

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "litemqtt/mqtt_helper.hpp"

namespace {

using namespace std::chrono;

constexpr uint16_t kDefaultPortBase = 18840;

/// One slot per scenario, so a listener left behind by a crashed run cannot
/// silently make a later scenario pass for the wrong reason.
enum port_slot : int {
    slot_reconnect = 1,
    slot_leak_nostop = 2,
    slot_restart = 3,
    slot_selfstop = 4,
    slot_bigpub = 5,
    slot_badcreds = 6,
    slot_framing = 7,
    slot_giveup = 8,
    slot_bigmsg = 9,
    slot_rearm = 10,
    slot_fixed_delay = 11,
    slot_backoff = 57,
    slot_stoplag = 58,
    slot_leak = 59,
};

uint16_t probe_port_base() {
    static const uint16_t base = []() -> uint16_t {
        const char* env = std::getenv("LITEMQTT_PROBE_PORT_BASE");
        const long v = env ? std::strtol(env, nullptr, 10) : 0;
        if (v <= 0 || v > 65000) return kDefaultPortBase;
        return static_cast<uint16_t>(v);
    }();
    return base;
}

uint16_t probe_port(port_slot slot) {
    return static_cast<uint16_t>(probe_port_base() + static_cast<int>(slot));
}

long probe_pid() {
#if defined(_WIN32)
    return static_cast<long>(::_getpid());
#elif defined(__linux__) || defined(__APPLE__)
    return static_cast<long>(::getpid());
#else
    return -1;
#endif
}

std::shared_ptr<litemqtt::mqtt_helper> make_helper(port_slot slot, const char* client_id) {
    auto helper = std::make_shared<litemqtt::mqtt_helper>("127.0.0.1", probe_port(slot), client_id);
    // Every scenario here talks to loopback, where a connect is sub-millisecond and a
    // refusal is instant on Linux.  Windows can take a couple of seconds to surface the
    // RST of a refused loopback connect (SYN retransmits), which would make the retry
    // cadences below measure the OS instead of the reconnect policy; cap each attempt so
    // even a port with no listener fails on our own terms.
    helper->set_connect_timeout(150);
    return helper;
}

/// Minimal broker: accepts a TCP connection, answers the CONNECT with a
/// success CONNACK, optionally pushes one QoS0 PUBLISH, then drops the link
/// after `drop_ms`.  It keeps accepting afterwards, so every reconnect attempt
/// is visible in accepted().
class dummy_broker {
public:
    explicit dummy_broker(uint16_t port, int drop_ms = 200, bool stall = false,
                          bool push_publish = false, int connack_rc = 0)
        : acc_(io_, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)),
          drop_ms_(drop_ms),
          stall_(stall),
          push_publish_(push_publish),
          connack_rc_(connack_rc) {
        do_accept();
        thread_ = std::thread([this]() { io_.run(); });
    }

    ~dummy_broker() {
        io_.stop();
        if (thread_.joinable()) thread_.join();
    }

    int accepted() const { return accepted_.load(); }
    int publishes() const { return publishes_.load(); }
    int closed() const { return closed_.load(); }
    int concurrent() const { return accepted_.load() - closed_.load(); }

private:
    void do_accept() {
        auto sock = std::make_shared<asio::ip::tcp::socket>(io_);
        acc_.async_accept(*sock, [this, sock](const asio::error_code& ec) {
            if (!ec) {
                ++accepted_;
                serve(sock);
            }
            do_accept();
        });
    }

    void serve(const std::shared_ptr<asio::ip::tcp::socket>& sock) {
        auto buf = std::make_shared<std::vector<uint8_t>>(4096);
        sock->async_read_some(asio::buffer(*buf), [this, sock, buf](const asio::error_code& ec,
                                                                  std::size_t n) {
            if (ec) return;
            if (n > 0 && (buf->at(0) >> 4) == 1) {  // CONNECT
                const uint8_t connack[4] = {0x20, 0x02, 0x00,
                                            static_cast<uint8_t>(connack_rc_)};
                asio::error_code ignored;
                asio::write(*sock, asio::buffer(connack, 4), ignored);
                if (push_publish_) {
                    // PUBLISH qos0, topic "t", payload "hi"
                    static const uint8_t pub[7] = {0x30, 0x05, 0x00, 0x01, 0x74, 0x68, 0x69};
                    asio::write(*sock, asio::buffer(pub, 7), ignored);
                }
            }
            if (stall_) {
                // Stop reading so that the peer's socket buffer fills up and its
                // async_write has to defer, i.e. the write operation outlives the
                // buffer it was given.
                auto timer = std::make_shared<asio::steady_timer>(io_);
                timer->expires_after(milliseconds(3000));
                timer->async_wait([this, sock, buf, timer](const asio::error_code& ec2) {
                    if (!ec2) drain(sock, buf);
                });
                return;
            }
            drain(sock, buf);
            drop_later(sock);
        });
    }

    void drain(const std::shared_ptr<asio::ip::tcp::socket>& sock,
               const std::shared_ptr<std::vector<uint8_t>>& buf) {
        sock->async_read_some(asio::buffer(*buf), [this, sock, buf](const asio::error_code& ec,
                                                                  std::size_t n) {
            if (ec) {
                ++closed_;
                return;
            }
            if (n > 0 && (buf->at(0) >> 4) == 3) ++publishes_;  // PUBLISH
            drain(sock, buf);
        });
    }

    void drop_later(const std::shared_ptr<asio::ip::tcp::socket>& sock) {
        auto timer = std::make_shared<asio::steady_timer>(io_);
        timer->expires_after(milliseconds(drop_ms_));
        timer->async_wait([sock, timer](const asio::error_code& ec) {
            if (ec) return;
            asio::error_code ignored;
            sock->shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
            sock->close(ignored);
        });
    }

    asio::io_context io_;
    asio::ip::tcp::acceptor acc_;
    std::thread thread_;
    std::atomic<int> accepted_{0};
    std::atomic<int> publishes_{0};
    std::atomic<int> closed_{0};
    int drop_ms_;
    bool stall_;
    bool push_publish_;
    int connack_rc_;
};

/// Broker that parses the MQTT framing of everything the client sends and
/// reports whether the byte stream stayed intact.  It also pushes a burst of
/// QoS1 PUBLISHes so that the client has to emit PUBACKs while a large publish
/// is still being written -- exactly the case an unserialized write path
/// corrupts.
class framing_broker {
public:
    explicit framing_broker(uint16_t port)
        : acc_(io_, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)) {
        do_accept();
        thread_ = std::thread([this]() { io_.run(); });
    }

    ~framing_broker() {
        io_.stop();
        if (thread_.joinable()) thread_.join();
    }

    int publishes() const { return publishes_.load(); }
    int pubacks() const { return pubacks_.load(); }
    int errors() const { return errors_.load(); }
    std::size_t bytes() const { return bytes_.load(); }

private:
    void do_accept() {
        auto sock = std::make_shared<asio::ip::tcp::socket>(io_);
        acc_.async_accept(*sock, [this, sock](const asio::error_code& ec) {
            if (!ec) {
                auto pending = std::make_shared<std::vector<char>>();
                read(sock, pending);
                push_qos1(sock, 1);
            }
            do_accept();
        });
    }

    void read(const std::shared_ptr<asio::ip::tcp::socket>& sock,
              const std::shared_ptr<std::vector<char>>& pending) {
        auto chunk = std::make_shared<std::vector<char>>(16384);
        sock->async_read_some(asio::buffer(*chunk),
                              [this, sock, pending, chunk](const asio::error_code& ec,
                                                           std::size_t n) {
                                  if (ec) return;
                                  bytes_ += n;
                                  pending->insert(pending->end(), chunk->begin(),
                                                  chunk->begin() + n);
                                  parse(sock, *pending);
                                  read(sock, pending);
                              });
    }

    void parse(const std::shared_ptr<asio::ip::tcp::socket>& sock, std::vector<char>& buf) {
        std::size_t pos = 0;
        while (buf.size() - pos >= 2) {
            const uint8_t type = static_cast<uint8_t>(buf[pos]) >> 4;
            std::size_t len = 0;
            int shift = 0;
            std::size_t i = pos + 1;
            bool done = false;
            for (; i < buf.size() && shift <= 21; ++i, shift += 7) {
                const uint8_t b = static_cast<uint8_t>(buf[i]);
                len |= static_cast<std::size_t>(b & 0x7f) << shift;
                if ((b & 0x80) == 0) {
                    done = true;
                    ++i;
                    break;
                }
            }
            if (!done) break;  // need more bytes for the length field
            if (len > (16u << 20)) {
                ++errors_;  // nonsense length -> framing is broken
                buf.clear();
                return;
            }
            if (buf.size() - i < len) break;  // body not complete yet
            const std::size_t total = (i - pos) + len;
            if (type == 3) {
                ++publishes_;
                if (len < 2) ++errors_;
            } else if (type == 4) {
                ++pubacks_;
            } else if (type == 1) {
                const uint8_t connack[4] = {0x20, 0x02, 0x00, 0x00};
                asio::error_code ignored;
                asio::write(*sock, asio::buffer(connack, 4), ignored);
            }
            pos += total;
        }
        if (pos > 0) buf.erase(buf.begin(), buf.begin() + static_cast<std::ptrdiff_t>(pos));
    }

    /// Pushes QoS1 PUBLISHes, 5 ms apart, to mix PUBACKs into the client's
    /// outgoing stream.
    void push_qos1(const std::shared_ptr<asio::ip::tcp::socket>& sock, int count) {
        auto timer = std::make_shared<asio::steady_timer>(io_);
        timer->expires_after(milliseconds(5));
        timer->async_wait([this, sock, timer, count](const asio::error_code& ec) {
            if (ec || count > 24) return;
            std::vector<uint8_t> pkt;
            const std::string topic = "srv";
            const std::string payload = "01234567";
            const std::size_t remaining = 2 + topic.size() + 2 + payload.size();
            pkt.push_back(0x32);  // PUBLISH, QoS1
            pkt.push_back(static_cast<uint8_t>(remaining));
            pkt.push_back(0x00);
            pkt.push_back(static_cast<uint8_t>(topic.size()));
            pkt.insert(pkt.end(), topic.begin(), topic.end());
            pkt.push_back(static_cast<uint8_t>(count >> 8));
            pkt.push_back(static_cast<uint8_t>(count & 0xff));
            pkt.insert(pkt.end(), payload.begin(), payload.end());
            asio::error_code ignored;
            asio::write(*sock, asio::buffer(pkt), ignored);
            push_qos1(sock, count + 1);
        });
    }

    asio::io_context io_;
    asio::ip::tcp::acceptor acc_;
    std::thread thread_;
    std::atomic<int> publishes_{0};
    std::atomic<int> pubacks_{0};
    std::atomic<int> errors_{0};
    std::atomic<std::size_t> bytes_{0};
};

/// Prints a symbolised backtrace on fatal signals where the platform allows it,
/// then dies the way it would have without the handler.
void crash_handler(int sig) {
#if defined(__linux__) || defined(__APPLE__)
    void* frames[64];
    const int n = ::backtrace(frames, 64);
    ::backtrace_symbols_fd(frames, n, 2);
#endif
    std::signal(sig, SIG_DFL);
    std::raise(sig);
}

/// Open descriptors of this process, or -1 on platforms without /proc.  The
/// scenarios still run in that case, they just cannot report the delta.
int count_fds() {
#if defined(__linux__)
    int n = 0;
    if (DIR* d = ::opendir("/proc/self/fd")) {
        while (dirent* e = ::readdir(d)) {
            if (e->d_name[0] != '.') ++n;
        }
        ::closedir(d);
    }
    return n;
#else
    return -1;
#endif
}

void install_logger(const char* tag) {
    litemqtt::set_log_handler([tag](const litemqtt::log_record& r) {
        std::printf("  [%s][%s] %s\n", tag, litemqtt::to_string(r.level), r.message.c_str());
    });
}

int probe_reconnect() {
    std::puts("=== scenario: broker drops the link, does the helper reconnect? ===");
    install_logger("client");
    dummy_broker broker(probe_port(slot_reconnect), 200);

    auto helper = make_helper(slot_reconnect, "reconnect");
    helper->set_reconnect_config(-1, 50);  // unlimited attempts, 50 ms (fast churn)
    const int fds_before = count_fds();
    helper->start();

    int up_samples = 0;
    for (int i = 0; i < 40; ++i) {
        std::this_thread::sleep_for(milliseconds(100));
        if (helper->is_connected()) ++up_samples;
    }
    const int accepted = broker.accepted();
    const bool connected = helper->is_connected();
    const int fds_after = count_fds();
    std::printf("--> accepted connections: %d, connected in %d/40 samples, now: %s, "
                "open fds: %d -> %d\n",
                accepted, up_samples, connected ? "yes" : "no", fds_before, fds_after);
    helper->stop();
    return 0;
}

int probe_leak() {
    std::puts("=== scenario: is mqtt_helper destroyed after stop() + reset? ===");
    install_logger("leak");
    std::weak_ptr<litemqtt::mqtt_helper> weak;
    {
        auto helper = make_helper(slot_leak, "leak");
        helper->set_reconnect_config(0, 10);
        helper->start();
        std::this_thread::sleep_for(milliseconds(300));
        helper->stop();
        weak = helper;
        helper.reset();
    }
    std::this_thread::sleep_for(milliseconds(200));
    const bool destroyed = weak.expired();
    std::printf("--> helper destroyed after stop()+reset: %s\n", destroyed ? "yes" : "NO (leak)");
    return destroyed ? 0 : 1;
}

int probe_leak_nostop() {
    std::puts("=== scenario: user drops the last shared_ptr WITHOUT stop() ===");
    install_logger("cyc");
    dummy_broker broker(probe_port(slot_leak_nostop), 10000);
    std::weak_ptr<litemqtt::mqtt_helper> weak;
    {
        auto helper = make_helper(slot_leak_nostop, "leak_nostop");
        helper->set_reconnect_config(0, 10);
        helper->start();
        std::this_thread::sleep_for(milliseconds(400));
        std::printf("--> connected before drop: %s, accept count: %d\n",
                    helper->is_connected() ? "yes" : "no", broker.accepted());
        weak = helper;
        helper.reset();  // user believes the helper is gone
    }
    std::this_thread::sleep_for(milliseconds(500));
    const bool destroyed = weak.expired();
    std::printf("--> helper+io thread released after dropping the shared_ptr: %s\n",
                destroyed ? "yes" : "NO (self-reference cycle -> leaked, thread still running)");
    return destroyed ? 0 : 1;
}

int probe_stoplag() {
    std::puts("=== scenario: how long does stop() block while reconnect backoff sleeps? ===");
    install_logger("lag");
    auto helper = make_helper(slot_stoplag, "stoplag");
    helper->set_reconnect_config(-1, 5000);  // 5 s backoff
    helper->start();
    std::this_thread::sleep_for(milliseconds(700));  // now inside the 5 s sleep

    const auto t0 = steady_clock::now();
    helper->stop();
    const auto ms = duration_cast<milliseconds>(steady_clock::now() - t0).count();
    std::printf("--> stop() blocked for %lld ms\n", static_cast<long long>(ms));
    return ms < 500 ? 0 : 1;
}

int probe_restart() {
    std::puts("=== scenario: stop() then start() again, does the second run work? ===");
    install_logger("restart");
    dummy_broker broker(probe_port(slot_restart), 5000);  // keep the link up

    auto helper = make_helper(slot_restart, "restart");
    helper->set_reconnect_config(0, 10);
    helper->start();
    std::this_thread::sleep_for(milliseconds(500));
    std::printf("--> first run connected: %s\n", helper->is_connected() ? "yes" : "no");
    helper->stop();

    helper->start();
    std::this_thread::sleep_for(milliseconds(700));
    const bool second = helper->is_connected();
    std::printf("--> second run connected: %s (accepted=%d)\n", second ? "yes" : "no",
                broker.accepted());
    helper->stop();
    return 0;
}

int probe_selfstop() {
    std::puts("=== scenario: user message callback calls helper->stop() (io thread) ===");
    install_logger("self");
    dummy_broker broker(probe_port(slot_selfstop), 10000, /*stall=*/false, /*push_publish=*/true);

    auto helper = make_helper(slot_selfstop, "selfstop");
    helper->set_reconnect_config(0, 10);

    std::atomic<bool> threw{false};
    std::string what;
    helper->on_message([&](std::string topic, std::string payload, uint8_t, uint16_t) {
        std::printf("--> got message on '%s' ('%s'), calling stop() from the io thread\n",
                    topic.c_str(), payload.c_str());
        try {
            helper->stop();
            std::puts("--> stop() returned normally");
        } catch (const std::exception& e) {
            threw = true;
            what = e.what();
            std::printf("--> stop() threw: %s\n", e.what());
        }
    });

    helper->start();
    std::this_thread::sleep_for(seconds(2));
    std::printf("--> stop() from io thread threw: %s%s%s, is_running=%s\n",
                threw.load() ? "yes (" : "no", threw.load() ? what.c_str() : "",
                threw.load() ? ")" : "",
                helper->is_running() ? "true" : "false");
    helper->stop();
    return threw.load() ? 1 : 0;
}

int probe_badcreds() {
    std::puts("=== scenario: broker rejects CONNECT (rc=5, not authorized) ===");
    install_logger("bad");
    dummy_broker broker(probe_port(slot_badcreds), 20000, /*stall=*/false, /*push_publish=*/false,
                        /*connack_rc=*/5);

    auto helper = make_helper(slot_badcreds, "badcreds");
    helper->set_reconnect_config(-1, 300);  // unlimited attempts, 300 ms backoff
    helper->start();

    std::this_thread::sleep_for(seconds(1));
    // Sample how often the broker still sees the rejected session open.  With
    // the client closing it right after CONNACK this should be ~never; if the
    // client leaves it to the next connect attempt it stays open for the whole
    // backoff window of every cycle.
    int open_samples = 0;
    int samples = 0;
    for (int i = 0; i < 100; ++i) {
        if (broker.concurrent() > 0) ++open_samples;
        ++samples;
        std::this_thread::sleep_for(milliseconds(20));
    }
    std::printf("--> accepted=%d still_open=%d, session seen open in %d/%d samples\n",
                broker.accepted(), broker.concurrent(), open_samples, samples);
    helper->stop();
    return 0;
}

int probe_framing() {
    std::puts("=== scenario: 200 x 32 KiB publishes while the broker pushes QoS1 ===");
    install_logger("framing");
    framing_broker broker(probe_port(slot_framing));

    auto helper = make_helper(slot_framing, "framing");
    helper->set_reconnect_config(0, 10);
    std::atomic<int> acked{0};
    std::atomic<int> received{0};
    helper->on_message(
        [&received](std::string, std::string, uint8_t, uint16_t) { ++received; });
    helper->start();
    std::this_thread::sleep_for(milliseconds(400));

    const std::string payload(32 * 1024, 'p');
    for (int i = 0; i < 200; ++i) {
        helper->publish("probe/framing", payload, 0,
                        [&acked](bool ok, std::string, uint8_t, uint16_t) {
                            if (ok) ++acked;
                        });
    }

    std::this_thread::sleep_for(seconds(3));
    std::printf("--> client publishes acked: %d/200, server saw publishes=%d pubacks=%d "
                "framing_errors=%d bytes=%zu, client received=%d\n",
                acked.load(), broker.publishes(), broker.pubacks(), broker.errors(),
                broker.bytes(), received.load());
    helper->stop();
    return (broker.errors() == 0 && acked.load() == 200) ? 0 : 1;
}

int probe_backoff_liveness() {
    std::puts("=== scenario: is the io thread still alive during a 5 s backoff? ===");
    install_logger("live");
    auto helper = make_helper(slot_backoff, "backoff");
    helper->set_reconnect_config(-1, 5000);  // 5 s backoff, no broker listening
    helper->start();
    std::this_thread::sleep_for(milliseconds(800));  // now inside the backoff wait

    std::atomic<bool> called{false};
    std::atomic<long long> latency{-1};
    const auto t0 = steady_clock::now();
    helper->publish("x/y", "z", 0, [&called, &latency, t0](bool, std::string, uint8_t, uint16_t) {
        latency.store(duration_cast<milliseconds>(steady_clock::now() - t0).count());
        called.store(true);
    });

    for (int i = 0; i < 40 && !called.load(); ++i) {
        std::this_thread::sleep_for(milliseconds(50));
    }
    std::printf("--> publish during backoff: called=%s after %lld ms "
                "(a blocking sleep_for would delay it by the rest of the 5 s)\n",
                called.load() ? "yes" : "NO", static_cast<long long>(latency.load()));
    helper->stop();
    return (called.load() && latency.load() < 500) ? 0 : 1;
}

std::atomic<int> g_reconnect_logs{0};

void install_counting_logger() {
    litemqtt::set_log_handler([](const litemqtt::log_record& r) {
        if (r.message.find("Reconnecting in") != std::string::npos) {
            ++g_reconnect_logs;
        }
    });
}

int probe_giveup_restart() {
    std::puts("=== scenario: start() after the helper gave up must retry again ===");
    install_counting_logger();
    auto helper = make_helper(slot_giveup, "giveup");
    helper->set_reconnect_config(2, 20);  // two attempts, 20 ms apart

    helper->start();
    std::this_thread::sleep_for(milliseconds(1200));
    const int session1 = g_reconnect_logs.load();
    std::printf("--> session 1 reconnect attempts: %d (gives up after 2)\n", session1);

    helper->stop();
    g_reconnect_logs.store(0);
    helper->start();
    std::this_thread::sleep_for(milliseconds(1200));
    const int session2 = g_reconnect_logs.load();
    std::printf("--> session 2 reconnect attempts: %d (0 would mean the counter was not reset)\n",
                session2);
    helper->stop();
    return session2 >= 2 ? 0 : 1;
}

int probe_rearm() {
    std::puts("=== scenario: rearm_reconnect() after the attempt limit was hit ===");
    install_logger("client");
    auto helper = make_helper(slot_rearm, "rearm");
    // Give up quickly: two attempts, 200 ms apart -- no broker is listening yet.
    helper->set_reconnect_config(2, 200);
    helper->start();

    bool gave_up = false;
    for (int i = 0; i < 30 && !gave_up; ++i) {
        std::this_thread::sleep_for(milliseconds(100));
        gave_up = !helper->is_reconnect_armed();
    }
    std::printf("--> gave up: %s (running: %s, connected: %s)\n", gave_up ? "yes" : "NO",
                helper->is_running() ? "yes" : "no", helper->is_connected() ? "yes" : "no");

    // The broker only shows up now.  While given up nothing may retry on its own.
    dummy_broker broker(probe_port(slot_rearm), 60000);
    std::this_thread::sleep_for(milliseconds(1000));
    const bool stayed_down = !helper->is_connected();

    const bool armed = helper->rearm_reconnect();
    bool connected = false;
    for (int i = 0; i < 30 && !connected; ++i) {
        std::this_thread::sleep_for(milliseconds(100));
        connected = helper->is_connected();
    }
    std::printf("--> stayed down while given up: %s, rearm_reconnect()=%s, "
                "connected afterwards: %s, accepted=%d\n",
                stayed_down ? "yes" : "NO", armed ? "true" : "false",
                connected ? "yes" : "NO", broker.accepted());

    // Idempotent while armed, refused once stopped.
    const bool again = helper->rearm_reconnect();
    helper->stop();
    const bool after_stop = helper->rearm_reconnect();
    std::printf("--> rearm again while armed: %s, rearm after stop(): %s\n",
                again ? "true" : "FALSE", after_stop ? "TRUE" : "false");

    return (gave_up && stayed_down && armed && connected && again && !after_stop) ? 0 : 1;
}

int probe_fixed_delay() {
    std::puts("=== scenario: set_reconnect_config(-1, 3000) retries forever every 3 s ===");
    // Record when each backoff announcement is logged, to time the interval.
    std::mutex mutex;
    std::vector<long long> stamps;
    const auto t0 = steady_clock::now();
    litemqtt::set_log_handler([&mutex, &stamps, t0](const litemqtt::log_record& r) {
        std::printf("  [client][%s] %s\n", litemqtt::to_string(r.level), r.message.c_str());
        if (r.message.find("Reconnecting in") != std::string::npos) {
            std::lock_guard<std::mutex> lock(mutex);
            stamps.push_back(duration_cast<milliseconds>(steady_clock::now() - t0).count());
        }
    });

    auto helper = make_helper(slot_fixed_delay, "fixed_delay");
    helper->set_reconnect_config(-1, 3000);
    helper->start();

    std::this_thread::sleep_for(milliseconds(7000));  // ~3 retries, no broker yet
    std::size_t early = 0;
    {
        std::lock_guard<std::mutex> lock(mutex);
        early = stamps.size();
    }

    // Still churning four seconds later: an attempt limit would have stopped it.
    std::this_thread::sleep_for(milliseconds(3500));
    std::vector<long long> all;
    {
        std::lock_guard<std::mutex> lock(mutex);
        all = stamps;
    }

    bool interval_ok = all.size() >= 2;
    for (std::size_t i = 1; i < all.size(); ++i) {
        const long long delta = all[i] - all[i - 1];
        std::printf("     retry %zu logged %lld ms after the previous one\n", i + 1, delta);
        if (delta < 2900 || delta > 3600) interval_ok = false;
    }
    std::printf("--> retries after 7 s: %zu, after 10.5 s: %zu (still retrying: %s), "
                "interval ~3000 ms: %s\n",
                early, all.size(), all.size() > early ? "yes" : "NO",
                interval_ok ? "yes" : "NO");

    // Let a broker appear: the unlimited loop has to pick it up by itself.
    dummy_broker broker(probe_port(slot_fixed_delay), 60000);
    bool connected = false;
    for (int i = 0; i < 60 && !connected; ++i) {
        std::this_thread::sleep_for(milliseconds(100));
        connected = helper->is_connected();
    }
    std::printf("--> connected once the broker appeared: %s (accepted=%d)\n",
                connected ? "yes" : "NO", broker.accepted());
    helper->stop();

    return (early >= 3 && interval_ok && all.size() > early && connected) ? 0 : 1;
}

int probe_bigmsg() {
    std::puts("=== scenario: inbound PUBLISH with 1/2/3-byte remaining lengths ===");
    install_logger("bigmsg");
    // 8 bytes -> 1 length byte, 300 bytes -> 2, 100 KiB -> 3
    const std::vector<std::size_t> sizes = {8, 300, 100 * 1024};

    asio::io_context bio;
    asio::ip::tcp::acceptor acc(
        bio, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), probe_port(slot_bigmsg)));
    std::thread server([&bio, &acc, &sizes]() {
        auto sock = std::make_shared<asio::ip::tcp::socket>(bio);
        asio::error_code ec;
        acc.accept(*sock, ec);
        if (ec) return;
        std::vector<char> buf(4096);
        sock->read_some(asio::buffer(buf), ec);  // CONNECT
        if (ec) return;
        const uint8_t connack[4] = {0x20, 0x02, 0x00, 0x00};
        asio::write(*sock, asio::buffer(connack, 4), ec);

        for (std::size_t payload_size : sizes) {
            const std::string topic = "big/topic";
            std::vector<uint8_t> pkt;
            pkt.push_back(0x30);  // PUBLISH, QoS0
            std::size_t len = 2 + topic.size() + payload_size;
            do {
                uint8_t b = static_cast<uint8_t>(len % 128);
                len /= 128;
                if (len != 0) b |= 0x80;
                pkt.push_back(b);
            } while (len != 0);
            pkt.push_back(0x00);
            pkt.push_back(static_cast<uint8_t>(topic.size()));
            pkt.insert(pkt.end(), topic.begin(), topic.end());
            pkt.insert(pkt.end(), payload_size, 'B');
            std::printf("  [server] wrote %zu byte packet (payload %zu)\n", pkt.size(),
                        payload_size);
            asio::write(*sock, asio::buffer(pkt), ec);
        }

        std::this_thread::sleep_for(seconds(3));  // keep the link open
        sock->close(ec);
    });

    auto helper = make_helper(slot_bigmsg, "bigmsg");
    helper->set_reconnect_config(0, 10);
    std::atomic<int> n{0};
    std::atomic<int> mismatches{0};
    std::vector<std::size_t> got(sizes.size(), 0);
    helper->on_message([&n, &mismatches, &got, &sizes](std::string topic, std::string payload,
                                                      uint8_t, uint16_t) {
        const int i = n.load();
        if (i < static_cast<int>(sizes.size())) {
            got[static_cast<std::size_t>(i)] = payload.size();
            const std::size_t expected = sizes[static_cast<std::size_t>(i)];
            const bool ok = (topic == "big/topic") && (payload.size() == expected);
            if (!ok) ++mismatches;
            std::printf("  [client] #%d topic='%s' payload=%zu (expected %zu) %s\n", i,
                        topic.substr(0, 12).c_str(), payload.size(), expected,
                        ok ? "ok" : "MISMATCH");
        }
        ++n;
    });
    helper->start();
    std::this_thread::sleep_for(seconds(2));

    std::printf("--> received=%d/3 mismatches=%d\n", n.load(), mismatches.load());
    helper->stop();
    server.join();
    return (n.load() == 3 && mismatches.load() == 0) ? 0 : 1;
}

int probe_bigpub() {
    std::puts("=== scenario: large publish while the broker stalls (deferred async_write) ===");
    install_logger("bigpub");
    dummy_broker broker(probe_port(slot_bigpub), 10000, /*stall=*/true);
    auto helper = make_helper(slot_bigpub, "bigpub");
    helper->set_reconnect_config(0, 10);
    helper->start();
    std::this_thread::sleep_for(milliseconds(400));

    const std::string payload(4 * 1024 * 1024, 'x');  // 4 MiB: forces partial writes
    for (int i = 0; i < 3; ++i) {
        helper->publish("probe/big", payload, 0, nullptr);
    }
    std::this_thread::sleep_for(seconds(1));
    std::puts("--> 3 x 4 MiB published, waiting for the broker to resume reading");
    std::this_thread::sleep_for(seconds(4));
    helper->stop();
    std::puts("--> done");
    return 0;
}

struct scenario {
    const char* name;
    const char* description;
    int (*run)();
};

const scenario g_scenarios[] = {
    {"reconnect", "broker drops the link; does the helper come back?", probe_reconnect},
    {"leak", "is the helper destroyed after stop() + reset?", probe_leak},
    {"leak_nostop", "last owner drops the helper without calling stop()", probe_leak_nostop},
    {"stoplag", "stop() latency while a long backoff timer is armed", probe_stoplag},
    {"restart", "does start() work after stop()?", probe_restart},
    {"selfstop", "stop() called from the io thread (message callback)", probe_selfstop},
    {"badcreds", "broker rejects CONNECT; is the session closed at once?", probe_badcreds},
    {"framing", "200 x 32 KiB publishes while the broker pushes QoS1", probe_framing},
    {"backoff", "is the io thread responsive during a 5 s backoff?", probe_backoff_liveness},
    {"giveup", "are the attempts reset by a later start()?", probe_giveup_restart},
    {"rearm", "rearm_reconnect() resumes after the attempt limit", probe_rearm},
    {"fixed_delay", "set_reconnect_config(-1, 3000) keeps retrying", probe_fixed_delay},
    {"bigmsg", "inbound PUBLISH with 1/2/3 byte remaining lengths", probe_bigmsg},
    {"bigpub", "3 x 4 MiB publish into a stalled peer", probe_bigpub},
};

const int g_scenario_count = static_cast<int>(sizeof(g_scenarios) / sizeof(g_scenarios[0]));

void print_usage() {
    std::printf("usage: reconnect_probe <scenario>|all\n\nscenarios:\n");
    for (int i = 0; i < g_scenario_count; ++i) {
        std::printf("  %-12s %s\n", g_scenarios[i].name, g_scenarios[i].description);
    }
    std::printf("\nports: tcp 127.0.0.1, base %u + one slot per scenario; "
                "override with LITEMQTT_PROBE_PORT_BASE\n",
                static_cast<unsigned>(probe_port_base()));
}

int run_scenario(const char* name) {
    for (int i = 0; i < g_scenario_count; ++i) {
        if (std::strcmp(name, g_scenarios[i].name) == 0) return g_scenarios[i].run();
    }
    return -1;
}

}  // namespace

int main(int argc, char** argv) {
    // Unbuffered, so the interesting part survives a crash mid-scenario.
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::setvbuf(stderr, nullptr, _IONBF, 0);
#if defined(__linux__)
    std::signal(SIGSEGV, crash_handler);
    std::signal(SIGABRT, crash_handler);
    std::signal(SIGBUS, crash_handler);
#endif

    const std::string what = argc > 1 ? argv[1] : "all";
    if (what == "-h" || what == "--help" || what == "help") {
        print_usage();
        return 0;
    }
    std::printf("MAIN pid=%ld\n", probe_pid());

    if (what != "all") {
        const int rc = run_scenario(what.c_str());
        if (rc < 0) {
            std::printf("unknown scenario '%s'\n\n", what.c_str());
            print_usage();
            return 2;
        }
        return rc;
    }

    int failures = 0;
    for (int i = 0; i < g_scenario_count; ++i) {
        const int rc = g_scenarios[i].run();
        std::printf(">>> %-12s %s\n", g_scenarios[i].name, rc == 0 ? "PASS" : "FAIL");
        if (rc != 0) ++failures;
    }
    std::printf(">>> %d/%d scenarios passed\n", g_scenario_count - failures, g_scenario_count);
    return failures == 0 ? 0 : 1;
}

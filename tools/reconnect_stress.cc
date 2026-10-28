// Chaos test for the reconnect path.
//
// Publishes at a steady rate while a local broker goes through the kinds of
// trouble a real deployment sees -- outage, blackhole (socket accepted, nothing
// comes back), flapping link, slow reader (backpressure) -- and checks the two
// things that matter:
//
//   * robustness: the helper keeps publishing, reconnects on its own and never
//     gets stuck in a half-open session;
//   * resources: descriptor count, thread count and RSS stay flat across
//     hundreds of reconnects, and everything is released after stop().
//
// Every publish must be reported exactly once (success or failure); a silent drop
// is a failure of the run, because a publisher loop that waits on the callback
// would stall.
//
// Build:
//   g++ -std=c++14 -O2 -Wall -Wextra -pthread -I inc -I 3rdparty/asio/include
//       -DASIO_STANDALONE tools/reconnect_stress.cc -o reconnect_stress
//   (or: cmake --build build --target reconnect_stress)
//
// Run:
//   ./reconnect_stress --help
//   ./reconnect_stress --scenario flap --duration 60
//   ./reconnect_stress --scenario soak --duration 300 --rate 200
//
// Exit code 0 means the run was clean.
#include <asio.hpp>

#if defined(__linux__)
#include <dirent.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "litemqtt/mqtt_helper.hpp"

namespace {

using namespace std::chrono;

// ---------------------------------------------------------------------------
// Process accounting (/proc based; the counters are -1 where unavailable)
// ---------------------------------------------------------------------------

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

int count_threads() {
#if defined(__linux__)
    int n = 0;
    if (FILE* f = std::fopen("/proc/self/status", "r")) {
        char line[256];
        while (std::fgets(line, sizeof(line), f)) {
            if (std::strncmp(line, "Threads:", 8) == 0) {
                n = std::atoi(line + 8);
                break;
            }
        }
        std::fclose(f);
    }
    return n;
#else
    return -1;
#endif
}

long rss_kb() {
#if defined(__linux__)
    long kb = -1;
    if (FILE* f = std::fopen("/proc/self/status", "r")) {
        char line[256];
        while (std::fgets(line, sizeof(line), f)) {
            if (std::strncmp(line, "VmRSS:", 6) == 0) {
                kb = std::atol(line + 6);
                break;
            }
        }
        std::fclose(f);
    }
    return kb;
#else
    return -1;
#endif
}

// ---------------------------------------------------------------------------
// Chaos broker: one io thread, a mode that the driver switches at will
// ---------------------------------------------------------------------------

enum class link_state {
    up,         // accept, CONNACK, PUBACK
    down,       // not listening at all (connection refused)
    blackhole,  // accept and then stay silent: no CONNACK, no PUBACK
    flap,       // normal, but every session is dropped after a short delay
    slow,       // normal, but reads are throttled (backpressure while publishing)
};

const char* to_string(link_state s) {
    switch (s) {
    case link_state::up: return "up";
    case link_state::down: return "down";
    case link_state::blackhole: return "blackhole";
    case link_state::flap: return "flap";
    case link_state::slow: return "slow";
    }
    return "?";
}

class chaos_broker {
public:
    // 0 asks the OS for a free port, which is what makes concurrent runs safe: the
    // client talks to this process' own listener and nobody else's.
    explicit chaos_broker(int port) : port_(port), acceptor_(io_) {
        work_.reset(new asio::io_context::work(io_));
    }

    ~chaos_broker() { shutdown(); }

    // Binds the listener on the calling thread and reports whether we own the port,
    // *before* the client is allowed to connect.  A silent bind failure used to leave
    // the client talking to whatever else held the port (a previous run, another
    // harness), and the run then measured someone else's broker: the counters we
    // print would come from a broker that never saw those publishes.
    bool start() {
        if (!open_listener()) return false;
        thread_ = std::thread([this]() { io_.run(); });
        return true;
    }

    // The port the broker actually owns (resolved after the first bind when 0 was asked
    // for); valid once start() has returned true.
    int port() const { return port_; }

    void shutdown() {
        if (!thread_.joinable()) return;
        // Clean up on the io thread (it owns the sockets) and stop the context from
        // there, then wait: stopping it here would drop the posted cleanup and leave
        // the descriptors open.
        asio::post(io_, [this]() {
            asio::error_code ec;
            acceptor_.close(ec);
            close_sessions();
            work_.reset();
            io_.stop();
        });
        thread_.join();
    }

    // Called from the driver thread; the switch itself happens on the io thread.
    void set_state(link_state s) {
        state_.store(s);
        asio::post(io_, [this]() { apply_state(); });
    }

    int accepted() const { return accepted_.load(); }
    int connacks() const { return connacks_.load(); }
    int publishes() const { return publishes_.load(); }
    int pubacks() const { return pubacks_.load(); }
    int framing_errors() const { return framing_errors_.load(); }
    int ordering_errors() const { return ordering_errors_.load(); }
    int resets() const { return resets_.load(); }
    int held_blackholes() const { return held_.load(); }
    link_state state_snapshot() const { return state_.load(); }
    // Set when the broker lost the port mid-run: every counter from then on describes
    // a different process, so the driver has to stop rather than report numbers.
    bool fatal() const { return fatal_.load(); }

private:
    struct session : std::enable_shared_from_this<session> {
        explicit session(asio::io_context& io) : sock(io), timer(io) {}
        asio::ip::tcp::socket sock;
        asio::steady_timer timer;
        std::string buf;
        uint16_t last_pid = 0;
        long long last_seq = -1;
        bool have_last = false;
    };

    void apply_state() {
        const link_state s = state_.load();
        if (s == link_state::down) {
            asio::error_code ec;
            acceptor_.close(ec);
            close_sessions();
            return;
        }
        if (!acceptor_.is_open() && !open_listener()) {
            // Losing the port between an outage and the recovery means the client will
            // reconnect to somebody else's listener; the run is meaningless from here.
            fatal_.store(true);
            io_.stop();
            return;
        }
        if (s == link_state::blackhole) {
            // Old sessions die, new ones are accepted and then ignored.
            close_sessions();
        }
        do_accept();
    }

    // Open / bind / listen the announced port.  Returns false (after printing why)
    // when we do not own it, which the caller treats as fatal.
    bool open_listener() {
        asio::error_code ec;
        acceptor_.open(asio::ip::tcp::v4(), ec);
        if (ec) {
            std::printf("  [broker] cannot open the listener: %s\n", ec.message().c_str());
            return false;
        }
#if defined(_WIN32)
        // Windows treats SO_REUSEADDR as "another process may also bind this port", and
        // then hands connections to whichever socket wins the race -- a second run would
        // steal half of the first one's sessions.  SO_EXCLUSIVEADDRUSE makes the second
        // bind fail, which is what tells us the port is not ours.
        {
            BOOL exclusive = TRUE;
            ::setsockopt(acceptor_.native_handle(), SOL_SOCKET, SO_EXCLUSIVEADDRUSE,
                         reinterpret_cast<const char*>(&exclusive), sizeof(exclusive));
        }
#else
        // Needed on POSIX so accepted sockets left in TIME_WAIT by the previous outage do
        // not stop us from re-binding the same port.
        acceptor_.set_option(asio::ip::tcp::acceptor::reuse_address(true), ec);
#endif
        acceptor_.bind(asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port_), ec);
        if (ec) {
            std::printf("  [broker] cannot bind port %u: %s\n", (unsigned)port_,
                        ec.message().c_str());
            // Do not leave a half-open socket behind: the next attempt must be a clean
            // open/bind, not a retry on this one.
            asio::error_code ignored;
            acceptor_.close(ignored);
            return false;
        }
        // port_ == 0 meant "any free port": remember what the OS picked so that the
        // re-bind after a scripted outage goes back to the port the client knows.
        asio::error_code lec;
        const int bound = acceptor_.local_endpoint(lec).port();
        if (!lec && bound > 0) port_ = bound;
        acceptor_.listen(asio::socket_base::max_connections, ec);
        if (ec) {
            std::printf("  [broker] cannot listen on port %u: %s\n", (unsigned)port_,
                        ec.message().c_str());
            return false;
        }
        return true;
    }

    void do_accept() {
        if (!acceptor_.is_open() || accepting_) return;
        accepting_ = true;
        auto s = std::make_shared<session>(io_);
        acceptor_.async_accept(s->sock, [this, s](const asio::error_code& ec) {
            accepting_ = false;
            if (!ec) {
                ++accepted_;
                on_session(s);
            }
            if (acceptor_.is_open()) do_accept();
        });
    }

    void on_session(const std::shared_ptr<session>& s) {
        sessions_.push_back(s);
        if (state_.load() == link_state::blackhole) {
            ++held_;  // accept and never say a word
            return;
        }
        if (state_.load() == link_state::flap) {
            auto weak = std::weak_ptr<session>(s);
            s->timer.expires_after(milliseconds(80));
            s->timer.async_wait([this, weak](const asio::error_code& ec) {
                if (ec) return;
                auto self = weak.lock();
                if (!self) return;
                asio::error_code ignored;
                self->sock.close(ignored);
                ++resets_;
                forget(self);
            });
        }
        read_loop(s);
    }

    void forget(const std::shared_ptr<session>& s) {
        for (std::size_t i = 0; i < sessions_.size(); ++i) {
            if (sessions_[i] == s) {
                sessions_.erase(sessions_.begin() + static_cast<long>(i));
                return;
            }
        }
    }

    void close_sessions() {
        std::vector<std::shared_ptr<session>> copy;
        copy.swap(sessions_);
        for (auto& s : copy) {
            asio::error_code ignored;
            s->timer.cancel(ignored);
            s->sock.close(ignored);
        }
    }

    void read_loop(const std::shared_ptr<session>& s) {
        auto self = s;
        auto buffer = std::make_shared<std::vector<uint8_t>>(4096);
        s->sock.async_read_some(asio::buffer(*buffer), [this, self, buffer](
                                                           const asio::error_code& ec,
                                                           std::size_t n) {
            if (ec) {
                forget(self);
                return;
            }
            self->buf.append(reinterpret_cast<const char*>(buffer->data()), n);
            process(self);
            if (!self->sock.is_open()) {
                forget(self);
                return;
            }
            if (state_.load() == link_state::slow) {
                // Throttle: one chunk every 3 ms keeps the client's write queue
                // backed up, which is what a congested link looks like.
                auto weak = std::weak_ptr<session>(self);
                self->timer.expires_after(milliseconds(3));
                self->timer.async_wait([this, weak](const asio::error_code& ec) {
                    if (ec) return;
                    if (auto locked = weak.lock()) read_loop(locked);
                });
                return;
            }
            read_loop(self);
        });
    }

    void process(const std::shared_ptr<session>& s) {
        for (;;) {
            if (s->buf.size() < 2) return;
            const uint8_t type = static_cast<uint8_t>(s->buf[0]);
            std::size_t remaining = 0;
            std::size_t multiplier = 1;
            std::size_t i = 1;
            bool complete_length = false;
            while (i < s->buf.size()) {
                const uint8_t byte = static_cast<uint8_t>(s->buf[i++]);
                remaining += static_cast<std::size_t>(byte & 0x7f) * multiplier;
                multiplier *= 128;
                if (multiplier > 128u * 128u * 128u) {
                    ++framing_errors_;
                    s->buf.clear();
                    return;
                }
                if ((byte & 0x80) == 0) {
                    complete_length = true;
                    break;
                }
            }
            if (!complete_length) return;
            if (s->buf.size() < i + remaining) return;

            const std::string body = s->buf.substr(i, remaining);
            s->buf.erase(0, i + remaining);

            handle_packet(s, type, body);
            if (!s->sock.is_open()) return;
        }
    }

    static long long payload_seq(const std::string& payload) {
        // payload is "seq=<n>|<filler>"
        if (payload.size() < 4 || payload.compare(0, 4, "seq=") != 0) return -1;
        return std::atoll(payload.c_str() + 4);
    }

    void handle_packet(const std::shared_ptr<session>& s, uint8_t type, const std::string& body) {
        const uint8_t kind = type & 0xf0;
        if (kind == 0x10) {  // CONNECT
            ++connacks_;
            send(s, {0x20, 0x02, 0x00, 0x00});
        } else if (kind == 0x30) {  // PUBLISH
            if (body.size() < 2) {
                ++framing_errors_;
                return;
            }
            const std::size_t topic_len =
                (static_cast<uint8_t>(body[0]) << 8) | static_cast<uint8_t>(body[1]);
            if (body.size() < 2 + topic_len) {
                ++framing_errors_;
                return;
            }
            std::size_t pos = 2 + topic_len;
            const uint8_t qos = (type >> 1) & 0x03;
            uint16_t pid = 0;
            if (qos > 0) {
                if (body.size() < pos + 2) {
                    ++framing_errors_;
                    return;
                }
                pid = static_cast<uint16_t>((static_cast<uint8_t>(body[pos]) << 8) |
                                            static_cast<uint8_t>(body[pos + 1]));
                pos += 2;
            }
            const std::string payload = body.substr(pos);
            const long long seq = payload_seq(payload);

            // Within one session the client must publish in order, and a repeated
            // message (QoS 1 retransmission) must keep its packet id.
            if (seq >= 0) {
                if (s->have_last) {
                    if (seq < s->last_seq) {
                        ++ordering_errors_;
                    } else if (seq == s->last_seq && pid != s->last_pid) {
                        ++ordering_errors_;
                    }
                }
                s->last_seq = seq;
                s->last_pid = pid;
                s->have_last = true;
            }

            ++publishes_;
            if (qos == 1) {
                ++pubacks_;
                send(s, {0x40, 0x02, static_cast<uint8_t>(pid >> 8), static_cast<uint8_t>(pid)});
            }
        } else if (kind == 0xc0) {  // PINGREQ
            send(s, {0xd0, 0x00});
        } else if (kind == 0xe0) {  // DISCONNECT
            asio::error_code ignored;
            s->sock.close(ignored);
        }
    }

    void send(const std::shared_ptr<session>& s, std::vector<uint8_t> data) {
        auto self = s;
        asio::async_write(s->sock, asio::buffer(data),
                          [self, data](const asio::error_code&, std::size_t) {});
    }

    int port_;
    asio::io_context io_;
    asio::ip::tcp::acceptor acceptor_;
    std::unique_ptr<asio::io_context::work> work_;
    std::thread thread_;
    std::vector<std::shared_ptr<session>> sessions_;
    bool accepting_ = false;
    std::atomic<link_state> state_{link_state::up};
    std::atomic<bool> fatal_{false};

    std::atomic<int> accepted_{0};
    std::atomic<int> connacks_{0};
    std::atomic<int> publishes_{0};
    std::atomic<int> pubacks_{0};
    std::atomic<int> framing_errors_{0};
    std::atomic<int> ordering_errors_{0};
    std::atomic<int> resets_{0};
    std::atomic<int> held_{0};
};

// ---------------------------------------------------------------------------
// Publish accounting: every message must come back exactly once
// ---------------------------------------------------------------------------

struct publish_stats {
    void record(long long seq, bool ok) {
        std::lock_guard<std::mutex> lock(mutex);
        const int count = ++calls[seq];
        if (count > 1) ++duplicates;
        if (ok) ++succeeded;
        else ++failed;
    }

    std::size_t pending() {
        std::lock_guard<std::mutex> lock(mutex);
        return static_cast<std::size_t>(issued - (succeeded + failed));
    }

    std::mutex mutex;
    std::map<long long, int> calls;
    long long issued = 0;
    long long succeeded = 0;
    long long failed = 0;
    long long duplicates = 0;
};

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

struct options {
    std::string scenario = "flap";
    int duration_s = 60;
    int rate_hz = 300;
    std::size_t payload = 64;
    int reconnect_delay_ms = 100;
    int max_attempts = -1;
    int connect_timeout_ms = 2000;
    long rss_limit_mb = 32;
    int port = 0;   // 0 = let the OS pick one (concurrent runs stay independent)
};

void print_usage() {
    std::printf(
        "usage: reconnect_stress [--scenario NAME] [--duration SECONDS] [--rate HZ]\n"
        "                        [--payload BYTES] [--reconnect-delay MS]\n"
        "                        [--max-attempts N] [--connect-timeout MS]\n"
        "                        [--rss-limit-mb N] [--port N]\n"
        "\n"
        "scenarios (all publish continuously while the link misbehaves):\n"
        "  flap      short outages, ~0.5 s up / ~0.5 s down\n"
        "  long      long outages: 10 s up, 15 s down\n"
        "  blackhole accepted sockets that never answer\n"
        "  weak      always reachable but the broker reads slowly (backpressure)\n"
        "  mixed     everything above in one run (used for soak testing)\n"
        "  soak      alias of mixed\n"
        "\n"
        "the broker listens on 127.0.0.1 and picks a free port unless --port is given;\n"
        "the client is always pointed at that broker, so runs never share traffic.\n"
        "give it at least ~30 s: weak/mixed start with a weak-but-working link, so a\n"
        "very short run can finish without ever reaching an outage (and then fails\n"
        "the \"link broke and came back\" check by design).\n"
        "\n"
        "exit code 0 = no lost callbacks, no descriptor/thread leak, stable RSS\n"
        "exit code 2 = --port is already taken by another process\n"
        "\n"
        "note: under AddressSanitizer the RSS check is meaningless (shadow memory and\n"
        "      quarantine), so raise or disable it with a large --rss-limit-mb value;\n"
        "      leaks are then reported by LeakSanitizer at exit instead.\n");
}

struct schedule_step {
    link_state state;
    int duration_ms;
};

std::vector<schedule_step> build_schedule(const std::string& name) {
    std::vector<schedule_step> s;
    if (name == "flap") {
        for (int i = 0; i < 120; ++i) {
            s.push_back({link_state::up, 500});
            s.push_back({link_state::down, 500});
        }
    } else if (name == "long") {
        for (int i = 0; i < 12; ++i) {
            s.push_back({link_state::up, 10000});
            s.push_back({link_state::down, 15000});
        }
    } else if (name == "blackhole") {
        for (int i = 0; i < 120; ++i) {
            s.push_back({link_state::blackhole, 2000});
            s.push_back({link_state::up, 2000});
        }
    } else if (name == "weak") {
        // Congested link: slow reads, plus the occasional drop when the backlog
        // makes the session time out -- that is where weak networks hurt.
        for (int i = 0; i < 120; ++i) {
            s.push_back({link_state::slow, 3000});
            s.push_back({link_state::down, 700});
            s.push_back({link_state::up, 1300});
        }
    } else {  // mixed / soak
        for (int i = 0; i < 200; ++i) {
            s.push_back({link_state::up, 4000});
            s.push_back({link_state::down, 3000});
            s.push_back({link_state::up, 2000});
            s.push_back({link_state::flap, 2000});
            s.push_back({link_state::up, 2000});
            s.push_back({link_state::blackhole, 3000});
            s.push_back({link_state::up, 2000});
            s.push_back({link_state::slow, 3000});
        }
    }
    return s;
}

}  // namespace

int main(int argc, char** argv) {
    options opt;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&]() -> const char* { return (i + 1 < argc) ? argv[++i] : ""; };
        if (a == "--help" || a == "-h") {
            print_usage();
            return 0;
        } else if (a == "--scenario") {
            opt.scenario = next();
        } else if (a == "--duration") {
            opt.duration_s = std::atoi(next());
        } else if (a == "--rate") {
            opt.rate_hz = std::atoi(next());
        } else if (a == "--payload") {
            opt.payload = static_cast<std::size_t>(std::atol(next()));
        } else if (a == "--reconnect-delay") {
            opt.reconnect_delay_ms = std::atoi(next());
        } else if (a == "--max-attempts") {
            opt.max_attempts = std::atoi(next());
        } else if (a == "--connect-timeout") {
            opt.connect_timeout_ms = std::atoi(next());
        } else if (a == "--port") {
            opt.port = std::atoi(next());
            if (opt.port < 0 || opt.port > 65535) {
                std::printf("--port needs a value between 0 (any free port) and 65535\n");
                return 2;
            }
        } else if (a == "--rss-limit-mb") {
            opt.rss_limit_mb = std::atol(next());
            if (opt.rss_limit_mb <= 0) opt.rss_limit_mb = 1;
        } else {
            std::printf("unknown option '%s'\n\n", a.c_str());
            print_usage();
            return 2;
        }
    }
    if (opt.rate_hz <= 0) opt.rate_hz = 1;
    if (opt.duration_s <= 0) opt.duration_s = 1;

    std::printf("=== reconnect_stress: scenario=%s duration=%ds rate=%dHz payload=%zuB "
                "reconnect=%dms max_attempts=%d connect_timeout=%dms\n",
                opt.scenario.c_str(), opt.duration_s, opt.rate_hz, opt.payload,
                opt.reconnect_delay_ms, opt.max_attempts, opt.connect_timeout_ms);

    const int fds_before = count_fds();
    const int threads_before = count_threads();
    const long rss_before = rss_kb();

    publish_stats stats;
    int max_fds = 0;
    int max_threads = 0;
    long max_rss = 0;
    int b_accepted = 0;
    int b_connacks = 0;
    bool b_fatal = false;
    int b_publishes = 0;
    int b_pubacks = 0;
    int b_framing_errors = 0;
    int b_ordering_errors = 0;
    int b_resets = 0;
    int b_held = 0;
    std::size_t still_pending = 0;
    long long stop_ms = 0;

    {
        // Kept in this scope so that both the broker and the helper are gone before
        // the final descriptor count: an io_context owns an epoll descriptor, which
        // would otherwise look like a leak of the run.
        std::unique_ptr<chaos_broker> broker(new chaos_broker(opt.port));
        if (!broker->start()) {
            // Do not go on: the client would connect to whoever owns this port and every
            // number below would describe that listener instead of this run.
            std::printf("\n*** port %u is not available -- is another reconnect_stress "
                        "running?  Drop --port to let the OS pick a free one. ***\n",
                        (unsigned)opt.port);
            return 2;
        }
        const uint16_t port = static_cast<uint16_t>(broker->port());
        std::printf("broker listening on 127.0.0.1:%u\n", (unsigned)port);

        auto helper = std::make_shared<litemqtt::mqtt_helper>("127.0.0.1", port, "stress");
        helper->set_connect_timeout(opt.connect_timeout_ms);
        helper->set_keep_alive(5);  // keep the keepalive path exercised too
        helper->set_reconnect_config(opt.max_attempts, opt.reconnect_delay_ms);
        helper->set_log_handler([](const litemqtt::log_record& r) {
            if (r.level == litemqtt::log_level::error || r.level == litemqtt::log_level::warn) {
                std::printf("  [helper][%s] %s\n", litemqtt::to_string(r.level),
                            r.message.c_str());
            }
        });

        std::atomic<bool> publishing{true};
        const auto start = steady_clock::now();

        helper->start();

        std::thread publisher([&]() {
            long long seq = 0;
            const auto period = microseconds(1000000 / opt.rate_hz);
            auto next_at = steady_clock::now();
            const std::string filler(std::max<std::size_t>(opt.payload, 8), 'x');
            while (publishing.load()) {
                ++seq;
                const std::string payload = "seq=" + std::to_string(seq) + "|" + filler;
                {
                    std::lock_guard<std::mutex> lock(stats.mutex);
                    ++stats.issued;
                }
                helper->publish("stress/topic", payload, 1,
                                [&stats, seq](bool ok, std::string, uint8_t, uint16_t) {
                                    stats.record(seq, ok);
                                });
                next_at += period;
                std::this_thread::sleep_until(next_at);
            }
        });

        // Drive the link through the schedule and sample the process while doing so.
        const auto schedule = build_schedule(opt.scenario);
        const auto deadline = start + seconds(opt.duration_s);
        std::size_t step = 0;
        auto step_started = steady_clock::now();
        if (!schedule.empty()) broker->set_state(schedule[0].state);

        int reconnects_seen = 0;
        auto last_sample = steady_clock::now();

        while (steady_clock::now() < deadline) {
            std::this_thread::sleep_for(milliseconds(50));
            const auto now = steady_clock::now();

            if (broker->fatal()) {
                // The broker lost the port: from here on the client talks to someone
                // else and the numbers below would be fiction.  Stop and fail.
                std::printf("  [broker] lost port %u mid-run -- aborting\n", (unsigned)port);
                break;
            }

            if (!schedule.empty() &&
                now - step_started > milliseconds(schedule[step].duration_ms)) {
                ++step;
                step_started = now;
                if (step < schedule.size()) {
                    broker->set_state(schedule[step].state);
                } else {
                    step = 0;  // restart the cycle
                    broker->set_state(schedule[0].state);
                }
            }

            if (now - last_sample >= seconds(1)) {
                last_sample = now;
                const int fds = count_fds();
                const int threads = count_threads();
                const long rss = rss_kb();
                max_fds = std::max(max_fds, fds);
                max_threads = std::max(max_threads, threads);
                max_rss = std::max(max_rss, rss);
                if (broker->accepted() > reconnects_seen) reconnects_seen = broker->accepted();
                std::printf("  [%4llds] %-9s fds=%3d threads=%2d rss=%5ldkB | sessions=%d "
                            "pubs=%d pubacks=%d ok=%lld fail=%lld\n",
                            (long long)duration_cast<seconds>(now - start).count(),
                            to_string(broker->state_snapshot()), fds, threads, rss,
                            broker->accepted(), broker->publishes(), broker->pubacks(),
                            (long long)stats.succeeded, (long long)stats.failed);
                std::fflush(stdout);
            }
        }

        // Stop publishing and give the in-flight publishes a chance to be reported.
        publishing.store(false);
        publisher.join();
        const auto drain_deadline = steady_clock::now() + seconds(10);
        while (stats.pending() > 0 && steady_clock::now() < drain_deadline) {
            std::this_thread::sleep_for(milliseconds(50));
        }
        still_pending = stats.pending();

        const auto stop_begin = steady_clock::now();
        helper->stop();
        stop_ms = duration_cast<milliseconds>(steady_clock::now() - stop_begin).count();
        helper.reset();
        broker->shutdown();

        b_accepted = broker->accepted();
        b_connacks = broker->connacks();
        b_fatal = broker->fatal();
        b_publishes = broker->publishes();
        b_pubacks = broker->pubacks();
        b_framing_errors = broker->framing_errors();
        b_ordering_errors = broker->ordering_errors();
        b_resets = broker->resets();
        b_held = broker->held_blackholes();
    }

    // Let anything that was queued for deletion land.
    std::this_thread::sleep_for(milliseconds(300));

    const int fds_after = count_fds();
    const int threads_after = count_threads();
    const long rss_after = rss_kb();

    std::printf("\n---- summary (%s, %ds) ----\n", opt.scenario.c_str(), opt.duration_s);
    std::printf("sessions      : %d accepted, %d CONNACKed, %d reset by broker, %d blackholed\n",
                b_accepted, b_connacks, b_resets, b_held);
    std::printf("publish       : %lld issued, %lld ok, %lld failed, %zu pending at end, "
                "%lld double-reported\n",
                stats.issued, stats.succeeded, stats.failed, still_pending, stats.duplicates);
    std::printf("broker        : %d PUBLISH received, %d PUBACK sent, %d framing errors, "
                "%d ordering errors\n",
                b_publishes, b_pubacks, b_framing_errors, b_ordering_errors);
    std::printf("resources     : fds %d -> %d (max %d), threads %d -> %d (max %d), "
                "rss %ld -> %ld kB (max %ld)\n",
                fds_before, fds_after, max_fds, threads_before, threads_after, max_threads,
                rss_before, rss_after, max_rss);
    std::printf("stop()        : %lld ms\n", stop_ms);

    int failures = 0;
    auto check = [&failures](bool ok, const char* what) {
        std::printf("  [%s] %s\n", ok ? "PASS" : "FAIL", what);
        if (!ok) ++failures;
    };

    check(still_pending == 0, "every publish was reported back");
    check(stats.duplicates == 0, "no publish was reported twice");
    check(!b_fatal, "the broker kept the port for the whole run");
    check(stats.succeeded > 0, "some publishes actually went through");
    check(b_publishes > 0, "the broker received publishes");
    check(b_framing_errors == 0, "the byte stream stayed in sync");
    check(b_ordering_errors == 0, "ordering and packet ids stayed consistent");
    check(b_accepted > 1, "the link actually broke and came back");
    check(stop_ms < 3000, "stop() does not hang");
    check(fds_after <= fds_before + 1, "no descriptor leak");
    check(threads_after <= threads_before, "no thread leak");
    check(rss_after - rss_before < opt.rss_limit_mb * 1024, "RSS did not grow unboundedly");

    std::printf("---- %s (%d failure(s)) ----\n", failures == 0 ? "CLEAN" : "PROBLEMS", failures);
    return failures == 0 ? 0 : 1;
}

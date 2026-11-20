#ifndef LITEMQTT_HELPER_HPP
#define LITEMQTT_HELPER_HPP

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "client.hpp"
#include "log.hpp"

namespace litemqtt {

using message_callback = std::function<void(
    std::string topic,      // Topic
    std::string payload,    // Message payload
    uint8_t qos,            // QoS level
    uint16_t packet_id      // Packet ID
)>;

/// Connection state transition.  Unlike mqtt_client's connect_cb this fires once
/// per real change: a state that is already reported is never reported again, and
/// a failed attempt is one event rather than a failure plus a close.
enum class connection_event {
    connected,      ///< CONNACK accepted, session usable (subscriptions replayed)
    disconnected,   ///< the connection that was up has been closed
    connect_failed  ///< one connect attempt failed (CONNACK != 0 / timeout / TCP)
};

/// `return_code` is the CONNACK return code for connect_failed (255 for a
/// transport level failure, see mqtt_client), and always 0 for the other events.
using connection_callback = std::function<void(connection_event event, uint8_t return_code)>;

class mqtt_helper : public std::enable_shared_from_this<mqtt_helper> {
public:
    mqtt_helper() = delete;
    explicit mqtt_helper(const std::string& host, uint16_t port,
                          const std::string& client_id);
    ~mqtt_helper();

    mqtt_helper(const mqtt_helper&) = delete;
    mqtt_helper& operator=(const mqtt_helper&) = delete;
    mqtt_helper(mqtt_helper&&) = delete;
    mqtt_helper& operator=(mqtt_helper&&) = delete;

    // Configuration methods
    void set_credentials(const std::string& username, const std::string& password);
    void set_keep_alive(uint16_t seconds);
    void set_clean_session(bool clean);
    /// Abandon a connect attempt (TCP connect + CONNACK wait) after `ms`
    /// milliseconds and let the reconnect policy take over; 0 disables it.
    /// Configure it before start().  Default 10000 ms.
    void set_connect_timeout(int ms);
    /// Largest inbound packet body accepted; 0 disables the check.  Configure it
    /// before start().  Default connection::kDefaultMaxPacketSize (1 MiB), which
    /// covers ordinary IoT payloads but not a large retained document.  A broker
    /// that declares a bigger packet has its read failed rather than honoured.
    void set_max_packet_size(std::size_t bytes);
    /// Upper bound on queued outbound frames; 0 disables the check.  Configure it
    /// before start().  Default connection::kDefaultMaxWriteQueue (1024).  Once
    /// the queue is full a publish fails through its callback rather than being
    /// buffered, so a broker that stops reading cannot grow it without bound.
    void set_max_write_queue(std::size_t frames);
    /// Automatic reconnection policy; configure it before start().
    ///   max_attempts == -1  retry forever, always waiting base_delay_ms
    ///                       (no exponential growth)
    ///   max_attempts >= 0   give up after that many consecutive failures, the
    ///                       delay growing as base_delay_ms * 2^attempt, capped
    ///                       at the value set by set_max_reconnect_delay()
    ///                       (30 s by default); a successful connect resets the
    ///                       counter
    /// After giving up, call rearm_reconnect() (or stop() + start()).
    /// A negative base_delay_ms is treated as 0.
    void set_reconnect_config(int max_attempts, int base_delay_ms);

    /// Upper bound for the exponential backoff, in milliseconds.  Defaults to
    /// 30 s; 0 or a negative value restores it.  Only used when
    /// set_reconnect_config() was given a non-negative max_attempts.
    void set_max_reconnect_delay(int ms);

    // Core operations
    void on_message(message_callback callback);

    /// Installs the connection state callback; a helper has one, so a second call
    /// replaces the first.  Pass an empty callback to uninstall it.  Safe to call
    /// from any thread.
    ///
    /// The callback runs on the io thread and must not block there: it would stall
    /// every read, write and timer of this helper.  Calling the helper's own
    /// methods from it is fine.  connected arrives after the subscriptions have
    /// been replayed, so a publish from the callback is not lost to a missing
    /// subscription.  Nothing is reported once stop() has been called.
    void on_connection(connection_callback callback);

    /// Number of successful connects since construction.  Monotonic, not reset by
    /// start(): compare it against the connected events you received to notice a
    /// callback that never arrived.  Safe to call from any thread.
    std::uint64_t connected_count() const;

    void subscribe(const std::string& topic, uint8_t qos = 1);
    void subscribe(const std::string& topic, uint8_t qos,
                   std::function<void(bool success, std::string topic, uint8_t qos)> callback);
    /// Removes every exact match from the reconnect subscription list, even
    /// while offline. Online success is reported only after UNSUBACK. Offline
    /// calls report false synchronously; no broker-side unsubscribe is queued
    /// (persistent sessions may still retain that subscription).
    void unsubscribe(const std::string& topic, unsubscribe_cb callback = nullptr);
    void publish(const std::string& topic, const std::string& payload,
                 uint8_t qos = 0);
    void publish(const std::string& topic, const std::string& payload,
                 uint8_t qos, std::function<void(bool success, std::string topic,
                                                uint8_t qos, uint16_t packet_id)> callback);

    // Lifecycle
    void start();
    void stop();
    bool is_connected() const;
    bool is_running() const;

    /// True while automatic reconnection is armed: the helper is running, was
    /// not stopped by the user, and has not given up.  do_reconnect() clears
    /// this when max_attempts is exhausted (see set_reconnect_config()).
    bool is_reconnect_armed() const;

    /// Re-arms automatic reconnection after do_reconnect() gave up because
    /// max_attempts was exhausted: resets the attempt counter and starts a new
    /// connection attempt right away (no backoff wait, no stop()/start(), so the
    /// io thread, the io_context and the subscriptions are kept).
    ///
    /// Returns true when the helper is retrying afterwards, false when it cannot
    /// (never started, or stopped -- call start() instead).  Calling it while a
    /// retry is already armed is a no-op that returns true.  Safe to call from
    /// any thread.
    bool rearm_reconnect();

    /// Per-helper log sink.  Takes precedence over the process wide handler
    /// installed with litemqtt::set_log_handler() for records emitted by this
    /// helper (and by the mqtt_client it owns).  Pass an empty handler to fall
    /// back to the global one.  Safe to call from any thread.
    void set_log_handler(log_handler handler);

    /// Currently installed per-helper sink (empty when the global one is used).
    log_handler get_log_handler() const { return log_channel_.handler(); }

    /// True when this helper has its own sink installed.
    bool has_log_handler() const { return log_channel_.has_handler(); }

private:
    static constexpr int kDefaultMaxReconnectDelayMs = 30000;

    void init_client();
    void start_io_thread();
    void release_client();
    /// Returns a snapshot of client_; callers must not hold the lock while
    /// invoking into it (that keeps the critical section tiny and avoids
    /// re-entering user code under a lock).
    std::shared_ptr<mqtt_client> lock_client() const;
    void do_connect();
    void do_reconnect();
    /// Called from both close notifications, so that neither path can lose the
    /// reconnect: on_close() reports an attempt that never came up, on_disconnected()
    /// the loss of a session that was established, and a session that goes away is
    /// exactly what has to bring the helper back.
    void reconnect_after_close();
    void setup_callbacks();
    /// Delivers one transition to the installed callback, unless the helper has
    /// been stopped.
    void emit_connection(connection_event event, uint8_t return_code);

    std::string host_;
    uint16_t port_;
    std::string client_id_;
    std::string username_;
    std::string password_;
    uint16_t keep_alive_seconds_ = 60;
    bool clean_session_ = true;
    int connect_timeout_ms_ = 10000;
    std::size_t max_packet_size_ = connection::kDefaultMaxPacketSize;
    std::size_t max_write_queue_ = connection::kDefaultMaxWriteQueue;

    int max_reconnect_attempts_ = 10;
    int base_reconnect_delay_ms_ = 1000;
    int max_reconnect_delay_ms_ = kDefaultMaxReconnectDelayMs;
    std::atomic<int> reconnect_attempts_{0};
    std::atomic<bool> initial_connect_{true};

    std::atomic<bool> running_{false};
    std::atomic<bool> should_reconnect_{false};
    // True while a retry is sitting in reconnect_timer_ (see do_reconnect()).
    std::atomic<bool> reconnect_pending_{false};
    std::atomic<bool> user_stopped_{false};

    asio::io_context io_;
    std::unique_ptr<asio::io_context::work> work_;
    /// Mutex-protected because start()/stop() may replace it while
    /// is_connected()/publish()/subscribe() run on another thread.
    std::shared_ptr<mqtt_client> client_;
    mutable std::mutex client_mutex_;
    std::thread io_thread_;
    asio::steady_timer reconnect_timer_;  ///< Backoff timer, never blocks the io thread

    std::vector<std::string> pending_topics_;
    std::vector<std::pair<std::string, uint8_t>> subscribes_;
    std::mutex pending_mutex_;

    message_callback message_callback_;
    connection_callback connection_callback_;
    std::mutex callback_mutex_;
    std::atomic<std::uint64_t> connected_count_{0};

    detail::log_channel log_channel_;
    std::mutex log_handler_mutex_;  ///< Guards creation of client_ vs. log config
};

inline mqtt_helper::mqtt_helper(const std::string& host, uint16_t port,
                                const std::string& client_id)
    : host_(host),
      port_(port),
      client_id_(client_id),
      should_reconnect_(true),
      reconnect_timer_(io_) {
}

inline mqtt_helper::~mqtt_helper() {
    stop();
}

inline void mqtt_helper::set_credentials(const std::string& username,
                                        const std::string& password) {
    username_ = username;
    password_ = password;
}

inline void mqtt_helper::set_keep_alive(uint16_t seconds) {
    keep_alive_seconds_ = seconds;
}

inline void mqtt_helper::set_clean_session(bool clean) {
    clean_session_ = clean;
}

inline void mqtt_helper::set_connect_timeout(int ms) {
    connect_timeout_ms_ = (ms < 0) ? 0 : ms;
}

inline void mqtt_helper::set_max_packet_size(std::size_t bytes) {
    max_packet_size_ = bytes;
}

inline void mqtt_helper::set_max_write_queue(std::size_t frames) {
    max_write_queue_ = frames;
}

inline void mqtt_helper::set_reconnect_config(int max_attempts,
                                              int base_delay_ms) {
    // -1 means unlimited; anything below it is meaningless, so normalise it to 0
    // (give up on the first failure, which is a legal configuration).
    max_reconnect_attempts_ = (max_attempts < -1) ? 0 : max_attempts;
    base_reconnect_delay_ms_ = (base_delay_ms < 0) ? 0 : base_delay_ms;
}

inline void mqtt_helper::set_max_reconnect_delay(int ms) {
    max_reconnect_delay_ms_ = (ms <= 0) ? kDefaultMaxReconnectDelayMs : ms;
}

inline void mqtt_helper::set_log_handler(log_handler handler) {
    std::lock_guard<std::mutex> lock(log_handler_mutex_);
    // Keep a copy for the helper's own records, hand the original to the client
    // so that transport level records (connect failures, protocol errors, ...)
    // are routed to the same backend.
    log_channel_.set_handler(handler);
    auto client = lock_client();
    if (client) {
        client->set_log_handler(std::move(handler));
    }
}

inline void mqtt_helper::on_message(message_callback callback) {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    message_callback_ = std::move(callback);
}

inline void mqtt_helper::on_connection(connection_callback callback) {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    connection_callback_ = std::move(callback);
}

inline std::uint64_t mqtt_helper::connected_count() const {
    return connected_count_.load();
}

inline void mqtt_helper::emit_connection(connection_event event,
                                         uint8_t return_code) {
    // stop() sets this first, so a teardown in progress reports nothing; the
    // events of a session that is going away are of no use to the application.
    if (user_stopped_.load()) {
        return;
    }
    // Copy under the lock, invoke without it: the callback may install another
    // one, which would deadlock on a non-recursive mutex.
    connection_callback callback;
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        callback = connection_callback_;
    }
    if (callback) {
        callback(event, return_code);
    }
}

inline void mqtt_helper::subscribe(const std::string& topic, uint8_t qos) {
    subscribe(topic, qos, nullptr);
}

inline void mqtt_helper::subscribe(
    const std::string& topic, uint8_t qos,
    std::function<void(bool success, std::string topic, uint8_t qos)> callback) {
    // Keep list changes and queued requests ordered with unsubscribe/replay.
    std::lock_guard<std::mutex> lock(pending_mutex_);
    subscribes_.emplace_back(topic, qos);

    if (!is_connected()) {
        return;
    }

    auto client = lock_client();
    if (!client) {
        return;
    }
    client->async_subscribe(topic, qos, std::move(callback));
}

inline void mqtt_helper::unsubscribe(const std::string& topic, unsubscribe_cb callback) {
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        subscribes_.erase(std::remove_if(subscribes_.begin(), subscribes_.end(),
            [&topic](const std::pair<std::string, uint8_t>& entry) {
                return entry.first == topic;
            }), subscribes_.end());

        auto client = lock_client();
        if (!user_stopped_.load() && client &&
            client->state() == connection_state::connected) {
            client->async_unsubscribe(topic, std::move(callback));
            return;
        }
    }
    // User callbacks may call subscribe/unsubscribe, so release the lock first.
    if (callback) callback(false, topic);
}

inline void mqtt_helper::publish(const std::string& topic,
                                  const std::string& payload, uint8_t qos) {
    publish(topic, payload, qos, nullptr);
}

inline void mqtt_helper::publish(
    const std::string& topic, const std::string& payload, uint8_t qos,
    std::function<void(bool success, std::string topic, uint8_t qos,
                       uint16_t packet_id)> callback) {
    auto client = lock_client();
    if (!client) {
        // Between stop() and start() there is nobody to hand the message to.  Report
        // it instead of dropping it silently: a publisher loop that waits for the
        // callback would otherwise stall for good.
        if (callback) {
            callback(false, topic, qos, 0);
        }
        return;
    }

    client->async_publish(topic, payload, qos, std::move(callback));
}

inline void mqtt_helper::start() {
    if (running_.load()) {
        return;
    }

    user_stopped_.store(false);
    should_reconnect_.store(true);
    reconnect_pending_.store(false);
    // A fresh start is a fresh session: an attempt counter left over from a
    // previous run would otherwise make do_reconnect() give up immediately.
    reconnect_attempts_.store(0);
    initial_connect_.store(true);

    // asio marks the io_context as stopped once run() has returned (which it
    // does as soon as the work guard is gone).  Without this restart the second
    // start()'s io thread would return immediately, so nothing would ever be
    // connected again.
    if (io_.stopped()) {
        io_.restart();
    }

    work_ = std::make_unique<asio::io_context::work>(io_);

    init_client();
    setup_callbacks();
    do_connect();
    start_io_thread();
}

inline void mqtt_helper::stop() {
    user_stopped_.store(true);
    should_reconnect_.store(false);
    // Cancels a pending backoff wait so that the io thread is not kept alive by
    // a reconnect that is about to be abandoned anyway.
    reconnect_timer_.cancel();
    reconnect_pending_.store(false);

    const bool on_io_thread =
        io_thread_.joinable() && (std::this_thread::get_id() == io_thread_.get_id());

    if (on_io_thread) {
        // stop() was called from a callback running on the io thread (a natural
        // thing to do from on_message).  Joining ourselves would throw
        // "Resource deadlock avoided", so only stop the loop here; the next
        // stop()/the destructor joins the thread and releases the client.
        running_.store(false);
        if (auto client = lock_client()) {
            client->async_disconnect([this]() { io_.stop(); });
        } else {
            io_.stop();
        }
        work_.reset();
        return;
    }

    const bool was_running = running_.exchange(false);
    if (was_running) {
        if (auto client = lock_client()) {
            // Wait (bounded) for the DISCONNECT to be flushed and the socket to
            // be closed.  The write completes on the io thread, so the wait has
            // to happen before io_.stop().
            std::mutex mutex;
            std::condition_variable cv;
            bool closed = false;
            client->async_disconnect([&mutex, &cv, &closed]() {
                std::lock_guard<std::mutex> lock(mutex);
                closed = true;
                cv.notify_all();
            });
            std::unique_lock<std::mutex> lock(mutex);
            cv.wait_for(lock, std::chrono::seconds(2), [&closed]() { return closed; });
        }
        work_.reset();

        // Force run() to return.  Releasing the work guard is not enough: the
        // resolver service keeps a work unit on this io_context for as long as
        // the connection (and with it the resolver) exists, so the io thread
        // would stay blocked in epoll_wait and join() would never return.
        io_.stop();
    }

    if (io_thread_.joinable()) {
        // Also covers the case where running_ was already false (e.g. after a
        // stop() from the io thread): run() must return for the join to finish.
        io_.stop();
        io_thread_.join();
    }

    // Runs even when stop() is entered with running_ == false (never started, or
    // already stopped) so that a helper that was only partially started does not
    // keep its client -- and through it this helper -- alive.
    release_client();
}

inline bool mqtt_helper::is_connected() const {
    auto client = lock_client();
    return client && client->state() == connection_state::connected;
}

inline std::shared_ptr<mqtt_client> mqtt_helper::lock_client() const {
    std::lock_guard<std::mutex> lock(client_mutex_);
    return client_;
}

inline bool mqtt_helper::is_running() const {
    return running_.load();
}

inline bool mqtt_helper::is_reconnect_armed() const {
    return running_.load() && should_reconnect_.load();
}

inline bool mqtt_helper::rearm_reconnect() {
    if (!running_.load() || user_stopped_.load()) {
        LITEMQTT_LOG_TO(log_channel_, log_level::warn)
            << "[mqtt_helper] rearm_reconnect() ignored: helper is not running";
        return false;
    }

    // Only the give-up path clears this flag.  When a retry is already armed,
    // doing nothing keeps the call idempotent and avoids a second connect
    // attempt racing with the pending backoff timer.
    bool expected = false;
    if (!should_reconnect_.compare_exchange_strong(expected, true)) {
        LITEMQTT_LOG_TO(log_channel_, log_level::debug)
            << "[mqtt_helper] rearm_reconnect(): already armed";
        return true;
    }

    reconnect_attempts_.store(0);
    initial_connect_.store(false);

    LITEMQTT_LOG_TO(log_channel_, log_level::info)
        << "[mqtt_helper] Reconnect re-armed, retrying immediately";

    // Hop onto the io thread: do_connect() and the client's state are not
    // thread-safe, and cancelling a timer is only safe from its own thread.
    std::weak_ptr<mqtt_helper> weak = shared_from_this();
    asio::post(io_, [weak]() {
        auto self = weak.lock();
        if (!self) {
            return;
        }
        if (!self->should_reconnect_.load() || self->user_stopped_.load()) {
            return;  // stop() won the race
        }
        asio::error_code ec;
        self->reconnect_timer_.cancel(ec);
        self->reconnect_pending_.store(false);
        self->do_connect();
    });
    return true;
}

inline void mqtt_helper::init_client() {
    auto client = std::make_shared<mqtt_client>(io_);
    client->set_client_id(client_id_);
    client->set_keep_alive(keep_alive_seconds_);
    client->set_clean_session(clean_session_);
    client->set_connect_timeout(connect_timeout_ms_);
    // Applied on every session, not just the first: start() (and a reconnect
    // through it) builds a fresh mqtt_client, which builds a fresh connection.
    client->set_max_packet_size(max_packet_size_);
    client->set_max_write_queue(max_write_queue_);
    if (!username_.empty()) {
        client->set_username(username_);
    }
    if (!password_.empty()) {
        client->set_password(password_);
    }
    {
        std::lock_guard<std::mutex> lock(log_handler_mutex_);
        // Propagate a handler installed before start() to the fresh client.
        if (log_channel_.has_handler()) {
            client->set_log_handler(log_channel_.handler());
        }
    }
    {
        std::lock_guard<std::mutex> lock(client_mutex_);
        client_ = std::move(client);
    }
}

inline void mqtt_helper::start_io_thread() {
    running_.store(true);
    io_thread_ = std::thread([this]() {
        try {
            io_.run();
        } catch (const std::exception& e) {
            LITEMQTT_LOG_TO(log_channel_, log_level::error)
                << "[mqtt_helper] io_context exception: " << e.what();
        } catch (...) {
            LITEMQTT_LOG_TO(log_channel_, log_level::error)
                << "[mqtt_helper] io_context unknown exception";
        }
        LITEMQTT_LOG_TO(log_channel_, log_level::debug)
            << "[mqtt_helper] io_context thread exiting";
    });
}

inline void mqtt_helper::release_client() {
    std::shared_ptr<mqtt_client> client;
    {
        std::lock_guard<std::mutex> lock(client_mutex_);
        client.swap(client_);
    }
    if (!client) {
        return;
    }

    // The client holds std::function objects that capture this helper.  Clear
    // them before dropping the client, otherwise the two keep each other alive
    // and ~mqtt_helper() (the only automatic stop()) never runs.
    client->on_message(nullptr);
    client->on_close(nullptr);
    client->on_disconnected(nullptr);
    client->on_connect(nullptr);

    // The io thread is gone at this point, so this is the last chance to fail
    // writes that will never complete (a peer that stopped reading, a socket that
    // never became writable).  Those handlers hold the client, and the client
    // holds the connection that owns them: left pending, they form a cycle that
    // outlives both.
    client->close();
}

inline void mqtt_helper::setup_callbacks() {
    // Captured weakly on purpose: this helper owns client_, so a shared_ptr to
    // the helper stored inside the client's callbacks would create the cycle
    // helper -> client -> callback -> helper.  Neither object would then ever
    // be destroyed: the io_context, the socket and the io thread would leak and
    // ~mqtt_helper() would never stop them.
    std::weak_ptr<mqtt_helper> weak = shared_from_this();
    auto client = lock_client();
    if (!client) {
        return;
    }

    client->on_message([weak](std::string topic, std::string payload,
                                uint8_t qos, uint16_t packet_id) {
        auto self = weak.lock();
        if (!self) {
            return;
        }
        // Copy the callback under the lock, then invoke it without: the handler
        // runs on the io thread and may legitimately call helper methods
        // (on_message() included), which would deadlock on a non-recursive
        // mutex.
        message_callback callback;
        {
            std::lock_guard<std::mutex> lock(self->callback_mutex_);
            callback = self->message_callback_;
        }
        if (callback) {
            callback(std::move(topic), std::move(payload), qos, packet_id);
        }
    });

    client->on_close([weak]() {
        auto self = weak.lock();
        if (!self) {
            return;
        }
        LITEMQTT_LOG_TO(self->log_channel_, log_level::info)
            << "[mqtt_helper] Connection closed";
        self->reconnect_after_close();
    });

    // A session that was up and went away.  Kept apart from on_close(), which
    // reports the attempts that never got their CONNACK: one failed attempt used
    // to show up as a close as well, which is what made an edge-triggered
    // application see two disconnects for one failure.
    client->on_disconnected([weak]() {
        auto self = weak.lock();
        if (!self) {
            return;
        }
        self->emit_connection(connection_event::disconnected, 0);
        // This is the path a session that was up takes when it goes away, so it is
        // also the one that has to bring the helper back.  Report it first: the
        // application hears about the disconnect before the retry is armed.
        self->reconnect_after_close();
    });

    client->on_connect([weak](bool success, uint8_t rc) {
        auto self = weak.lock();
        if (!self) {
            return;
        }
        LITEMQTT_LOG_TO(self->log_channel_, log_level::debug)
            << "[mqtt_helper] Connection callback fired: success=" << success
            << ", rc=" << static_cast<int>(rc);
        if (!success) {
            LITEMQTT_LOG_TO(self->log_channel_, log_level::error)
                << "[mqtt_helper] Connection failed, return code: " << static_cast<int>(rc);
            //if (self->initial_connect_.load()) {
            //    self->should_reconnect_.store(false);
            //    return;
            //}
            self->emit_connection(connection_event::connect_failed, rc);
            self->do_reconnect();
            return;
        }

        LITEMQTT_LOG_TO(self->log_channel_, log_level::info)
            << "[mqtt_helper] Connected to broker";
        self->reconnect_attempts_.store(0);
        self->initial_connect_.store(false);

        // Restore subscriptions
        if (auto client = self->lock_client()) {
            std::lock_guard<std::mutex> lock(self->pending_mutex_);
            for (auto& item : self->subscribes_) {
                client->async_subscribe(item.first, item.second, nullptr);
            }
        }

        // Counted before it is reported, so that an application comparing the
        // counter in its periodic tick never sees a connected event it has not
        // been told about yet.  After the replay above: the session is only
        // announced once it is usable.
        self->connected_count_.fetch_add(1);
        self->emit_connection(connection_event::connected, 0);
    });
}

inline void mqtt_helper::do_connect() {
    auto client = lock_client();
    if (!client) {
        LITEMQTT_LOG_TO(log_channel_, log_level::error) << "[mqtt_helper] client_ is null!";
        return;
    }

    LITEMQTT_LOG_TO(log_channel_, log_level::info)
        << "[mqtt_helper] Initiating connection to " << host_ << ":" << port_ << "...";

    client->async_connect(host_, port_);
}

inline void mqtt_helper::reconnect_after_close() {
    if (!should_reconnect_.load() || user_stopped_.load()) {
        return;
    }
    do_reconnect();
}

inline void mqtt_helper::do_reconnect() {
    if (!should_reconnect_.load() || user_stopped_.load()) {
        return;
    }

    // One retry at a time: a single failure can be reported by more than one path
    // (a failed connect callback *and* a close notification, or a publish that
    // fails while the link is down).  Re-arming the timer for each of them would
    // count the attempt several times and push the backoff out with it.
    if (reconnect_pending_.exchange(true)) {
        return;
    }

    int attempts = reconnect_attempts_.load();
    if ((max_reconnect_attempts_ != -1) && (attempts >= max_reconnect_attempts_)) {
        LITEMQTT_LOG_TO(log_channel_, log_level::warn)
            << "[mqtt_helper] Max reconnect attempts reached (" << max_reconnect_attempts_
            << "), giving up; call rearm_reconnect() to try again";
        should_reconnect_.store(false);
        reconnect_pending_.store(false);
        return;
    }

    int delay_ms;
    if (max_reconnect_attempts_ == -1) {
        delay_ms = base_reconnect_delay_ms_;
    } else {
        // Cap the shift before it turns into undefined behaviour on long attempt
        // sequences; the value is clamped to the configured maximum afterwards
        // anyway.
        const int shift = std::min(attempts, 20);
        const long long grown =
            static_cast<long long>(base_reconnect_delay_ms_) * (1LL << shift);
        delay_ms = static_cast<int>(std::min<long long>(grown, max_reconnect_delay_ms_));
    }
    
    // Saturate rather than overflow: with max_attempts == -1 a helper that keeps
    // failing for days would otherwise eventually wrap the counter (UB), and the
    // value is only used for the log line and for the backoff growth anyway.
    if (attempts < 1000000) {
        reconnect_attempts_.fetch_add(1);
    }

    const int attempt_no = attempts + 1;
    if (max_reconnect_attempts_ == -1) {
        // Unlimited mode retries at a constant interval -- no exponential growth.
        LITEMQTT_LOG_TO(log_channel_, log_level::info)
            << "[mqtt_helper] Reconnecting in " << delay_ms << "ms (attempt " << attempt_no
            << ", unlimited)...";
    } else {
        LITEMQTT_LOG_TO(log_channel_, log_level::info)
            << "[mqtt_helper] Reconnecting in " << delay_ms << "ms (attempt " << attempt_no << "/"
            << max_reconnect_attempts_ << ")...";
    }

    // Wait asynchronously instead of sleeping on the io thread.  A sleep would
    // freeze reads, writes, ping and timers for the whole backoff window, and
    // would make stop() block for up to 30 s while joining the thread.
    std::weak_ptr<mqtt_helper> weak = shared_from_this();
    reconnect_timer_.expires_after(std::chrono::milliseconds(delay_ms));
    reconnect_timer_.async_wait([weak](const asio::error_code& ec) {
        auto self = weak.lock();
        if (!self) {
            return;
        }
        self->reconnect_pending_.store(false);
        if (ec) {
            return;  // cancelled by stop()/start()
        }
        if (self->should_reconnect_.load() && !self->user_stopped_.load()) {
            self->do_connect();
        }
    });
}
}  // namespace litemqtt

#endif  // LITEMQTT_HELPER_HPP

#ifndef LITEMQTT_LOG_HPP
#define LITEMQTT_LOG_HPP

#include <atomic>
#include <functional>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>

namespace litemqtt {

/// Severity of a log record.  Higher values are more severe; log_level::off
/// may be used as a threshold to disable the library output completely.
enum class log_level : int {
    trace = 0,
    debug = 1,
    info = 2,
    warn = 3,
    error = 4,
    off = 5,
};

/// Human readable name of @p level (never returns nullptr).
inline const char* to_string(log_level level) {
    switch (level) {
        case log_level::trace: return "trace";
        case log_level::debug: return "debug";
        case log_level::info:  return "info";
        case log_level::warn:  return "warn";
        case log_level::error: return "error";
        case log_level::off:   return "off";
    }
    return "unknown";
}

/// A single log event handed to the user supplied handler.
struct log_record {
    log_level level = log_level::info;
    const char* file = nullptr;  ///< Source file that produced the record.
    int line = 0;                ///< Source line that produced the record.
    std::string message;         ///< Formatted message (no trailing newline).
};

/// Sink installed by the application, e.g. a bridge to spdlog / glog / QDebug.
/// It is invoked from whichever thread triggered the event (an asio io_context
/// thread for most of the client code) and must therefore be thread-safe.
using log_handler = std::function<void(const log_record&)>;

namespace detail {

/// Default sink, keeps the historical behaviour: warnings and errors go to
/// stderr, everything else goes to stdout.
inline void default_log_sink(const log_record& record) {
    std::ostream& os = (record.level >= log_level::warn) ? std::cerr : std::cout;
    os << "[litemqtt][" << to_string(record.level) << "] " << record.message << std::endl;
}

/// Process wide logger: holds the installed handler and the severity threshold.
class logger {
public:
    static logger& instance() {
        static logger inst;
        return inst;
    }

    /// Installs a new sink.  Passing an empty function restores the default
    /// sink (std::cout / std::cerr).
    void set_handler(log_handler handler) {
        std::lock_guard<std::mutex> lock(mutex_);
        handler_ = std::move(handler);
    }

    log_handler handler() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return handler_;
    }

    /// Records below this level are discarded.
    void set_level(log_level level) { level_.store(static_cast<int>(level)); }

    log_level level() const { return static_cast<log_level>(level_.load()); }

    bool is_enabled(log_level level) const { return static_cast<int>(level) >= level_.load(); }

    void write(log_level level, const char* file, int line, std::string message) {
        if (!is_enabled(level)) {
            return;
        }
        // Copy the handler while holding the lock, invoke it outside the lock so
        // that a sink may safely call back into the logger (reconfigure, etc.).
        dispatch(this->handler(), level, file, line, std::move(message));
    }

    /// Shared dispatch used by both the global logger and per-object channels.
    /// Uses @p handler when it is set, otherwise the default sink.  Never throws:
    /// a misbehaving sink must not propagate into the library, we are often
    /// called from a destructor or an io callback.
    static void dispatch(const log_handler& handler, log_level level, const char* file, int line,
                         std::string message) {
        log_record record;
        record.level = level;
        record.file = file;
        record.line = line;
        record.message = std::move(message);
        try {
            if (handler) {
                handler(record);
            } else {
                default_log_sink(record);
            }
        } catch (...) {
        }
    }

private:
    logger() = default;
    logger(const logger&) = delete;
    logger& operator=(const logger&) = delete;

    mutable std::mutex mutex_;
    log_handler handler_;
    std::atomic<int> level_{static_cast<int>(log_level::trace)};
};

/// Per-object log channel.  When a handler is installed here it takes precedence
/// over the process wide handler for records emitted by the owning object
/// (mqtt_client / mqtt_helper); when it is empty the global logger is used, so a
/// per-object handler is purely optional.  Thread-safe: mqtt_client installs it
/// from the application thread while records are produced from the io thread.
class log_channel {
public:
    /// Passing an empty handler detaches the channel from its own sink.
    void set_handler(log_handler handler) {
        std::lock_guard<std::mutex> lock(mutex_);
        handler_ = std::move(handler);
    }

    log_handler handler() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return handler_;
    }

    bool has_handler() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return static_cast<bool>(handler_);
    }

    void write(log_level level, const char* file, int line, std::string message) const {
        log_handler handler = this->handler();
        if (handler) {
            logger::dispatch(handler, level, file, line, std::move(message));
        } else {
            logger::instance().write(level, file, line, std::move(message));
        }
    }

private:
    mutable std::mutex mutex_;
    log_handler handler_;
};

/// Stream helper: formats the message and forwards it to the logger in its
/// destructor.  When the level is disabled nothing is formatted or dispatched.
class log_stream {
public:
    /// Streams to the process wide logger.
    log_stream(log_level level, const char* file, int line)
        : channel_(nullptr), level_(level), file_(file), line_(line),
          enabled_(logger::instance().is_enabled(level)) {}

    /// Streams to @p channel, falling back to the process wide logger when the
    /// channel has no handler of its own.
    log_stream(const log_channel* channel, log_level level, const char* file, int line)
        : channel_(channel), level_(level), file_(file), line_(line),
          enabled_(logger::instance().is_enabled(level)) {}

    log_stream(const log_stream&) = delete;
    log_stream& operator=(const log_stream&) = delete;

    log_stream(log_stream&& other) noexcept
        : channel_(other.channel_), level_(other.level_), file_(other.file_), line_(other.line_),
          enabled_(other.enabled_), buffer_(std::move(other.buffer_)) {
        // The moved-from object must not dispatch on destruction.
        other.enabled_ = false;
    }

    ~log_stream() {
        if (!enabled_) {
            return;
        }
        if (channel_ != nullptr) {
            channel_->write(level_, file_, line_, buffer_.str());
        } else {
            logger::instance().write(level_, file_, line_, buffer_.str());
        }
    }

    template <typename T>
    log_stream& operator<<(const T& value) {
        if (enabled_) {
            buffer_ << value;
        }
        return *this;
    }

private:
    const log_channel* channel_;
    log_level level_;
    const char* file_;
    int line_;
    bool enabled_;
    std::ostringstream buffer_;
};

}  // namespace detail

/// Installs the application log sink (process wide).  Pass a default
/// constructed log_handler to restore output on std::cout / std::cerr.
inline void set_log_handler(log_handler handler) {
    detail::logger::instance().set_handler(std::move(handler));
}

/// Returns the currently installed sink (empty when the default sink is used).
inline log_handler get_log_handler() {
    return detail::logger::instance().handler();
}

/// Sets the minimum severity that is dispatched (default: log_level::trace).
inline void set_log_level(log_level level) {
    detail::logger::instance().set_level(level);
}

inline log_level get_log_level() {
    return detail::logger::instance().level();
}

/// True when a record of @p level would currently be dispatched.
inline bool log_enabled(log_level level) {
    return detail::logger::instance().is_enabled(level);
}

/// Convenience adapter for backends that only care about level + message,
/// e.g. logger::instance().set_handler(litemqtt::make_log_handler(
///     [](litemqtt::log_level lvl, const std::string& msg) { SPDLOG_INFO("{}", msg); }));
inline log_handler make_log_handler(std::function<void(log_level, const std::string&)> fn) {
    return [fn = std::move(fn)](const log_record& record) { fn(record.level, record.message); };
}

}  // namespace litemqtt

// The macros capture the source location for free and let call sites keep the
// familiar streaming syntax: LITEMQTT_LOG_ERROR << "connect failed: " << ec.message();
#define LITEMQTT_LOG(level) ::litemqtt::detail::log_stream((level), __FILE__, __LINE__)

// Streams through a per-object channel, e.g. a member of type
// litemqtt::detail::log_channel: LITEMQTT_LOG_TO(channel_, log_level::warn) << "hi";
#define LITEMQTT_LOG_TO(channel, level) \
    ::litemqtt::detail::log_stream(&(channel), (level), __FILE__, __LINE__)

#define LITEMQTT_LOG_TRACE LITEMQTT_LOG(::litemqtt::log_level::trace)
#define LITEMQTT_LOG_DEBUG LITEMQTT_LOG(::litemqtt::log_level::debug)
#define LITEMQTT_LOG_INFO LITEMQTT_LOG(::litemqtt::log_level::info)
#define LITEMQTT_LOG_WARN LITEMQTT_LOG(::litemqtt::log_level::warn)
#define LITEMQTT_LOG_ERROR LITEMQTT_LOG(::litemqtt::log_level::error)

#endif  // LITEMQTT_LOG_HPP

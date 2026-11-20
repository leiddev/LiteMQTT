#ifndef LITEMQTT_CONNECTION_HPP
#define LITEMQTT_CONNECTION_HPP

#include <array>
#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <vector>

#include <asio.hpp>

#include "buffer_reader.hpp"
#include "error.hpp"
#include "packet_type.hpp"

namespace litemqtt {

enum class connection_state {
    disconnected,
    connecting,
    handshaking,
    connected,
};

using connect_handler = std::function<void(const asio::error_code&)>;
using read_handler = std::function<void(const asio::error_code&, const std::vector<uint8_t>&)>;
using write_handler = std::function<void(const asio::error_code&)>;
using close_handler = std::function<void()>;

class connection : public std::enable_shared_from_this<connection> {
public:
    explicit connection(asio::io_context& io);

    asio::ip::tcp::socket& socket() {
        return socket_;
    }

    void async_connect(const std::string& host, uint16_t port, connect_handler handler);
    void async_read_packet(read_handler handler);

    /// Queues one frame for writing.  The buffer is taken by value and owned by
    /// the queue until the write completes, so callers may pass a temporary
    /// (e.g. `pkt.serialize()`) and need not keep it alive.  Frames are written
    /// strictly one after another: overlapping asio::async_write() calls on the
    /// same socket are not allowed and would corrupt the byte stream.
    void async_write_packet(std::vector<uint8_t> data, write_handler handler);

    void mark_connected();
    void close();
    /// Re-arms the object after close(), for a caller that closes the socket
    /// itself and then starts a new attempt on the same connection.
    void reset_after_close();

    void set_close_handler(close_handler handler) {
        close_handler_ = std::move(handler);
    }

    static constexpr std::size_t kDefaultMaxPacketSize = 1024 * 1024;

    /// Largest body this connection will read.  A PUBLISH larger than this is
    /// reported as asio::error::message_size instead of being buffered.  The
    /// default leaves room for a 1 MiB payload; MQTT itself allows up to
    /// 268435455, so raise it if the application really exchanges larger
    /// messages.  0 disables the check, which gives the peer control over how
    /// much the client allocates.
    void set_max_packet_size(std::size_t bytes) { max_packet_size_ = bytes; }

    std::size_t max_packet_size() const { return max_packet_size_; }

    static constexpr std::size_t kDefaultMaxWriteQueue = 1024;

    /// Upper bound on queued frames waiting to be written.  A peer that stops
    /// reading stalls the socket, and this queue is what grows while it stays
    /// that way; once it is full further frames are failed with
    /// asio::error::no_buffer_space instead of being buffered.  0 disables the
    /// check, which lets a slow peer drive the client's memory use.
    void set_max_write_queue(std::size_t frames) { max_write_queue_ = frames; }

    std::size_t max_write_queue() const { return max_write_queue_; }

    connection_state state() const {
        return state_.load();
    }

private:
    void read_fixed_header(read_handler handler);
    void read_remaining_length(read_handler handler, uint8_t first_byte);

    // Kept alive by the async handlers of read_remaining_length_step() so that a
    // multi-byte length can be assembled across several async_read calls.
    struct remaining_length_state {
        std::array<uint8_t, 4> byte_buffer{};
        std::size_t bytes_read = 0;
        std::size_t multiplier = 1;
        std::size_t remaining_length = 0;
    };
    void read_remaining_length_step(read_handler handler,
                                    std::shared_ptr<remaining_length_state> st);

    void read_packet_body(read_handler handler, std::size_t remaining_length);

    struct write_item {
        std::vector<uint8_t> buffer;
        write_handler handler;
    };

    void enqueue_write(std::vector<uint8_t> data, write_handler handler);
    void do_write();
    void fail_pending_writes(const asio::error_code& ec);

    asio::io_context& io_;
    asio::ip::tcp::socket socket_;
    asio::ip::tcp::resolver resolver_;
    // Written on the io thread, read from user threads (is_connected()).
    std::atomic<connection_state> state_{connection_state::disconnected};

    std::array<uint8_t, 2> fixed_header_{};
    // Raw fixed header of the packet being read: type byte plus *all* remaining
    // length bytes.  Packets are handed to the parser with their fixed header,
    // so every length byte has to be kept -- dropping the continuation bytes
    // makes the parser re-read the body as a length field.
    std::vector<uint8_t> read_header_;
    int reading_depth_ = 0;

    close_handler close_handler_;

    /// Guard against a peer asking for a huge allocation (see read_packet_body).
    std::size_t max_packet_size_ = kDefaultMaxPacketSize;

    /// Guard against a peer that stops reading (see enqueue_write).
    std::size_t max_write_queue_ = kDefaultMaxWriteQueue;

    // Write queue: only touched from the io thread (async_write_packet() posts
    // the enqueue), which keeps it lock free.  The item being written is owned by
    // its own completion handler (see do_write), so writing_ is all that is left
    // to say whether one is out.
    std::deque<write_item> write_queue_;
    bool writing_ = false;
    // Set by close(); makes close() idempotent and keeps enqueue_write() from
    // parking handlers on a dead connection.
    bool closed_ = false;
};

inline connection::connection(asio::io_context& io)
    : io_(io), socket_(io), resolver_(io) {
}

inline void connection::async_connect(const std::string& host, uint16_t port, connect_handler handler) {
    state_ = connection_state::connecting;
    closed_ = false;  // the object may be reused for a new attempt

    auto self = shared_from_this();
    resolver_.async_resolve(host, std::to_string(port),
        [this, self, handler](const asio::error_code& ec, asio::ip::tcp::resolver::results_type results) {
            if (ec) {
                handler(ec);
                return;
            }

            asio::async_connect(socket_, results,
                [this, self, handler](const asio::error_code& ec, const asio::ip::tcp::endpoint&) {
                    if (!ec) {
                        state_ = connection_state::handshaking;
                    } else {
                        state_ = connection_state::disconnected;
                    }
                    handler(ec);
                });
        });
}

inline void connection::async_read_packet(read_handler handler) {
    read_fixed_header(handler);
}

inline void connection::read_fixed_header(read_handler handler) {
    if (reading_depth_ > 0) return;
    ++reading_depth_;

    auto self = shared_from_this();
    asio::async_read(socket_,
        asio::buffer(fixed_header_.data(), 2),
        [this, self, handler](const asio::error_code& ec, std::size_t /*bytes_transferred*/) {
            if (ec) {
                reading_depth_ = 0;
                io_.post([handler, ec]() { handler(ec, {}); });
                return;
            }

            uint8_t packet_type_byte = fixed_header_[0];
            if (!is_valid_packet_type(packet_type_byte >> 4)) {
                reading_depth_ = 0;
                io_.post([handler]() { handler(asio::error::make_error_code(asio::error::invalid_argument), {}); });
                return;
            }

            read_header_.clear();
            read_header_.push_back(fixed_header_[0]);
            read_header_.push_back(fixed_header_[1]);

            read_remaining_length(handler, fixed_header_[1]);
        });
}

inline void connection::read_remaining_length(read_handler handler, uint8_t first_byte) {
    auto st = std::make_shared<remaining_length_state>();
    st->byte_buffer[0] = first_byte;
    st->bytes_read = 1;
    st->remaining_length = (first_byte & 0x7F) * st->multiplier;
    st->multiplier = 128;

    if ((first_byte & 0x80) != 0) {
        read_remaining_length_step(handler, st);
    } else {
        read_packet_body(handler, st->remaining_length);
    }
}

inline void connection::read_remaining_length_step(read_handler handler,
                                                   std::shared_ptr<remaining_length_state> st) {
    // This used to be a local std::function captured by reference: it dangled as
    // soon as read_remaining_length() returned, so the continuation invoked a
    // destroyed std::function for any packet whose length needs 3+ bytes.
    // Passing the shared state down the (non-growing) recursion keeps it alive
    // for exactly as long as the async operation needs it.

    // A remaining length is at most four bytes.  The bound has to be checked
    // before the next byte is read, not after: byte_buffer holds exactly four,
    // so a fifth continuation byte addressed byte_buffer[4], one past the end.
    if (st->bytes_read >= 4) {
        reading_depth_ = 0;
        io_.post([handler]() { handler(asio::error::make_error_code(asio::error::message_size), {}); });
        return;
    }

    auto self = shared_from_this();
    asio::async_read(socket_,
        asio::buffer(st->byte_buffer.data() + st->bytes_read, 1),
        [this, self, st, handler](const asio::error_code& ec, std::size_t) {
            if (ec) {
                reading_depth_ = 0;
                io_.post([handler, ec]() { handler(ec, {}); });
                return;
            }

            const uint8_t b = st->byte_buffer[st->bytes_read];
            ++st->bytes_read;
            st->remaining_length += (b & 0x7F) * st->multiplier;
            st->multiplier *= 128;

            // Keep this length byte: the parser re-reads the fixed header, so the
            // packet handed over must contain all of it.
            read_header_.push_back(b);

            if ((b & 0x80) != 0) {
                read_remaining_length_step(handler, st);
            } else {
                read_packet_body(handler, st->remaining_length);
            }
        });
}

inline void connection::read_packet_body(read_handler handler, std::size_t remaining_length) {
    // A peer controls this number, and the buffer below is allocated at its full
    // size.  Without a bound, a four-byte length of 268435455 asks for 256 MiB
    // before a single payload byte has arrived.
    if (remaining_length > max_packet_size_) {
        reading_depth_ = 0;
        io_.post([handler]() {
            handler(asio::error::make_error_code(asio::error::message_size), {});
        });
        return;
    }

    if (remaining_length == 0) {
        std::vector<uint8_t> packet(read_header_);
        reading_depth_ = 0;
        io_.post([handler, packet]() { handler(asio::error_code(), packet); });
        return;
    }

    auto self = shared_from_this();
    auto body_buf = std::make_shared<std::vector<uint8_t>>(remaining_length);
    const std::vector<uint8_t> header = read_header_;

    asio::async_read(socket_,
        asio::buffer(body_buf->data(), remaining_length),
        [this, self, handler, body_buf, header](const asio::error_code& ec, std::size_t) {
            if (ec) {
                reading_depth_ = 0;
                io_.post([handler, ec]() { handler(ec, {}); });
                return;
            }

            std::vector<uint8_t> packet;
            packet.reserve(header.size() + body_buf->size());
            packet.insert(packet.end(), header.begin(), header.end());
            packet.insert(packet.end(), body_buf->begin(), body_buf->end());

            reading_depth_ = 0;
            io_.post([handler, packet]() { handler(asio::error_code(), packet); });
        });
}

inline void connection::async_write_packet(std::vector<uint8_t> data, write_handler handler) {
    // Hop to the io thread first: callers include user threads (e.g. a
    // disconnect issued from another thread), while the queue below must only
    // ever be touched from one thread.  Posting also preserves the call order,
    // so frames reach the wire in the order they were queued.
    auto self = shared_from_this();
    io_.post([self, data = std::move(data), handler = std::move(handler)]() mutable {
        self->enqueue_write(std::move(data), std::move(handler));
    });
}

inline void connection::enqueue_write(std::vector<uint8_t> data, write_handler handler) {
    if (closed_) {
        // Never park work on a dead connection: a queued handler keeps whatever it
        // captured alive (mqtt_client holds this connection), so an item that can
        // never run forms a cycle nothing else can break.  Report the failure
        // asynchronously instead, so callers never see a callback from inside
        // async_write_packet().
        auto self = shared_from_this();
        io_.post([self, handler = std::move(handler)]() {
            if (handler) {
                handler(asio::error::operation_aborted);
            }
        });
        return;
    }
    if (max_write_queue_ != 0 && write_queue_.size() >= max_write_queue_) {
        // A peer that stops reading lets this queue grow without bound: nothing
        // drains it until the socket becomes writable.  Refuse the frame rather
        // than buffer it, and report that through the handler the same way the
        // closed-socket path does.
        auto self = shared_from_this();
        io_.post([self, handler = std::move(handler)]() {
            if (handler) {
                handler(asio::error::no_buffer_space);
            }
        });
        return;
    }
    write_queue_.push_back(write_item{std::move(data), std::move(handler)});
    if (!writing_) {
        do_write();
    }
}

inline void connection::do_write() {
    if (writing_) {
        return;
    }
    if (write_queue_.empty()) {
        return;
    }

    // Only a handshaking or established socket can make progress.  An open but
    // never connected socket never becomes writable (epoll never reports it), so
    // starting a write on it would leave the item -- and everything its handler
    // keeps alive -- stuck forever.  Fail those writes instead.
    if (!socket_.is_open() || state_ == connection_state::disconnected ||
        state_ == connection_state::connecting) {
        auto self = shared_from_this();
        io_.post([self]() { self->fail_pending_writes(asio::error::bad_descriptor); });
        return;
    }

    writing_ = true;

    // The buffer travels with the operation, not in a member: close() can run while
    // the write is still queued in asio, and a member it owned would be destroyed
    // with the connection object while asio still holds a view of it.  The completion
    // handler is what keeps the bytes alive, so they last exactly as long as the
    // operation.
    auto item = std::make_shared<write_item>(std::move(write_queue_.front()));
    write_queue_.pop_front();

    auto self = shared_from_this();
    asio::async_write(socket_,
        asio::buffer(item->buffer),
        [this, self, item](const asio::error_code& ec, std::size_t /*bytes*/) {
            writing_ = false;

            if (item->handler) {
                item->handler(ec);
            }

            if (ec) {
                // The stream is unusable (e.g. a close() racing this write, or a
                // reset peer): drop the socket so later writes fail fast instead
                // of waiting for writability that never comes.
                asio::error_code ignored;
                socket_.close(ignored);
                state_ = connection_state::disconnected;
                fail_pending_writes(ec);
            } else if (!writing_ && !write_queue_.empty()) {
                do_write();
            }
        });
}

inline void connection::fail_pending_writes(const asio::error_code& ec) {
    std::deque<write_item> pending;
    pending.swap(write_queue_);
    writing_ = false;

    for (auto& item : pending) {
        if (item.handler) {
            item.handler(ec);
        }
    }
}

inline void connection::mark_connected() {
    state_ = connection_state::connected;
}

inline void connection::close() {
    if (closed_) {
        return;
    }
    closed_ = true;

    // Closing the socket cancels the write that is in flight.  Its handler already
    // owns the bytes being written, so it is left alone here: the aborted completion
    // reports the failure, and its own error path drops the socket and fails the
    // queue.  Taking the operation over from here would destroy a buffer the pending
    // operation still points at.
    asio::error_code ec;
    socket_.close(ec);
    state_ = connection_state::disconnected;

    // Fail everything that is still pending.  Queued handlers may hold a
    // shared_ptr to the owner of this connection (mqtt_client owns it), so an
    // item left in the queue would form a cycle neither object can break; the
    // caller also has to hear about the failure rather than wait forever.
    fail_pending_writes(asio::error::operation_aborted);

    // The read was abandoned in flight, so its completion -- the one that would
    // reset this -- may never run.  Left at 1, async_read_packet() then does
    // nothing for the next session started on this object: the transport comes up
    // and never hears a CONNACK.
    reading_depth_ = 0;
    read_header_.clear();

    if (close_handler_) {
        close_handler_();
    }
}

/// Re-arms the object for another attempt.  close() leaves it unusable on purpose
/// -- every write is failed rather than parked on a dead socket -- and only
/// async_connect() clears that, which is too late for a caller that tears a
/// socket down itself and then starts a new attempt on this same connection.
inline void connection::reset_after_close() {
    closed_ = false;
    state_ = connection_state::disconnected;
}

}  // namespace litemqtt

#endif  // LITEMQTT_CONNECTION_HPP

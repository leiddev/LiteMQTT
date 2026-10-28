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

    void set_close_handler(close_handler handler) {
        close_handler_ = std::move(handler);
    }

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

    // Write queue: only touched from the io thread (async_write_packet() posts
    // the enqueue), which keeps it lock free.
    std::deque<write_item> write_queue_;
    // The item currently being written.  It is taken out of write_queue_ so that
    // close() can fail everything that is still waiting without disturbing the
    // buffer asio is using right now.
    std::unique_ptr<write_item> in_flight_;
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

            if (st->bytes_read > 4) {
                reading_depth_ = 0;
                io_.post([handler]() { handler(asio::error::make_error_code(asio::error::message_size), {}); });
                return;
            }

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
    in_flight_.reset(new write_item(std::move(write_queue_.front())));
    write_queue_.pop_front();

    auto self = shared_from_this();
    asio::async_write(socket_,
        asio::buffer(in_flight_->buffer),
        [this, self](const asio::error_code& ec, std::size_t /*bytes*/) {
            if (!in_flight_) {
                // close() already took it over and reported the failure.
                return;
            }
            // The buffer being written lives in in_flight_ and is only released
            // here, once asio is done with it -- this is what makes passing a
            // temporary serialized packet safe.
            std::unique_ptr<write_item> item(std::move(in_flight_));
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

    // Closing the socket cancels the write that is in flight; its completion
    // handler sees an empty in_flight_ and stays quiet.
    asio::error_code ec;
    socket_.close(ec);
    state_ = connection_state::disconnected;

    // Fail everything that is still pending.  Queued handlers may hold a
    // shared_ptr to the owner of this connection (mqtt_client owns it), so an
    // item left in the queue would form a cycle neither object can break; the
    // caller also has to hear about the failure rather than wait forever.
    std::unique_ptr<write_item> in_flight(std::move(in_flight_));
    writing_ = false;
    fail_pending_writes(asio::error::operation_aborted);
    if (in_flight && in_flight->handler) {
        in_flight->handler(asio::error::operation_aborted);
    }

    if (close_handler_) {
        close_handler_();
    }
}

}  // namespace litemqtt

#endif  // LITEMQTT_CONNECTION_HPP

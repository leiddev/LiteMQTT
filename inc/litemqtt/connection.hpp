#ifndef LITEMQTT_CONNECTION_HPP
#define LITEMQTT_CONNECTION_HPP

#include <array>
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
    void async_write_packet(const std::vector<uint8_t>& data, write_handler handler);
    void close();

    void set_close_handler(close_handler handler) {
        close_handler_ = std::move(handler);
    }

    connection_state state() const {
        return state_;
    }

private:
    void read_fixed_header(read_handler handler);
    void read_remaining_length(read_handler handler, uint8_t first_byte);
    void read_packet_body(read_handler handler, std::size_t remaining_length);

    asio::io_context& io_;
    asio::ip::tcp::socket socket_;
    asio::ip::tcp::resolver resolver_;
    connection_state state_ = connection_state::disconnected;

    std::array<uint8_t, 2> fixed_header_{};
    int reading_depth_ = 0;

    close_handler close_handler_;
};

inline connection::connection(asio::io_context& io)
    : io_(io), socket_(io), resolver_(io) {
}

inline void connection::async_connect(const std::string& host, uint16_t port, connect_handler handler) {
    state_ = connection_state::connecting;

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

            read_remaining_length(handler, fixed_header_[1]);
        });
}

inline void connection::read_remaining_length(read_handler handler, uint8_t first_byte) {
    auto self = shared_from_this();

    struct state {
        std::array<uint8_t, 4> byte_buffer{};
        std::size_t bytes_read = 0;
        std::size_t multiplier = 1;
        std::size_t remaining_length = 0;
    };
    auto st = std::make_shared<state>();
    st->byte_buffer[0] = first_byte;
    st->bytes_read = 1;
    st->remaining_length = (first_byte & 0x7F) * st->multiplier;
    st->multiplier = 128;

    std::function<void()> do_read;
    do_read = [this, self, st, handler, &do_read]() {
        asio::async_read(socket_,
            asio::buffer(st->byte_buffer.data() + st->bytes_read, 1),
            [this, self, st, handler, &do_read](const asio::error_code& ec, std::size_t) {
                if (ec) {
                    reading_depth_ = 0;
                    io_.post([handler, ec]() { handler(ec, {}); });
                    return;
                }

                uint8_t b = st->byte_buffer[st->bytes_read];
                ++st->bytes_read;
                st->remaining_length += (b & 0x7F) * st->multiplier;
                st->multiplier *= 128;

                if (st->bytes_read > 4) {
                    reading_depth_ = 0;
                    io_.post([handler]() { handler(asio::error::make_error_code(asio::error::message_size), {}); });
                    return;
                }

                if ((b & 0x80) != 0) {
                    do_read();
                } else {
                    read_packet_body(handler, st->remaining_length);
                }
            });
    };

    if ((first_byte & 0x80) != 0) {
        do_read();
    } else {
        read_packet_body(handler, st->remaining_length);
    }
}

inline void connection::read_packet_body(read_handler handler, std::size_t remaining_length) {
    if (remaining_length == 0) {
        std::vector<uint8_t> packet;
        packet.reserve(2);
        packet.push_back(fixed_header_[0]);
        packet.push_back(fixed_header_[1]);
        reading_depth_ = 0;
        io_.post([handler, packet]() { handler(asio::error_code(), packet); });
        return;
    }

    auto self = shared_from_this();
    auto body_buf = std::make_shared<std::vector<uint8_t>>(remaining_length);

    asio::async_read(socket_,
        asio::buffer(body_buf->data(), remaining_length),
        [this, self, handler, body_buf](const asio::error_code& ec, std::size_t) {
            if (ec) {
                reading_depth_ = 0;
                io_.post([handler, ec]() { handler(ec, {}); });
                return;
            }

            std::vector<uint8_t> packet;
            packet.reserve(2 + body_buf->size());
            packet.push_back(fixed_header_[0]);
            packet.push_back(fixed_header_[1]);
            packet.insert(packet.end(), body_buf->begin(), body_buf->end());

            reading_depth_ = 0;
            io_.post([handler, packet]() { handler(asio::error_code(), packet); });
        });
}

inline void connection::async_write_packet(const std::vector<uint8_t>& data, write_handler handler) {
    auto self = shared_from_this();
    asio::async_write(socket_,
        asio::buffer(data.data(), data.size()),
        [this, self, handler](const asio::error_code& ec, std::size_t /*bytes*/) {
            handler(ec);
        });
}

inline void connection::close() {
    asio::error_code ec;
    socket_.close(ec);
    state_ = connection_state::disconnected;

    if (close_handler_) {
        close_handler_();
    }
}

}  // namespace litemqtt

#endif  // LITEMQTT_CONNECTION_HPP

#ifndef LITEMQTT_BUFFER_WRITER_HPP
#define LITEMQTT_BUFFER_WRITER_HPP

#include <cstdint>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

namespace litemqtt {

class buffer_writer {
public:
    buffer_writer() : owns_buf_(true) {}

    explicit buffer_writer(std::vector<uint8_t>& buf) : owns_buf_(false), buf_(&buf) {}

    buffer_writer(buffer_writer&& other) noexcept
        : owns_buf_(other.owns_buf_), buf_(other.buf_), own_(std::move(other.own_)) {
        other.owns_buf_ = true;
        other.buf_ = nullptr;
    }

    buffer_writer& operator=(buffer_writer&& other) noexcept {
        if (this != &other) {
            owns_buf_ = other.owns_buf_;
            buf_ = other.buf_;
            own_ = std::move(other.own_);
            other.owns_buf_ = true;
            other.buf_ = nullptr;
        }
        return *this;
    }

    buffer_writer(const buffer_writer&) = delete;
    buffer_writer& operator=(const buffer_writer&) = delete;

    std::vector<uint8_t>& buffer() {
        return owns_buf_ ? own_ : *buf_;
    }

    void write_uint8(uint8_t value) {
        buffer().push_back(value);
    }

    void write_uint16(uint16_t value) {
        auto& b = buffer();
        b.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
        b.push_back(static_cast<uint8_t>(value & 0xFF));
    }

    void write_uint32(uint32_t value) {
        auto& b = buffer();
        b.push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
        b.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
        b.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
        b.push_back(static_cast<uint8_t>(value & 0xFF));
    }

    void write_string(const std::string& str) {
        write_uint16(static_cast<uint16_t>(str.size()));
        if (!str.empty()) {
            auto& b = buffer();
            b.insert(b.end(), str.begin(), str.end());
        }
    }

    void write_bytes(const uint8_t* data, std::size_t len) {
        if (len == 0) return;
        auto& b = buffer();
        b.insert(b.end(), data, data + len);
    }

    void write_remaining_length(std::size_t length) {
        auto& b = buffer();
        uint8_t digit = 0;
        uint32_t val = static_cast<uint32_t>(length);

        do {
            digit = static_cast<uint8_t>(val % 128);
            val /= 128;
            if (val > 0) {
                digit |= 0x80;
            }
            b.push_back(digit);
        } while (val > 0);
    }

    std::size_t size() const {
        return owns_buf_ ? own_.size() : buf_->size();
    }

    const uint8_t* data() const {
        return owns_buf_ ? own_.data() : buf_->data();
    }

private:
    bool owns_buf_;
    std::vector<uint8_t>* buf_;
    std::vector<uint8_t> own_;
};

}  // namespace litemqtt

#endif  // LITEMQTT_BUFFER_WRITER_HPP
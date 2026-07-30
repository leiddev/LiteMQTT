#ifndef LITEMQTT_BUFFER_READER_HPP
#define LITEMQTT_BUFFER_READER_HPP

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace litemqtt {

class buffer_reader {
public:
    explicit buffer_reader(const std::vector<uint8_t>& buf)
        : buf_(buf), pos_(0) {}

    explicit buffer_reader(const uint8_t* data, std::size_t len)
        : pos_(0) {
        buf_.reserve(len);
        if (len > 0) {
            buf_.assign(data, data + len);
        }
    }

    bool read_uint8(uint8_t& value) {
        if (pos_ >= buf_.size()) return false;
        value = buf_[pos_++];
        return true;
    }

    bool read_uint16(uint16_t& value) {
        if (pos_ + 2 > buf_.size()) return false;
        value = (static_cast<uint16_t>(buf_[pos_]) << 8) |
                static_cast<uint16_t>(buf_[pos_ + 1]);
        pos_ += 2;
        return true;
    }

    bool read_uint32(uint32_t& value) {
        if (pos_ + 4 > buf_.size()) return false;
        value = (static_cast<uint32_t>(buf_[pos_]) << 24) |
                (static_cast<uint32_t>(buf_[pos_ + 1]) << 16) |
                (static_cast<uint32_t>(buf_[pos_ + 2]) << 8) |
                static_cast<uint32_t>(buf_[pos_ + 3]);
        pos_ += 4;
        return true;
    }

    bool read_string(std::string& str) {
        uint16_t len = 0;
        if (!read_uint16(len)) return false;
        if (len == 0) {
            str.clear();
            return true;
        }
        if (pos_ + len > buf_.size()) return false;
        str.assign(reinterpret_cast<const char*>(&buf_[pos_]), len);
        pos_ += len;
        return true;
    }

    bool read_bytes(uint8_t* data, std::size_t len) {
        if (pos_ + len > buf_.size()) return false;
        std::memcpy(data, &buf_[pos_], len);
        pos_ += len;
        return true;
    }

    bool read_remaining_length(std::size_t& value) {
        value = 0;
        uint32_t multiplier = 1;
        uint8_t digit = 0;
        int bytes_read = 0;

        do {
            if (!read_uint8(digit)) return false;
            if (++bytes_read > 4) return false;

            value += (digit & 0x7F) * multiplier;
            multiplier *= 128;
        } while ((digit & 0x80) != 0);

        return true;
    }

    bool empty() const {
        return pos_ >= buf_.size();
    }

    std::size_t remaining() const {
        return buf_.size() - pos_;
    }

    void skip(std::size_t n) {
        pos_ += n;
    }

private:
    std::vector<uint8_t> buf_;
    std::size_t pos_;
};

class buffer_reader_mutable {
public:
    explicit buffer_reader_mutable(std::vector<uint8_t>& buf)
        : buf_(buf), pos_(0) {}

    bool read_uint8(uint8_t& value) {
        if (pos_ >= buf_.size()) return false;
        value = buf_[pos_++];
        return true;
    }

    bool read_uint16(uint16_t& value) {
        if (pos_ + 2 > buf_.size()) return false;
        value = (static_cast<uint16_t>(buf_[pos_]) << 8) |
                static_cast<uint16_t>(buf_[pos_ + 1]);
        pos_ += 2;
        return true;
    }

    bool read_string(std::string& str) {
        uint16_t len = 0;
        if (!read_uint16(len)) return false;
        if (len == 0) {
            str.clear();
            return true;
        }
        if (pos_ + len > buf_.size()) return false;
        str.assign(reinterpret_cast<const char*>(&buf_[pos_]), len);
        pos_ += len;
        return true;
    }

    bool read_remaining_length(std::size_t& value) {
        value = 0;
        uint32_t multiplier = 1;
        uint8_t digit = 0;
        int bytes_read = 0;

        do {
            if (!read_uint8(digit)) return false;
            if (++bytes_read > 4) return false;

            value += (digit & 0x7F) * multiplier;
            multiplier *= 128;
        } while ((digit & 0x80) != 0);

        return true;
    }

    bool empty() const {
        return pos_ >= buf_.size();
    }

    std::size_t remaining() const {
        return buf_.size() - pos_;
    }

private:
    std::vector<uint8_t>& buf_;
    std::size_t pos_;
};

}  // namespace litemqtt

#endif  // LITEMQTT_BUFFER_READER_HPP

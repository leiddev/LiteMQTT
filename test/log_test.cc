#include <gtest/gtest.h>

#include <asio.hpp>

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "litemqtt/log.hpp"
#include "litemqtt/mqtt_helper.hpp"

namespace {

using namespace litemqtt;

/// Restores the global logger state between tests so that the process wide
/// configuration does not leak from one test to the next.
class log_test : public ::testing::Test {
protected:
    void TearDown() override {
        set_log_handler(log_handler{});
        set_log_level(log_level::trace);
    }

    /// Produces one error record from @p client without touching the network:
    /// an out-of-range QoS is rejected (and logged) before any socket I/O.
    static void emit_invalid_qos(asio::io_context& io, const std::shared_ptr<mqtt_client>& client) {
        client->async_publish("t", "p", 3);
        io.restart();
        io.run();
    }
};

TEST_F(log_test, MacroFormatsMessageAndLevel) {
    std::vector<std::string> captured;
    set_log_handler([&captured](const log_record& record) {
        captured.push_back(std::string(to_string(record.level)) + ": " + record.message);
        EXPECT_NE(record.file, nullptr);
        EXPECT_GT(record.line, 0);
    });

    LITEMQTT_LOG_INFO << "hello " << 42;
    LITEMQTT_LOG_ERROR << "bad";

    ASSERT_EQ(captured.size(), 2u);
    EXPECT_EQ(captured[0], "info: hello 42");
    EXPECT_EQ(captured[1], "error: bad");
}

TEST_F(log_test, LevelThresholdFiltersRecords) {
    int count = 0;
    set_log_handler([&count](const log_record&) { ++count; });
    set_log_level(log_level::warn);

    LITEMQTT_LOG_DEBUG << "dropped";
    LITEMQTT_LOG_INFO << "dropped";
    LITEMQTT_LOG_WARN << "kept";
    LITEMQTT_LOG_ERROR << "kept";

    EXPECT_EQ(count, 2);
    EXPECT_FALSE(log_enabled(log_level::info));
    EXPECT_TRUE(log_enabled(log_level::error));
    EXPECT_EQ(get_log_level(), log_level::warn);
}

TEST_F(log_test, OffDisablesEverything) {
    int count = 0;
    set_log_handler([&count](const log_record&) { ++count; });
    set_log_level(log_level::off);

    LITEMQTT_LOG_ERROR << "dropped";

    EXPECT_EQ(count, 0);
    EXPECT_FALSE(log_enabled(log_level::error));
}

TEST_F(log_test, AdapterExposesLevelAndMessageOnly) {
    std::string last;
    set_log_handler(make_log_handler([&last](log_level level, const std::string& message) {
        last = std::string(to_string(level)) + "|" + message;
    }));

    LITEMQTT_LOG_WARN << "from adapter";

    EXPECT_EQ(last, "warn|from adapter");
}

TEST_F(log_test, ThrowingHandlerDoesNotEscape) {
    set_log_handler([](const log_record&) { throw std::runtime_error("boom"); });

    EXPECT_NO_THROW(LITEMQTT_LOG_ERROR << "should not propagate");
}

TEST_F(log_test, ReplacingHandlerWhileLoggingIsSafe) {
    // The handler may reconfigure the logger from inside the callback: the
    // logger invokes the sink outside its own lock on purpose.
    int nested = 0;
    set_log_handler([&nested](const log_record&) {
        ++nested;
        set_log_level(log_level::error);
        set_log_handler(log_handler{});
    });

    LITEMQTT_LOG_INFO << "reconfigure inside handler";

    EXPECT_EQ(nested, 1);
}

TEST_F(log_test, GetHandlerReturnsInstalledSink) {
    EXPECT_FALSE(static_cast<bool>(get_log_handler()));
    set_log_handler([](const log_record&) {});
    EXPECT_TRUE(static_cast<bool>(get_log_handler()));
}

TEST_F(log_test, PerInstanceHandlerTakesPrecedenceOverGlobal) {
    std::vector<std::string> global;
    std::vector<log_record> instance;
    set_log_handler([&global](const log_record& record) { global.push_back(record.message); });

    asio::io_context io;
    auto client = std::make_shared<mqtt_client>(io);
    client->set_log_handler([&instance](const log_record& record) { instance.push_back(record); });
    EXPECT_TRUE(client->has_log_handler());
    EXPECT_TRUE(static_cast<bool>(client->get_log_handler()));

    emit_invalid_qos(io, client);

    ASSERT_EQ(instance.size(), 1u);
    EXPECT_EQ(instance[0].level, log_level::error);
    EXPECT_NE(instance[0].message.find("async_publish: invalid QoS 3"), std::string::npos);
    // The record must be attributed to the emitting line inside client.hpp.
    ASSERT_NE(instance[0].file, nullptr);
    EXPECT_NE(std::string(instance[0].file).find("client.hpp"), std::string::npos);
    EXPECT_GT(instance[0].line, 0);
    EXPECT_TRUE(global.empty());

    // Two different call sites must report two different lines (guards against
    // the location being captured at the declaration instead of the call site).
    client->async_subscribe("t", 3);
    io.restart();
    io.run();
    ASSERT_EQ(instance.size(), 2u);
    EXPECT_NE(instance[0].line, instance[1].line);
}

TEST_F(log_test, PerInstanceHandlerFallsBackToGlobalWhenCleared) {
    std::vector<std::string> global;
    int instance_count = 0;
    set_log_handler([&global](const log_record& record) { global.push_back(record.message); });

    asio::io_context io;
    auto client = std::make_shared<mqtt_client>(io);
    client->set_log_handler([&instance_count](const log_record&) { ++instance_count; });

    emit_invalid_qos(io, client);
    EXPECT_EQ(instance_count, 1);
    EXPECT_TRUE(global.empty());

    client->set_log_handler(log_handler{});
    EXPECT_FALSE(client->has_log_handler());
    EXPECT_FALSE(static_cast<bool>(client->get_log_handler()));

    emit_invalid_qos(io, client);
    EXPECT_EQ(instance_count, 1);  // unchanged: the per-instance sink is detached
    ASSERT_EQ(global.size(), 1u);  // the global sink took over
}

TEST_F(log_test, PerInstanceHandlersDoNotCrossTalk) {
    int first = 0;
    int second = 0;
    asio::io_context io;
    auto a = std::make_shared<mqtt_client>(io);
    auto b = std::make_shared<mqtt_client>(io);
    a->set_log_handler([&first](const log_record&) { ++first; });
    b->set_log_handler([&second](const log_record&) { ++second; });

    emit_invalid_qos(io, a);
    EXPECT_EQ(first, 1);
    EXPECT_EQ(second, 0);

    emit_invalid_qos(io, b);
    EXPECT_EQ(first, 1);
    EXPECT_EQ(second, 1);
}

TEST_F(log_test, GlobalLevelAlsoFiltersInstanceHandlers) {
    int count = 0;
    asio::io_context io;
    auto client = std::make_shared<mqtt_client>(io);
    client->set_log_handler([&count](const log_record&) { ++count; });

    set_log_level(log_level::off);
    emit_invalid_qos(io, client);
    EXPECT_EQ(count, 0);

    set_log_level(log_level::error);
    emit_invalid_qos(io, client);
    EXPECT_EQ(count, 1);
}

TEST_F(log_test, ChannelMacroRoutesToChannelHandler) {
    detail::log_channel channel;
    std::vector<log_record> records;
    channel.set_handler([&records](const log_record& record) { records.push_back(record); });

    const int expected_line = __LINE__ + 1;
    LITEMQTT_LOG_TO(channel, log_level::warn) << "hi " << 7;

    ASSERT_EQ(records.size(), 1u);
    EXPECT_EQ(records[0].message, "hi 7");
    // Source location must be the macro call site, not the macro/declaration.
    EXPECT_EQ(records[0].line, expected_line);
    ASSERT_NE(records[0].file, nullptr);
    EXPECT_NE(std::string(records[0].file).find("log_test.cc"), std::string::npos);
}

TEST_F(log_test, HelperExposesPerInstanceHandlerAndForwardsToClient) {
    auto helper = std::make_shared<mqtt_helper>("127.0.0.1", static_cast<uint16_t>(1), "log-test");
    EXPECT_FALSE(helper->has_log_handler());

    helper->set_log_handler([](const log_record&) {});
    EXPECT_TRUE(helper->has_log_handler());
    EXPECT_TRUE(static_cast<bool>(helper->get_log_handler()));

    helper->set_log_handler(log_handler{});
    EXPECT_FALSE(helper->has_log_handler());
}

}  // namespace

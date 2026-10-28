// Tests for the lifetime / reconnect behaviour of mqtt_helper.  None of them
// needs a broker: a refused connect is enough to exercise the reconnect path.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>

#include "litemqtt/mqtt_helper.hpp"

namespace {

using namespace std::chrono;

/// Port 1 has no listener, so every connect attempt is refused immediately.
constexpr uint16_t kRefusedPort = 1;

bool wait_for(const std::function<bool()>& pred, int timeout_ms) {
    const auto deadline = steady_clock::now() + milliseconds(timeout_ms);
    while (steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(milliseconds(5));
    }
    return pred();
}

std::shared_ptr<litemqtt::mqtt_helper> make_helper(const std::string& id) {
    auto helper = std::make_shared<litemqtt::mqtt_helper>("127.0.0.1", kRefusedPort, id);
    helper->set_log_handler([](const litemqtt::log_record&) {});
    return helper;
}

}  // namespace

// The helper owns the client, and the client's callbacks used to hold a
// shared_ptr back to the helper, so nothing was ever destroyed.
TEST(MqttHelperLifetime, IsDestroyedAfterStopAndReset) {
    std::weak_ptr<litemqtt::mqtt_helper> weak;
    {
        auto helper = make_helper("lifetime-stop");
        helper->set_reconnect_config(0, 10);
        helper->start();
        std::this_thread::sleep_for(milliseconds(100));
        helper->stop();
        weak = helper;
        helper.reset();
    }
    EXPECT_TRUE(wait_for([&weak]() { return weak.expired(); }, 2000))
        << "mqtt_helper leaked after stop(): the client's callbacks still own it";
}

// Same, but the user never calls stop(): ~mqtt_helper() must still tear the
// session down instead of leaking a running io thread.
TEST(MqttHelperLifetime, IsDestroyedWhenLastOwnerDropsWithoutStop) {
    std::weak_ptr<litemqtt::mqtt_helper> weak;
    {
        auto helper = make_helper("lifetime-drop");
        helper->set_reconnect_config(0, 10);
        helper->start();
        std::this_thread::sleep_for(milliseconds(100));
        weak = helper;
        helper.reset();
    }
    EXPECT_TRUE(wait_for([&weak]() { return weak.expired(); }, 2000))
        << "mqtt_helper leaked when the last shared_ptr was dropped";
}

// asio stops an io_context as soon as run() returns, so start() has to restart
// it; otherwise the second session silently does nothing.  A publish callback
// proves work is being processed again.
TEST(MqttHelperLifetime, RestartAfterStopRunsTheIoThreadAgain) {
    auto helper = make_helper("restart");
    helper->set_reconnect_config(0, 10);

    helper->start();
    std::this_thread::sleep_for(milliseconds(100));
    helper->stop();

    helper->start();
    std::atomic<bool> called{false};
    helper->publish("restart/topic", "payload", 0,
                    [&called](bool, std::string, uint8_t, uint16_t) { called.store(true); });

    EXPECT_TRUE(wait_for([&called]() { return called.load(); }, 2000))
        << "publish callback never ran: the io_context was not restarted";
    helper->stop();
}

// The reconnect backoff used to sleep on the io thread, which made stop() wait
// for the rest of the delay (up to 30 s).
TEST(MqttHelperLifetime, StopDoesNotWaitForReconnectBackoff) {
    auto helper = make_helper("backoff");
    helper->set_reconnect_config(-1, 3000);  // retry forever, 3 s apart
    helper->start();
    std::this_thread::sleep_for(milliseconds(300));  // now inside the 3 s wait

    const auto t0 = steady_clock::now();
    helper->stop();
    const auto elapsed = duration_cast<milliseconds>(steady_clock::now() - t0).count();

    EXPECT_LT(elapsed, 1000) << "stop() waited " << elapsed
                             << " ms for the reconnect backoff to finish";
}

// stop() called from a callback runs on the io thread; joining that same thread
// used to throw "Resource deadlock avoided" and leave running_ set.
TEST(MqttHelperLifetime, StopFromIoThreadDoesNotThrow) {
    auto helper = make_helper("selfstop");
    helper->set_reconnect_config(0, 10);
    helper->start();
    std::this_thread::sleep_for(milliseconds(100));

    std::atomic<bool> returned{false};
    std::atomic<bool> threw{false};
    helper->publish("self/topic", "payload", 0,
                    [&helper, &returned, &threw](bool, std::string, uint8_t, uint16_t) {
                        try {
                            helper->stop();
                            returned.store(true);
                        } catch (...) {
                            threw.store(true);
                        }
                    });

    EXPECT_TRUE(wait_for([&returned, &threw]() { return returned.load() || threw.load(); }, 2000));
    EXPECT_FALSE(threw.load()) << "stop() from the io thread threw";
    EXPECT_FALSE(helper->is_running());
    helper->stop();  // finish the teardown from this thread
}

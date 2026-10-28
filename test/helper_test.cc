// Tests for the lifetime / reconnect behaviour of mqtt_helper.  None of them
// needs a broker: a refused connect is enough to exercise the reconnect path.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

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
    // A refused connect on loopback is instant on Linux but can take a couple of
    // seconds on Windows (SYN retransmits before the RST surfaces), which would put
    // every retry cadence below on the OS instead of on the reconnect logic.  Cap the
    // attempt so the timing assertions stay about the helper.
    helper->set_connect_timeout(150);
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

// A session that never got connected has nothing to flush, so stop() must not sit
// in the graceful-shutdown timeout waiting for a DISCONNECT that cannot be sent.
TEST(MqttHelperLifetime, StopAfterAFailedConnectReturnsQuickly) {
    auto helper = make_helper("failed-connect");
    helper->set_reconnect_config(0, 10);
    helper->start();
    ASSERT_TRUE(wait_for([&helper]() { return !helper->is_reconnect_armed(); }, 2000));

    const auto begin = steady_clock::now();
    helper->stop();
    const auto elapsed = duration_cast<milliseconds>(steady_clock::now() - begin).count();
    EXPECT_LT(elapsed, 1000) << "stop() waited " << elapsed << " ms for an unsendable DISCONNECT";
}

// Once the attempt limit was hit nothing retries any more; rearm_reconnect()
// must resume from a clean counter, without stop() + start().
TEST(MqttHelperReconnect, RearmRetriesAfterGivingUp) {
    auto helper = make_helper("rearm");
    std::atomic<int> backoffs{0};
    std::atomic<int> giveups{0};
    helper->set_log_handler([&backoffs, &giveups](const litemqtt::log_record& r) {
        if (r.message.find("Reconnecting in") != std::string::npos) ++backoffs;
        if (r.message.find("giving up") != std::string::npos) ++giveups;
    });
    helper->set_reconnect_config(1, 10);
    helper->start();

    ASSERT_TRUE(wait_for([&helper]() { return !helper->is_reconnect_armed(); }, 2000))
        << "the helper never gave up";
    EXPECT_FALSE(helper->is_connected());
    EXPECT_TRUE(helper->is_running()) << "giving up must not stop the io thread";

    const int backoffs_before = backoffs.load();
    const int giveups_before = giveups.load();
    EXPECT_GE(backoffs_before, 1);

    // Given up means given up: no retry happens on its own any more.
    std::this_thread::sleep_for(milliseconds(200));
    EXPECT_EQ(backoffs.load(), backoffs_before);

    EXPECT_TRUE(helper->rearm_reconnect());
    EXPECT_TRUE(helper->is_reconnect_armed()) << "re-arm left the flag cleared";
    EXPECT_TRUE(wait_for([&backoffs, backoffs_before]() { return backoffs.load() > backoffs_before; },
                         2000))
        << "no retry after rearm_reconnect()";
    EXPECT_TRUE(wait_for([&giveups, giveups_before]() { return giveups.load() > giveups_before; },
                         2000))
        << "the counter was not reset: the fresh budget was not used up again";

    helper->stop();
    EXPECT_FALSE(helper->rearm_reconnect()) << "rearm must be refused once stopped";
}

// set_reconnect_config(-1, delay): retry forever, always at the same delay.
TEST(MqttHelperReconnect, UnlimitedWaitsAFixedInterval) {
    auto helper = make_helper("fixed-delay");
    std::mutex mutex;
    std::vector<long long> stamps;
    std::atomic<int> giveups{0};
    const auto t0 = steady_clock::now();
    helper->set_log_handler([&](const litemqtt::log_record& r) {
        if (r.message.find("Reconnecting in") != std::string::npos) {
            std::lock_guard<std::mutex> lock(mutex);
            stamps.push_back(duration_cast<milliseconds>(steady_clock::now() - t0).count());
        }
        if (r.message.find("giving up") != std::string::npos) ++giveups;
    });
    helper->set_reconnect_config(-1, 100);
    helper->start();

    const bool got_five = wait_for(
        [&mutex, &stamps]() {
            std::lock_guard<std::mutex> lock(mutex);
            return stamps.size() >= 5;
        },
        3000);
    helper->stop();

    ASSERT_TRUE(got_five) << "(-1, 100) stopped retrying instead of retrying forever";
    EXPECT_EQ(giveups.load(), 0) << "unlimited mode gave up";

    std::lock_guard<std::mutex> lock(mutex);
    for (std::size_t i = 1; i < stamps.size(); ++i) {
        const long long gap = stamps[i] - stamps[i - 1];
        EXPECT_GE(gap, 90) << "gap before retry " << (i + 1) << " was only " << gap << " ms";
        EXPECT_LT(gap, 300) << "gap before retry " << (i + 1) << " grew to " << gap
                            << " ms; unlimited mode must use a constant delay";
    }
}

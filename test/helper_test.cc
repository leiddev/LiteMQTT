// Tests for the lifetime, reconnect and connect/keepalive behaviour of
// mqtt_helper.  The lifetime and backoff cases need no broker -- a refused
// connect is enough -- while the connect, keepalive and publish-before-start
// cases use the scripted loopback peer.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "litemqtt/mqtt_helper.hpp"
#include "loopback_broker.hpp"

namespace {

using namespace std::chrono;
using litemqtt_test::loopback_broker;
using litemqtt_test::wait_for;

/// Port 1 has no listener, so every connect attempt is refused immediately.
constexpr uint16_t kRefusedPort = 1;

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

// --- operations without a session ------------------------------------------

// Between stop() and start() there is no client to hand the message to.  A
// publisher loop waiting for the callback would stall for good if the failure
// were not reported.
TEST(MqttHelperOperations, PublishAfterStopReportsFailure) {
    auto helper = make_helper("publish-after-stop");
    helper->set_reconnect_config(0, 10);
    helper->start();
    std::this_thread::sleep_for(milliseconds(100));
    helper->stop();

    std::atomic<bool> called{false};
    std::atomic<bool> success{true};
    std::atomic<uint16_t> packet_id{1};
    helper->publish("t", "p", 0,
                    [&called, &success, &packet_id](bool ok, std::string, uint8_t, uint16_t id) {
                        called = true;
                        success = ok;
                        packet_id = id;
                    });

    EXPECT_TRUE(wait_for([&called]() { return called.load(); }, 2000))
        << "the publish callback never ran, a waiting caller would hang";
    EXPECT_FALSE(success.load());
    EXPECT_EQ(packet_id.load(), 0);
}

// subscribe() before start() must not crash and must not fire a callback that
// can never be answered; the topic is recorded and carried out on connect.
TEST(MqttHelperOperations, SubscribeWithoutClientIsANoOp) {
    auto helper = make_helper("subscribe-no-client");
    std::atomic<int> callbacks{0};
    EXPECT_NO_THROW(
        helper->subscribe("t", 1, [&callbacks](bool, std::string, uint8_t) { ++callbacks; }));
    std::this_thread::sleep_for(milliseconds(50));
    EXPECT_EQ(callbacks.load(), 0);
    EXPECT_FALSE(helper->is_connected());
}

// --- inbound packet size ---------------------------------------------------

// The cap has to actually reach the connection.  The helper does not expose its
// client, so this checks the observable effect on a real session.  It runs
// against the first (and only) connection the loopback peer accepts, which is
// also why the "rebuilt on the next start()" path is covered by inspection
// rather than by a second session here.
TEST(MqttHelperPacketSize, CapIsAppliedOnConnect) {
    loopback_broker broker;

    auto helper = std::make_shared<litemqtt::mqtt_helper>("127.0.0.1", broker.port(), "cap-apply");
    helper->set_log_handler([](const litemqtt::log_record&) {});
    helper->set_reconnect_config(0, 10);
    // 2 KiB, well below the 1 MiB default: the 4 KiB publish below must fail.
    helper->set_max_packet_size(2048);

    std::atomic<int> messages{0};
    std::mutex topic_mutex;
    std::string last_topic;
    helper->on_message([&](std::string topic, std::string, uint8_t, uint16_t) {
        ++messages;
        std::lock_guard<std::mutex> lock(topic_mutex);
        last_topic = std::move(topic);
    });

    helper->start();
    ASSERT_TRUE(wait_for([&helper]() { return helper->is_connected(); }, 3000));

    // Under the cap: delivered, so the session is known good before the next step.
    broker.reply(litemqtt_test::make_raw_publish("small", std::string(512, 'x')));
    ASSERT_TRUE(wait_for([&messages]() { return messages.load() >= 1; }, 3000))
        << "a packet under the cap was not delivered";
    {
        std::lock_guard<std::mutex> lock(topic_mutex);
        EXPECT_EQ(last_topic, "small");
    }

    // Over the cap: refused, so the message callback must not see it.
    broker.reply(litemqtt_test::make_raw_publish("big", std::string(4096, 'x')));
    std::this_thread::sleep_for(milliseconds(400));
    EXPECT_EQ(messages.load(), 1) << "a packet above the cap was delivered";
    helper->stop();
}

// --- backoff growth and its ceiling ---------------------------------------

// The ceiling is what keeps a long outage from pushing the retry out to
// minutes; it has to be configurable so this can be checked without waiting
// 30 seconds.
TEST(MqttHelperReconnect, BackoffStopsGrowingAtTheConfiguredCeiling) {
    auto helper = make_helper("backoff-cap");
    std::mutex mutex;
    std::vector<std::pair<int, long long>> attempts;  // (delay_ms, stamp)
    const auto t0 = steady_clock::now();
    helper->set_log_handler([&](const litemqtt::log_record& r) {
        const std::string marker = "Reconnecting in ";
        const auto at = r.message.find(marker);
        if (at == std::string::npos) {
            return;
        }
        const int delay = std::atoi(r.message.c_str() + at + marker.size());
        std::lock_guard<std::mutex> lock(mutex);
        attempts.emplace_back(delay, duration_cast<milliseconds>(steady_clock::now() - t0).count());
    });
    // base 20 ms: 20, 40, 80, 160 ... capped at 100.
    helper->set_reconnect_config(8, 20);
    helper->set_max_reconnect_delay(100);
    helper->start();

    ASSERT_TRUE(wait_for([&mutex, &attempts]() {
        std::lock_guard<std::mutex> lock(mutex);
        return attempts.size() >= 5;  // 20, 40, 80, 100, 100
    }, 3000));
    helper->stop();

    std::lock_guard<std::mutex> lock(mutex);
    ASSERT_GE(attempts.size(), 5u);
    // base * 2^attempt, clamped to the ceiling: 20, 40, 80, then 160 -> 100 and
    // everything after it stays there.
    EXPECT_EQ(attempts[0].first, 20);
    EXPECT_EQ(attempts[1].first, 40);
    EXPECT_EQ(attempts[2].first, 80);
    for (std::size_t i = 3; i < attempts.size(); ++i) {
        EXPECT_EQ(attempts[i].first, 100)
            << "attempt " << (i + 1) << " ran at " << attempts[i].first
            << " ms instead of the 100 ms ceiling";
    }
}

// --- connect timeout -------------------------------------------------------

// A peer that accepts the socket and then says nothing never completes the
// handshake; without the timeout the session would sit in "handshaking" for as
// long as the keepalive allows.
TEST(MqttHelperConnect, TimeoutReportsFailureOnce) {
    loopback_broker broker;
    broker.set_handshake(loopback_broker::handshake::silent);

    auto helper = std::make_shared<litemqtt::mqtt_helper>("127.0.0.1", broker.port(), "timeout");
    std::atomic<int> failures{0};
    std::atomic<int> successes{0};
    helper->set_log_handler([&failures, &successes](const litemqtt::log_record& r) {
        if (r.message.find("Connect timed out") != std::string::npos) {
            ++failures;
        }
        if (r.message.find("Connected to broker") != std::string::npos) {
            ++successes;
        }
    });
    helper->set_connect_timeout(150);
    helper->set_reconnect_config(0, 10);  // one attempt, no retry
    helper->start();

    ASSERT_TRUE(wait_for([&failures]() { return failures.load() >= 1; }, 3000))
        << "the connect attempt was never abandoned";
    EXPECT_EQ(successes.load(), 0);
    EXPECT_FALSE(helper->is_connected());
    helper->stop();
}

// --- keepalive -------------------------------------------------------------

// keep_alive == 0 disables the keepalive entirely: no PINGREQ is sent.
TEST(MqttHelperKeepAlive, ZeroSendsNoPing) {
    loopback_broker broker;
    auto helper = std::make_shared<litemqtt::mqtt_helper>("127.0.0.1", broker.port(), "ka-zero");
    helper->set_log_handler([](const litemqtt::log_record&) {});
    helper->set_keep_alive(0);
    helper->set_reconnect_config(0, 10);
    helper->start();

    ASSERT_TRUE(wait_for([&helper]() { return helper->is_connected(); }, 3000));
    std::this_thread::sleep_for(milliseconds(700));
    helper->stop();

    int pings = 0;
    for (const auto& frame : broker.received()) {
        if (!frame.empty() && (frame[0] >> 4) == 12) {
            ++pings;
        }
    }
    EXPECT_EQ(pings, 0) << "keep_alive 0 must not send PINGREQ";
}

// A broker that never answers PINGRESP must be detected and the session
// dropped: 1.5 x keep_alive is the only thing that notices a half-dead link.
TEST(MqttHelperKeepAlive, MissingPingrespClosesTheSession) {
    loopback_broker broker;  // never replies to anything after CONNACK

    auto helper = std::make_shared<litemqtt::mqtt_helper>("127.0.0.1", broker.port(), "ka-dead");
    std::atomic<int> closes{0};
    helper->set_log_handler(
        [&closes](const litemqtt::log_record& r) {
            if (r.message.find("Connection closed") != std::string::npos) {
                ++closes;
            }
        });
    helper->set_keep_alive(1);
    helper->set_reconnect_config(0, 10);
    helper->start();

    ASSERT_TRUE(wait_for([&helper]() { return helper->is_connected(); }, 3000));
    // PINGREQ goes out after 1 s; the 0.5 s wait for PINGRESP then expires.
    EXPECT_TRUE(wait_for([&closes]() { return closes.load() >= 1; }, 4000))
        << "a missing PINGRESP never closed the session";
    EXPECT_FALSE(helper->is_connected());
    helper->stop();
}

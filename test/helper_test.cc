// Tests for the lifetime, reconnect and connect/keepalive behaviour of
// mqtt_helper.  The lifetime and backoff cases need no broker -- a refused
// connect is enough -- while the connect, keepalive and publish-before-start
// cases use the scripted loopback peer.
#include <gtest/gtest.h>

#include <algorithm>
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
    // The session is up when the keepalive notices the dead link, so its teardown
    // is a connection event rather than the "Connection closed" log line, which
    // belongs to attempts that never came up.
    std::atomic<int> closes{0};
    helper->on_connection([&closes](litemqtt::connection_event e, uint8_t) {
        if (e == litemqtt::connection_event::disconnected) {
            ++closes;
        }
    });
    helper->set_log_handler([](const litemqtt::log_record&) {});
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

// --- connection events -----------------------------------------------------

namespace {

/// Records every connection event together with its return code.
struct event_log {
    std::mutex mutex;
    std::vector<std::pair<litemqtt::connection_event, uint8_t>> events;

    void add(litemqtt::connection_event event, uint8_t rc) {
        std::lock_guard<std::mutex> lock(mutex);
        events.emplace_back(event, rc);
    }

    std::vector<std::pair<litemqtt::connection_event, uint8_t>> snapshot() {
        std::lock_guard<std::mutex> lock(mutex);
        return events;
    }

    std::size_t count(litemqtt::connection_event event) {
        std::lock_guard<std::mutex> lock(mutex);
        return static_cast<std::size_t>(std::count_if(
            events.begin(), events.end(),
            [event](const std::pair<litemqtt::connection_event, uint8_t>& e) {
                return e.first == event;
            }));
    }
};

}  // namespace

// A refused CONNACK used to surface twice -- once through the connect callback
// and once as a close notification -- which is what an edge-triggered
// application saw as two disconnects for one failure.
TEST(MqttHelperEvents, RefusedConnackIsOneFailureCarryingItsCode) {
    loopback_broker broker;
    broker.set_handshake(loopback_broker::handshake::refuse);
    broker.set_refuse_code(5);  // not authorized

    auto helper = std::make_shared<litemqtt::mqtt_helper>("127.0.0.1", broker.port(), "ev-refuse");
    event_log log;
    helper->on_connection([&log](litemqtt::connection_event e, uint8_t rc) { log.add(e, rc); });
    helper->set_log_handler([](const litemqtt::log_record&) {});
    helper->set_connect_timeout(300);
    helper->set_reconnect_config(-1, 50);  // keep retrying so there is more than one attempt
    helper->start();

    ASSERT_TRUE(wait_for([&log]() {
        return log.count(litemqtt::connection_event::connect_failed) >= 2;
    }, 3000)) << "no connect_failed events arrived";
    helper->stop();

    auto events = log.snapshot();
    ASSERT_FALSE(events.empty());
    EXPECT_EQ(log.count(litemqtt::connection_event::disconnected), 0u)
        << "a connect attempt that never came up must not also report a disconnect";
    EXPECT_EQ(log.count(litemqtt::connection_event::connected), 0u);
    EXPECT_EQ(events[0].first, litemqtt::connection_event::connect_failed);
    for (const auto& e : events) {
        EXPECT_EQ(e.first, litemqtt::connection_event::connect_failed)
            << "every event belongs to a failed attempt";
        // The broker that refused the session closes the socket right after, so
        // an attempt can fail either way.  Both carry the CONNACK code the broker
        // returned; only a timeout has none (it reports 255).
        EXPECT_TRUE(static_cast<int>(e.second) == 5 || static_cast<int>(e.second) == 255)
            << "unexpected return code " << static_cast<int>(e.second);
    }
    EXPECT_EQ(helper->connected_count(), 0u);
}

// A peer that accepts the socket and never answers: the timeout path reports one
// failure per attempt, with the transport level code.
TEST(MqttHelperEvents, ConnectTimeoutIsOneFailure) {
    loopback_broker broker;
    broker.set_handshake(loopback_broker::handshake::silent);

    auto helper = std::make_shared<litemqtt::mqtt_helper>("127.0.0.1", broker.port(), "ev-timeout");
    event_log log;
    helper->on_connection([&log](litemqtt::connection_event e, uint8_t rc) { log.add(e, rc); });
    helper->set_log_handler([](const litemqtt::log_record&) {});
    helper->set_connect_timeout(150);
    // Retry once: the failure that ends an attempt must not spill into the next
    // one, which is what counting the attempts here verifies.
    helper->set_reconnect_config(1, 300);
    helper->start();

    ASSERT_TRUE(wait_for([&log]() {
        return log.count(litemqtt::connection_event::connect_failed) >= 1;
    }, 3000)) << "the abandoned attempt was never reported";
    std::this_thread::sleep_for(milliseconds(450));  // past the retry, still silent
    std::this_thread::sleep_for(milliseconds(100));
    helper->stop();

    auto events = log.snapshot();
    ASSERT_FALSE(events.empty());
    // A peer that accepts and says nothing can also drop the socket first, which
    // is likewise a transport level failure: both report 255.
    for (const auto& e : events) {
        EXPECT_EQ(e.first, litemqtt::connection_event::connect_failed);
        EXPECT_EQ(static_cast<int>(e.second), 255)
            << "a transport level failure carries 255, as mqtt_client defines it";
    }
    // Two attempts at most (the first, then the single retry), never a third from
    // a timer that outlived its attempt.
    EXPECT_LE(events.size(), 2u) << "one attempt was reported more than once";
    EXPECT_EQ(log.count(litemqtt::connection_event::disconnected), 0u);
}

// The edge an edge-triggered application needs: up, down, each exactly once.
TEST(MqttHelperEvents, EstablishedSessionReportsConnectedAndDisconnectedInOrder) {
    loopback_broker broker;

    auto helper = std::make_shared<litemqtt::mqtt_helper>("127.0.0.1", broker.port(), "ev-edges");
    event_log log;
    helper->on_connection([&log](litemqtt::connection_event e, uint8_t rc) { log.add(e, rc); });
    helper->set_log_handler([](const litemqtt::log_record&) {});
    // No retry: the loopback peer serves one connection, so a second attempt
    // would hang in the backoff and say nothing about the events.
    helper->set_reconnect_config(0, 10);
    helper->start();

    ASSERT_TRUE(wait_for([&log]() { return log.count(litemqtt::connection_event::connected) == 1; },
                         3000)) << "the first connect was never reported";
    EXPECT_EQ(helper->connected_count(), 1u);

    broker.close_client_connection();
    ASSERT_TRUE(wait_for([&log]() {
        return log.count(litemqtt::connection_event::disconnected) == 1;
    }, 3000)) << "the teardown of an established session was not reported";

    // Nothing else follows: the single teardown must not be reported twice.
    std::this_thread::sleep_for(milliseconds(200));
    helper->stop();

    auto events = log.snapshot();
    ASSERT_EQ(events.size(), 2u) << "one connect and one teardown, nothing else";
    EXPECT_EQ(events[0].first, litemqtt::connection_event::connected);
    EXPECT_EQ(events[1].first, litemqtt::connection_event::disconnected);
    for (const auto& e : events) {
        EXPECT_EQ(static_cast<int>(e.second), 0)
            << "connected and disconnected always report return code 0";
    }
}

// connected arrives once the subscriptions are usable, so a publish from the
// callback is not lost to a missing subscription.
TEST(MqttHelperEvents, ConnectedArrivesAfterTheSubscriptionsAreReplayed) {
    loopback_broker broker;

    auto helper = std::make_shared<litemqtt::mqtt_helper>("127.0.0.1", broker.port(), "ev-replay");
    helper->set_log_handler([](const litemqtt::log_record&) {});
    helper->set_reconnect_config(0, 10);
    helper->subscribe("replay/topic", 1);

    std::atomic<bool> connected{false};
    helper->on_connection([&](litemqtt::connection_event e, uint8_t) {
        if (e == litemqtt::connection_event::connected) {
            connected.store(true);
        }
    });
    helper->start();

    // The replay runs just before the callback, so by the time connected is
    // reported the SUBSCRIBE is already on its way to the peer.
    ASSERT_TRUE(wait_for([&connected]() { return connected.load(); }, 3000))
        << "connected was never reported";
    ASSERT_TRUE(wait_for([&broker]() {
        for (const auto& frame : broker.received()) {
            if (!frame.empty() && (frame[0] >> 4) == 8) {  // SUBSCRIBE
                return true;
            }
        }
        return false;
    }, 3000)) << "the subscription was not replayed";
    helper->stop();
}

// The callback runs on the io thread, where re-entering the helper must not
// deadlock on the mutex guarding the callback itself.
TEST(MqttHelperEvents, ReentrantCallsFromTheCallbackAreSafe) {
    loopback_broker broker;

    auto helper = std::make_shared<litemqtt::mqtt_helper>("127.0.0.1", broker.port(), "ev-reentry");
    helper->set_log_handler([](const litemqtt::log_record&) {});
    helper->set_reconnect_config(0, 10);

    std::atomic<int> reentered{0};
    helper->on_connection([&helper, &reentered](litemqtt::connection_event e, uint8_t) {
        if (e != litemqtt::connection_event::connected) {
            return;
        }
        helper->on_message([](std::string, std::string, uint8_t, uint16_t) {});
        (void)helper->is_connected();
        (void)helper->connected_count();
        helper->on_connection([](litemqtt::connection_event, uint8_t) {});  // uninstalls itself
        ++reentered;
    });
    helper->start();

    ASSERT_TRUE(wait_for([&reentered]() { return reentered.load() == 1; }, 3000))
        << "calling helper methods from the callback deadlocked";
    helper->stop();
}

TEST(MqttHelperEvents, EmptyCallbackUninstalls) {
    loopback_broker broker;

    auto helper = std::make_shared<litemqtt::mqtt_helper>("127.0.0.1", broker.port(), "ev-uninst");
    helper->set_log_handler([](const litemqtt::log_record&) {});
    helper->set_reconnect_config(-1, 20);

    std::atomic<int> events{0};
    helper->on_connection([&events](litemqtt::connection_event, uint8_t) { ++events; });
    helper->start();

    ASSERT_TRUE(wait_for([&helper]() { return helper->is_connected(); }, 3000));
    ASSERT_TRUE(wait_for([&events]() { return events.load() >= 1; }, 3000));
    helper->on_connection(litemqtt::connection_callback{});
    const int after_uninstall = events.load();

    broker.close_client_connection();
    std::this_thread::sleep_for(milliseconds(300));
    EXPECT_EQ(events.load(), after_uninstall) << "an empty callback must stop the reports";
    helper->stop();
}

TEST(MqttHelperEvents, NoEventsAfterStop) {
    loopback_broker broker;

    auto helper = std::make_shared<litemqtt::mqtt_helper>("127.0.0.1", broker.port(), "ev-stop");
    helper->set_log_handler([](const litemqtt::log_record&) {});
    helper->set_reconnect_config(-1, 20);

    std::atomic<int> events{0};
    helper->on_connection([&events](litemqtt::connection_event, uint8_t) { ++events; });
    helper->start();

    ASSERT_TRUE(wait_for([&helper]() { return helper->is_connected(); }, 3000));
    ASSERT_TRUE(wait_for([&events]() { return events.load() >= 1; }, 3000));
    helper->stop();

    const int after_stop = events.load();
    std::this_thread::sleep_for(milliseconds(300));
    EXPECT_EQ(events.load(), after_stop) << "stop() must silence the callback";
    // The counter keeps what it counted; it is what the application compares
    // against after a restart.
    EXPECT_EQ(helper->connected_count(), 1u);
}

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <memory>
#include <thread>

#include <asio.hpp>

#include "litemqtt/client.hpp"

namespace {
std::atomic<bool> g_should_exit{false};
}

static void signal_handler(int /*signal*/) {
    g_should_exit = true;
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    uint16_t port = 1883;
    std::string client_id = "litemqtt_cpp_demo";
    std::string subscribe_topic = "litemqtt/demo";
    std::string publish_topic = "litemqtt/demo";
    std::string payload = "Hello from LiteMQTT!";
    std::string username;
    std::string password;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--host" && i + 1 < argc) host = argv[++i];
        else if (arg == "--port" && i + 1 < argc) port = static_cast<uint16_t>(std::stoi(argv[++i]));
        else if (arg == "--id" && i + 1 < argc) client_id = argv[++i];
        else if (arg == "--subscribe" && i + 1 < argc) subscribe_topic = argv[++i];
        else if (arg == "--publish" && i + 1 < argc) publish_topic = argv[++i];
        else if (arg == "--payload" && i + 1 < argc) payload = argv[++i];
        else if (arg == "--username" && i + 1 < argc) username = argv[++i];
        else if (arg == "--password" && i + 1 < argc) password = argv[++i];
    }

    std::signal(SIGINT, signal_handler);

    std::cout << "LiteMQTT Demo Client" << std::endl;
    std::cout << "  Host: " << host << ":" << port << std::endl;
    std::cout << "  Client ID: " << client_id << std::endl;
    if (!username.empty()) {
        std::cout << "  Username: " << username << std::endl;
    }
    std::cout << "  Subscribe topic: " << subscribe_topic << std::endl;
    std::cout << "  Publish topic: " << publish_topic << std::endl;

    asio::io_context io;

    auto client = std::make_shared<litemqtt::mqtt_client>(io);
    client->set_client_id(client_id);
    client->set_keep_alive(60);
    client->set_clean_session(true);
    if (!username.empty()) {
        client->set_username(username);
    }
    if (!password.empty()) {
        client->set_password(password);
    }

    auto do_subscribe = [client, &subscribe_topic]() {
        client->async_subscribe(subscribe_topic, 2, [](bool success, std::string topic, uint8_t qos) {
            if (success) {
                std::cout << "Subscribed to: " << topic << " with QoS " << static_cast<int>(qos) << std::endl;
            } else {
                std::cerr << "Failed to subscribe to: " << topic << std::endl;
            }
        });
    };

    constexpr int max_reconnect_attempts = 10;
    constexpr int base_reconnect_delay_ms = 1000;

    int reconnect_attempts = 0;
    bool initial_connect = true;

    auto do_reconnect = [client, host, port, &reconnect_attempts,
                    max_reconnect_attempts, base_reconnect_delay_ms]() {
        if (reconnect_attempts >= max_reconnect_attempts) {
            std::cerr << "Max reconnect attempts reached, exiting." << std::endl;
            g_should_exit = true;
            return;
        }

        int delay_ms = std::min(base_reconnect_delay_ms * (1 << reconnect_attempts), 30000);
        reconnect_attempts++;
        std::cout << "Reconnecting in " << delay_ms << "ms (attempt " << reconnect_attempts << "/" << max_reconnect_attempts << ")..." << std::endl;

        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
        client->async_connect(host, port);
    };

    client->on_connect([client, subscribe_topic, publish_topic, payload,
                        &initial_connect, &reconnect_attempts, do_subscribe, do_reconnect](bool success, uint8_t rc) {
        if (!success) {
            std::cerr << "Connection failed, return code: " << static_cast<int>(rc) << std::endl;
            if (initial_connect) {
                g_should_exit = true;
                return;
            }
            do_reconnect();
            return;
        }

        std::cout << "Connected to broker" << std::endl;
        reconnect_attempts = 0;
        initial_connect = false;

        do_subscribe();

        client->async_publish(publish_topic, payload, 1, [](bool success, std::string topic, uint8_t qos, uint16_t packet_id) {
            if (success) {
                std::cout << "Published message id=" << packet_id << " on: " << topic << " (QoS " << static_cast<int>(qos) << ")" << std::endl;
            } else {
                std::cerr << "Failed to publish id=" << packet_id << " on: " << topic << std::endl;
            }
        });
    });

    client->on_message([](std::string topic, std::string msg, uint8_t qos, uint16_t packet_id) {
        std::cout << "Received message id=" << packet_id << " on [" << topic << "] (QoS " << static_cast<int>(qos) << "): " << msg << std::endl;
    });

    client->on_close([do_reconnect]() {
        std::cout << "Connection closed" << std::endl;
        if (g_should_exit) return;
        do_reconnect();
    });

    client->async_connect(host, port);

    std::thread io_thread([&io]() {
        try {
            io.run();
        }
        catch (const std::exception& e) {
            std::cerr << "io_context exception: " << e.what() << std::endl;
        }
        catch (...) {
            std::cerr << "io_context unknown exception" << std::endl;
        }
    });

    while (!g_should_exit) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        if (client->state() == litemqtt::connection_state::connected) {
            client->async_publish(publish_topic, payload, 2);
        }
    }

    client->async_disconnect();
    io_thread.join();

    std::cout << "Demo exiting" << std::endl;
    return 0;
}

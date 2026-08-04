#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <memory>
#include <thread>

#include <asio.hpp>

#include "litemqtt/mqtt_helper.hpp"

namespace {
std::atomic<bool> g_should_exit{false};
}

static void signal_handler(int) {
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

    auto mqtt = std::make_shared<litemqtt::mqtt_helper>(host, port, client_id);
    mqtt->set_keep_alive(60);
    mqtt->set_clean_session(true);
    if (!username.empty() && !password.empty()) {
        mqtt->set_credentials(username, password);
    }

    mqtt->on_message([](std::string topic, std::string msg, uint8_t qos, uint16_t packet_id) {
        std::cout << "Received message id=" << packet_id << " on [" << topic << "] (QoS " << static_cast<int>(qos) << "): " << msg << std::endl;
    });

    mqtt->subscribe(subscribe_topic, 2, [](bool success, std::string topic, uint8_t qos) {
        if (success) {
            std::cout << "Subscribed to: " << topic << " with QoS " << static_cast<int>(qos) << std::endl;
        } else {
            std::cerr << "Failed to subscribe to: " << topic << std::endl;
        }
    });

    mqtt->start();

    while (!g_should_exit) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        if (mqtt->is_connected()) {
            mqtt->publish(publish_topic, payload, 2);
        }
    }

    mqtt->stop();
    std::cout << "Demo exiting" << std::endl;
    return 0;
}

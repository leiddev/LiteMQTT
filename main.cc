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

void signal_handler(int /*signal*/) {
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

        client->on_connect([client, subscribe_topic, publish_topic, payload](bool success, uint8_t rc) {
        if (!success) {
            std::cerr << "Connection failed, return code: " << static_cast<int>(rc) << std::endl;
            g_should_exit = true;
            return;
        }

        std::cout << "Connected to broker" << std::endl;

        // Subscribe with callback
        client->async_subscribe(subscribe_topic, [](bool success, std::string topic, uint8_t qos) {
            if (success) {
                std::cout << "Subscribed to: " << topic << " with QoS " << static_cast<int>(qos) << std::endl;
            } else {
                std::cerr << "Failed to subscribe to: " << topic << std::endl;
            }
        });

        // Publish with callback (QoS 1 for delivery confirmation)
        client->async_publish(publish_topic, payload, 1, [](bool success, std::string topic, uint8_t qos) {
            if (success) {
                std::cout << "Published message on: " << topic << " (QoS " << static_cast<int>(qos) << ")" << std::endl;
            } else {
                std::cerr << "Failed to publish on: " << topic << std::endl;
            }
        });
    });

    client->on_message([](std::string topic, std::string msg, uint8_t qos) {
        std::cout << "Received message on [" << topic << "] (QoS " << static_cast<int>(qos) << "): " << msg << std::endl;
    });

    client->on_close([]() {
        std::cout << "Connection closed" << std::endl;
        g_should_exit = true;
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
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    client->async_disconnect();
    io_thread.join();

    std::cout << "Demo exiting" << std::endl;
    return 0;
}

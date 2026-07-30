# LiteMQTT设计文档

基于 **C++14 / Standalone Asio / TCP / MQTT 3.1.1** 的技术栈，构建出一个轻量、高效且无重度依赖（如无需编译庞大的 Boost 库）的客户端。

以下是该系统的软件架构设计方案。

---

### 一、 整体架构图

系统的整体架构可以划分为四个主要层次：**API接口层**、**业务与状态控制层**、**网络与IO层**、**协议解析与序列化层**。

```text
+-------------------------------------------------------------+
|                        应用层 (Application)                  |
+-------------------------------------------------------------+
                               |
                               v
+-------------------------------------------------------------+
|                      API 接口层 (Public API)                |
|  - mqtt::client                                             |
+-------------------------------------------------------------+
                               |
                               v
+-------------------------------------------------------------+
|               业务与状态控制层 (Session & State)              |
|  - Keep-Alive 计时器 (asio::steady_timer)                    |
|  - 状态机 (Disconnected, Connecting, Connected)              |
|  - 消息分发器 (Dispatcher)                                   |
|  - 报文标识符生成器 (Packet ID Generator)                     |
+-------------------------------------------------------------+
         |                                             ^
         v                                             | (解析后的报文)
+---------------------------------+           +---------------------------------+
|       协议序列化 (Serializer)     |           |        协议反序列化 (Parser)      |
|  - 构造二进制流 (Buffer)          |           |  - 解析固定头部 (Fixed Header)   |
|  - 计算 Remaining Length        |           |  - 解析可变头部与有效载荷        |
+---------------------------------+           +---------------------------------+
         |                                             ^
         v (字节流)                                     | (字节流)
+-------------------------------------------------------------+
|                     网络与 IO 层 (Network IO)                |
|  - Connection (包装 asio::ip::tcp::socket)                  |
|  - 异步读写循环 (Async Read/Write Loop)                      |
+-------------------------------------------------------------+
                               |
                               v
+-------------------------------------------------------------+
|                      Standalone Asio / OS                   |
+-------------------------------------------------------------+
```

---

### 二、 核心组件设计

#### 1. 协议解析与序列化层 (Protocol/Codec Layer)
这一层负责将 C++ 的结构体对象与 MQTT 二进制字节流进行互相转换。由于 MQTT 是面向字节的紧凑协议，需要特别注意字节序（MQTT 采用大端序 Big-Endian）和变长长度字段（Remaining Length）的计算。

*   **`packet_type.hpp`**: 定义 MQTT 3.1.1 的报文类型枚举（如 `CONNECT`, `CONNACK`, `PUBLISH`, `SUBSCRIBE` 等）。
*   **`buffer_writer.hpp` / `buffer_reader.hpp`**: 辅助类，用于向缓冲区写入或从中读取基本数据类型（如 `uint8_t`, `uint16_t` 以及 MQTT 格式的 UTF-8 编码字符串）。
*   **`packets.hpp`**: 定义各种报文的结构体。例如：
    ```cpp
    struct ConnectPacket {
        std::string client_id;
        uint16_t keep_alive_seconds;
        bool clean_session;
        // MQTT 3.1.1 协议标识符为 "MQTT"，版本号为 4
    };
    ```

#### 2. 网络与 IO 层 (Network IO Layer)
负责管理底层的 TCP 连接，执行实际的非阻塞读写操作。

*   **`connection` 类**:
    *   继承自 `std::enable_shared_from_this<connection>`，用于在异步回调中安全地管理自身生命周期。
    *   持有 `asio::ip::tcp::socket`。
    *   提供 `async_read_packet()` 接口：首先异步读取 1 字节（报文类型），再读取 1-4 字节（解析出 Remaining Length），最后根据长度读取剩余数据。
    *   提供 `async_write_packet()` 接口：将序列化后的缓冲区通过 `asio::async_write` 发送出去。

#### 3. 业务与状态控制层 (Session & State Layer)
这一层是客户端的核心逻辑所在，负责维护连接状态、处理心跳、管理 QoS 事务。

*   **状态管理**: 定义客户端生命周期状态（`Disconnected` -> `Connecting` -> `Handshaking` -> `Connected`）。
*   **`keep_alive_timer`**: 使用 `asio::steady_timer`。在连接成功后启动，每次向服务器发送报文时重置。如果超时未发送数据，则自动发送 `PINGREQ`；如果在规定时间内未收到 `PINGRESP`，则断开连接。
*   **Packet ID 生成器**: 用于生成 `PUBLISH` (QoS > 0), `SUBSCRIBE`, `UNSUBSCRIBE` 所需的 16 位无符号整数标识符。
*   **重发与确认队列 (针对 QoS 1/2)**: 维护未收到确认（如 `PUBACK`, `SUBACK`）的发送中报文。由于只实现客户端，这里可以根据实际需求简化（例如首期仅支持 QoS 0 ）。

#### 4. API 接口层 (Public API Layer)
面向库的使用者，提供简洁易用的接口。

*   **`client` 类**:
    *   持有 `asio::io_context` 的引用或管理其生命周期。
    *   提供成员函数如 `connect()`, `disconnect()`, `publish()`, `subscribe()`, `unsubscribe()`。
    *   提供回调注册接口：
        ```cpp
        void set_connect_handler(std::function<void(bool)> handler);
        void set_message_handler(std::function<void(std::string topic, std::string payload)> handler);
        ```

---

### 三、 关键技术细节与 C++14 实现建议

1.  **内存管理与生命周期**：
    在异步编程中，回调函数的触发时机是不确定的。必须确保在回调执行时，相关的连接对象和缓冲区依然有效。建议使用 `std::shared_ptr` 配合 `std::enable_shared_from_this`。
2.  **移动语义 (Move Semantics)**：
    充分利用 C++14 的 `std::move` 转移大块的 `std::vector<uint8_t>` 缓冲区，避免数据拷贝。
3.  **泛型 Lambda 表达式 (C++14 特性)**：
    在 Asio 的异步回调中，可以使用 C++14 的泛型 Lambda 来简化代码结构，例如 `auto` 形参：
    ```cpp
    asio::async_read(socket_, buffer, [self = shared_from_this()](const auto& error, std::size_t bytes_transferred) {
        // 处理读取到的数据
    });
    ```
4.  **.1MQTT 3.1 的特殊性**：
    
    *   协议名：MQTT 3.1 的协议名（Protocol Name）为 `"MQTT"`，对应的协议版本（Protocol Level）为 4。
    *   客户端标识符（Client ID）：支持 0 字节到 65535 字节的 UTF-8 编码字符串。支持空 Client ID（0 字节长度）：如果客户端发送空的 Client ID，只要 CleanSession 标志设为 1，服务器就必须为该客户端自动分配一个唯一的 Client ID。

### 四、 开发步骤建议

如果您准备着手实现，建议按照以下阶段分步推进：

*   **阶段 1**：实现 `buffer_writer` / `buffer_reader`，并针对 `CONNECT`, `CONNACK`, `PINGREQ`, `PINGRESP` 的结构体编写单元测试，确保序列化与反序列化字节流正确。
*   **阶段 2**：使用 Standalone Asio 建立 TCP 连接，并完成手动的 `CONNECT` / `CONNACK` 握手流程。
*   **阶段 3**：引入 `asio::steady_timer` 实现 Keep-Alive 心跳机制（`PINGREQ` / `PINGRESP`）。
*   **阶段 4**：实现 QoS 0 的 `PUBLISH` 以及 `SUBSCRIBE` / `SUBACK` 流程，此时已可进行基本的数据收发。
*   **阶段 5**（可选）：根据需求，完善 QoS 1 的重发机制和边界异常处理。

# Redis Clone - Java Implementation

A multi-threaded Redis server clone written in Java with no external dependencies. This implementation follows the Redis Serialization Protocol (RESP) and demonstrates practical application of object-oriented design principles.

## Project Structure

The codebase is organized into six focused classes:

```
RedisServer.java       Main server class and entry point
ClientHandler.java     Manages individual client connections  
CommandExecutor.java   Routes and executes Redis commands
RedisStorage.java      Thread-safe data storage layer
RespParser.java        Parses incoming RESP protocol messages
RespBuilder.java       Formats responses in RESP protocol
```

## Architecture

The design follows a dependency injection pattern where the main server creates core components and injects them into handler classes. This approach keeps the code modular and testable.

**RedisServer** sets up the server socket and manages a thread pool for handling concurrent connections. It creates the storage layer and command executor, then passes these to each client handler.

**ClientHandler** manages the lifecycle of a single client connection. It reads commands from the socket, delegates execution to the command executor, and sends responses back to the client.

**CommandExecutor** receives parsed commands and routes them to the appropriate handler methods. It interacts with the storage layer to perform operations and returns properly formatted RESP responses.

**RedisStorage** encapsulates all data storage operations using ConcurrentHashMap for thread safety. It handles both regular key-value storage and TTL (time-to-live) expiration using lazy deletion.

**RespParser and RespBuilder** handle protocol conversion. The parser reads incoming RESP messages and converts them to Java data structures. The builder does the reverse, formatting responses according to RESP specifications.

## Supported Commands

The server implements these Redis commands:

- **PING** - Returns PONG (health check)
- **ECHO** - Returns the provided message
- **SET** - Stores a key-value pair with optional expiration (PX/EX)
- **GET** - Retrieves a value by key
- **DEL** - Deletes one or more keys
- **CONFIG GET** - Returns configuration parameters (stub for redis-cli compatibility)

## Building and Running

Compile all source files:
```bash
javac *.java
```

Start the server on the default port (6379):
```bash
java RedisServer
```

Or specify a custom port:
```bash
java RedisServer 7000
```

## Testing

The server works with the standard Redis CLI. If you have Redis installed:

```bash
redis-cli -p 6379
```

Example session:
```
127.0.0.1:6379> PING
PONG

127.0.0.1:6379> SET message "Hello, Redis"
OK

127.0.0.1:6379> GET message
"Hello, Redis"

127.0.0.1:6379> SET temporary "expires soon" PX 5000
OK

127.0.0.1:6379> GET temporary
"expires soon"

# Wait 6 seconds

127.0.0.1:6379> GET temporary
(nil)
```

If redis-cli is not available, you can test using netcat:
```bash
printf "*1\r\n\$4\r\nPING\r\n" | nc localhost 6379
```

## Implementation Details

**Thread Safety**: The storage layer uses ConcurrentHashMap internally, which provides thread-safe operations without explicit locking. Each client handler runs in its own thread and operates independently.

**TTL Expiration**: Keys with expiration times are checked lazily during GET operations. When a GET request is made for an expired key, the key is removed from storage and null is returned. This approach avoids the need for background cleanup threads.

**Protocol Handling**: The parser uses BufferedReader instead of Scanner because Scanner consumes whitespace characters that are significant in the RESP protocol. All responses include explicit CRLF line endings as required by the specification.

**Concurrency Model**: The server uses a cached thread pool (Java 17+) or can be configured to use virtual threads (Java 21+). The cached pool automatically scales the number of threads based on demand.

## Design Rationale

The multi-class structure serves several purposes. Each class has a single, well-defined responsibility, which makes the code easier to understand and modify. Dependencies are injected rather than created internally, which allows for easier testing and flexibility in swapping implementations.

The separation of protocol handling (parsing and building) from business logic (command execution) means protocol changes don't affect command implementations and vice versa. Similarly, isolating storage operations makes it straightforward to add persistence or change the underlying storage mechanism.

This structure also facilitates parallel development. Multiple developers can work on different classes simultaneously without conflicts, and new commands can be added by modifying only the CommandExecutor class.

## Technical Considerations

**Java Version**: Requires Java 17 or higher. The code uses standard library features and does not depend on any external packages.

**Performance**: The server handles concurrent connections efficiently through thread pooling. Response times are typically sub-millisecond for simple operations. The main bottleneck is typically network I/O rather than computation.

**Memory**: All data is stored in memory with no persistence layer. The server's memory footprint grows with the number of stored keys and the size of their values.

**Limitations**: This implementation prioritizes clarity and educational value over performance optimization. A production Redis server includes many optimizations not present here, such as pipelining, pub/sub, persistence, replication, and more sophisticated data structures.

## Future Enhancements

Several features could be added to make this more complete:

- Additional data structures (lists, sets, sorted sets, hashes)
- Persistence through append-only files or snapshots
- Pub/sub messaging
- Authentication and access control
- Pipelining for batch operations
- Transaction support (MULTI/EXEC)
- Active expiration of old keys
- Monitoring and statistics

## Notes

This project was created as a learning exercise to demonstrate understanding of network programming, protocol implementation, and concurrent systems design. The code emphasizes readability and proper software engineering practices over raw performance.

The implementation follows the official Redis protocol specification where applicable, but does not attempt to replicate all of Redis's behavior or performance characteristics.

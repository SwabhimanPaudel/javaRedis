# javaRedis Architecture Review & Improvement Plan

## 1. Architectural Review

### Current Structure

The project follows a clean 6-class separation:
- **RedisServer**: Main entry point, manages ServerSocket and thread pool
- **ClientHandler**: Per-connection handler (Runnable)
- **CommandExecutor**: Command routing and execution
- **RedisStorage**: Thread-safe key-value store with TTL
- **RespParser**: Parses incoming RESP protocol
- **RespBuilder**: Builds RESP responses

### Strengths

**Clean Separation of Concerns**
- Protocol layer (Parser/Builder) isolated from business logic
- Storage abstracted from command execution
- Each class has a single, clear responsibility

**Thread Safety**
- ConcurrentHashMap usage for storage
- No shared mutable state between client handlers
- Thread-per-connection model is simple and correct

**Educational Value**
- Code is readable and well-structured
- No complex abstractions that obscure learning
- RESP protocol implementation is clear

### Critical Problems

**HIGH SEVERITY**

1. **Race Condition in TTL Operations**
   ```java
   // RedisStorage.java, lines 26-29 and 39-42
   public void set(String key, String value) {
       data.put(key, value);
       expirations.remove(key);  // RACE: Not atomic with put()
   }
   ```
   **Issue**: Between `put()` and `remove()`, another thread could insert an expiration, leading to:
   - Keys with values but missing expirations
   - Keys with expirations but missing values
   
2. **Resource Leak in Executor Shutdown**
   ```java
   // RedisServer.java, line 105
   executor.shutdown();  // Missing awaitTermination()
   ```
   **Issue**: Server stops but threads may still be processing, causing:
   - Incomplete command processing
   - Socket resources not properly released
   - Ungraceful client disconnections
   
3. **No Backpressure Control**
   ```java
   // RedisServer.java, line 51
   return Executors.newCachedThreadPool();
   ```
   **Issue**: Unbounded thread creation can exhaust resources under load
   - 10,000 clients = 10,000 threads
   - Each thread consumes ~1MB stack space
   - No connection limits

4. **Storage Memory Leak**
   ```java
   // RedisStorage.java - no active expiration
   ```
   **Issue**: Expired keys remain in memory until accessed
   - Keys set with TTL but never GET'd leak forever
   - Memory grows unbounded over time
   
**MEDIUM SEVERITY**

5. **GET Operation Race Condition**
   ```java
   // RedisStorage.java, lines 51-58
   public String get(String key) {
       if (isExpired(key)) {
           delete(key);  // RACE: Key could be re-set between check and delete
           return null;
       }
       return data.get(key);  // RACE: Key could expire here
   }
   ```

6. **Inefficient DEL Command**
   ```java
   // CommandExecutor.java, lines 159-164
   for (int i = 1; i < command.size(); i++) {
       String key = command.get(i);
       if (storage.delete(key)) {  // Separate map operations per key
           deletedCount++;
       }
   }
   ```

7. **No Connection Timeout**
   - Idle clients hold threads forever
   - Single slow client blocks a thread indefinitely

8. **Synchronous I/O Bottleneck**
   - Each thread blocks on I/O operations
   - Poor CPU utilization with many clients

**LOW SEVERITY**

9. **StringBuilder Inefficiency in RespBuilder**
   - Line 82: Creating StringBuilder per array
   
10. **Duplicate Socket Close Logic**
    - ClientHandler has two `closeQuietly()` methods

---

## 2. Priority Improvements

### HIGH IMPACT (Critical for Correctness)

#### 1. Fix TTL Atomic Operations
**Problem**: set() and setWithExpiry() have race conditions

**Solution**: Use compute() for atomic operations
```java
public void set(String key, String value) {
    data.put(key, value);
    expirations.compute(key, (k, v) -> null);  // Atomic remove
}

public void setWithExpiry(String key, String value, long expirationMillis) {
    long expiryTime = System.currentTimeMillis() + expirationMillis;
    data.compute(key, (k, v) -> value);  // Atomic put
    expirations.compute(key, (k, v) -> expiryTime);
}
```

Better: Use a single atomic operation
```java
public void set(String key, String value) {
    data.put(key, value);
    expirations.remove(key);
}

// Make both operations atomic using a wrapper class
private static class ValueWithExpiry {
    final String value;
    final Long expiryTime;  // null = no expiry
    
    ValueWithExpiry(String value, Long expiryTime) {
        this.value = value;
        this.expiryTime = expiryTime;
    }
}

// Single ConcurrentHashMap<String, ValueWithExpiry>
```

#### 2. Implement Graceful Shutdown
```java
public void stop() {
    try {
        if (serverSocket != null && !serverSocket.isClosed()) {
            serverSocket.close();
        }
        
        executor.shutdown();  // Stop accepting new tasks
        
        // Wait for existing connections to finish
        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
            executor.shutdownNow();  // Force shutdown after timeout
            
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                System.err.println("Thread pool did not terminate");
            }
        }
        
        System.out.println("Server stopped.");
    } catch (IOException | InterruptedException e) {
        executor.shutdownNow();
        Thread.currentThread().interrupt();
        System.err.println("Error stopping server: " + e.getMessage());
    }
}
```

#### 3. Add Connection Limits
```java
private static final int MAX_CONNECTIONS = 1000;
private final Semaphore connectionLimiter = new Semaphore(MAX_CONNECTIONS);

private void acceptConnections() {
    while (!serverSocket.isClosed()) {
        try {
            if (!connectionLimiter.tryAcquire(100, TimeUnit.MILLISECONDS)) {
                continue;  // Max connections reached, wait
            }
            
            Socket clientSocket = serverSocket.accept();
            handleClientConnection(clientSocket);
            
        } catch (IOException e) {
            if (!serverSocket.isClosed()) {
                System.err.println("Error accepting connection: " + e.getMessage());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
        }
    }
}

// In ClientHandler.run():
try {
    // ... existing code ...
} finally {
    cleanup();
    connectionLimiter.release();  // Release permit
}
```

### MEDIUM IMPACT (Performance & Scalability)

#### 4. Add Active Expiration (Background Cleanup)
```java
public class RedisStorage {
    private final ScheduledExecutorService cleanupExecutor;
    private static final int CLEANUP_INTERVAL_MS = 1000;  // 1 second
    private static final int KEYS_PER_CLEANUP = 100;
    
    public RedisStorage() {
        this.data = new ConcurrentHashMap<>();
        this.expirations = new ConcurrentHashMap<>();
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
        
        // Start periodic cleanup
        cleanupExecutor.scheduleAtFixedRate(
            this::cleanupExpiredKeys,
            CLEANUP_INTERVAL_MS,
            CLEANUP_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        );
    }
    
    private void cleanupExpiredKeys() {
        long now = System.currentTimeMillis();
        int cleaned = 0;
        
        // Sample random keys (don't scan entire map)
        for (Map.Entry<String, Long> entry : expirations.entrySet()) {
            if (cleaned >= KEYS_PER_CLEANUP) break;
            
            if (entry.getValue() <= now) {
                String key = entry.getKey();
                delete(key);
                cleaned++;
            }
        }
    }
    
    public void shutdown() {
        cleanupExecutor.shutdown();
    }
}
```

#### 5. Implement Fixed Thread Pool
```java
private static final int CORE_THREADS = Runtime.getRuntime().availableProcessors() * 2;
private static final int MAX_THREADS = 200;
private static final int QUEUE_SIZE = 1000;

private ExecutorService createExecutorService() {
    return new ThreadPoolExecutor(
        CORE_THREADS,
        MAX_THREADS,
        60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(QUEUE_SIZE),
        new ThreadPoolExecutor.CallerRunsPolicy()  // Backpressure
    );
}
```

#### 6. Add Socket Timeouts
```java
// In ClientHandler.initializeStreams()
private void initializeStreams() throws IOException {
    socket.setSoTimeout(30000);  // 30 second read timeout
    socket.setTcpNoDelay(true);   // Disable Nagle's algorithm
    socket.setKeepAlive(true);     // Enable TCP keepalive
    
    InputStream inputStream = socket.getInputStream();
    OutputStream outputStream = socket.getOutputStream();
    
    reader = new BufferedReader(new InputStreamReader(inputStream));
    writer = new PrintWriter(new OutputStreamWriter(outputStream), true);
}
```

#### 7. Optimize GET Operation
```java
public String get(String key) {
    // Atomic check-and-get using compute
    return data.computeIfPresent(key, (k, v) -> {
        Long expiry = expirations.get(k);
        if (expiry != null && System.currentTimeMillis() > expiry) {
            // Expired - trigger removal
            expirations.remove(k);
            return null;  // Removes from data map
        }
        return v;  // Keep in map
    });
}
```

### LOW IMPACT (Code Quality)

#### 8. Extract Configuration Class
```java
public class RedisConfig {
    private final int port;
    private final int maxConnections;
    private final int coreThreads;
    private final int maxThreads;
    private final int socketTimeoutMs;
    
    private RedisConfig(Builder builder) {
        this.port = builder.port;
        this.maxConnections = builder.maxConnections;
        this.coreThreads = builder.coreThreads;
        this.maxThreads = builder.maxThreads;
        this.socketTimeoutMs = builder.socketTimeoutMs;
    }
    
    public static class Builder {
        private int port = 6379;
        private int maxConnections = 1000;
        private int coreThreads = Runtime.getRuntime().availableProcessors() * 2;
        private int maxThreads = 200;
        private int socketTimeoutMs = 30000;
        
        public Builder port(int port) {
            this.port = port;
            return this;
        }
        
        public RedisConfig build() {
            return new RedisConfig(this);
        }
    }
}
```

#### 9. Add Metrics Collection
```java
public class ServerMetrics {
    private final AtomicLong totalConnections = new AtomicLong();
    private final AtomicLong activeConnections = new AtomicLong();
    private final AtomicLong totalCommands = new AtomicLong();
    private final ConcurrentHashMap<String, AtomicLong> commandCounts = new ConcurrentHashMap<>();
    
    public void recordConnection() {
        totalConnections.incrementAndGet();
        activeConnections.incrementAndGet();
    }
    
    public void recordDisconnection() {
        activeConnections.decrementAndGet();
    }
    
    public void recordCommand(String cmdName) {
        totalCommands.incrementAndGet();
        commandCounts.computeIfAbsent(cmdName, k -> new AtomicLong()).incrementAndGet();
    }
    
    public void printStats() {
        System.out.println("Connections: " + activeConnections.get() + " active, " + 
                          totalConnections.get() + " total");
        System.out.println("Commands: " + totalCommands.get());
    }
}
```

---

## 3. Code-Level Suggestions

### Refactoring 1: Unified Storage Entry

**Problem**: Separate maps for data and expirations create race conditions

**Solution**: Single map with compound value
```java
public class RedisStorage {
    private static class Entry {
        final String value;
        final long expiryTime;  // 0 = no expiry
        
        Entry(String value) {
            this(value, 0);
        }
        
        Entry(String value, long expiryTime) {
            this.value = value;
            this.expiryTime = expiryTime;
        }
        
        boolean isExpired() {
            return expiryTime > 0 && System.currentTimeMillis() > expiryTime;
        }
    }
    
    private final ConcurrentHashMap<String, Entry> store;
    
    public void set(String key, String value) {
        store.put(key, new Entry(value));
    }
    
    public void setWithExpiry(String key, String value, long expirationMillis) {
        long expiryTime = System.currentTimeMillis() + expirationMillis;
        store.put(key, new Entry(value, expiryTime));
    }
    
    public String get(String key) {
        Entry entry = store.computeIfPresent(key, (k, v) -> 
            v.isExpired() ? null : v
        );
        return entry != null ? entry.value : null;
    }
    
    public boolean delete(String key) {
        return store.remove(key) != null;
    }
}
```

### Refactoring 2: Command Registry Pattern

**Problem**: Large switch statement in CommandExecutor is hard to extend

**Solution**: Command pattern with registry
```java
public interface Command {
    String execute(List<String> args, RedisStorage storage);
    int minArgs();
    int maxArgs();
}

public class CommandRegistry {
    private final Map<String, Command> commands = new ConcurrentHashMap<>();
    
    public void register(String name, Command command) {
        commands.put(name.toUpperCase(), command);
    }
    
    public String execute(List<String> command, RedisStorage storage) {
        if (command == null || command.isEmpty()) {
            return RespBuilder.buildError("ERR empty command");
        }
        
        String cmdName = command.get(0).toUpperCase();
        Command cmd = commands.get(cmdName);
        
        if (cmd == null) {
            return RespBuilder.buildError("ERR unknown command '" + cmdName + "'");
        }
        
        try {
            return cmd.execute(command.subList(1, command.size()), storage);
        } catch (Exception e) {
            return RespBuilder.buildError("ERR " + e.getMessage());
        }
    }
}

// Example command implementation
public class GetCommand implements Command {
    @Override
    public String execute(List<String> args, RedisStorage storage) {
        if (args.size() < 1) {
            return RespBuilder.buildError("ERR wrong number of arguments for 'get' command");
        }
        String value = storage.get(args.get(0));
        return RespBuilder.buildBulkString(value);
    }
    
    @Override
    public int minArgs() { return 1; }
    
    @Override
    public int maxArgs() { return 1; }
}

// Usage
CommandRegistry registry = new CommandRegistry();
registry.register("GET", new GetCommand());
registry.register("SET", new SetCommand());
```

### Refactoring 3: NIO for Better Scalability

**Problem**: Thread-per-connection doesn't scale to thousands of clients

**Solution**: Non-blocking I/O with Selector (advanced)
```java
public class NioRedisServer {
    private Selector selector;
    private ServerSocketChannel serverChannel;
    private final CommandExecutor executor;
    
    public void start(int port) throws IOException {
        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(port));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        
        System.out.println("NIO server listening on port " + port);
        
        while (true) {
            selector.select();  // Block until events
            
            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();
                
                if (key.isAcceptable()) {
                    handleAccept();
                } else if (key.isReadable()) {
                    handleRead(key);
                }
            }
        }
    }
    
    private void handleAccept() throws IOException {
        SocketChannel client = serverChannel.accept();
        client.configureBlocking(false);
        client.register(selector, SelectionKey.OP_READ);
    }
    
    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        
        int bytesRead = channel.read(buffer);
        if (bytesRead == -1) {
            channel.close();
            return;
        }
        
        // Parse and execute command...
    }
}
```

---

## 4. Testing & Verification Plan

### Unit Tests

```java
// RedisStorageTest.java
public class RedisStorageTest {
    private RedisStorage storage;
    
    @BeforeEach
    void setUp() {
        storage = new RedisStorage();
    }
    
    @Test
    void testSetAndGet() {
        storage.set("key1", "value1");
        assertEquals("value1", storage.get("key1"));
    }
    
    @Test
    void testExpiration() throws InterruptedException {
        storage.setWithExpiry("temp", "value", 100);  // 100ms TTL
        assertEquals("value", storage.get("temp"));
        
        Thread.sleep(150);
        assertNull(storage.get("temp"));
    }
    
    @Test
    void testConcurrentAccess() throws InterruptedException {
        int numThreads = 10;
        int opsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                for (int j = 0; j < opsPerThread; j++) {
                    String key = "key-" + threadId + "-" + j;
                    storage.set(key, "value-" + j);
                    assertEquals("value-" + j, storage.get(key));
                }
            });
        }
        
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        
        // Verify no data corruption
        assertEquals(numThreads * opsPerThread, storage.size());
    }
    
    @Test
    void testRaceConditionInSetWithExpiry() throws Exception {
        // Simulate race between set() and setWithExpiry()
        ExecutorService executor = Executors.newFixedThreadPool(2);
        
        for (int i = 0; i < 10000; i++) {
            storage.clear();
            final String key = "racekey";
            
            Future<?> f1 = executor.submit(() -> storage.set(key, "v1"));
            Future<?> f2 = executor.submit(() -> storage.setWithExpiry(key, "v2", 10000));
            
            f1.get();
            f2.get();
            
            // Verify consistency: either no expiry or has expiry, but not orphaned
            String value = storage.get(key);
            assertNotNull(value);  // Should never be null from race
        }
        
        executor.shutdown();
    }
}
```

### Integration Tests

```java
// RedisServerIntegrationTest.java
public class RedisServerIntegrationTest {
    private RedisServer server;
    private final int testPort = 16379;
    
    @BeforeEach
    void startServer() throws IOException {
        server = new RedisServer(testPort);
        new Thread(() -> {
            try {
                server.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
        
        // Wait for server to start
        try { Thread.sleep(500); } catch (InterruptedException e) {}
    }
    
    @AfterEach
    void stopServer() {
        server.stop();
    }
    
    @Test
    void testBasicCommandFlow() throws IOException {
        try (Socket socket = new Socket("localhost", testPort);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            
            // PING
            out.print("*1\r\n$4\r\nPING\r\n");
            out.flush();
            assertEquals("+PONG", in.readLine());
            
            // SET
            out.print("*3\r\n$3\r\nSET\r\n$4\r\ntest\r\n$5\r\nvalue\r\n");
            out.flush();
            assertEquals("+OK", in.readLine());
            
            // GET
            out.print("*2\r\n$3\r\nGET\r\n$4\r\ntest\r\n");
            out.flush();
            assertEquals("$5", in.readLine());
            assertEquals("value", in.readLine());
        }
    }
    
    @Test
    void testConcurrentClients() throws Exception {
        int numClients = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numClients);
        
        List<Future<Boolean>> futures = new ArrayList<>();
        for (int i = 0; i < numClients; i++) {
            final int clientId = i;
            futures.add(executor.submit(() -> {
                try (Socket socket = new Socket("localhost", testPort)) {
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    
                    // Each client sets and gets its own key
                    String key = "client-" + clientId;
                    String value = "value-" + clientId;
                    
                    out.print(String.format("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
                        key.length(), key, value.length(), value));
                    out.flush();
                    in.readLine();  // Read OK
                    
                    out.print(String.format("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", 
                        key.length(), key));
                    out.flush();
                    in.readLine();  // Read length
                    String result = in.readLine();
                    
                    return value.equals(result);
                } catch (IOException e) {
                    return false;
                }
            }));
        }
        
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
        
        for (Future<Boolean> future : futures) {
            assertTrue(future.get());
        }
    }
}
```

### RESP Parser Tests

```java
public class RespParserTest {
    @Test
    void testParseSimpleString() throws IOException {
        String input = "+OK\r\n";
        List<String> result = parseFromString(input);
        assertEquals(List.of("OK"), result);
    }
    
    @Test
    void testParseBulkString() throws IOException {
        String input = "$5\r\nhello\r\n";
        List<String> result = parseFromString(input);
        assertEquals(List.of("hello"), result);
    }
    
    @Test
    void testParseArray() throws IOException {
        String input = "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        List<String> result = parseFromString(input);
        assertEquals(List.of("GET", "key"), result);
    }
    
    @Test
    void testParseNullBulkString() throws IOException {
        String input = "$-1\r\n";
        List<String> result = parseFromString(input);
        assertTrue(result.isEmpty());
    }
    
    private List<String> parseFromString(String input) throws IOException {
        BufferedReader reader = new BufferedReader(new StringReader(input));
        return RespParser.parse(reader);
    }
}
```

### Performance/Load Tests

```java
public class RedisServerLoadTest {
    @Test
    void testThroughput() throws Exception {
        // Measure ops/sec
        int duration = 10;  // seconds
        AtomicLong opsCompleted = new AtomicLong();
        
        ExecutorService executor = Executors.newFixedThreadPool(10);
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < 10; i++) {
            executor.submit(() -> {
                try (Socket socket = new Socket("localhost", 16379)) {
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    
                    while (System.currentTimeMillis() - startTime < duration * 1000) {
                        out.print("*1\r\n$4\r\nPING\r\n");
                        out.flush();
                        in.readLine();
                        opsCompleted.incrementAndGet();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        
        executor.shutdown();
        executor.awaitTermination(duration + 5, TimeUnit.SECONDS);
        
        long totalOps = opsCompleted.get();
        double opsPerSec = totalOps / (double) duration;
        System.out.println("Throughput: " + opsPerSec + " ops/sec");
        
        assertTrue(opsPerSec > 1000, "Expected >1000 ops/sec, got " + opsPerSec);
    }
}
```

---

## 5. Extended Roadmap

### Phase 1: Foundation (Weeks 1-2)
- [ ] Fix critical race conditions in RedisStorage
- [ ] Implement graceful shutdown
- [ ] Add connection limits and backpressure
- [ ] Add socket timeouts
- [ ] Create unit test suite
- [ ] Add metrics collection

### Phase 2: Scalability (Weeks 3-4)
- [ ] Implement active expiration background task
- [ ] Switch to bounded thread pool
- [ ] Add command execution benchmarks
- [ ] Optimize hot paths (GET/SET)
- [ ] Add connection pooling on client side (test harness)

### Phase 3: Redis Compatibility (Weeks 5-6)
- [ ] Implement more data types:
  - [ ] INCR/DECR (atomic integers)
  - [ ] LPUSH/RPUSH/LPOP/RPOP (lists)
  - [ ] SADD/SMEMBERS (sets)
  - [ ] HSET/HGET (hashes)
- [ ] Add KEYS/SCAN commands
- [ ] Implement EXPIRE/TTL commands
- [ ] Add EXISTS command

### Phase 4: Advanced Features (Weeks 7-8)
- [ ] Pub/Sub:
  ```java
  // Simple approach
  public class PubSubManager {
      private final ConcurrentHashMap<String, Set<ClientHandler>> subscriptions;
      
      public void subscribe(String channel, ClientHandler client) {
          subscriptions.computeIfAbsent(channel, k -> ConcurrentHashMap.newKeySet())
                      .add(client);
      }
      
      public void publish(String channel, String message) {
          Set<ClientHandler> subscribers = subscriptions.get(channel);
          if (subscribers != null) {
              String resp = RespBuilder.buildArray(List.of("message", channel, message));
              subscribers.forEach(client -> client.send(resp));
          }
      }
  }
  ```

- [ ] Transactions (MULTI/EXEC):
  ```java
  public class Transaction {
      private final List<List<String>> commands = new ArrayList<>();
      private boolean inTransaction = false;
      
      public void multi() {
          inTransaction = true;
          commands.clear();
      }
      
      public String exec(RedisStorage storage) {
          if (!inTransaction) {
              return RespBuilder.buildError("ERR EXEC without MULTI");
          }
          
          List<String> results = new ArrayList<>();
          for (List<String> cmd : commands) {
              results.add(executor.execute(cmd));
          }
          
          inTransaction = false;
          commands.clear();
          return RespBuilder.buildArray(results);
      }
  }
  ```

### Phase 5: Persistence (Weeks 9-10)
- [ ] Implement AOF (Append-Only File):
  ```java
  public class AOFLogger {
      private final FileWriter writer;
      
      public void logCommand(List<String> command) throws IOException {
          // Write in RESP format for easy replay
          String resp = formatCommandAsResp(command);
          writer.write(resp);
          writer.flush();
      }
      
      public void replay(CommandExecutor executor) throws IOException {
          BufferedReader reader = new BufferedReader(new FileReader("redis.aof"));
          List<String> command;
          while ((command = RespParser.parse(reader)) != null) {
              executor.execute(command);
          }
      }
  }
  ```

- [ ] Implement RDB-style snapshots:
  ```java
  public class RDBSnapshot {
      public void save(RedisStorage storage, String filename) throws IOException {
          try (ObjectOutputStream out = new ObjectOutputStream(
                   new FileOutputStream(filename))) {
              out.writeObject(storage.getAllEntries());
          }
      }
      
      public void load(RedisStorage storage, String filename) throws IOException {
          try (ObjectInputStream in = new ObjectInputStream(
                   new FileInputStream(filename))) {
              @SuppressWarnings("unchecked")
              Map<String, String> data = (Map<String, String>) in.readObject();
              storage.loadAll(data);
          }
      }
  }
  ```

### Phase 6: NIO Migration (Advanced - Weeks 11-12)
- [ ] Implement non-blocking I/O with Selector
- [ ] Add pipeline support
- [ ] Benchmark blocking vs. non-blocking
- [ ] Compare resource usage

---

## Summary

**Immediate Actions (Do This Week)**
1. Fix RedisStorage race conditions (HIGH)
2. Implement graceful shutdown (HIGH)
3. Add connection limits (HIGH)
4. Create basic test suite (HIGH)

**Next Steps (Next Month)**
5. Add active expiration cleanup (MEDIUM)
6. Switch to bounded thread pool (MEDIUM)
7. Implement socket timeouts (MEDIUM)
8. Add more Redis commands (MEDIUM)

**Future Enhancements (Learning Goals)**
9. Pub/Sub messaging
10. Persistence (AOF/RDB)
11. NIO-based server
12. Advanced data structures

The current architecture is a solid foundation for learning. The priority improvements focus on correctness first, performance second, and features third. This maintains the educational value while making the server production-grade.

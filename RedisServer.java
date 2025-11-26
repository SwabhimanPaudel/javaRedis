import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * RedisServer - Main server class
 * 
 * Sets up a TCP server that accepts connections and handles Redis commands.
 * Uses a bounded thread pool with connection limits for production readiness.
 * 
 * @version 2.1
 */
public class RedisServer {

    private static final int DEFAULT_PORT = 6379;
    private static final String SERVER_VERSION = "2.1.0";

    // Thread pool configuration
    private static final int CORE_THREADS = Runtime.getRuntime().availableProcessors() * 2;
    private static final int MAX_THREADS = 200;
    private static final int QUEUE_SIZE = 1000;

    // Connection limits
    private static final int MAX_CONNECTIONS = 1000;

    private final int port;
    private final RedisStorage storage;
    private final CommandExecutor commandExecutor;
    private final ExecutorService executor;
    private final Semaphore connectionLimiter;
    private ServerSocket serverSocket;
    private volatile boolean running = true;

    public RedisServer(int port) {
        this.port = port;
        this.storage = new RedisStorage();
        this.commandExecutor = new CommandExecutor(storage);
        this.executor = createExecutorService();
        this.connectionLimiter = new Semaphore(MAX_CONNECTIONS);
    }

    /**
     * Create a bounded thread pool with backpressure control.
     * CallerRunsPolicy provides natural backpressure when queue is full.
     */
    private ExecutorService createExecutorService() {
        return new ThreadPoolExecutor(
                CORE_THREADS,
                MAX_THREADS,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(QUEUE_SIZE),
                new ThreadPoolExecutor.CallerRunsPolicy() // Backpressure
        );
    }

    /**
     * Start the Redis server and accept connections.
     */
    public void start() throws IOException {
        printBanner();

        serverSocket = new ServerSocket(port);

        System.out.println("Listening on port " + port);
        System.out.println("Max connections: " + MAX_CONNECTIONS);
        System.out.println("Thread pool: " + CORE_THREADS + "-" + MAX_THREADS + " threads");
        System.out.println("Waiting for connections...");
        System.out.println();

        acceptConnections();
    }

    /**
     * Accept client connections with connection limiting.
     */
    private void acceptConnections() {
        while (running && !serverSocket.isClosed()) {
            try {
                // Try to acquire connection permit (with timeout to check running flag)
                if (!connectionLimiter.tryAcquire(100, TimeUnit.MILLISECONDS)) {
                    continue; // Max connections reached, wait and retry
                }

                Socket clientSocket = serverSocket.accept();
                handleClientConnection(clientSocket);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("Server interrupted, stopping...");
                break;
            } catch (IOException e) {
                if (running && !serverSocket.isClosed()) {
                    System.err.println("Error accepting connection: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Handle a new client connection by submitting to thread pool.
     */
    private void handleClientConnection(Socket clientSocket) {
        ClientHandler handler = new ClientHandler(clientSocket, commandExecutor, connectionLimiter);
        executor.submit(handler);
    }

    /**
     * Stop the server gracefully.
     * Waits for existing connections to finish with timeout.
     */
    public void stop() {
        running = false;

        try {
            // Close server socket to stop accepting new connections
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }

            // Shutdown executor and wait for tasks to complete
            executor.shutdown();
            System.out.println("Waiting for active connections to finish...");

            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                System.out.println("Timeout reached, forcing shutdown...");
                executor.shutdownNow();

                // Wait a bit more for forced shutdown
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    System.err.println("Thread pool did not terminate cleanly");
                }
            }

            // Shutdown storage background tasks
            storage.shutdown();

            System.out.println("Server stopped.");

        } catch (IOException e) {
            System.err.println("Error closing server socket: " + e.getMessage());
        } catch (InterruptedException e) {
            executor.shutdownNow();
            storage.shutdown();
            Thread.currentThread().interrupt();
            System.err.println("Shutdown interrupted");
        }
    }

    private void printBanner() {
        System.out.println("Redis server starting...");
        System.out.println("Version: " + SERVER_VERSION);
        System.out.println();
    }

    public static void main(String[] args) {
        int port = parsePort(args);

        RedisServer server = new RedisServer(port);

        // Shutdown hook for graceful shutdown on Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            server.stop();
        }));

        try {
            server.start();
        } catch (IOException e) {
            System.err.println("Failed to start server: " + e.getMessage());
            System.exit(1);
        }
    }

    private static int parsePort(String[] args) {
        if (args.length > 0) {
            try {
                return Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number. Using default: " + DEFAULT_PORT);
            }
        }
        return DEFAULT_PORT;
    }
}

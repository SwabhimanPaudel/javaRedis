import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * RedisServer - Main server class
 * 
 * Sets up a TCP server that accepts connections and handles Redis commands.
 * Uses a thread pool to handle multiple clients concurrently.
 * 
 * @version 2.0
 */
public class RedisServer {

    private static final int DEFAULT_PORT = 6379;
    private static final String SERVER_VERSION = "2.0.0";

    private final int port;
    private final RedisStorage storage;
    private final CommandExecutor commandExecutor;
    private final ExecutorService executor;
    private ServerSocket serverSocket;

    /**
     * Constructs a new RedisServer on the specified port
     * 
     * @param port The port to listen on
     */
    public RedisServer(int port) {
        this.port = port;
        this.storage = new RedisStorage();
        this.commandExecutor = new CommandExecutor(storage);
        this.executor = createExecutorService();
    }

    /**
     * Create the executor service for handling client connections
     * 
     * For Java 21+, use: Executors.newVirtualThreadPerTaskExecutor()
     * For Java 17+, use: Executors.newCachedThreadPool()
     * 
     * @return The executor service
     */
    private ExecutorService createExecutorService() {
        // For Java 21+ Virtual Threads, uncomment the following line:
        // return Executors.newVirtualThreadPerTaskExecutor();

        // For Java 17+ compatibility:
        return Executors.newCachedThreadPool();
    }

    /**
     * Start the Redis server
     * 
     * @throws IOException If the server cannot be started
     */
    public void start() throws IOException {
        printBanner();

        serverSocket = new ServerSocket(port);

        System.out.println("Listening on port " + port);
        System.out.println("Waiting for connections...");
        System.out.println();

        acceptConnections();
    }

    /**
     * Accept client connections in a loop
     */
    private void acceptConnections() {
        while (!serverSocket.isClosed()) {
            try {
                Socket clientSocket = serverSocket.accept();
                handleClientConnection(clientSocket);
            } catch (IOException e) {
                if (!serverSocket.isClosed()) {
                    System.err.println("Error accepting connection: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Handle a new client connection by submitting it to the thread pool
     * 
     * @param clientSocket The client socket
     */
    private void handleClientConnection(Socket clientSocket) {
        ClientHandler handler = new ClientHandler(clientSocket, commandExecutor);
        executor.submit(handler);
    }

    /**
     * Stop the Redis server and clean up resources
     */
    public void stop() {
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
            executor.shutdown();
            System.out.println("Server stopped.");
        } catch (IOException e) {
            System.err.println("Error stopping server: " + e.getMessage());
        }
    }

    /**
     * Print the server startup banner
     */
    private void printBanner() {
        System.out.println("Redis server starting...");
        System.out.println("Version: " + SERVER_VERSION);
        System.out.println();
    }

    /**
     * Main entry point
     * 
     * @param args Command line arguments (optional: port number)
     */
    public static void main(String[] args) {
        int port = parsePort(args);

        RedisServer server = new RedisServer(port);

        // Add shutdown hook for graceful shutdown
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

    /**
     * Parse the port number from command line arguments
     * 
     * @param args The command line arguments
     * @return The port number to use
     */
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

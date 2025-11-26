import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * Handles the lifecycle of a single client connection.
 * Each connection runs in its own thread with configured timeouts.
 */
public class ClientHandler implements Runnable {

    private static final int SOCKET_TIMEOUT_MS = 30000; // 30 seconds

    private final Socket socket;
    private final CommandExecutor commandExecutor;
    private final Semaphore connectionLimiter;
    private BufferedReader reader;
    private PrintWriter writer;

    public ClientHandler(Socket socket, CommandExecutor commandExecutor, Semaphore connectionLimiter) {
        this.socket = socket;
        this.commandExecutor = commandExecutor;
        this.connectionLimiter = connectionLimiter;
    }

    @Override
    public void run() {
        String clientAddress = getClientAddress();
        System.out.println("Client connected: " + clientAddress);

        try {
            initializeStreams();
            processCommands();
        } catch (SocketTimeoutException e) {
            System.out.println("Client timeout: " + clientAddress);
        } catch (IOException e) {
            if (!socket.isClosed()) {
                System.err.println("Connection error: " + e.getMessage());
            }
        } finally {
            cleanup();
            connectionLimiter.release(); // Release connection permit
            System.out.println("Client disconnected: " + clientAddress);
        }
    }

    /**
     * Initialize streams with socket timeouts and TCP optimizations.
     */
    private void initializeStreams() throws IOException {
        // Configure socket for production use
        socket.setSoTimeout(SOCKET_TIMEOUT_MS); // Read timeout
        socket.setTcpNoDelay(true); // Disable Nagle's algorithm for low latency
        socket.setKeepAlive(true); // Enable TCP keepalive

        InputStream inputStream = socket.getInputStream();
        OutputStream outputStream = socket.getOutputStream();

        reader = new BufferedReader(new InputStreamReader(inputStream));
        writer = new PrintWriter(new OutputStreamWriter(outputStream), true);
    }

    /**
     * Process commands in a loop until client disconnects.
     */
    private void processCommands() throws IOException {
        while (!socket.isClosed()) {
            try {
                List<String> command = RespParser.parse(reader);

                if (command == null || command.isEmpty()) {
                    break; // Client closed connection
                }

                String response = commandExecutor.execute(command);
                sendResponse(response);

            } catch (IllegalArgumentException e) {
                sendResponse(RespBuilder.buildError("ERR " + e.getMessage()));
            }
        }
    }

    private void sendResponse(String response) {
        writer.print(response);
        writer.flush();
    }

    private String getClientAddress() {
        return socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
    }

    /**
     * Clean up all resources.
     */
    private void cleanup() {
        closeQuietly(reader);
        closeQuietly(writer);
        closeQuietly(socket);
    }

    private void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    private void closeQuietly(Socket socket) {
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }
}

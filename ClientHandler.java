import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;

/**
 * ClientHandler - Handles the lifecycle of a single client connection
 * 
 * Each client connection is handled by a separate thread. This class reads
 * commands from the client, executes them via CommandExecutor, and sends
 * responses back to the client.
 * 
 * @author Senior Java Systems Engineer
 * @version 1.0
 */
public class ClientHandler implements Runnable {

    private final Socket socket;
    private final CommandExecutor commandExecutor;
    private BufferedReader reader;
    private PrintWriter writer;

    /**
     * Constructs a new ClientHandler for the given socket
     * 
     * @param socket          The client socket
     * @param commandExecutor The command executor to use
     */
    public ClientHandler(Socket socket, CommandExecutor commandExecutor) {
        this.socket = socket;
        this.commandExecutor = commandExecutor;
    }

    @Override
    public void run() {
        String clientAddress = getClientAddress();
        System.out.println("Client connected: " + clientAddress);

        try {
            initializeStreams();
            processCommands();
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        } finally {
            cleanup();
            System.out.println("Client disconnected: " + clientAddress);
        }
    }

    /**
     * Initialize input and output streams
     * CRITICAL: Uses BufferedReader, NOT Scanner (Scanner breaks RESP protocol)
     */
    private void initializeStreams() throws IOException {
        InputStream inputStream = socket.getInputStream();
        OutputStream outputStream = socket.getOutputStream();

        reader = new BufferedReader(new InputStreamReader(inputStream));
        writer = new PrintWriter(new OutputStreamWriter(outputStream), true);
    }

    /**
     * Process commands in a loop until the client disconnects
     */
    private void processCommands() throws IOException {
        while (!socket.isClosed()) {
            try {
                // Parse the incoming RESP command
                List<String> command = RespParser.parse(reader);

                if (command == null || command.isEmpty()) {
                    // Client closed connection
                    break;
                }

                // Execute the command and get the response
                String response = commandExecutor.execute(command);

                // Send response back to client
                sendResponse(response);

            } catch (IllegalArgumentException e) {
                // Protocol error
                sendResponse(RespBuilder.buildError("ERR " + e.getMessage()));
            }
        }
    }

    /**
     * Send a response to the client
     * 
     * @param response The RESP-formatted response
     */
    private void sendResponse(String response) {
        writer.print(response);
        writer.flush();
    }

    /**
     * Get the client address for logging
     * 
     * @return The client address as a string
     */
    private String getClientAddress() {
        return socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
    }

    /**
     * Clean up resources
     */
    private void cleanup() {
        closeQuietly(reader);
        closeQuietly(writer);
        closeQuietly(socket);
    }

    /**
     * Close a Closeable resource without throwing exceptions
     * 
     * @param closeable The resource to close
     */
    private void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    /**
     * Close a socket without throwing exceptions
     * 
     * @param socket The socket to close
     */
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

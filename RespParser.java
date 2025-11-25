import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * RespParser - Parser for the Redis Serialization Protocol (RESP)
 * 
 * Parses incoming RESP messages from clients according to the official
 * Redis protocol specification.
 * 
 * Supported RESP types:
 * - Simple Strings: +OK\r\n
 * - Errors: -ERR message\r\n
 * - Integers: :1000\r\n
 * - Bulk Strings: $5\r\nhello\r\n (or $-1\r\n for null)
 * - Arrays: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
 * 
 * @author Senior Java Systems Engineer
 * @version 1.0
 */
public class RespParser {

    /**
     * Parse a RESP message from the input stream.
     * 
     * @param reader The BufferedReader to read from
     * @return A list of strings representing the parsed command, or null if
     *         connection closed
     * @throws IOException              If an I/O error occurs
     * @throws IllegalArgumentException If the RESP format is invalid
     */
    public static List<String> parse(BufferedReader reader) throws IOException {
        String line = reader.readLine();

        if (line == null) {
            return null; // Connection closed
        }

        if (line.isEmpty()) {
            throw new IllegalArgumentException("Empty line received");
        }

        char type = line.charAt(0);

        switch (type) {
            case '*': // Array
                return parseArray(reader, line);
            case '$': // Bulk String
                String bulkStr = parseBulkString(reader, line);
                return bulkStr != null ? Collections.singletonList(bulkStr) : Collections.emptyList();
            case '+': // Simple String
                return Collections.singletonList(line.substring(1));
            case '-': // Error
                throw new IllegalArgumentException(line.substring(1));
            case ':': // Integer
                return Collections.singletonList(line.substring(1));
            default:
                throw new IllegalArgumentException("Unknown RESP type: " + type);
        }
    }

    /**
     * Parse a RESP Array (e.g., *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n)
     * 
     * @param reader    The BufferedReader to read from
     * @param firstLine The first line containing the array length
     * @return A list of strings representing the array elements
     * @throws IOException If an I/O error occurs
     */
    private static List<String> parseArray(BufferedReader reader, String firstLine) throws IOException {
        int count = Integer.parseInt(firstLine.substring(1));

        if (count < 0) {
            return Collections.emptyList(); // Null array
        }

        List<String> result = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            String line = reader.readLine();

            if (line == null) {
                throw new IllegalArgumentException("Unexpected end of stream in array");
            }

            if (line.charAt(0) == '$') {
                // Bulk String
                String bulkStr = parseBulkString(reader, line);
                if (bulkStr != null) {
                    result.add(bulkStr);
                }
            } else {
                // Other types (for compatibility)
                result.add(line.substring(1));
            }
        }

        return result;
    }

    /**
     * Parse a RESP Bulk String (e.g., $5\r\nhello\r\n)
     * 
     * @param reader     The BufferedReader to read from
     * @param lengthLine The line containing the string length
     * @return The parsed string, or null for null bulk strings
     * @throws IOException If an I/O error occurs
     */
    private static String parseBulkString(BufferedReader reader, String lengthLine) throws IOException {
        int length = Integer.parseInt(lengthLine.substring(1));

        if (length == -1) {
            return null; // Null bulk string
        }

        if (length == 0) {
            reader.readLine(); // Consume the empty line
            return "";
        }

        // Read exactly 'length' characters
        char[] buffer = new char[length];
        int totalRead = 0;

        while (totalRead < length) {
            int read = reader.read(buffer, totalRead, length - totalRead);
            if (read == -1) {
                throw new IllegalArgumentException("Unexpected end of stream in bulk string");
            }
            totalRead += read;
        }

        // Consume the trailing \r\n
        reader.readLine();

        return new String(buffer);
    }
}

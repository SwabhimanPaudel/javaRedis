import java.util.List;

/**
 * RespBuilder - Builder for Redis Serialization Protocol (RESP) responses
 * 
 * Provides utility methods to format different data types into RESP protocol
 * format for sending responses to clients.
 * 
 * @author Senior Java Systems Engineer
 * @version 1.0
 */
public class RespBuilder {

    /**
     * Build a RESP Simple String response
     * 
     * Format: +OK\r\n
     * 
     * @param str The string to format
     * @return The RESP-formatted simple string
     */
    public static String buildSimpleString(String str) {
        return "+" + str + "\r\n";
    }

    /**
     * Build a RESP Error response
     * 
     * Format: -ERR message\r\n
     * 
     * @param message The error message
     * @return The RESP-formatted error
     */
    public static String buildError(String message) {
        return "-" + message + "\r\n";
    }

    /**
     * Build a RESP Integer response
     * 
     * Format: :1000\r\n
     * 
     * @param value The integer value
     * @return The RESP-formatted integer
     */
    public static String buildInteger(long value) {
        return ":" + value + "\r\n";
    }

    /**
     * Build a RESP Bulk String response
     * 
     * Format: $5\r\nhello\r\n
     * Null format: $-1\r\n
     * 
     * @param str The string to format, or null for a null bulk string
     * @return The RESP-formatted bulk string
     */
    public static String buildBulkString(String str) {
        if (str == null) {
            return "$-1\r\n"; // Null bulk string
        }

        return "$" + str.length() + "\r\n" + str + "\r\n";
    }

    /**
     * Build a RESP Array response
     * 
     * Format: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
     * Null format: *-1\r\n
     * 
     * @param elements The list of elements (each will be formatted as a bulk
     *                 string)
     * @return The RESP-formatted array
     */
    public static String buildArray(List<String> elements) {
        if (elements == null) {
            return "*-1\r\n"; // Null array
        }

        StringBuilder sb = new StringBuilder();
        sb.append("*").append(elements.size()).append("\r\n");

        for (String element : elements) {
            sb.append(buildBulkString(element));
        }

        return sb.toString();
    }

    /**
     * Build a RESP OK response
     * 
     * Convenience method for the common "+OK\r\n" response
     * 
     * @return The RESP OK response
     */
    public static String ok() {
        return buildSimpleString("OK");
    }

    /**
     * Build a RESP PONG response
     * 
     * Convenience method for the common "+PONG\r\n" response
     * 
     * @return The RESP PONG response
     */
    public static String pong() {
        return buildSimpleString("PONG");
    }
}

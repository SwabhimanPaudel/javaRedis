import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Executes Redis commands and returns RESP-formatted responses.
 * Routes commands to the appropriate handler methods.
 */
public class CommandExecutor {

    private final RedisStorage storage;

    /**
     * Constructs a new CommandExecutor with the given storage
     * 
     * @param storage The RedisStorage instance to use
     */
    public CommandExecutor(RedisStorage storage) {
        this.storage = storage;
    }

    /**
     * Execute a command and return the RESP-formatted response
     * 
     * @param command The command as a list of strings (command name + arguments)
     * @return The RESP-formatted response
     */
    public String execute(List<String> command) {
        if (command == null || command.isEmpty()) {
            return RespBuilder.buildError("ERR empty command");
        }

        // Convert command name to uppercase for case-insensitive matching
        String cmdName = command.get(0).toUpperCase();

        try {
            switch (cmdName) {
                case "PING":
                    return executePing(command);

                case "ECHO":
                    return executeEcho(command);

                case "SET":
                    return executeSet(command);

                case "GET":
                    return executeGet(command);

                case "DEL":
                    return executeDel(command);

                case "CONFIG":
                    return executeConfig(command);

                case "COMMAND":
                    // Stub for redis-cli compatibility
                    return RespBuilder.buildArray(Collections.emptyList());

                default:
                    return RespBuilder.buildError("ERR unknown command '" + cmdName + "'");
            }
        } catch (Exception e) {
            return RespBuilder.buildError("ERR " + e.getMessage());
        }
    }

    /**
     * PING [message]
     * Returns PONG or the provided message
     */
    private String executePing(List<String> command) {
        if (command.size() == 1) {
            return RespBuilder.pong();
        } else {
            return RespBuilder.buildBulkString(command.get(1));
        }
    }

    /**
     * ECHO message
     * Returns the message as a bulk string
     */
    private String executeEcho(List<String> command) {
        if (command.size() < 2) {
            return RespBuilder.buildError("ERR wrong number of arguments for 'echo' command");
        }

        return RespBuilder.buildBulkString(command.get(1));
    }

    /**
     * SET key value [PX milliseconds] [EX seconds]
     * Stores a key-value pair with optional expiration
     */
    private String executeSet(List<String> command) {
        if (command.size() < 3) {
            return RespBuilder.buildError("ERR wrong number of arguments for 'set' command");
        }

        String key = command.get(1);
        String value = command.get(2);

        // Handle optional expiration parameters
        if (command.size() >= 5) {
            String option = command.get(3).toUpperCase();

            try {
                if (option.equals("PX")) {
                    // PX - expiration in milliseconds
                    long millis = Long.parseLong(command.get(4));
                    storage.setWithExpiry(key, value, millis);
                } else if (option.equals("EX")) {
                    // EX - expiration in seconds
                    long seconds = Long.parseLong(command.get(4));
                    storage.setWithExpiry(key, value, seconds * 1000);
                } else {
                    return RespBuilder.buildError("ERR syntax error");
                }
            } catch (NumberFormatException e) {
                return RespBuilder.buildError("ERR value is not an integer or out of range");
            }
        } else {
            // No expiration
            storage.set(key, value);
        }

        return RespBuilder.ok();
    }

    /**
     * GET key
     * Returns the value associated with the key
     * Implements lazy expiration - checks if key is expired before returning
     */
    private String executeGet(List<String> command) {
        if (command.size() < 2) {
            return RespBuilder.buildError("ERR wrong number of arguments for 'get' command");
        }

        String key = command.get(1);
        String value = storage.get(key);

        return RespBuilder.buildBulkString(value);
    }

    /**
     * DEL key [key ...]
     * Deletes one or more keys
     * Returns the number of keys that were deleted
     */
    private String executeDel(List<String> command) {
        if (command.size() < 2) {
            return RespBuilder.buildError("ERR wrong number of arguments for 'del' command");
        }

        int deletedCount = 0;

        for (int i = 1; i < command.size(); i++) {
            String key = command.get(i);
            if (storage.delete(key)) {
                deletedCount++;
            }
        }

        return RespBuilder.buildInteger(deletedCount);
    }

    /**
     * CONFIG GET parameter
     * Stub implementation for redis-cli compatibility
     */
    private String executeConfig(List<String> command) {
        if (command.size() < 2) {
            return RespBuilder.buildError("ERR wrong number of arguments for 'config' command");
        }

        String subCommand = command.get(1).toUpperCase();

        if (subCommand.equals("GET")) {
            if (command.size() < 3) {
                return RespBuilder.buildError("ERR wrong number of arguments for 'config get' command");
            }

            String param = command.get(2);

            // Return a stub response to keep redis-cli happy
            List<String> response = new ArrayList<>();
            response.add(param);
            response.add(""); // Empty value

            return RespBuilder.buildArray(response);
        }

        return RespBuilder.buildError("ERR unknown CONFIG subcommand");
    }
}

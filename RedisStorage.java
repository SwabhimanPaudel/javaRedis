import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe storage for key-value pairs with TTL support.
 * Uses ConcurrentHashMap internally for concurrent access.
 */
public class RedisStorage {

    // Thread-safe storage for key-value pairs
    private final ConcurrentHashMap<String, String> data;

    // Thread-safe storage for key expiration timestamps (Unix millis)
    private final ConcurrentHashMap<String, Long> expirations;

    public RedisStorage() {
        this.data = new ConcurrentHashMap<>();
        this.expirations = new ConcurrentHashMap<>();
    }

    /**
     * Store a key-value pair
     * 
     * @param key   The key to store
     * @param value The value to store
     */
    public void set(String key, String value) {
        data.put(key, value);
        // Remove any existing expiration when setting without TTL
        expirations.remove(key);
    }

    /**
     * Store a key-value pair with expiration time
     * 
     * @param key              The key to store
     * @param value            The value to store
     * @param expirationMillis Expiration time in milliseconds from now
     */
    public void setWithExpiry(String key, String value, long expirationMillis) {
        data.put(key, value);
        long expiryTime = System.currentTimeMillis() + expirationMillis;
        expirations.put(key, expiryTime);
    }

    /**
     * Retrieve a value by key, respecting TTL expiration (lazy deletion)
     * 
     * @param key The key to retrieve
     * @return The value, or null if not found or expired
     */
    public String get(String key) {
        // Check if key has expired (lazy expiration)
        if (isExpired(key)) {
            delete(key);
            return null;
        }

        return data.get(key);
    }

    /**
     * Delete a key and its expiration
     * 
     * @param key The key to delete
     * @return true if the key existed, false otherwise
     */
    public boolean delete(String key) {
        expirations.remove(key);
        return data.remove(key) != null;
    }

    /**
     * Check if a key exists and is not expired
     * 
     * @param key The key to check
     * @return true if the key exists and is not expired
     */
    public boolean exists(String key) {
        if (isExpired(key)) {
            delete(key);
            return false;
        }
        return data.containsKey(key);
    }

    /**
     * Check if a key has expired
     * 
     * @param key The key to check
     * @return true if the key has expired
     */
    private boolean isExpired(String key) {
        Long expiryTime = expirations.get(key);
        return expiryTime != null && System.currentTimeMillis() > expiryTime;
    }

    /**
     * Get the number of keys in storage (excluding expired keys)
     * 
     * @return The number of keys
     */
    public int size() {
        return data.size();
    }

    /**
     * Clear all data and expirations
     */
    public void clear() {
        data.clear();
        expirations.clear();
    }
}

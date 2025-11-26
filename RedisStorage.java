import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Thread-safe storage for key-value pairs with TTL support.
 * Uses a unified Entry class to eliminate race conditions between data and
 * expiration maps.
 */
public class RedisStorage {

    /**
     * Internal entry that combines value and expiration time.
     * This eliminates race conditions from using separate maps.
     */
    private static class Entry {
        final String value;
        final long expiryTime; // 0 = no expiry, otherwise Unix timestamp in millis

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

    // Single map eliminates race conditions
    private final ConcurrentHashMap<String, Entry> store;

    // Background cleanup for active expiration
    private final ScheduledExecutorService cleanupExecutor;
    private static final int CLEANUP_INTERVAL_MS = 1000; // Run every second
    private static final int KEYS_PER_CLEANUP = 100; // Sample size per cycle

    public RedisStorage() {
        this.store = new ConcurrentHashMap<>();
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "expiry-cleanup");
            t.setDaemon(true); // Don't prevent JVM shutdown
            return t;
        });

        // Start periodic cleanup for active expiration
        cleanupExecutor.scheduleAtFixedRate(
                this::cleanupExpiredKeys,
                CLEANUP_INTERVAL_MS,
                CLEANUP_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Store a key-value pair without expiration.
     * ATOMIC: Single operation, no race conditions.
     */
    public void set(String key, String value) {
        store.put(key, new Entry(value));
    }

    /**
     * Store a key-value pair with expiration time.
     * ATOMIC: Single operation, no race conditions.
     */
    public void setWithExpiry(String key, String value, long expirationMillis) {
        long expiryTime = System.currentTimeMillis() + expirationMillis;
        store.put(key, new Entry(value, expiryTime));
    }

    /**
     * Retrieve a value by key, respecting TTL expiration.
     * ATOMIC: Uses computeIfPresent for atomic check-and-remove.
     */
    public String get(String key) {
        Entry entry = store.computeIfPresent(key, (k, v) -> v.isExpired() ? null : v // Remove if expired, keep if valid
        );
        return entry != null ? entry.value : null;
    }

    /**
     * Delete a key.
     * Returns true if the key existed.
     */
    public boolean delete(String key) {
        return store.remove(key) != null;
    }

    /**
     * Check if a key exists and is not expired.
     */
    public boolean exists(String key) {
        Entry entry = store.computeIfPresent(key, (k, v) -> v.isExpired() ? null : v);
        return entry != null;
    }

    /**
     * Get the number of keys in storage.
     * Note: May include some expired keys that haven't been cleaned yet.
     */
    public int size() {
        return store.size();
    }

    /**
     * Clear all data.
     */
    public void clear() {
        store.clear();
    }

    /**
     * Active expiration: periodically sample and remove expired keys.
     * This prevents memory leaks from keys that are set with TTL but never
     * accessed.
     */
    private void cleanupExpiredKeys() {
        try {
            long now = System.currentTimeMillis();
            int cleaned = 0;

            // Sample random keys without blocking for too long
            for (Map.Entry<String, Entry> entry : store.entrySet()) {
                if (cleaned >= KEYS_PER_CLEANUP) {
                    break; // Don't block too long
                }

                if (entry.getValue().isExpired()) {
                    // Atomic remove if still expired
                    store.computeIfPresent(entry.getKey(), (k, v) -> v.isExpired() ? null : v);
                    cleaned++;
                }
            }

            // Optional: log cleanup stats (comment out in production)
            // if (cleaned > 0) {
            // System.out.println("Cleaned up " + cleaned + " expired keys");
            // }

        } catch (Exception e) {
            // Don't let cleanup errors crash the background thread
            System.err.println("Error during expiration cleanup: " + e.getMessage());
        }
    }

    /**
     * Shutdown the background cleanup executor.
     * Should be called when stopping the server.
     */
    public void shutdown() {
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

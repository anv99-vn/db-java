package storage;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * BlocksStorage - Read/write data.bin file using FileChannel
 * Manages all table data persistence using block-based storage
 * <p>
 * Thread-safety strategy: per-block ReadWriteLock + in-memory cache
 * <p>
 * Locking design (2 tầng):
 * blockLocks  — per-block ReentrantReadWriteLock, chỉ lock đúng block đang truy cập.
 * Các block khác nhau chạy hoàn toàn song song.
 * fileLock    — bảo vệ metadata (fileChannel.size, close, allocate).
 * Không can thiệp vào read/write block thông thường.
 * <p>
 * Cache design:
 * blockCache  — ConcurrentHashMap<Integer, byte[]> lưu bản sao bytes của block trên RAM.
 * - getBlock : cache-hit → trả về từ RAM, không đọc file.
 * - putBlock : ghi file xong → cập nhật cache trong cùng write lock (cache luôn nhất quán).
 * - Cache lưu defensive copy (Arrays.copyOf) để tránh caller mutate làm corrupt cache.
 */
public class BlocksStorage {
    static final String DEFAULT_DATA_PATH = "data.bin";
    public static final int BLOCK_SIZE = 4096; // 4KB per block
    static final int DEFAULT_CACHE_CAPACITY = 100;
    private static final BlocksStorage instance;

    static {
        try {
            instance = new BlocksStorage();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static BlocksStorage getInstance() {
        return instance;
    }

    private final RandomAccessFile dataFile;
    private final FileChannel fileChannel;

    // Per-block locks — concurrent reads on same block allowed, write is exclusive per block.
    private final ConcurrentHashMap<Integer, ReentrantReadWriteLock> blockLocks =
            new ConcurrentHashMap<>();

    // In-memory block cache: blockId -> defensive copy of block bytes.
    // Always kept in sync with file: updated atomically inside write lock.
    // Uses LRU strategy via LinkedHashMap access-order
    private final Map<Integer, byte[]> blockCache;

    // Guards file-level metadata only (size, close, allocateAndWrite).
    private final ReentrantReadWriteLock fileLock = new ReentrantReadWriteLock();

    public BlocksStorage() throws IOException {
        this(DEFAULT_DATA_PATH, DEFAULT_CACHE_CAPACITY);
    }

    public BlocksStorage(String dataPath) throws IOException {
        this(dataPath, DEFAULT_CACHE_CAPACITY);
    }

    public BlocksStorage(String dataPath, int cacheCapacity) throws IOException {
        this.dataFile = new RandomAccessFile(dataPath, "rw");
        this.fileChannel = dataFile.getChannel();

        // Initialize LRU cache
        this.blockCache = Collections.synchronizedMap(
                new LinkedHashMap<>(cacheCapacity, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<Integer, byte[]> eldest) {
                        return size() > cacheCapacity;
                    }
                }
        );
    }

    // Returns the lock for a block, creating it atomically if absent.
    private ReentrantReadWriteLock lockForBlock(int id) {
        return blockLocks.computeIfAbsent(id, k -> new ReentrantReadWriteLock());
    }

    /**
     * Get a block by its ID.
     * <p>
     * Cache behavior:
     * HIT  — return a copy from RAM, file is not touched.
     * MISS — read from file, populate cache, return block.
     * <p>
     * Thread-safety: read lock on block `id` only — other blocks unaffected.
     *
     * @param id Block identifier
     * @return Block containing the data, or null if not found
     */
    public Block getBlock(int id, Consumer<byte[]> consumer) throws IOException {
        ReentrantReadWriteLock lock = lockForBlock(id);
        lock.readLock().lock();
        try {
            // Cache hit — serve from RAM, no file I/O needed.
            byte[] cached = blockCache.get(id);
            if (cached != null) {
                Block block = new Block(Arrays.copyOf(cached, cached.length)); // defensive copy
                block.id = id;
                return block;
            }

            // Cache miss — check file bounds, then load from disk.
            fileLock.readLock().lock();
            long fileSize;
            try {
                fileSize = fileChannel.size();
            } finally {
                fileLock.readLock().unlock();
            }

            long offset = (long) id * BLOCK_SIZE;
            if (offset >= fileSize) {
                return null;
            }

            ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);
            fileChannel.read(buffer, offset);
            buffer.flip();

            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);

            // Populate cache with a defensive copy so cache entry is independent.
            byte[] value = Arrays.copyOf(data, data.length);
            blockCache.put(id, value);

            Block block = new Block(data);
            block.id = id;
            consumer.accept(value);
            return block;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void updateBlock(int id, Consumer<byte[]> consumer) throws IOException {
        ReentrantReadWriteLock lock = lockForBlock(id);
        lock.writeLock().lock();
        try {
            byte[] data;
            byte[] cached = blockCache.get(id);
            if (cached != null) {
                // Since we're going to mutate it, we should use a copy if we want to follow defensive patterns,
                // or just mutate it directly then put it back. 
                // blockCache.put(id, copy) is used in putBlock.
                data = Arrays.copyOf(cached, cached.length);
            } else {
                // Miss
                fileLock.readLock().lock();
                try {
                    long offset = (long) id * BLOCK_SIZE;
                    if (offset >= fileChannel.size()) return;
                    ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);
                    fileChannel.read(buffer, offset);
                    data = buffer.array();
                } finally {
                    fileLock.readLock().unlock();
                }
            }

            consumer.accept(data);

            // Persist
            writeBlockToChannel(id, new Block(data));
            blockCache.put(id, Arrays.copyOf(data, data.length));
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Write a block to storage.
     * <p>
     * Cache behaviour: after writing to file, cache is updated atomically
     * inside the same write lock — cache and file are always consistent.
     * <p>
     * Thread-safety: write lock on block `id` only — other blocks unaffected.
     *
     * @param id    Block identifier
     * @param block Block data to write
     */
    public void putBlock(int id, Block block) throws IOException {
        ReentrantReadWriteLock lock = lockForBlock(id);
        lock.writeLock().lock();
        try {
            writeBlockToChannel(id, block);
            // Update cache inside write lock — no reader can observe stale data.
            blockCache.put(id, Arrays.copyOf(block.bytes, block.bytes.length));
        } finally {
            lock.writeLock().unlock();
        }
    }

    // Raw channel write — caller must hold write lock for the given block id.
    private void writeBlockToChannel(int id, Block block) throws IOException {
        long offset = (long) id * BLOCK_SIZE;

        byte[] paddedData = new byte[BLOCK_SIZE];
        System.arraycopy(block.bytes, 0, paddedData, 0,
                Math.min(block.bytes.length, BLOCK_SIZE));

        ByteBuffer buffer = ByteBuffer.wrap(paddedData);
        fileChannel.write(buffer, offset);
        fileChannel.force(false);
    }

    /**
     * Atomically allocate the next block ID and write the block.
     * <p>
     * Prevents TOCTOU: "read size → derive id → write" is one atomic operation
     * under fileLock. Per-block write lock is also held to block any concurrent
     * reader from seeing a partial write on the newly allocated block.
     * <p>
     * Cache is updated inside the combined lock scope.
     *
     * @param block Block data to append
     * @return The block ID that was assigned
     */
    public int allocateAndWrite(Block block) throws IOException {
        fileLock.writeLock().lock();
        try {
            int newId = (int) (fileChannel.size() / BLOCK_SIZE);

            ReentrantReadWriteLock blockLock = lockForBlock(newId);
            blockLock.writeLock().lock();
            try {
                block.id = newId;
                writeBlockToChannel(newId, block);
                // Cache updated atomically while both locks are held.
                blockCache.put(newId, Arrays.copyOf(block.bytes, block.bytes.length));
            } finally {
                blockLock.writeLock().unlock();
            }

            return newId;
        } finally {
            fileLock.writeLock().unlock();
        }
    }

    /**
     * Invalidate a single block from cache.
     * Useful when an external process may have modified the file directly.
     * <p>
     * Thread-safety: write lock on block `id`.
     *
     * @param id Block identifier to evict
     */
    public void invalidateCache(int id) {
        ReentrantReadWriteLock lock = lockForBlock(id);
        lock.writeLock().lock();
        try {
            blockCache.remove(id);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Clear the entire in-memory cache.
     * Forces all subsequent reads to go back to disk.
     * <p>
     * Thread-safety: file write lock (blocks all I/O during clear).
     */
    public void clearCache() {
        fileLock.writeLock().lock();
        try {
            blockCache.clear();
        } finally {
            fileLock.writeLock().unlock();
        }
    }

    /**
     * Get total number of blocks in file.
     * <p>
     * Thread-safety: file read lock only.
     */
    public int getTotalBlockCount() throws IOException {
        fileLock.readLock().lock();
        try {
            return (int) (fileChannel.size() / BLOCK_SIZE);
        } finally {
            fileLock.readLock().unlock();
        }
    }

    /**
     * Get storage and cache statistics.
     * <p>
     * fileChannel.size() is called once for a consistent snapshot.
     * Thread-safety: file read lock only.
     *
     * @return Map with storage and cache information
     */
    public Map<String, Integer> getCacheStats() throws IOException {
        fileLock.readLock().lock();
        try {
            long fileSizeBytes = fileChannel.size(); // captured once — consistent snapshot
            Map<String, Integer> stats = new HashMap<>();
            stats.put("block_size", BLOCK_SIZE);
            stats.put("file_size_bytes", (int) fileSizeBytes);
            stats.put("total_blocks", (int) (fileSizeBytes / BLOCK_SIZE));
            stats.put("cached_blocks", blockCache.size());
            return stats;
        } finally {
            fileLock.readLock().unlock();
        }
    }

    /**
     * Close the storage (flush and close file channel).
     * <p>
     * isOpen check and close() are atomic under file write lock — prevents
     * double-close race. Cache is also cleared on close.
     */
    public void close() throws IOException {
        fileLock.writeLock().lock();
        try {
            blockCache.clear();
            if (fileChannel != null && fileChannel.isOpen()) {
                fileChannel.close();
            }
            if (dataFile != null) {
                dataFile.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing storage: " + e.getMessage());
            throw e;
        } finally {
            fileLock.writeLock().unlock();
        }
    }

}
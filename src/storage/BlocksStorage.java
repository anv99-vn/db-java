package storage;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * BlocksStorage - Read/write data.bin file using FileChannel
 * Optimized with AtomicLong to minimize fileLock contention and System Calls.
 */
public class BlocksStorage {
    static final String DEFAULT_DATA_PATH = "data.bin";
    public static final int BLOCK_SIZE = 4096;
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
    
    // Thuộc tính mới: Cache kích thước file để tránh gọi fileChannel.size() liên tục
    private final AtomicLong currentFileSize = new AtomicLong(0);

    private static final int STRIPE_COUNT = 1024; // Số lượng lock cố định để tránh leak memory
    private final ReentrantReadWriteLock[] blockLocks = new ReentrantReadWriteLock[STRIPE_COUNT];

    private final Map<Integer, byte[]> blockCache;

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
        
        // Khởi tạo kích thước file hiện tại
        this.currentFileSize.set(fileChannel.size());

        for (int i = 0; i < STRIPE_COUNT; i++) {
            this.blockLocks[i] = new ReentrantReadWriteLock();
        }

        this.blockCache = Collections.synchronizedMap(
                new LinkedHashMap<>(cacheCapacity, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<Integer, byte[]> eldest) {
                        return size() > cacheCapacity;
                    }
                }
        );
    }

    private ReentrantReadWriteLock lockForBlock(int id) {
        return blockLocks[Math.abs(id % STRIPE_COUNT)];
    }

    /**
     * Get a block by its ID.
     * Đã loại bỏ fileLock và System Call size() để tăng hiệu năng đọc.
     */
    public Block getBlock(int id, Consumer<byte[]> consumer) throws IOException {
        ReentrantReadWriteLock lock = lockForBlock(id);
        lock.readLock().lock();
        try {
            byte[] cached = blockCache.get(id);
            if (cached != null) {
                Block block = new Block(Arrays.copyOf(cached, cached.length));
                block.id = id;
                return block;
            }

            // Sử dụng AtomicLong thay vì fileLock.readLock() và fileChannel.size()
            long offset = (long) id * BLOCK_SIZE;
            if (offset >= currentFileSize.get()) {
                return null;
            }

            ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);
            int bytesRead = fileChannel.read(buffer, offset);
            
            if (bytesRead <= 0) return null;

            buffer.flip();
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);

            byte[] valueForCache = Arrays.copyOf(data, data.length);
            blockCache.put(id, valueForCache);

            Block block = new Block(data);
            block.id = id;
            
            if (consumer != null) {
                consumer.accept(valueForCache);
            }
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
                data = Arrays.copyOf(cached, cached.length);
            } else {
                long offset = (long) id * BLOCK_SIZE;
                // Kiểm tra bounds nhanh qua AtomicLong
                if (offset >= currentFileSize.get()) return;
                
                ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);
                fileChannel.read(buffer, offset);
                data = buffer.array();
            }

            if (consumer != null) consumer.accept(data);

            writeBlockToChannel(id, new Block(data));
            blockCache.put(id, Arrays.copyOf(data, data.length));
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void putBlock(int id, Block block) throws IOException {
        fileLock.readLock().lock();
        try {
            ReentrantReadWriteLock lock = lockForBlock(id);
            lock.writeLock().lock();
            try {
                writeBlockToChannel(id, block);
                blockCache.put(id, Arrays.copyOf(block.bytes, block.bytes.length));
                
                // Cập nhật currentFileSize nếu ghi vào vị trí mới
                long endOffset = (long) (id + 1) * BLOCK_SIZE;
                currentFileSize.accumulateAndGet(endOffset, Math::max);
            } finally {
                lock.writeLock().unlock();
            }
        } finally {
            fileLock.readLock().unlock();
        }
    }

    private void writeBlockToChannel(int id, Block block) throws IOException {
        long offset = (long) id * BLOCK_SIZE;
        byte[] paddedData = new byte[BLOCK_SIZE];
        System.arraycopy(block.bytes, 0, paddedData, 0,
                Math.min(block.bytes.length, BLOCK_SIZE));

        ByteBuffer buffer = ByteBuffer.wrap(paddedData);
        fileChannel.write(buffer, offset);
        fileChannel.force(false);
    }

    public int allocateAndWrite(Block block) throws IOException {
        fileLock.readLock().lock();
        try {
            // Sử dụng AtomicLong.getAndAdd để đặt vị trí (offset) mới mà không cần lock toàn bộ file.
            // Trả về offset cũ và tăng kích thước file lên BLOCK_SIZE.
            long oldFileSize = currentFileSize.getAndAdd(BLOCK_SIZE);
            int newId = (int) (oldFileSize / BLOCK_SIZE);

            ReentrantReadWriteLock blockLock = lockForBlock(newId);
            blockLock.writeLock().lock();
            try {
                block.id = newId;
                writeBlockToChannel(newId, block);
                blockCache.put(newId, Arrays.copyOf(block.bytes, block.bytes.length));
            } finally {
                blockLock.writeLock().unlock();
            }

            return newId;
        } finally {
            fileLock.readLock().unlock();
        }
    }

    public void invalidateCache(int id) {
        ReentrantReadWriteLock lock = lockForBlock(id);
        lock.writeLock().lock();
        try {
            blockCache.remove(id);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void clearCache() {
        fileLock.writeLock().lock();
        try {
            blockCache.clear();
        } finally {
            fileLock.writeLock().unlock();
        }
    }

    public int getTotalBlockCount() {
        // Trả về từ memory, không cần lock file hay gọi OS
        return (int) (currentFileSize.get() / BLOCK_SIZE);
    }

    public Map<String, Integer> getCacheStats() {
        long fileSizeBytes = currentFileSize.get(); // Snapshot nhanh từ memory
        Map<String, Integer> stats = new HashMap<>();
        stats.put("block_size", BLOCK_SIZE);
        stats.put("file_size_bytes", (int) fileSizeBytes);
        stats.put("total_blocks", (int) (fileSizeBytes / BLOCK_SIZE));
        stats.put("cached_blocks", blockCache.size());
        return stats;
    }

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
package storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class BlocksStorageTest {

    private static final String TEST_FILE = "test_data.bin";
    private BlocksStorage storage;

    @BeforeEach
    public void setUp() throws IOException {
        // Ensure clean state
        deleteTestFile();
        storage = new BlocksStorage(TEST_FILE);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (storage != null) {
            storage.close();
        }
        deleteTestFile();
    }

    private void deleteTestFile() {
        File file = new File(TEST_FILE);
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    @DisplayName("Test allocate, write, and retrieve a block")
    public void testAllocateAndWriteAndGet() throws IOException {
        System.out.println("Step 1: Preparing data...");
        byte[] data = new byte[100];
        Arrays.fill(data, (byte) 1);
        Block block = new Block();
        block.insert(data);

        System.out.println("Step 2: Allocating and writing block...");
        int blockId = storage.allocateAndWrite(block);
        assertEquals(1, blockId, "First allocated block ID should be 1 (Block 0 is reserved for Schema)");
        System.out.println("  -> Block allocated with ID: " + blockId);

        System.out.println("Step 3: Retrieving block...");
        Block readBlock = storage.getBlock(blockId, b -> {
        });
        assertNotNull(readBlock, "Block should exist");
        assertEquals(blockId, readBlock.id);

        System.out.println("Step 4: Verifying content...");
        // Verify content
        byte[] readData = new byte[100];
        System.arraycopy(readBlock.bytes, Block.HEADER_TOTAL_SIZE, readData, 0, 100);
        assertArrayEquals(data, readData, "Data should match written data");
        System.out.println("  -> Content verified successfully.");
    }

    @Test
    @DisplayName("Test updating an existing block using putBlock")
    public void testPutBlock() throws IOException {
        System.out.println("Step 1: Creating initial block...");
        // Create initial block
        byte[] data1 = new byte[50];
        Arrays.fill(data1, (byte) 'A');
        Block block = new Block();
        block.insert(data1);
        int blockId = storage.allocateAndWrite(block);
        System.out.println("  -> Written block " + blockId + " with data 'A's");

        System.out.println("Step 2: Updating block...");
        // Update block
        byte[] data2 = new byte[50];
        Arrays.fill(data2, (byte) 'B');
        Block updateBlock = new Block();
        updateBlock.id = blockId;
        updateBlock.insert(data2);

        storage.putBlock(blockId, updateBlock);
        System.out.println("  -> Overwritten block " + blockId + " with data 'B's");

        System.out.println("Step 3: Verifying update...");
        // Verify update
        Block readBlock = storage.getBlock(blockId, b -> {
        });
        byte[] readData = new byte[50];
        System.arraycopy(readBlock.bytes, Block.HEADER_TOTAL_SIZE, readData, 0, 50);
        assertArrayEquals(data2, readData, "Updated data should match");
        System.out.println("  -> Update verified successfully.");
    }

    @Test
    @DisplayName("Test data persistence across storage close and reopen")
    public void testPersistence() throws IOException {
        System.out.println("Step 1: Writing data...");
        // Write data
        byte[] data = new byte[]{1, 2, 3, 4, 5};
        Block block = new Block();
        block.insert(data);
        int blockId = storage.allocateAndWrite(block);
        System.out.println("  -> Written block " + blockId);

        System.out.println("Step 2: Closing storage...");
        // Close storage
        storage.close();
        storage = null;

        System.out.println("Step 3: Reopening storage...");
        // Reopen storage
        BlocksStorage reopenStorage = new BlocksStorage(TEST_FILE);
        try {
            System.out.println("Step 4: Reading block after reopen...");
            Block readBlock = reopenStorage.getBlock(blockId, b -> {
            });
            assertNotNull(readBlock);

            byte[] readData = new byte[5];
            System.arraycopy(readBlock.bytes, Block.HEADER_TOTAL_SIZE, readData, 0, 5);
            assertArrayEquals(data, readData, "Data should persist after close/reopen");
            System.out.println("  -> Data persisted correctly.");

            // Total blocks = 1 (Schema) + 1 (Allocated) = 2
            assertEquals(2, reopenStorage.getTotalBlockCount());
        } finally {
            reopenStorage.close();
        }
    }

    @Test
    @DisplayName("Test cache statistics and cache clearing")
    public void testCacheStats() throws IOException {
        System.out.println("Step 1: Writing block 1...");
        Block b1 = new Block();
        storage.allocateAndWrite(b1);

        System.out.println("Step 2: Writing block 2...");
        Block b2 = new Block();
        storage.allocateAndWrite(b2);

        System.out.println("Step 3: Checking cache stats...");
        Map<String, Integer> stats = storage.getCacheStats();
        System.out.println("  -> Stats: " + stats);
        // Total blocks = 1 (Schema) + 2 (Allocated) = 3
        assertEquals(3, stats.get("total_blocks"));
        assertEquals(2, stats.get("cached_blocks")); // Cache is populated on write for the 2 allocated blocks

        System.out.println("Step 4: Clearing cache...");
        storage.clearCache();
        stats = storage.getCacheStats();
        System.out.println("  -> Stats after clear: " + stats);
        assertEquals(0, stats.get("cached_blocks"));
    }

    @Test
    @DisplayName("Test concurrent writes from multiple threads")
    public void testConcurrentWrites() throws InterruptedException, IOException {
        int threadCount = 10;
        int writesPerThread = 5;
        AtomicInteger errorCount = new AtomicInteger(0);
        System.out.println("Step 1: Starting " + threadCount + " threads, each writing " + writesPerThread + " blocks...");

        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < writesPerThread; j++) {
                        Block b = new Block();
                        b.insert(new byte[]{(byte) j});
                        storage.allocateAndWrite(b);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        System.out.println("Step 2: Waiting for threads to finish...");
        boolean finished = latch.await(10, TimeUnit.SECONDS);
        assertTrue(finished, "Threads did not finish in time");
        assertEquals(0, errorCount.get(), "Should involve no IOExceptions");
        System.out.println("  -> Threads finished successfully.");

        System.out.println("Step 3: verifying total block count...");
        // total = 1 (schema) + threads * writes
        assertEquals(1 + (threadCount * writesPerThread), storage.getTotalBlockCount());
        executor.shutdown();
        System.out.println("  -> Verification complete.");
    }

    @Test
    @DisplayName("Test LRU cache eviction and hit/miss behavior")
    public void testLRUCacheEviction() throws IOException {
        String tempPath = "lru_test_data.bin";
        File f = new File(tempPath);
        if (f.exists()) f.delete();

        System.out.println("Step 0: Initialize storage with cache size 2");
        // Initialize storage with small cache size of 2
        BlocksStorage lruStorage = new BlocksStorage(tempPath, 2);

        try {
            // 1. Create 3 blocks with distinct data
            System.out.println("Step 1: Creating 3 blocks with distinct data (10, 11, 12)");
            Block b0 = new Block();
            Arrays.fill(b0.bytes, (byte) 10);

            Block b1 = new Block();
            Arrays.fill(b1.bytes, (byte) 11);
            Block b2 = new Block();

            // 2. Write b0. Cache: [0]
            System.out.println("Step 2: Writing Block 0... (which will be ID 1)");
            byte[] raw10 = new byte[BlocksStorage.BLOCK_SIZE - Block.HEADER_TOTAL_SIZE];
            Arrays.fill(raw10, (byte) 10);
            b0.insert(raw10);
            int id1 = lruStorage.allocateAndWrite(b0);
            assertEquals(1, id1);

            // Verify b0 via cache hit (implicit, assume file not modified yet)
            // But we can check stats
            assertEquals(1, lruStorage.getCacheStats().get("cached_blocks"));

            // 2b. Write b1. Cache: [0, 1]
            System.out.println("Step 2b: Writing Block 1... (which will be ID 2)");
            byte[] raw11 = new byte[BlocksStorage.BLOCK_SIZE - Block.HEADER_TOTAL_SIZE];
            Arrays.fill(raw11, (byte) 11);
            b1.insert(raw11);
            int id2 = lruStorage.allocateAndWrite(b1);
            assertEquals(2, id2);
            assertEquals(2, lruStorage.getCacheStats().get("cached_blocks"));

            // 3. Access b0 to make it MRU. Cache order: [1, 0]
            System.out.println("Step 3: Accessing Block 1 to make it Read-Recently-Used...");
            // Accessing b0 should keep it in cache.
            lruStorage.getBlock(id1, b -> {
            });

            // 4. Write b2. This should evict b1 (LRU). Cache: [0, 2]
            System.out.println("Step 4: Writing Block 2 (Expect eviction of Block 2)...");
            byte[] raw12 = new byte[BlocksStorage.BLOCK_SIZE - Block.HEADER_TOTAL_SIZE];
            Arrays.fill(raw12, (byte) 12);
            b2.insert(raw12);
            int id3 = lruStorage.allocateAndWrite(b2);
            assertEquals(3, id3);
            assertEquals(2, lruStorage.getCacheStats().get("cached_blocks"));

            // IMMEDIATE check: b3 should be in cache.
            // If we read it now, it should return 12.
            Block checkB3 = lruStorage.getBlock(id3, b -> {
            });
            assertEquals(12, checkB3.bytes[Block.HEADER_TOTAL_SIZE], "Immediate read of b3 failed");

            // 5. Verify cache stats
            System.out.println("Step 5: Verifying cache capacity...");
            Map<String, Integer> stats = lruStorage.getCacheStats();
            assertEquals(2, stats.get("cached_blocks"), "Cache size should be capped at capacity");

            // 6. BACKDOOR: Modify the file on disk directly
            System.out.println("Step 6: Modifying file on disk (Backdoor)... Block 2: 99");
            try (java.io.RandomAccessFile backdoor = new java.io.RandomAccessFile(tempPath, "rw")) {
                byte[] blockOnDisk = new byte[BlocksStorage.BLOCK_SIZE];
                // Fill payload area with 99
                Arrays.fill(blockOnDisk, (byte) 99);
                // Set a valid size in the header so computeChecksum doesn't crash (e.g. 4084)
                ByteBuffer.wrap(blockOnDisk).putInt(Block.OFFSET_SIZE, BlocksStorage.BLOCK_SIZE - Block.HEADER_TOTAL_SIZE);
                
                backdoor.seek((long) id2 * BlocksStorage.BLOCK_SIZE);
                backdoor.write(blockOnDisk);
            }

            // 7. Verify reads
            System.out.println("Step 7: Verifying reads...");

            // Block 0 (Schema) was accessed, putting it into cache and evicting others.
            // Current cache [0, 3] if capacity is 2. (assuming LRU is 2 and 1)
            
            // Block 2 (b1) should be evicted -> return 99
            System.out.println("  -> Reading Block 2 (Expect Evicted ID 2: 99)...");
            Block readB2_disk = lruStorage.getBlock(2, b -> {});
            assertEquals(99, readB2_disk.bytes[Block.HEADER_TOTAL_SIZE]);

            // Block 3 should be in cache -> return 12
            System.out.println("  -> Reading Block 3 (Expect Cached ID 3: 12)...");
            Block readB3 = lruStorage.getBlock(3, b -> {});
            assertEquals(12, readB3.bytes[Block.HEADER_TOTAL_SIZE]);

        } finally {
            lruStorage.close();
            if (f.exists()) f.delete();
        }
    }
}

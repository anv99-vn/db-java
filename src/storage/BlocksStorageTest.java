package storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
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
        System.arraycopy(data, 0, block.bytes, 0, data.length);

        System.out.println("Step 2: Allocating and writing block...");
        int blockId = storage.allocateAndWrite(block);
        assertEquals(0, blockId, "First block ID should be 0");
        System.out.println("  -> Block allocated with ID: " + blockId);

        System.out.println("Step 3: Retrieving block...");
        Block readBlock = storage.getBlock(blockId, b -> {
        });
        assertNotNull(readBlock, "Block should exist");
        assertEquals(blockId, readBlock.id);

        System.out.println("Step 4: Verifying content...");
        // Verify content
        byte[] readData = new byte[100];
        System.arraycopy(readBlock.bytes, 0, readData, 0, 100);
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
        System.arraycopy(data1, 0, block.bytes, 0, data1.length);
        int blockId = storage.allocateAndWrite(block);
        System.out.println("  -> Written block " + blockId + " with data 'A's");

        System.out.println("Step 2: Updating block...");
        // Update block
        byte[] data2 = new byte[50];
        Arrays.fill(data2, (byte) 'B');
        Block updateBlock = new Block();
        updateBlock.id = blockId;
        System.arraycopy(data2, 0, updateBlock.bytes, 0, data2.length);

        storage.putBlock(blockId, updateBlock);
        System.out.println("  -> Overwritten block " + blockId + " with data 'B's");

        System.out.println("Step 3: Verifying update...");
        // Verify update
        Block readBlock = storage.getBlock(blockId, b -> {
        });
        byte[] readData = new byte[50];
        System.arraycopy(readBlock.bytes, 0, readData, 0, 50);
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
        System.arraycopy(data, 0, block.bytes, 0, data.length);
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
            System.arraycopy(readBlock.bytes, 0, readData, 0, 5);
            assertArrayEquals(data, readData, "Data should persist after close/reopen");
            System.out.println("  -> Data persisted correctly.");

            assertEquals(1, reopenStorage.getTotalBlockCount());
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
        assertEquals(2, stats.get("total_blocks"));
        assertEquals(2, stats.get("cached_blocks")); // Cache is populated on write

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
                        b.bytes[0] = (byte) j;
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
        assertEquals(threadCount * writesPerThread, storage.getTotalBlockCount());
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
            Arrays.fill(b2.bytes, (byte) 12);

            // 2. Write b0. Cache: [0]
            System.out.println("Step 2: Writing Block 0...");
            int id0 = lruStorage.allocateAndWrite(b0);
            assertEquals(0, id0);

            // Verify b0 via cache hit (implicit, assume file not modified yet)
            // But we can check stats
            assertEquals(1, lruStorage.getCacheStats().get("cached_blocks"));

            // 2b. Write b1. Cache: [0, 1]
            System.out.println("Step 2b: Writing Block 1...");
            int id1 = lruStorage.allocateAndWrite(b1);
            assertEquals(1, id1);
            assertEquals(2, lruStorage.getCacheStats().get("cached_blocks"));

            // 3. Access b0 to make it MRU. Cache order: [1, 0]
            System.out.println("Step 3: Accessing Block 0 to make it Read-Recently-Used...");
            // Accessing b0 should keep it in cache.
            lruStorage.getBlock(id0, b -> {
            });

            // 4. Write b2. This should evict b1 (LRU). Cache: [0, 2]
            System.out.println("Step 4: Writing Block 2 (Expect eviction of Block 1)...");
            int id2 = lruStorage.allocateAndWrite(b2);
            assertEquals(2, id2);
            assertEquals(2, lruStorage.getCacheStats().get("cached_blocks"));

            // IMMEDIATE check: b2 should be in cache.
            // If we read it now, it should return 12.
            Block checkB2 = lruStorage.getBlock(id2, b -> {
            });
            assertEquals(12, checkB2.bytes[0], "Immediate read of b2 failed");

            // 5. Verify cache stats
            System.out.println("Step 5: Verifying cache capacity...");
            Map<String, Integer> stats = lruStorage.getCacheStats();
            assertEquals(2, stats.get("cached_blocks"), "Cache size should be capped at capacity");

            // 6. BACKDOOR: Modify the file on disk directly
            System.out.println("Step 6: Modifying file on disk (Backdoor)... Block 1: 99");
            try (java.io.RandomAccessFile backdoor = new java.io.RandomAccessFile(tempPath, "rw")) {
                byte[] modifiedData = new byte[BlocksStorage.BLOCK_SIZE];
                Arrays.fill(modifiedData, (byte) 99);

                // Overwrite all 3 blocks on disk with "99"
                backdoor.seek((long) id0 * BlocksStorage.BLOCK_SIZE);
                backdoor.write(modifiedData);

                backdoor.seek((long) id1 * BlocksStorage.BLOCK_SIZE);
                backdoor.write(modifiedData);

                backdoor.seek((long) id2 * BlocksStorage.BLOCK_SIZE);
                backdoor.write(modifiedData);
            }

            // 7. Verify reads
            System.out.println("Step 7: Verifying reads...");

            // Block 0 should be in cache -> return 10
            System.out.println("  -> Reading Block 0 (Expect Cached: 10)...");
            Block readB0 = lruStorage.getBlock(id0, b -> {
            });
            assertEquals(10, readB0.bytes[0], "Block 0 should be served from cache (original data) - MRU was respected?");

            // Block 2 should be in cache -> return 12
            // Note: getBlock(id0) above made id0 MRU. Order was [0, 2] -> [2, 0].
            // So b2 is still in cache.
            System.out.println("  -> Reading Block 2 (Expect Cached: 12)...");
            Block readB2 = lruStorage.getBlock(id2, b -> {
            });
            assertEquals(12, readB2.bytes[0], "Block 2 should be in cache (original data)");

            // Block 1 was evicted -> return 99
            System.out.println("  -> Reading Block 1 (Expect Evicted/Disk: 99)...");
            Block readB1 = lruStorage.getBlock(id1, b -> {
            });
            assertEquals(99, readB1.bytes[0], "Block 1 should be evicted and read from disk (modified data)");

        } finally {
            lruStorage.close();
            if (f.exists()) f.delete();
        }
    }
}

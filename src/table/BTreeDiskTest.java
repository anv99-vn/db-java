package table;

import org.junit.jupiter.api.*;
import storage.BlocksStorage;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Disk-based B-Tree Tests")
class BTreeDiskTest {

    private static final String TEST_DB_FILE = "btree_disk_test.bin";
    private BlocksStorage storage;

    @BeforeEach
    void setUp() throws IOException {
        deleteFile(TEST_DB_FILE);
        storage = new BlocksStorage(TEST_DB_FILE);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (storage != null) {
            storage.close();
        }
        deleteFile(TEST_DB_FILE);
    }

    @Test
    @DisplayName("Test 1: Insert and Search with Integer keys")
    void testInsertAndSearch() throws IOException {
        BTreeDisk<Integer> tree = new BTreeDisk<>(storage, 0, 3);

        int[] keys = {10, 20, 5, 6, 12, 30, 7, 17};
        for (int k : keys) {
            tree.insert(new BKey<>(k), (long) k * 10);
        }

        for (int k : keys) {
            BKey<Integer> result = tree.search(new BKey<>(k));
            assertNotNull(result, "Should find key " + k);
            assertEquals(k, result.getKey());
            assertTrue(result.getRecordPointers().contains((long) k * 10));
        }

        assertNull(tree.search(new BKey<>(999)), "Key 999 should not exist");
        tree.close();
    }

    @Test
    @DisplayName("Test 2: Range Query [lower, upper]")
    void testRangeQuery() throws IOException {
        BTreeDisk<Integer> tree = new BTreeDisk<>(storage, 0, 3);

        int[] keys = {10, 20, 5, 6, 12, 30, 7, 17};
        for (int k : keys) {
            tree.insert(new BKey<>(k), (long) k * 10);
        }

        List<BKey<Integer>> range = tree.findInRange(6, 17);
        
        // Expected keys: 6, 7, 10, 12, 17 (Total 5)
        assertEquals(5, range.size());
        int[] expected = {6, 7, 10, 12, 17};
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], range.get(i).getKey());
        }
        tree.close();
    }

    @Test
    @DisplayName("Test 3: Delete Pointer (Partial Delete)")
    void testDeletePointer() throws IOException {
        BTreeDisk<Integer> tree = new BTreeDisk<>(storage, 0, 3);

        tree.insert(new BKey<>(42), 100L);
        tree.insert(new BKey<>(42), 200L);

        BKey<Integer> found = tree.search(new BKey<>(42));
        assertEquals(2, found.getRecordPointers().size());

        tree.delete(new BKey<>(42), 100L);

        found = tree.search(new BKey<>(42));
        assertNotNull(found, "Key 42 should still exist");
        assertEquals(1, found.getRecordPointers().size());
        assertEquals(200L, found.getRecordPointers().get(0));
        tree.close();
    }

    @Test
    @DisplayName("Test 4: Delete Key (Total Delete)")
    void testDeleteKey() throws IOException {
        BTreeDisk<Integer> tree = new BTreeDisk<>(storage, 0, 3);

        int[] keys = {10, 20, 5, 6, 12, 30, 7, 17};
        for (int k : keys) {
            tree.insert(new BKey<>(k), (long) k * 10);
        }

        tree.delete(new BKey<>(6), 60L);
        tree.delete(new BKey<>(20), 200L);

        assertNull(tree.search(new BKey<>(6)));
        assertNull(tree.search(new BKey<>(20)));
        assertNotNull(tree.search(new BKey<>(10)));
        tree.close();
    }

    @Test
    @DisplayName("Test 5: Persistence (Close and Reopen)")
    void testPersistence() throws IOException {
        // Build tree
        BTreeDisk<Integer> tree = new BTreeDisk<>(storage, 0, 3);
        int[] keys = {10, 20, 5, 30};
        for (int k : keys) tree.insert(new BKey<>(k), (long) k);
        tree.close();
        storage.close();

        // Reopen same file
        storage = new BlocksStorage(TEST_DB_FILE);
        BTreeDisk<Integer> reopenedTree = new BTreeDisk<>(storage, 0, 0); // t is loaded from disk

        for (int k : keys) {
            assertNotNull(reopenedTree.search(new BKey<>(k)), "Key " + k + " must persist");
        }
        reopenedTree.close();
    }

    @Test
    @DisplayName("Test 6: Stress with Root Splits (Degree 2)")
    void testStressRootSplits() throws IOException {
        BTreeDisk<Integer> tree = new BTreeDisk<>(storage, 0, 2);
        int N = 50;
        for (int i = 1; i <= N; i++) {
            tree.insert(new BKey<>(i), (long) i);
        }

        for (int i = 1; i <= N; i++) {
            assertNotNull(tree.search(new BKey<>(i)), "Should find i=" + i);
        }

        List<BKey<Integer>> range = tree.findInRange(10, 40);
        assertEquals(31, range.size());
        tree.close();
    }

    @Test
    @DisplayName("Test 7: String Keys Persistence")
    void testStringKeys() throws IOException {
        BTreeDisk<String> tree = new BTreeDisk<>(storage, 0, 2);
        String[] names = {"Alice", "Bob", "Charlie", "Dave", "Eve"};
        for (String n : names) tree.insert(new BKey<>(n), 888L);

        for (String n : names) {
            BKey<String> res = tree.search(new BKey<>(n));
            assertNotNull(res);
            assertEquals(n, res.getKey());
        }
        tree.close();
    }

    private void deleteFile(String path) {
        File f = new File(path);
        if (f.exists()) {
            f.delete();
        }
    }
}

package table;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

class BTreeTest {

    @Test
    void testInsertAndSearch() {
        BTree<Integer> tree = new BTree<>(2); // Min degree 2: 1-3 keys per node

        tree.insert(new BKey<>(10), 100L);
        tree.insert(new BKey<>(20), 200L);
        tree.insert(new BKey<>(5), 50L);

        BKey<Integer> find10 = tree.search(new BKey<>(10));
        Assertions.assertNotNull(find10);
        Assertions.assertEquals(10, find10.getKey());
        Assertions.assertTrue(find10.getRecordPointers().contains(100L));

        BKey<Integer> find5 = tree.search(new BKey<>(5));
        Assertions.assertNotNull(find5);
        Assertions.assertEquals(5, find5.getKey());

        Assertions.assertNull(tree.search(new BKey<>(99)));
    }

    @Test
    void testDuplicateKeysPointers() {
        BTree<String> tree = new BTree<>(3);

        tree.insert(new BKey<>("A"), 1L);
        tree.insert(new BKey<>("A"), 2L);
        tree.insert(new BKey<>("B"), 3L);

        BKey<String> findA = tree.search(new BKey<>("A"));
        Assertions.assertNotNull(findA);
        List<Long> pointers = findA.getRecordPointers();
        Assertions.assertEquals(2, pointers.size());
        Assertions.assertTrue(pointers.contains(1L));
        Assertions.assertTrue(pointers.contains(2L));
    }

    @Test
    void testDeletePointer() {
        BTree<Integer> tree = new BTree<>(2);

        tree.insert(new BKey<>(10), 100L);
        tree.insert(new BKey<>(10), 200L);

        tree.delete(new BKey<>(10), 100L);

        BKey<Integer> find10 = tree.search(new BKey<>(10));
        Assertions.assertNotNull(find10);
        Assertions.assertEquals(1, find10.getRecordPointers().size());
        Assertions.assertFalse(find10.getRecordPointers().contains(100L));
        Assertions.assertTrue(find10.getRecordPointers().contains(200L));

        // Delete last pointer -> key should be removed from tree
        tree.delete(new BKey<>(10), 200L);
        Assertions.assertNull(tree.search(new BKey<>(10)));
    }

    @Test
    void testLargeInsertsSplitting() {
        BTree<Integer> tree = new BTree<>(2);
        // Degree 2 means max 3 keys per node. 
        // Inserting 10 keys will force multiple splits.
        for (int i = 1; i <= 10; i++) {
            tree.insert(new BKey<>(i), i);
        }

        for (int i = 1; i <= 10; i++) {
            BKey<Integer> result = tree.search(new BKey<>(i));
            Assertions.assertNotNull(result, "Failed to find key " + i);
            Assertions.assertEquals(i, result.getKey());
        }
    }

    @Test
    void testDeletionsAndMerging() {
        BTree<Integer> tree = new BTree<>(2);
        for (int i = 1; i <= 10; i++) {
            tree.insert(new BKey<>(i), i);
        }

        // Delete some and check merging/rebalancing
        tree.delete(new BKey<>(5), 5L);
        tree.delete(new BKey<>(1), 1L);
        tree.delete(new BKey<>(10), 10L);

        Assertions.assertNull(tree.search(new BKey<>(5)));
        Assertions.assertNull(tree.search(new BKey<>(1)));
        Assertions.assertNull(tree.search(new BKey<>(10)));

        Assertions.assertNotNull(tree.search(new BKey<>(2)));
        Assertions.assertNotNull(tree.search(new BKey<>(9)));
    }

    @Test
    void testBKeyWithCompositeObject() {
        // Static inner class for composite key
        class CompositeKey implements Comparable<CompositeKey> {
            final int id;
            final String category;

            CompositeKey(int id, String category) {
                this.id = id;
                this.category = category;
            }

            @Override
            public int compareTo(CompositeKey o) {
                int res = Integer.compare(this.id, o.id);
                if (res != 0) return res;
                return this.category.compareTo(o.category);
            }
        }

        BTree<CompositeKey> tree = new BTree<>(3);
        CompositeKey k1 = new CompositeKey(1, "A");
        CompositeKey k2 = new CompositeKey(1, "B");
        CompositeKey k3 = new CompositeKey(2, "A");

        tree.insert(new BKey<>(k1), 101L);
        tree.insert(new BKey<>(k2), 102L);
        tree.insert(new BKey<>(k3), 103L);

        BKey<CompositeKey> res1 = tree.search(new BKey<>(k1));
        Assertions.assertNotNull(res1);
        Assertions.assertTrue(res1.getRecordPointers().contains(101L));

        Assertions.assertNull(tree.search(new BKey<>(new CompositeKey(1, "C"))));
    }

    @Test
    void testStressRandomInserts() {
        BTree<Integer> tree = new BTree<>(5);
        int numKeys = 500; // Reduced for faster execution in tests
        Random rand = new Random(42);
        Set<Integer> insertedKeys = new HashSet<>();

        for (int i = 0; i < numKeys; i++) {
            int k = rand.nextInt(10000);
            insertedKeys.add(k);
            tree.insert(new BKey<>(k), (long) i);
        }

        for (int k : insertedKeys) {
            Assertions.assertNotNull(tree.search(new BKey<>(k)), "Key " + k + " should be in the tree");
        }

        // Delete some keys
        List<Integer> list = new ArrayList<>(insertedKeys);
        for (int i = 0; i < list.size() / 2; i++) {
            int k = list.get(i);
            // Search to get the actual BKey object with all its pointers
            BKey<Integer> bkey = tree.search(new BKey<>(k));
            for (long ptr : new ArrayList<>(bkey.getRecordPointers())) {
                tree.delete(new BKey<>(k), ptr);
            }
            Assertions.assertNull(tree.search(new BKey<>(k)), "Key " + k + " should have been deleted");
        }
    }

    @Test
    void testSpecialCharacters() {
        BTree<String> tree = new BTree<>(2);
        String[] keys = {"", " ", "!", "\n", "tab\tspace", "unicode_emoji_🔥", "Z"};

        for (int i = 0; i < keys.length; i++) {
            tree.insert(new BKey<>(keys[i]), (long) i);
        }

        for (int i = 0; i < keys.length; i++) {
            BKey<String> result = tree.search(new BKey<>(keys[i]));
            Assertions.assertNotNull(result, "Failed to find key: [" + keys[i] + "]");
            Assertions.assertEquals(keys[i], result.getKey());
        }
    }

    @Test
    void testManyPointersForOneKey() {
        BTree<Integer> tree = new BTree<>(3);
        int targetKey = 50;
        int numPointers = 200;

        for (int i = 0; i < numPointers; i++) {
            tree.insert(new BKey<>(targetKey), (long) i);
        }

        BKey<Integer> result = tree.search(new BKey<>(targetKey));
        Assertions.assertNotNull(result);
        Assertions.assertEquals(numPointers, result.getRecordPointers().size());

        // Delete a few pointers
        tree.delete(new BKey<>(targetKey), 0L);
        tree.delete(new BKey<>(targetKey), 100L);

        result = tree.search(new BKey<>(targetKey));
        Assertions.assertEquals(numPointers - 2, result.getRecordPointers().size());
        Assertions.assertFalse(result.getRecordPointers().contains(0L));
    }

    @Test
    void testFindInRangeBasic() {
        BTree<Integer> tree = new BTree<>(2);
        for (int i = 1; i <= 10; i++) {
            tree.insert(new BKey<>(i), i);
        }

        // Search range [3, 7]
        List<BKey<Integer>> result = tree.findInRange(3, 7);
        tree.print();
        Assertions.assertEquals(5, result.size());
        Assertions.assertEquals(3, result.get(0).getKey());
        Assertions.assertEquals(7, result.get(4).getKey());
    }

    @Test
    void testFindInRangeEmpty() {
        BTree<Integer> tree = new BTree<>(2);
        tree.insert(new BKey<>(10), 100L);
        tree.insert(new BKey<>(20), 200L);

        // Range [1, 5] - no results
        List<BKey<Integer>> result = tree.findInRange(1, 5);
        Assertions.assertTrue(result.isEmpty());

        // Range [25, 30] - no results
        result = tree.findInRange(25, 30);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    void testFindInRangeSinglePoint() {
        BTree<Integer> tree = new BTree<>(2);
        tree.insert(new BKey<>(10), 100L);
        tree.insert(new BKey<>(20), 200L);

        // Range [10, 10]
        List<BKey<Integer>> result = tree.findInRange(10, 10);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(10, result.get(0).getKey());
    }

    @Test
    void testFindInRangeAll() {
        BTree<Integer> tree = new BTree<>(2);
        for (int i = 1; i <= 5; i++) {
            tree.insert(new BKey<>(i), (long) i);
        }

        // Range [0, 10]
        List<BKey<Integer>> result = tree.findInRange(0, 10);
        Assertions.assertEquals(5, result.size());
    }

    @Test
    void testFindInRangeLargeTree() {
        BTree<Integer> tree = new BTree<>(3);
        int numKeys = 100;
        for (int i = 0; i < numKeys; i++) {
            tree.insert(new BKey<>(i), (long) i);
        }

        // Range [20, 80]
        List<BKey<Integer>> result = tree.findInRange(20, 80);
        Assertions.assertEquals(61, result.size());
        for (int i = 0; i < result.size(); i++) {
            Assertions.assertEquals(20 + i, result.get(i).getKey());
        }
    }
}

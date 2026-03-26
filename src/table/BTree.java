package table;

import java.util.ArrayList;
import java.util.List;

/**
 * Bậc t (minimum degree): mỗi node có tối thiểu t-1 keys, tối đa 2t-1 keys
 */
public class BTree<T extends Comparable<T>> {

    private BTreeNode<T> root;

    // ==================== BTREE CLASS ====================
    private final int t; // Minimum degree

    @SuppressWarnings("unused")
    private BTree() {
        this.t = 0;
        this.root = null;
    }

    public BTree(int t) {
        this.t = t;
        this.root = null;
    }

    // Tìm kiếm
    public BKey<T> search(BKey<T> key) {
        if (root == null) return null;
        return root.search(key);
    }

    // Insert with record pointer
    public void insert(BKey<T> key, long pointer) {
        BKey<T> existing = search(key);
        if (existing != null) {
            existing.addPointer(pointer);
        } else {
            key.addPointer(pointer);
            insert(key);
        }
    }

    // Insert
    public void insert(BKey<T> key) {
        if (root == null) {
            root = new BTreeNode<>(t, true);
            root.keys[0] = key;
            root.n = 1;
            return;
        }

        // Nếu root đầy thì split root
        if (root.n == 2 * t - 1) {
            BTreeNode<T> newRoot = new BTreeNode<>(t, false);
            newRoot.children[0] = root;
            newRoot.splitChild(0, root);

            int i = (newRoot.keys[0].isLessThan(key)) ? 1 : 0;
            newRoot.children[i].insertNonFull(key);
            root = newRoot;
        } else {
            root.insertNonFull(key);
        }
    }

    // Delete with record pointer
    public void delete(BKey<T> key, long pointer) {
        BKey<T> existing = search(key);
        if (existing != null) {
            existing.removePointer(pointer);
            if (existing.getRecordPointers().isEmpty()) {
                delete(existing);
            }
        }
    }

    // Delete
    public void delete(BKey<T> key) {
        if (root == null) {
            System.out.println("Cây rỗng.");
            return;
        }

        root.delete(key);

        // Nếu root không còn key nào thì hạ root xuống
        if (root.n == 0) {
            root = root.isLeaf ? null : root.children[0];
        }
    }

    // Tìm các key trong khoảng [lower, upper]
    public List<BKey<T>> findInRange(T lower, T upper) {
        List<BKey<T>> result = new ArrayList<>();
        if (root != null) {
            root.findInRange(lower, upper, result);
        }
        return result;
    }

    // In cây dạng cây thư mục
    public void print() {
        if (root == null) {
            System.out.println("(Cây rỗng)");
            return;
        }
        System.out.print("Root → ");
        System.out.print("[");
        for (int i = 0; i < root.n; i++) {
            System.out.print(root.keys[i].key);
            if (i < root.n - 1) System.out.print(", ");
        }
        System.out.println("]");

        String indent = "";
        for (int i = 0; i <= root.n; i++) {
            if (!root.isLeaf && root.children[i] != null) {
                root.children[i].print(indent, i == root.n);
            }
        }
    }
}

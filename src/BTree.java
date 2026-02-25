import java.util.ArrayList;

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
    public boolean search(BKey<T> key) {
        if (root == null) return false;
        return root.search(key) != null;
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

    // ==================== MAIN (Demo) ====================
    public static void main(String[] args) {
        int t = 3;
        System.out.println("=== B-Tree Demo (bậc t=" +
                t +
                ", tối đa " +
                (2 * t - 1) +
                " keys/node) ===\n");

        BTree<Integer> tree = new BTree<>(t);

        // Insert từ lớn đến nhỏ (10 → 1)
        System.out.print("--- Insert: ");
        ArrayList<BKey<Integer>> values = new ArrayList<>();
        for (int i = 19; i >= 0; i--) {
            values.add(new BKey<>(i));
            System.out.print(i);
            System.out.print(i != 0 ? ", " : " ");
        }
        System.out.println("---");
        for (BKey<Integer> v : values) {
            tree.insert(v);
            System.out.println("Insert " + v.key + ":");
            tree.print();
            System.out.println();
        }

        // Search
        System.out.println("--- Tìm kiếm ---");
        System.out.println("Tìm 7: " + (tree.search(new BKey<>(7)) ? "Tìm thấy ✓" : "Không tìm thấy ✗"));
        System.out.println("Tìm 11: " + (tree.search(new BKey<>(11)) ? "Tìm thấy ✓" : "Không tìm thấy ✗"));

        // Delete
        System.out.println("\n--- Xóa key 6 ---");
        tree.delete(new BKey<>(6));
        tree.print();

        System.out.println("\n--- Xóa key 3 ---");
        tree.delete(new BKey<>(3));
        tree.print();
    }
}

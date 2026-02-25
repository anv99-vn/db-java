// ==================== NODE CLASS ====================
class BTreeNode<T extends Comparable<T>> {
    BKey<T>[] keys;         // Mảng chứa keys
    int t;              // Minimum degree
    BTreeNode<T>[] children; // Mảng chứa con trỏ đến các node con
    int n;              // Số key hiện tại
    boolean isLeaf;     // True nếu là lá

    @SuppressWarnings("unused")
    private BTreeNode() {
    }

    @SuppressWarnings("unchecked")
    BTreeNode(int t, boolean isLeaf) {
        this.t = t;
        this.isLeaf = isLeaf;
        this.keys = (BKey<T>[]) new BKey[2 * t - 1];
        this.children = (BTreeNode<T>[]) new BTreeNode[2 * t];
        this.n = 0;
    }

    // Tìm kiếm key trong node và các node con
    BTreeNode<T> search(BKey<T> key) {
        int i = 0;
        while (i < n && key.isGreaterThan(keys[i])) i++;

        if (i < n && key.isEqual(keys[i])) return this;
        if (isLeaf) return null;
        return children[i].search(key);
    }

    // Insert key vào node chưa đầy
    void insertNonFull(BKey<T> key) {
        int i = n - 1;

        if (isLeaf) {
            // Dịch các key lớn hơn sang phải để nhường chỗ
            while (i >= 0 && keys[i].isGreaterThan(key)) {
                keys[i + 1] = keys[i];
                i--;
            }
            keys[i + 1] = key;
            n++;
        } else {
            // Tìm vị trí node con phù hợp
            while (i >= 0 && keys[i].isGreaterThan(key)) i--;
            i++;

            // Nếu node con đầy thì split trước
            if (children[i].n == 2 * t - 1) {
                splitChild(i, children[i]);
                if (keys[i].isLessThan(key)) i++;
            }
            children[i].insertNonFull(key);
        }
    }

    // Split node con children[i] đang đầy
    void splitChild(int i, BTreeNode<T> child) {
        BTreeNode<T> newNode = new BTreeNode<>(child.t, child.isLeaf);
        newNode.n = t - 1;

        // Copy t-1 keys cuối của child sang newNode
        if (t - 1 >= 0) System.arraycopy(child.keys, t, newNode.keys, 0, t - 1);

        // Copy t node con cuối nếu child không phải lá
        if (!child.isLeaf) {
            if (t >= 0) System.arraycopy(child.children, t, newNode.children, 0, t);
        }

        child.n = t - 1;

        // Dịch con trỏ con của node hiện tại sang phải
        for (int j = n; j >= i + 1; j--)
            children[j + 1] = children[j];
        children[i + 1] = newNode;

        // Dịch keys sang phải và đưa key giữa của child lên
        for (int j = n - 1; j >= i; j--)
            keys[j + 1] = keys[j];
        keys[i] = child.keys[t - 1];
        n++;
    }

    // ==================== DELETE ====================

    void delete(BKey<T> key) {
        int i = findKey(key);

        if (i < n && keys[i].isEqual(key)) {
            // Key nằm trong node này
            if (isLeaf)
                removeFromLeaf(i);
            else
                removeFromNonLeaf(i);
        } else {
            // Key không nằm trong node này
            if (isLeaf) {
                System.out.println("Key " + key.key + " không tồn tại trong cây.");
                return;
            }

            boolean isLastChild = (i == n);
            if (children[i].n < t)
                fill(i);

            // Sau fill, node con có thể đã merge với anh em trước
            if (isLastChild && i > n)
                children[i - 1].delete(key);
            else
                children[i].delete(key);
        }
    }

    int findKey(BKey<T> key) {
        int i = 0;
        while (i < n && keys[i].isLessThan(key)) i++;
        return i;
    }

    void removeFromLeaf(int i) {
        for (int j = i + 1; j < n; j++)
            keys[j - 1] = keys[j];
        n--;
    }

    void removeFromNonLeaf(int i) {
        BKey<T> k = keys[i];

        if (children[i].n >= t) {
            // Lấy predecessor (key lớn nhất bên trái)
            BKey<T> pred = getPredecessor(i);
            keys[i] = pred;
            children[i].delete(pred);
        } else if (children[i + 1].n >= t) {
            // Lấy successor (key nhỏ nhất bên phải)
            BKey<T> succ = getSuccessor(i);
            keys[i] = succ;
            children[i + 1].delete(succ);
        } else {
            // Merge hai node con
            merge(i);
            children[i].delete(k);
        }
    }

    BKey<T> getPredecessor(int i) {
        BTreeNode<T> cur = children[i];
        while (!cur.isLeaf)
            cur = cur.children[cur.n];
        return cur.keys[cur.n - 1];
    }

    BKey<T> getSuccessor(int i) {
        BTreeNode<T> cur = children[i + 1];
        while (!cur.isLeaf)
            cur = cur.children[0];
        return cur.keys[0];
    }

    void fill(int i) {
        if (i != 0 && children[i - 1].n >= t)
            borrowFromPrev(i);
        else if (i != n && children[i + 1].n >= t)
            borrowFromNext(i);
        else {
            if (i != n) merge(i);
            else merge(i - 1);
        }
    }

    void borrowFromPrev(int i) {
        BTreeNode<T> child = children[i];
        BTreeNode<T> sibling = children[i - 1];

        for (int j = child.n - 1; j >= 0; j--)
            child.keys[j + 1] = child.keys[j];

        if (!child.isLeaf) {
            for (int j = child.n; j >= 0; j--)
                child.children[j + 1] = child.children[j];
        }

        child.keys[0] = keys[i - 1];
        if (!child.isLeaf)
            child.children[0] = sibling.children[sibling.n];

        keys[i - 1] = sibling.keys[sibling.n - 1];
        child.n++;
        sibling.n--;
    }

    void borrowFromNext(int i) {
        BTreeNode<T> child = children[i];
        BTreeNode<T> sibling = children[i + 1];

        child.keys[child.n] = keys[i];
        if (!child.isLeaf)
            child.children[child.n + 1] = sibling.children[0];

        keys[i] = sibling.keys[0];

        for (int j = 1; j < sibling.n; j++)
            sibling.keys[j - 1] = sibling.keys[j];

        if (!sibling.isLeaf) {
            for (int j = 1; j <= sibling.n; j++)
                sibling.children[j - 1] = sibling.children[j];
        }

        child.n++;
        sibling.n--;
    }

    void merge(int i) {
        BTreeNode<T> child = children[i];
        BTreeNode<T> sibling = children[i + 1];

        child.keys[t - 1] = keys[i];

        if (sibling.n >= 0) System.arraycopy(sibling.keys, 0, child.keys, t, sibling.n);

        if (!child.isLeaf) {
            if (sibling.n + 1 >= 0)
                System.arraycopy(sibling.children, 0, child.children, t, sibling.n + 1);
        }

        for (int j = i + 1; j < n; j++)
            keys[j - 1] = keys[j];

        for (int j = i + 2; j <= n; j++)
            children[j - 1] = children[j];

        child.n += sibling.n + 1;
        n--;
    }

    // In node theo dạng level-order (dùng cho traverse)
    void print(String indent, boolean isLast) {
        System.out.print(indent);
        System.out.print(isLast ? "└── " : "├── ");
        System.out.print("[");
        for (int i = 0; i < n; i++) {
            System.out.print(keys[i].key);
            if (i < n - 1) System.out.print(", ");
        }
        System.out.println("]");

        String childIndent = indent + (isLast ? "    " : "│   ");
        for (int i = 0; i <= n; i++) {
            if (!isLeaf && children[i] != null) {
                children[i].print(childIndent, i == n);
            }
        }
    }
}

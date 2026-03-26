package table;

import storage.Block;
import storage.BlocksStorage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * BTreeDisk - Disk-based B-Tree sử dụng {@link BlocksStorage} để lưu node trên disk.
 *
 * <h2>Cơ chế lưu trữ</h2>
 * <ul>
 *   <li>Mỗi node chiếm đúng 1 block (4 096 bytes).</li>
 *   <li>Block 0 dành riêng cho <b>metadata</b>: rootPageId (int) + degree t (int).</li>
 *   <li>Các block từ 1 trở đi lưu node B-Tree.</li>
 * </ul>
 *
 * <h2>Quy tắc node I/O</h2>
 * <ul>
 *   <li>Node có {@code pageId == -1}: chưa ghi lần nào → dùng {@link #allocateNode}.</li>
 *   <li>Node có {@code pageId >= 0}: đã tồn tại trên disk → dùng {@link #writeNode}.</li>
 * </ul>
 *
 * @param <T> Kiểu của key, phải implement {@link Comparable}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class BTreeDisk<T extends Comparable<T>> {

    // ── Hằng số metadata ──────────────────────────────────────────────────────
    private final int metadataBlockId;
    private static final int META_OFFSET_ROOT  = Block.HEADER_TOTAL_SIZE;      // 12
    private static final int META_OFFSET_T     = Block.HEADER_TOTAL_SIZE + 4;  // 16
    private static final int META_PAYLOAD_LEN  = 8;

    // ── Trường ────────────────────────────────────────────────────────────────
    private final BlocksStorage storage;
    private final int t;
    private int rootPageId; // -1 nếu cây rỗng

    // ── Khởi tạo ──────────────────────────────────────────────────────────────

    /**
     * Tạo mới hoặc load BTreeDisk từ storage đã có.
     *
     * @param storage          BlocksStorage đã mở
     * @param metadataBlockId  Id của block chứa metadata (rootPageId, t)
     * @param t                minimum degree (chỉ dùng khi tạo mới)
     */
    public BTreeDisk(BlocksStorage storage, int metadataBlockId, int t) throws IOException {
        this.storage = storage;
        this.metadataBlockId = metadataBlockId;
        Block metaBlock = storage.getBlock(metadataBlockId, null);
        if (metaBlock != null && metaBlock.getSize() >= META_PAYLOAD_LEN) {
            ByteBuffer buf = ByteBuffer.wrap(metaBlock.bytes);
            this.rootPageId = buf.getInt(META_OFFSET_ROOT);
            this.t          = buf.getInt(META_OFFSET_T);
        } else {
            this.t          = t;
            this.rootPageId = -1;
            flushMetadata();
        }
    }

    // ── Search ─────────────────────────────────────────────────────────────────

    public BKey<T> search(T key) throws IOException {
        if (key == null) return null;
        return search(new BKey<>(key));
    }

    public BKey<T> search(BKey<T> key) throws IOException {
        if (rootPageId == -1) return null;
        return searchNode(rootPageId, key);
    }

    private BKey<T> searchNode(int pageId, BKey<T> key) throws IOException {
        BTreeDiskNode node = readNode(pageId);
        int i = 0;
        while (i < node.n && key.isGreaterThan(node.keys[i])) i++;
        if (i < node.n && key.isEqual(node.keys[i])) return (BKey<T>) node.keys[i];
        if (node.isLeaf) return null;
        return searchNode(node.childrenPageIds[i], key);
    }

    // ── Insert ─────────────────────────────────────────────────────────────────

    /**
     * Chèn key + pointer. Nếu key đã tồn tại, thêm pointer vào danh sách.
     */
    public void insert(BKey<T> key, long pointer) throws IOException {
        BKey<T> existing = search(key);
        if (existing != null) {
            existing.addPointer(pointer);
            updateKeyInTree(rootPageId, existing);
        } else {
            key.addPointer(pointer);
            insert(key);
        }
    }

    /**
     * Chèn key mới (chưa tồn tại) vào cây.
     */
    public void insert(BKey<T> key) throws IOException {
        if (rootPageId == -1) {
            // Cây rỗng
            BTreeDiskNode root = new BTreeDiskNode(t, true);
            root.keys[0] = key;
            root.n = 1;
            rootPageId = allocateNode(root);
            flushMetadata();
            return;
        }

        BTreeDiskNode root = readNode(rootPageId);

        if (root.n == 2 * t - 1) {
            // Root đầy – tạo newRoot mới, split root cũ
            BTreeDiskNode newRoot = new BTreeDiskNode(t, false);
            // Allocate newRoot ngay để có pageId hợp lệ trước khi ghi
            int newRootPageId = allocateNode(newRoot);

            newRoot.childrenPageIds[0] = rootPageId;

            // splitChild không ghi parent; sau đó ta ghi newRoot thủ công
            splitChildNoWriteParent(newRoot, 0, root);

            // Xác định nhánh chèn
            int i = newRoot.keys[0].isLessThan(key) ? 1 : 0;
            BTreeDiskNode targetChild = readNode(newRoot.childrenPageIds[i]);
            insertNonFull(targetChild, key);

            // Ghi newRoot (có pageId hợp lệ) và cập nhật rootPageId
            writeNode(newRoot);
            rootPageId = newRootPageId;
            flushMetadata();
        } else {
            insertNonFull(root, key);
        }
    }

    private void insertNonFull(BTreeDiskNode node, BKey<T> key) throws IOException {
        int i = node.n - 1;

        if (node.isLeaf) {
            while (i >= 0 && node.keys[i].isGreaterThan(key)) {
                node.keys[i + 1] = node.keys[i];
                i--;
            }
            node.keys[i + 1] = key;
            node.n++;
            writeNode(node);
        } else {
            while (i >= 0 && node.keys[i].isGreaterThan(key)) i--;
            i++;

            BTreeDiskNode child = readNode(node.childrenPageIds[i]);
            if (child.n == 2 * t - 1) {
                splitChild(node, i, child);
                if (node.keys[i].isLessThan(key)) i++;
                child = readNode(node.childrenPageIds[i]);
            }
            insertNonFull(child, key);
            // Ghi lại node (childrenPageIds đã được cập nhật trong splitChild)
            writeNode(node);
        }
    }

    // ── splitChild variants ────────────────────────────────────────────────────

    /**
     * Split node con đầy (parent.children[i] = child).
     * Ghi child, newNode và parent lên disk.
     * Yêu cầu: parent.pageId >= 0.
     */
    private void splitChild(BTreeDiskNode parent, int i, BTreeDiskNode child) throws IOException {
        splitChildInternal(parent, i, child);
        writeNode(parent);
    }

    /**
     * Giống splitChild nhưng KHÔNG ghi parent (caller tự ghi).
     * Dùng khi parent chưa được allocate tại thời điểm gọi.
     */
    private void splitChildNoWriteParent(BTreeDiskNode parent, int i, BTreeDiskNode child) throws IOException {
        splitChildInternal(parent, i, child);
    }

    /**
     * Thực hiện split: tạo newNode, cập nhật child.n, cập nhật parent keys/children.
     * Ghi child và newNode; KHÔNG ghi parent.
     */
    private void splitChildInternal(BTreeDiskNode parent, int i, BTreeDiskNode child) throws IOException {
        BTreeDiskNode newNode = new BTreeDiskNode(t, child.isLeaf);
        newNode.n = t - 1;

        System.arraycopy(child.keys, t, newNode.keys, 0, t - 1);
        if (!child.isLeaf) {
            System.arraycopy(child.childrenPageIds, t, newNode.childrenPageIds, 0, t);
        }

        child.n = t - 1;

        // Ghi child và newNode để lấy pageId cho newNode
        int newNodePageId = allocateNode(newNode); // newNode.pageId được set
        writeNode(child);                           // child.pageId đã có sẵn

        // Dịch children của parent sang phải
        for (int j = parent.n; j >= i + 1; j--)
            parent.childrenPageIds[j + 1] = parent.childrenPageIds[j];
        parent.childrenPageIds[i + 1] = newNodePageId;

        // Dịch keys sang phải và đưa key giữa lên
        for (int j = parent.n - 1; j >= i; j--)
            parent.keys[j + 1] = parent.keys[j];
        parent.keys[i] = child.keys[t - 1];
        parent.n++;
        // parent KHÔNG được ghi ở đây
    }

    // ── Delete ─────────────────────────────────────────────────────────────────

    /**
     * Xoá một pointer khỏi key. Nếu key không còn pointer nào thì xoá key khỏi cây.
     */
    public void delete(BKey<T> key, long pointer) throws IOException {
        BKey<T> existing = search(key);
        if (existing != null) {
            existing.removePointer(pointer);
            if (existing.getRecordPointers().isEmpty()) {
                delete(existing);
            } else {
                updateKeyInTree(rootPageId, existing);
            }
        }
    }

    /**
     * Xoá key khỏi cây.
     */
    public void delete(BKey<T> key) throws IOException {
        if (rootPageId == -1) return;

        BTreeDiskNode root = readNode(rootPageId);
        deleteFromNode(root, key);

        // Reload root để kiểm tra (writeNode đã cập nhật trên disk)
        root = readNode(rootPageId);
        if (root.n == 0) {
            if (!root.isLeaf) {
                rootPageId = root.childrenPageIds[0];
            } else {
                rootPageId = -1;
            }
            flushMetadata();
        }
    }

    private void deleteFromNode(BTreeDiskNode node, BKey<T> key) throws IOException {
        int i = node.findKeyIndex(key);

        if (i < node.n && key.isEqual(node.keys[i])) {
            if (node.isLeaf) {
                removeFromLeaf(node, i);
            } else {
                removeFromNonLeaf(node, i);
            }
        } else {
            if (node.isLeaf) return; // Không tồn tại

            boolean isLastChild = (i == node.n);
            BTreeDiskNode child = readNode(node.childrenPageIds[i]);

            if (child.n < t) {
                fill(node, i);
                // Reload node sau fill (n có thể giảm sau merge)
                node = readNode(node.pageId);
                if (isLastChild && i > node.n) {
                    deleteFromNode(readNode(node.childrenPageIds[i - 1]), key);
                } else {
                    deleteFromNode(readNode(node.childrenPageIds[i]), key);
                }
            } else {
                deleteFromNode(child, key);
            }
        }
    }

    private void removeFromLeaf(BTreeDiskNode node, int i) throws IOException {
        for (int j = i + 1; j < node.n; j++)
            node.keys[j - 1] = node.keys[j];
        node.keys[node.n - 1] = null;
        node.n--;
        writeNode(node);
    }

    private void removeFromNonLeaf(BTreeDiskNode node, int i) throws IOException {
        BKey<T> k = (BKey<T>) node.keys[i];
        BTreeDiskNode leftChild  = readNode(node.childrenPageIds[i]);
        BTreeDiskNode rightChild = readNode(node.childrenPageIds[i + 1]);

        if (leftChild.n >= t) {
            BKey<T> pred = getPredecessor(leftChild);
            node.keys[i] = pred;
            writeNode(node);
            deleteFromNode(readNode(node.childrenPageIds[i]), pred);
        } else if (rightChild.n >= t) {
            BKey<T> succ = getSuccessor(rightChild);
            node.keys[i] = succ;
            writeNode(node);
            deleteFromNode(readNode(node.childrenPageIds[i + 1]), succ);
        } else {
            merge(node, i);
            deleteFromNode(readNode(node.childrenPageIds[i]), k);
        }
    }

    private BKey<T> getPredecessor(BTreeDiskNode node) throws IOException {
        while (!node.isLeaf)
            node = readNode(node.childrenPageIds[node.n]);
        return (BKey<T>) node.keys[node.n - 1];
    }

    private BKey<T> getSuccessor(BTreeDiskNode node) throws IOException {
        while (!node.isLeaf)
            node = readNode(node.childrenPageIds[0]);
        return (BKey<T>) node.keys[0];
    }

    private void fill(BTreeDiskNode parent, int i) throws IOException {
        BTreeDiskNode left  = (i != 0)        ? readNode(parent.childrenPageIds[i - 1]) : null;
        BTreeDiskNode right = (i != parent.n) ? readNode(parent.childrenPageIds[i + 1]) : null;

        if (left != null && left.n >= t) {
            borrowFromPrev(parent, i, left);
        } else if (right != null && right.n >= t) {
            borrowFromNext(parent, i, right);
        } else {
            if (i != parent.n) merge(parent, i);
            else               merge(parent, i - 1);
        }
    }

    private void borrowFromPrev(BTreeDiskNode parent, int i, BTreeDiskNode sibling) throws IOException {
        BTreeDiskNode child = readNode(parent.childrenPageIds[i]);

        for (int j = child.n - 1; j >= 0; j--)
            child.keys[j + 1] = child.keys[j];
        if (!child.isLeaf) {
            for (int j = child.n; j >= 0; j--)
                child.childrenPageIds[j + 1] = child.childrenPageIds[j];
        }

        child.keys[0] = parent.keys[i - 1];
        if (!child.isLeaf)
            child.childrenPageIds[0] = sibling.childrenPageIds[sibling.n];

        parent.keys[i - 1] = sibling.keys[sibling.n - 1];
        sibling.keys[sibling.n - 1] = null;
        child.n++;
        sibling.n--;

        writeNode(child);
        writeNode(sibling);
        writeNode(parent);
    }

    private void borrowFromNext(BTreeDiskNode parent, int i, BTreeDiskNode sibling) throws IOException {
        BTreeDiskNode child = readNode(parent.childrenPageIds[i]);

        child.keys[child.n] = parent.keys[i];
        if (!child.isLeaf)
            child.childrenPageIds[child.n + 1] = sibling.childrenPageIds[0];

        parent.keys[i] = sibling.keys[0];

        for (int j = 1; j < sibling.n; j++)
            sibling.keys[j - 1] = sibling.keys[j];
        sibling.keys[sibling.n - 1] = null;
        if (!sibling.isLeaf) {
            for (int j = 1; j <= sibling.n; j++)
                sibling.childrenPageIds[j - 1] = sibling.childrenPageIds[j];
        }

        child.n++;
        sibling.n--;

        writeNode(child);
        writeNode(sibling);
        writeNode(parent);
    }

    private void merge(BTreeDiskNode parent, int i) throws IOException {
        BTreeDiskNode child   = readNode(parent.childrenPageIds[i]);
        BTreeDiskNode sibling = readNode(parent.childrenPageIds[i + 1]);

        child.keys[t - 1] = parent.keys[i];
        System.arraycopy(sibling.keys, 0, child.keys, t, sibling.n);
        if (!child.isLeaf) {
            System.arraycopy(sibling.childrenPageIds, 0, child.childrenPageIds, t, sibling.n + 1);
        }
        child.n += sibling.n + 1;

        for (int j = i + 1; j < parent.n; j++)
            parent.keys[j - 1] = parent.keys[j];
        parent.keys[parent.n - 1] = null;
        for (int j = i + 2; j <= parent.n; j++)
            parent.childrenPageIds[j - 1] = parent.childrenPageIds[j];
        parent.n--;

        writeNode(child);
        writeNode(parent);
        // sibling block bị "orphan" – BlocksStorage chưa hỗ trợ deallocation
    }

    // ── Range Query ────────────────────────────────────────────────────────────

    public List<BKey<T>> findInRange(T lower, T upper) throws IOException {
        List<BKey<T>> result = new ArrayList<>();
        if (rootPageId != -1) findInRange(rootPageId, lower, upper, result);
        return result;
    }

    private void findInRange(int pageId, T lower, T upper, List<BKey<T>> result) throws IOException {
        BTreeDiskNode node = readNode(pageId);
        int i = 0;
        while (i < node.n && ((Comparable<T>) node.keys[i].getKey()).compareTo(lower) < 0) i++;

        while (i < node.n && ((Comparable<T>) node.keys[i].getKey()).compareTo(upper) <= 0) {
            if (!node.isLeaf) findInRange(node.childrenPageIds[i], lower, upper, result);
            result.add((BKey<T>) node.keys[i]);
            i++;
        }
        if (!node.isLeaf && node.childrenPageIds[i] != -1)
            findInRange(node.childrenPageIds[i], lower, upper, result);
    }

    // ── Print ──────────────────────────────────────────────────────────────────

    public void print() throws IOException {
        if (rootPageId == -1) {
            System.out.println("(Cây rỗng)");
            return;
        }
        BTreeDiskNode root = readNode(rootPageId);
        System.out.print("Root(p" + root.pageId + ") → [");
        for (int i = 0; i < root.n; i++) {
            System.out.print(root.keys[i].getKey());
            if (i < root.n - 1) System.out.print(", ");
        }
        System.out.println("]");
        for (int i = 0; i <= root.n; i++) {
            if (!root.isLeaf && root.childrenPageIds[i] != -1)
                printNode(root.childrenPageIds[i], "", i == root.n);
        }
    }

    private void printNode(int pageId, String indent, boolean isLast) throws IOException {
        BTreeDiskNode node = readNode(pageId);
        System.out.print(indent + (isLast ? "└── " : "├── "));
        System.out.print("(p" + node.pageId + ") [");
        for (int i = 0; i < node.n; i++) {
            System.out.print(node.keys[i].getKey());
            if (i < node.n - 1) System.out.print(", ");
        }
        System.out.println("]");
        String ci = indent + (isLast ? "    " : "│   ");
        for (int i = 0; i <= node.n; i++) {
            if (!node.isLeaf && node.childrenPageIds[i] != -1)
                printNode(node.childrenPageIds[i], ci, i == node.n);
        }
    }

    // ── Metadata ───────────────────────────────────────────────────────────────

    private void flushMetadata() throws IOException {
        byte[] payload = new byte[META_PAYLOAD_LEN];
        ByteBuffer.wrap(payload).putInt(rootPageId).putInt(t);

        if (storage.getTotalBlockCount() <= metadataBlockId) {
            // Allocate blank blocks if needed until metadataBlockId
            while (storage.getTotalBlockCount() <= metadataBlockId) {
                storage.allocateAndWrite(new Block());
            }
        }
        
        storage.updateBlock(metadataBlockId, data -> {
            ByteBuffer buf = ByteBuffer.wrap(data);
            buf.putInt(Block.OFFSET_SIZE,   META_PAYLOAD_LEN);
            buf.putInt(META_OFFSET_ROOT,    rootPageId);
            buf.putInt(META_OFFSET_T,       t);
        });
    }

    // ── Node I/O ───────────────────────────────────────────────────────────────

    /** Đọc node từ block pageId. */
    private BTreeDiskNode readNode(int pageId) throws IOException {
        Block block = storage.getBlock(pageId, null);
        if (block == null) throw new IOException("Block không tồn tại: pageId=" + pageId);
        byte[] raw     = block.bytes;
        int payloadLen = raw.length - Block.HEADER_TOTAL_SIZE;
        byte[] payload = new byte[payloadLen];
        System.arraycopy(raw, Block.HEADER_TOTAL_SIZE, payload, 0, payloadLen);
        BTreeDiskNode node = BTreeDiskNode.deserialize(payload, t);
        node.pageId = pageId;
        return node;
    }

    /**
     * Ghi node lên disk (node.pageId >= 0 đã tồn tại).
     */
    private void writeNode(BTreeDiskNode node) throws IOException {
        if (node.pageId < 0)
            throw new IllegalStateException("writeNode: pageId chưa được set: " + node);
        byte[] payload = node.serialize(BlocksStorage.BLOCK_SIZE - Block.HEADER_TOTAL_SIZE);
        storage.updateBlock(node.pageId, data -> {
            ByteBuffer.wrap(data).putInt(Block.OFFSET_SIZE, payload.length);
            System.arraycopy(payload, 0, data, Block.HEADER_TOTAL_SIZE, payload.length);
        });
    }

    /**
     * Cấp phát block mới cho node chưa có pageId.
     * Sau khi gọi, node.pageId được set thành block id mới.
     */
    private int allocateNode(BTreeDiskNode node) throws IOException {
        byte[] payload = node.serialize(BlocksStorage.BLOCK_SIZE - Block.HEADER_TOTAL_SIZE);
        Block block = new Block();
        block.insert(payload);
        int pageId = storage.allocateAndWrite(block);
        node.pageId = pageId;
        return pageId;
    }

    /** Cập nhật key (đã thay đổi pointers) trên disk bằng cách duyệt cây. */
    private boolean updateKeyInTree(int pageId, BKey<T> key) throws IOException {
        BTreeDiskNode node = readNode(pageId);
        for (int i = 0; i < node.n; i++) {
            if (key.isEqual(node.keys[i])) {
                node.keys[i] = key;
                writeNode(node);
                return true;
            }
        }
        if (!node.isLeaf) {
            int idx = node.findKeyIndex(key);
            // Tìm trong cả 2 nhánh: nhánh trái (idx) và nhánh phải (idx+1)
            // để xử lý trường hợp key bằng với key phân chia
            for (int c = 0; c <= node.n; c++) {
                if (node.childrenPageIds[c] != -1) {
                    if (updateKeyInTree(node.childrenPageIds[c], key)) return true;
                }
            }
        }
        return false;
    }

    // ── Close ──────────────────────────────────────────────────────────────────

    public void close() throws IOException {
        flushMetadata();
    }

    // ── Getters ────────────────────────────────────────────────────────────────

    public int getRootPageId() { return rootPageId; }
    public int getDegree()     { return t; }
    public boolean isEmpty()   { return rootPageId == -1; }
}

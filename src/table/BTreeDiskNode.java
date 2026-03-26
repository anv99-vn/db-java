package table;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * BTreeDiskNode - Node của B-Tree được lưu trên disk.
 *
 * <p>Mỗi node chiếm đúng 1 block (4096 bytes) trong BlocksStorage.
 * pageId == -1 nghĩa là node chưa được cấp phát (chưa ghi xuống disk).
 *
 * <p>Layout trong block (ByteBuffer):
 * <pre>
 *  Offset  Size  Field
 *  ------  ----  -----
 *  0       1     isLeaf  (0 or 1)
 *  1       4     n       (số key hiện tại)
 *  5       4     pageId  (block id trên disk)
 *  9       ...   keys[]  (serialize từng key)
 *                  each key:
 *                    4   keyType  (0=Int, 1=Long, 2=Double, 3=String)
 *                    N   keyValue (4/8/8/... bytes tuỳ theo type)
 *                    4   pointerCount
 *                    8*pointerCount  recordPointers (long[])
 *  ...     ...   children[] — (2t+1) x 4 bytes pageId khi !isLeaf
 * </pre>
 *
 * Để đơn giản, class này chỉ hỗ trợ key kiểu {@link Integer}, {@link Long},
 * {@link Double} và {@link String} (tối đa 255 ký tự UTF-8).
 */
public class BTreeDiskNode {

    // ── Hằng số kiểu key ──────────────────────────────────────────────────────
    public static final int KEY_TYPE_INT    = 0;
    public static final int KEY_TYPE_LONG   = 1;
    public static final int KEY_TYPE_DOUBLE = 2;
    public static final int KEY_TYPE_STRING = 3;

    // ── Hằng số cố định ───────────────────────────────────────────────────────
    /** Số byte header của node trong block. */
    public static final int NODE_HEADER_BYTES = 9; // isLeaf(1) + n(4) + pageId(4)

    // ── Trường dữ liệu ────────────────────────────────────────────────────────
    public int     pageId;   // Block ID trên disk; -1 khi chưa ghi
    public int     t;        // Minimum degree
    public boolean isLeaf;
    public int     n;        // Số key hiện tại

    /** Keys lưu dưới dạng BKey<Comparable>. Dùng raw-type để linh hoạt. */
    @SuppressWarnings("rawtypes")
    public BKey[] keys;

    /** pageId của các node con; chỉ hợp lệ khi !isLeaf. */
    public int[] childrenPageIds;

    // ── Constructor ────────────────────────────────────────────────────────────

    @SuppressWarnings({"unchecked", "rawtypes"})
    public BTreeDiskNode(int t, boolean isLeaf) {
        this.t       = t;
        this.isLeaf  = isLeaf;
        this.n       = 0;
        this.pageId  = -1;
        this.keys    = new BKey[2 * t - 1];
        this.childrenPageIds = new int[2 * t];
        for (int i = 0; i < 2 * t; i++) childrenPageIds[i] = -1;
    }

    // ── Serialize / Deserialize ────────────────────────────────────────────────

    /**
     * Ghi node vào mảng byte (kích thước blockSize).
     */
    public byte[] serialize(int blockSize) {
        ByteBuffer buf = ByteBuffer.allocate(blockSize);

        buf.put((byte) (isLeaf ? 1 : 0));  // 1 byte
        buf.putInt(n);                     // 4 bytes
        buf.putInt(pageId);                // 4 bytes

        // Keys
        for (int i = 0; i < n; i++) {
            serializeKey(buf, keys[i]);
        }

        // Children page IDs (chỉ khi không phải lá)
        if (!isLeaf) {
            for (int i = 0; i <= n; i++) {
                buf.putInt(childrenPageIds[i]);
            }
        }

        return buf.array();
    }

    /**
     * Đọc node từ mảng byte đã được ghi bởi {@link #serialize}.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static BTreeDiskNode deserialize(byte[] data, int t) {
        ByteBuffer buf = ByteBuffer.wrap(data);

        boolean isLeaf = buf.get() == 1;
        int n          = buf.getInt();
        int pageId     = buf.getInt();

        BTreeDiskNode node = new BTreeDiskNode(t, isLeaf);
        node.n      = n;
        node.pageId = pageId;

        for (int i = 0; i < n; i++) {
            node.keys[i] = deserializeKey(buf);
        }

        if (!isLeaf) {
            for (int i = 0; i <= n; i++) {
                node.childrenPageIds[i] = buf.getInt();
            }
        }

        return node;
    }

    // ── Helpers serialization ─────────────────────────────────────────────────

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void serializeKey(ByteBuffer buf, BKey key) {
        Object k = key.getKey();

        if (k instanceof Integer) {
            buf.putInt(KEY_TYPE_INT);
            buf.putInt((Integer) k);
        } else if (k instanceof Long) {
            buf.putInt(KEY_TYPE_LONG);
            buf.putLong((Long) k);
        } else if (k instanceof Double) {
            buf.putInt(KEY_TYPE_DOUBLE);
            buf.putDouble((Double) k);
        } else {
            // String (tối đa 255 bytes UTF-8)
            byte[] strBytes = k.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            int len = Math.min(strBytes.length, 255);
            buf.putInt(KEY_TYPE_STRING);
            buf.put((byte) len);
            buf.put(strBytes, 0, len);
        }

        // Record pointers
        List<Long> pointers = key.getRecordPointers();
        buf.putInt(pointers.size());
        for (long p : pointers) buf.putLong(p);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static BKey deserializeKey(ByteBuffer buf) {
        int keyType = buf.getInt();

        Comparable value;
        switch (keyType) {
            case KEY_TYPE_INT:    value = buf.getInt();    break;
            case KEY_TYPE_LONG:   value = buf.getLong();   break;
            case KEY_TYPE_DOUBLE: value = buf.getDouble(); break;
            case KEY_TYPE_STRING: {
                int len = Byte.toUnsignedInt(buf.get());
                byte[] strBytes = new byte[len];
                buf.get(strBytes);
                value = new String(strBytes, java.nio.charset.StandardCharsets.UTF_8);
                break;
            }
            default: throw new IllegalStateException("Unknown key type: " + keyType);
        }

        BKey key = new BKey<>(value);
        int pointerCount = buf.getInt();
        for (int i = 0; i < pointerCount; i++) {
            key.addPointer(buf.getLong());
        }
        return key;
    }

    // ── Tính toán kích thước ──────────────────────────────────────────────────

    /**
     * Ước tính kích thước tối đa (bytes) của một node với degree t và
     * kiểu key cho trước, giúp kiểm tra xem 1 node có vừa trong 1 block không.
     *
     * @param t        minimum degree
     * @param keyType  KEY_TYPE_INT / LONG / DOUBLE / STRING
     * @param maxStrLen byte length tối đa của string (bỏ qua nếu không phải STRING)
     * @param maxPointers số pointer tối đa mỗi key
     */
    public static int estimateNodeSize(int t, int keyType, int maxStrLen, int maxPointers) {
        int keySize;
        switch (keyType) {
            case KEY_TYPE_INT:    keySize = 4 + 4; break;              // type(4) + int(4)
            case KEY_TYPE_LONG:   keySize = 4 + 8; break;              // type(4) + long(8)
            case KEY_TYPE_DOUBLE: keySize = 4 + 8; break;              // type(4) + double(8)
            case KEY_TYPE_STRING: keySize = 4 + 1 + maxStrLen; break;  // type(4) + len(1) + str
            default:              keySize = 4 + 8; break;
        }
        keySize += 4 + maxPointers * 8; // pointerCount(4) + pointers

        int maxKeys     = 2 * t - 1;
        int maxChildren = 2 * t;

        return NODE_HEADER_BYTES
                + maxKeys * keySize
                + maxChildren * 4; // child pageIds
    }

    // ── Search / Insert helpers ────────────────────────────────────────────────

    /** Trả về index i sao cho keys[0..i-1] < key <= keys[i..n-1]. */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public int findKeyIndex(BKey key) {
        int i = 0;
        while (i < n && keys[i].isLessThan(key)) i++;
        return i;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Node{pageId=").append(pageId)
          .append(", isLeaf=").append(isLeaf)
          .append(", n=").append(n)
          .append(", keys=[");
        for (int i = 0; i < n; i++) {
            if (i > 0) sb.append(", ");
            sb.append(keys[i].getKey());
        }
        sb.append("]}");
        return sb.toString();
    }
}

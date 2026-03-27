package b_tree;

import table.CompositeKey;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * BTreeDiskNode - Node của B-Tree được lưu trên disk.
 */
public class BTreeDiskNode {

    public static final int KEY_TYPE_INT    = 0;
    public static final int KEY_TYPE_LONG   = 1;
    public static final int KEY_TYPE_DOUBLE = 2;
    public static final int KEY_TYPE_STRING = 3;
    public static final int KEY_TYPE_COMPOSITE = 4;

    public static final int NODE_HEADER_BYTES = 9; // isLeaf(1) + n(4) + pageId(4)

    public int     pageId;
    public int     t;
    public boolean isLeaf;
    public int     n;

    @SuppressWarnings("rawtypes")
    public BKey[] keys;
    public int[] childrenPageIds;

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

    public byte[] serialize(int blockSize) {
        ByteBuffer buf = ByteBuffer.allocate(blockSize);
        buf.put((byte) (isLeaf ? 1 : 0));
        buf.putInt(n);
        buf.putInt(pageId);
        for (int i = 0; i < n; i++) {
            serializeKey(buf, keys[i]);
        }
        if (!isLeaf) {
            for (int i = 0; i <= n; i++) {
                buf.putInt(childrenPageIds[i]);
            }
        }
        return buf.array();
    }

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

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void serializeKey(ByteBuffer buf, BKey key) {
        serializeValue(buf, key.getKey());
        List<Long> pointers = key.getRecordPointers();
        buf.putInt(pointers.size());
        for (long p : pointers) buf.putLong(p);
    }

    private static void serializeValue(ByteBuffer buf, Object k) {
        if (k instanceof Integer) {
            buf.putInt(KEY_TYPE_INT);
            buf.putInt((Integer) k);
        } else if (k instanceof Long) {
            buf.putInt(KEY_TYPE_LONG);
            buf.putLong((Long) k);
        } else if (k instanceof Double) {
            buf.putInt(KEY_TYPE_DOUBLE);
            buf.putDouble((Double) k);
        } else if (k instanceof String) {
            byte[] strBytes = k.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            int len = Math.min(strBytes.length, 255);
            buf.putInt(KEY_TYPE_STRING);
            buf.put((byte) len);
            buf.put(strBytes, 0, len);
        } else if (k instanceof CompositeKey) {
            buf.putInt(KEY_TYPE_COMPOSITE);
            Object[] values = ((CompositeKey) k).getValues();
            buf.putInt(values.length);
            for (Object v : values) {
                serializeValue(buf, v);
            }
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static BKey deserializeKey(ByteBuffer buf) {
        Comparable value = (Comparable) deserializeValue(buf);
        BKey key = new BKey<>(value);
        int pointerCount = buf.getInt();
        for (int i = 0; i < pointerCount; i++) {
            key.addPointer(buf.getLong());
        }
        return key;
    }

    private static Object deserializeValue(ByteBuffer buf) {
        int keyType = buf.getInt();
        switch (keyType) {
            case KEY_TYPE_INT:    return buf.getInt();
            case KEY_TYPE_LONG:   return buf.getLong();
            case KEY_TYPE_DOUBLE: return buf.getDouble();
            case KEY_TYPE_STRING: {
                int len = Byte.toUnsignedInt(buf.get());
                byte[] strBytes = new byte[len];
                buf.get(strBytes);
                return new String(strBytes, java.nio.charset.StandardCharsets.UTF_8);
            }
            case KEY_TYPE_COMPOSITE: {
                int num = buf.getInt();
                Object[] values = new Object[num];
                for (int i = 0; i < num; i++) {
                    values[i] = deserializeValue(buf);
                }
                return new CompositeKey(values);
            }
            default: throw new IllegalStateException("Unknown key type: " + keyType);
        }
    }

    public static int estimateNodeSize(int t, int keyType, int maxStrLen, int maxPointers) {
        int keySize;
        switch (keyType) {
            case KEY_TYPE_INT:    keySize = 8; break;
            case KEY_TYPE_LONG:   keySize = 12; break;
            case KEY_TYPE_DOUBLE: keySize = 12; break;
            case KEY_TYPE_STRING: keySize = 5 + maxStrLen; break;
            case KEY_TYPE_COMPOSITE: keySize = 100; break; // Rough estimate
            default:              keySize = 12; break;
        }
        keySize += 4 + maxPointers * 8;
        return NODE_HEADER_BYTES + (2 * t - 1) * keySize + (2 * t) * 4;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public int findKeyIndex(BKey key) {
        int i = 0;
        while (i < n && keys[i].isLessThan(key)) i++;
        return i;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Node{pageId=").append(pageId).append(", n=").append(n).append(", keys=[");
        for (int i = 0; i < n; i++) {
            if (i > 0) sb.append(", ");
            sb.append(keys[i].getKey());
        }
        sb.append("]}");
        return sb.toString();
    }
}

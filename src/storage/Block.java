package storage;

import java.nio.ByteBuffer;

public class Block {
    private int size;
    private int nextBlockId; // Thuộc tính mới
    public int id;
    public byte[] bytes;

    // Định nghĩa các hằng số để dễ quản lý header
    static final int OFFSET_SIZE = 0;
    public static final int OFFSET_NEXT_BLOCK = 4;
    public static final int HEADER_TOTAL_SIZE = 8; // 4 (size) + 4 (nextBlockId)

    public Block() {
        this.bytes = new byte[BlocksStorage.BLOCK_SIZE];
        this.size = 0;
        this.nextBlockId = -1; // -1 thường đại diện cho không có block kế tiếp

        // Khởi tạo header mặc định trong mảng bytes
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        wrap.putInt(OFFSET_SIZE, size);
        wrap.putInt(OFFSET_NEXT_BLOCK, nextBlockId);
    }

    public Block(byte[] bytes) {
        this.bytes = bytes;
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        this.size = wrap.getInt(OFFSET_SIZE);
        this.nextBlockId = wrap.getInt(OFFSET_NEXT_BLOCK);
    }

    public int getSize() {
        return size;
    }

    public int getNextBlockId() {
        return nextBlockId;
    }

    public void setNextBlockId(int nextBlockId) {
        this.nextBlockId = nextBlockId;
        ByteBuffer.wrap(bytes).putInt(OFFSET_NEXT_BLOCK, nextBlockId);
    }

    public void insert(byte[] array) {
        // Kiểm tra giới hạn: HEADER + dữ liệu hiện tại + dữ liệu mới
        if (HEADER_TOTAL_SIZE + size + array.length > bytes.length) {
            throw new IllegalArgumentException("Block full");
        }

        // Copy dữ liệu mới vào sau vùng dữ liệu cũ (sau Header và size cũ)
        System.arraycopy(array, 0, bytes, HEADER_TOTAL_SIZE + size, array.length);

        // Cập nhật size
        size += array.length;

        // Ghi size mới vào header trong mảng bytes
        ByteBuffer.wrap(bytes).putInt(OFFSET_SIZE, size);
    }
}
package storage;

import java.nio.ByteBuffer;

public class Block {
    private int size;
    private int nextBlockId; // Thuộc tính mới
    public int id;
    public byte[] bytes;

    // Định nghĩa các hằng số để dễ quản lý header
    public static final int OFFSET_SIZE = 0;
    public static final int OFFSET_NEXT_BLOCK = 4;
    public static final int OFFSET_CHECKSUM = 8;
    public static final int HEADER_TOTAL_SIZE = 12; // 4 (size) + 4 (nextBlockId) + 4 (checksum)
    private int checksum;

    public Block() {
        this.bytes = new byte[BlocksStorage.BLOCK_SIZE];
        this.size = 0;
        this.nextBlockId = -1; // -1 thường đại diện cho không có block kế tiếp

        // Khởi tạo header mặc định trong mảng bytes
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        wrap.putInt(OFFSET_SIZE, size);
        wrap.putInt(OFFSET_NEXT_BLOCK, nextBlockId);
        updateChecksum();
    }

    public Block(byte[] bytes) {
        this.bytes = bytes;
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        this.size = wrap.getInt(OFFSET_SIZE);
        this.nextBlockId = wrap.getInt(OFFSET_NEXT_BLOCK);
        // Recompute checksum from payload and store into header (ensures consistency after modifications)
        this.checksum = computeChecksum(bytes, this.size);
        wrap.putInt(OFFSET_CHECKSUM, this.checksum);
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
        updateChecksum();
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
        updateChecksum();
    }

    private int computeChecksum(byte[] bytes, int size) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        if (size > 0) {
            crc.update(bytes, HEADER_TOTAL_SIZE, size);
        }
        return (int) crc.getValue();
    }

    private void updateChecksum() {
        this.checksum = computeChecksum(this.bytes, this.size);
        ByteBuffer.wrap(this.bytes).putInt(OFFSET_CHECKSUM, this.checksum);
    }
}
package storage;

import java.nio.ByteBuffer;

public class Block {
    private int size;
    public int id;
    public byte[] bytes;

    public Block() {
        this.bytes = new byte[BlocksStorage.BLOCK_SIZE];
        size = 0;
    }

    public Block(byte[] bytes) {
        this.bytes = bytes;
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        size = wrap.getInt();
    }


    public int getSize() {
        return size;
    }

    public void insert(byte[] array) {
        if (size + array.length + 4 > bytes.length) {
            throw new IllegalArgumentException("Block full");
        }
        System.arraycopy(array, 0, bytes, 4 + size, array.length);
        size += array.length;
        ByteBuffer.wrap(bytes).putInt(0, size);
    }
}

package storage;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class BlockTest {

    @Test
    @DisplayName("Test Block initialization and default values")
    void testBlockInitialization() {
        Block block = new Block();
        assertNotNull(block.bytes);
        assertEquals(BlocksStorage.BLOCK_SIZE, block.bytes.length);
        assertEquals(0, block.getSize());
    }

    @Test
    @DisplayName("Test Block creation from existing bytes")
    void testBlockFromBytes() {
        byte[] data = new byte[BlocksStorage.BLOCK_SIZE];
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.putInt(42); // Size
        buffer.put("Hello".getBytes());
        
        Block block = new Block(data);
        assertEquals(42, block.getSize());
        assertEquals(data, block.bytes);
    }

    @Test
    @DisplayName("Test insert method")
    void testInsert() {
        Block block = new Block();
        String text = "Hello World";
        byte[] data = text.getBytes();
        
        // This expects insert to append data to the block
        block.insert(data);
        
        assertEquals(data.length, block.getSize(), "Size should update after insert");
        
        // Verify header size
        assertEquals(data.length, ByteBuffer.wrap(block.bytes).getInt(), "Header size should match");
        
        // Verify content
        byte[] content = new byte[data.length];
        System.arraycopy(block.bytes, 4, content, 0, data.length);
        assertArrayEquals(data, content, "Content should match inserted data");
    }

    @Test
    @DisplayName("Test insert with insufficient space")
    void testInsertOverflow() {
        Block block = new Block();
        byte[] hugeData = new byte[BlocksStorage.BLOCK_SIZE + 1];
        assertThrows(IllegalArgumentException.class, () -> block.insert(hugeData));
    }

    @Test
    @DisplayName("Test insert when block already has data")
    void testInsertWithExistingData() {
        Block block = new Block();
        String text1 = "Hello";
        String text2 = " World";
        
        block.insert(text1.getBytes());
        block.insert(text2.getBytes());
        
        String expectedText = text1 + text2;
        byte[] expectedData = expectedText.getBytes();
        
        assertEquals(expectedData.length, block.getSize(), "Size should include both insertions");
        assertEquals(expectedData.length, ByteBuffer.wrap(block.bytes).getInt(), "Header size should match total length");
        
        byte[] content = new byte[expectedData.length];
        System.arraycopy(block.bytes, 4, content, 0, expectedData.length);
        assertArrayEquals(expectedData, content, "Content should match concatenated data");
    }
}

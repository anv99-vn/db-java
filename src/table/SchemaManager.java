package table;

import storage.Block;
import storage.BlocksStorage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SchemaManager - Quản lý việc lưu trữ Schema (metadata) của các bảng xuống Disk.
 * Sử dụng Block 0 (Master Block) để lưu danh sách các bảng.
 * 
 * Layout Master Block (Simplified sqlite_master equivalent):
 * [Header (12)] | [numTables (4)] | Table1... | Table2... 
 */
public class SchemaManager {

    private final BlocksStorage storage;
    private static final int MASTER_BLOCK_ID = 0;

    public SchemaManager(BlocksStorage storage) {
        this.storage = storage;
    }

    /**
     * Lưu danh sách các Table vào Block 0.
     * Cảnh báo: Block 0 có sức chứa có hạn (4096 bytes). 
     * Với cấu trúc này, có thể lưu khoảng 50-100 bảng tùy số cột.
     */
    public void saveSchemas(List<Table> tables) throws IOException {
        // Prepare payload (exclude header)
        ByteBuffer buffer = ByteBuffer.allocate(BlocksStorage.BLOCK_SIZE - Block.HEADER_TOTAL_SIZE);
        
        buffer.putInt(tables.size());
        
        for (Table table : tables) {
            // Table Name
            String name = table.getName();
            if (name == null) name = "UNTITLED";
            byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
            buffer.putInt(nameBytes.length);
            buffer.put(nameBytes);
            
            // Block IDs
            buffer.putInt(table.getFirstBlock());
            buffer.putInt(table.getLastBlock());
            
            // Columns
            Map<String, DataType> columns = table.getColumn();
            Map<String, Integer> sizes = table.getColumnSizes();
            if (columns == null) {
                buffer.putInt(0);
            } else {
                buffer.putInt(columns.size());
                for (Map.Entry<String, DataType> entry : columns.entrySet()) {
                    String colName = entry.getKey();
                    byte[] colNameBytes = colName.getBytes(StandardCharsets.UTF_8);
                    buffer.putInt(colNameBytes.length);
                    buffer.put(colNameBytes);
                    
                    buffer.putInt(entry.getValue().getId());
                    buffer.putInt(sizes.get(colName));
                }
            }
        }

        // Tạo block để ghi
        Block masterBlock = new Block();
        // Pack data
        int dataSize = buffer.position();
        System.arraycopy(buffer.array(), 0, masterBlock.bytes, Block.HEADER_TOTAL_SIZE, dataSize);
        // Set size in block logic for integrity
        ByteBuffer.wrap(masterBlock.bytes).putInt(Block.OFFSET_SIZE, dataSize);
        
        // Write to storage: Always write to Block 0
        // If file is empty, this will expand it to 1 block size.
        storage.putBlock(MASTER_BLOCK_ID, masterBlock);
    }

    /**
     * Load danh sách tables từ Block 0.
     */
    public List<Table> loadSchemas() throws IOException {
        Block masterBlock = storage.getBlock(MASTER_BLOCK_ID, null);
        if (masterBlock == null || masterBlock.getSize() == 0) {
            return new ArrayList<>();
        }

        ByteBuffer buffer = ByteBuffer.wrap(masterBlock.bytes);
        buffer.position(Block.HEADER_TOTAL_SIZE);
        
        int numTables = buffer.getInt();
        List<Table> tables = new ArrayList<>();
        
        for (int i = 0; i < numTables; i++) {
            Table table = new Table();
            
            // Table Name
            int nameLen = buffer.getInt();
            byte[] nameBytes = new byte[nameLen];
            buffer.get(nameBytes);
            table.setName(new String(nameBytes, StandardCharsets.UTF_8));
            
            // Data Block IDs
            int first = buffer.getInt();
            int last = buffer.getInt();
            table.setFirstBlock(first);
            table.setLastBlock(last);
            
            // Columns
            int numCols = buffer.getInt();
            for (int j = 0; j < numCols; j++) {
                int colNameLen = buffer.getInt();
                byte[] colNameBytes = new byte[colNameLen];
                buffer.get(colNameBytes);
                String colName = new String(colNameBytes, StandardCharsets.UTF_8);
                
                int typeId = buffer.getInt();
                int size = buffer.getInt();
                
                table.addColumn(colName, DataType.fromId(typeId), size);
            }
            // Add to list
            tables.add(table);
        }
        
        return tables;
    }
}

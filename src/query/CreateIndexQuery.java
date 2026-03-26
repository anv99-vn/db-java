package query;

import storage.Block;
import storage.BlocksStorage;
import table.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses and runs:  CREATE INDEX ON tableName (columnName)
 *
 * Creates a disk-backed B-Tree index for the given column.
 * The index metadata block is allocated from BlocksStorage and registered
 * on the table object, so it is persisted by SchemaManager.
 */
public class CreateIndexQuery implements Query {

    public String tableName;
    public String columnName;

    @Override
    public void parse(String query) {
        SqlTokenizer tokenizer = new SqlTokenizer(query);
        List<String> tokens = tokenizer.tokenize();

        // Syntax: CREATE INDEX ON <table> ( <column> )
        if (tokens.size() < 6
                || !tokens.get(0).equalsIgnoreCase("CREATE")
                || !tokens.get(1).equalsIgnoreCase("INDEX")
                || !tokens.get(2).equalsIgnoreCase("ON")) {
            throw new IllegalArgumentException(
                    "Invalid syntax. Expected: CREATE INDEX ON <table> (<column>)");
        }

        this.tableName = tokens.get(3);

        if (!tokens.get(4).equals("(")) {
            throw new IllegalArgumentException("Expected '(' after table name");
        }
        this.columnName = tokens.get(5);

        if (tokens.size() < 7 || !tokens.get(6).equals(")")) {
            throw new IllegalArgumentException("Expected ')' after column name");
        }
    }

    @Override
    public void run(Table table) throws IOException {
        LinkedHashMap<String, DataType> schema = table.getColumn();
        if (schema == null || !schema.containsKey(columnName)) {
            throw new IllegalArgumentException("Column '" + columnName + "' not found in table");
        }

        if (table.getIndexes().containsKey(columnName)) {
            throw new IllegalArgumentException("Index already exists on column '" + columnName + "'");
        }

        DataType type = schema.get(columnName);

        // Allocate a fresh block for B-Tree metadata
        int metaBlockId = BlocksStorage.getInstance().allocateAndWrite(new Block());
        table.addIndex(columnName, metaBlockId);
        Index index = table.getIndexes().get(columnName);
        
        // Backfill existing data into index
        backfillIndex(table, index, type);
        
        System.out.println("[Index] Created index on " + table.getName() + "." + columnName
                + " (metaBlock=" + metaBlockId + ")");
    }

    private void backfillIndex(Table table, Index index, DataType type) throws IOException {
        BlocksStorage storage = BlocksStorage.getInstance();
        LinkedHashMap<String, DataType> schema = table.getColumn();
        int targetColIdx = new ArrayList<>(schema.keySet()).indexOf(columnName);
        
        int blockId = table.getFirstBlock();
        while (blockId != -1) {
            Block block = storage.getBlock(blockId, null);
            if (block == null) break;
            
            ByteBuffer buf = ByteBuffer.wrap(block.bytes);
            int blockSize = buf.getInt(Block.OFFSET_SIZE);
            int nextBlockId = buf.getInt(Block.OFFSET_NEXT_BLOCK);
            
            int pos = Block.HEADER_TOTAL_SIZE;
            while (pos < Block.HEADER_TOTAL_SIZE + blockSize) {
                int recordStart = pos;
                buf.position(pos);
                
                Object val = null;
                int colIdx = 0;
                for (Map.Entry<String, DataType> entry : schema.entrySet()) {
                    Object currentVal = null;
                    switch (entry.getValue()) {
                        case INT -> currentVal = buf.getInt();
                        case FLOAT -> currentVal = buf.getFloat();
                        case STRING -> {
                            int s = table.getColumnSizes().get(entry.getKey());
                            byte[] bytes = new byte[s];
                            buf.get(bytes);
                            currentVal = new String(bytes).trim();
                        }
                    }
                    if (colIdx == targetColIdx) val = currentVal;
                    colIdx++;
                }
                
                long pointer = ((long) blockId << 32) | (recordStart & 0xFFFFFFFFL);
                insertIntoIndex(index, val, pointer, type);
                pos = buf.position();
            }
            blockId = nextBlockId;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void insertIntoIndex(Index index, Object val, long pointer, DataType type) {
        BTreeDisk tree = index.getBTree();
        try {
            switch (type) {
                case INT -> tree.insert(new BKey<>((Integer) val), pointer);
                case FLOAT -> tree.insert(new BKey<>((Float) val), pointer);
                case STRING -> tree.insert(new BKey<>((String) val), pointer);
            }
        } catch (IOException e) {
            System.err.println("Backfill error: " + e.getMessage());
        }
    }
}

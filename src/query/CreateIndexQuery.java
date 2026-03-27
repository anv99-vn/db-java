package query;

import b_tree.BKey;
import b_tree.BTreeDisk;
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
public class CreateIndexQuery implements DatabaseQuery {

    public String tableName;
    public java.util.List<String> columnNames = new java.util.ArrayList<>();

    @Override
    public void parse(String query) {
        SqlTokenizer tokenizer = new SqlTokenizer(query);
        List<String> tokens = tokenizer.tokenize();

        // Standard: CREATE INDEX [name] ON <table> ( <col1>, <col2>... )
        if (tokens.size() < 4 || !tokens.get(0).equalsIgnoreCase("CREATE") || !tokens.get(1).equalsIgnoreCase("INDEX")) {
            throw new IllegalArgumentException("Invalid syntax. Expected: CREATE INDEX [name] ON <table> (<col1>)");
        }

        int onIndex = -1;
        for (int i = 2; i < tokens.size(); i++) {
            if (tokens.get(i).equalsIgnoreCase("ON")) {
                onIndex = i;
                break;
            }
        }

        if (onIndex == -1 || onIndex + 2 >= tokens.size()) {
            throw new IllegalArgumentException("Invalid syntax. Missing ON or table info.");
        }

        // Optional: index name could be at tokens.get(2) if onIndex == 3
        this.tableName = tokens.get(onIndex + 1);

        if (!tokens.get(onIndex + 2).equals("(")) {
            throw new IllegalArgumentException("Expected '(' after table name");
        }

        int i = onIndex + 3;
        while (i < tokens.size() && !tokens.get(i).equals(")")) {
            String col = tokens.get(i);
            if (!col.equals(",")) {
                this.columnNames.add(col);
            }
            i++;
        }

        if (i >= tokens.size() || !tokens.get(i).equals(")")) {
            throw new IllegalArgumentException("Expected ')' after column names");
        }
    }

    @Override
    public void run(SchemaManager schemaManager) throws IOException {
        List<Table> tables = schemaManager.loadSchemas();
        Table table = tables.stream()
                .filter(t -> t.getName().equalsIgnoreCase(tableName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No such table: " + tableName));

        LinkedHashMap<String, DataType> schema = table.getColumn();
        for (String col : columnNames) {
            if (schema == null || !schema.containsKey(col)) {
                throw new IllegalArgumentException("Column '" + col + "' not found in table");
            }
        }

        // Use concatenated columns as registration key
        String indexKey = String.join(",", columnNames);
        
        // Allocate a fresh block for B-Tree metadata
        int metaBlockId = BlocksStorage.getInstance().allocateAndWrite(new Block());
        table.addIndex(columnNames, metaBlockId);
        Index index = table.getIndexes().get(indexKey);
        
        // Backfill existing data into index
        backfillIndex(table, index);

        // PERSIST the updated schema (containing the new index)
        schemaManager.saveSchemas(tables);
        
        System.out.println("[Index] Created index on " + table.getName() + " (" + String.join(", ", columnNames)
                + ") (metaBlock=" + metaBlockId + ")");
    }

    private void backfillIndex(Table table, Index index) throws IOException {
        BlocksStorage storage = BlocksStorage.getInstance();
        LinkedHashMap<String, DataType> schema = table.getColumn();
        List<String> schemaKeys = new ArrayList<>(schema.keySet());
        
        List<Integer> targetColIndices = new ArrayList<>();
        for (String col : columnNames) {
            targetColIndices.add(schemaKeys.indexOf(col));
        }
        
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
                
                Object[] values = new Object[columnNames.size()];
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
                    int insideIdx = targetColIndices.indexOf(colIdx);
                    if (insideIdx != -1) {
                        values[insideIdx] = currentVal;
                    }
                    colIdx++;
                }
                
                long pointer = ((long) blockId << 32) | (recordStart & 0xFFFFFFFFL);
                Object keyObj;
                if (values.length == 1) {
                    keyObj = values[0];
                } else {
                    keyObj = new CompositeKey(values);
                }
                insertIntoIndex(index, keyObj, pointer);
                pos = buf.position();
            }
            blockId = nextBlockId;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void insertIntoIndex(Index index, Object val, long pointer) throws IOException {
        BTreeDisk tree = index.getBTree();
        tree.insert(new BKey((Comparable) val), pointer);
    }
}

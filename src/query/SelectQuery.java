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

public class SelectQuery implements Query {
    public String tableName;
    public List<String> columns = new ArrayList<>();
    public boolean selectAll = false;
    private final List<List<Object>> results = new ArrayList<>();
    
    // Condition parts
    private Condition condition;

    @Override
    public void parse(String query) {
        SqlTokenizer tokenizer = new SqlTokenizer(query);
        List<String> tokens = tokenizer.tokenize();

        if (tokens.isEmpty() || !tokens.get(0).equalsIgnoreCase("SELECT")) {
            throw new IllegalArgumentException("Invalid query syntax: must start with SELECT");
        }

        int i = 1;
        if (tokens.get(i).equals("*")) {
            selectAll = true;
            i++;
        } else {
            while (i < tokens.size() && !tokens.get(i).equalsIgnoreCase("FROM")) {
                if (!tokens.get(i).equals(",")) {
                    columns.add(tokens.get(i));
                }
                i++;
            }
        }

        if (i >= tokens.size() || !tokens.get(i).equalsIgnoreCase("FROM")) {
            throw new IllegalArgumentException("Invalid query syntax: missing FROM clause");
        }
        i++;

        if (i >= tokens.size()) {
            throw new IllegalArgumentException("Invalid query syntax: missing table name");
        }
        this.tableName = tokens.get(i);
        i++;

        if (i < tokens.size() && tokens.get(i).equalsIgnoreCase("WHERE")) {
            i++;
            StringBuilder whereClause = new StringBuilder();
            while (i < tokens.size()) {
                whereClause.append(tokens.get(i)).append(" ");
                i++;
            }
            this.condition = new Condition();
            this.condition.parse(whereClause.toString().trim());
        }
    }

    // Parsing logic moved to Condition.java

    @Override
    public void run(Table table) throws IOException {
        results.clear();
        LinkedHashMap<String, DataType> schema = table.getColumn();
        List<String> schemaKeys = new ArrayList<>(schema.keySet());
        
        // Try to use index optimization
        if (condition != null) {
            String col = condition.getColumnName();
            String op = condition.getOperator();
            if (op.equals("=") && table.getIndexes().containsKey(col)) {
                handleIndexSearch(table, table.getIndexes().get(col), condition.getValue());
                return;
            }
        }

        // Full-scan if no index optimization found
        fullScan(table, schema, schemaKeys);
    }

    private void fullScan(Table table, LinkedHashMap<String, DataType> schema, List<String> schemaKeys) throws IOException {
        BlocksStorage blocksStorage = BlocksStorage.getInstance();
        int whereColIndex = -1;
        DataType whereColType = null;

        if (condition != null) {
            whereColIndex = schemaKeys.indexOf(condition.getColumnName());
            if (whereColIndex == -1) {
                throw new IllegalArgumentException("Column not found: " + condition.getColumnName());
            }
            whereColType = schema.get(condition.getColumnName());
        }

        int blockId = table.getFirstBlock();
        while (blockId != -1) {
            final int[] nextBlockId = {-1};
            Block block = blocksStorage.getBlock(blockId, bytes -> {
                ByteBuffer headerBuffer = ByteBuffer.wrap(bytes);
                headerBuffer.getInt(Block.OFFSET_SIZE); // read current block size
                nextBlockId[0] = headerBuffer.getInt(Block.OFFSET_NEXT_BLOCK);
                // checksum at OFFSET_CHECKSUM is ignored here
            });
            
            if (block == null) break;
            
            ByteBuffer buffer = ByteBuffer.wrap(block.bytes);
            int blockSize = buffer.getInt(Block.OFFSET_SIZE); // Read total size of data in block

            int currentPos = Block.HEADER_TOTAL_SIZE; // Start after the full header (size + next + checksum)
            while (currentPos < Block.HEADER_TOTAL_SIZE + blockSize) {
                buffer.position(currentPos);
                List<Object> record = parseSingleRecord(table, buffer, schema);
                
                if (condition == null || condition.evaluate(record, whereColIndex, whereColType)) {
                    results.add(record);
                }
                
                currentPos = buffer.position();
            }
            blockId = nextBlockId[0];
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void handleIndexSearch(Table table, Index index, String valueStr) throws IOException {
        BTreeDisk tree = index.getBTree();
        BKey searchKey;
        switch (index.getType()) {
            case INT -> searchKey = new BKey<>(Integer.parseInt(valueStr));
            case FLOAT -> searchKey = new BKey<>(Float.parseFloat(valueStr));
            case STRING -> searchKey = new BKey<>(valueStr);
            default -> throw new IllegalStateException();
        }

        BKey<?> found = tree.search(searchKey);
        if (found == null) return;

        BlocksStorage storage = BlocksStorage.getInstance();
        LinkedHashMap<String, DataType> schema = table.getColumn();

        for (long pointer : found.getRecordPointers()) {
            int blockId = (int) (pointer >> 32);
            int offset = (int) (pointer & 0xFFFFFFFFL);

            Block block = storage.getBlock(blockId, null);
            if (block == null) continue;

            ByteBuffer buf = ByteBuffer.wrap(block.bytes);
            buf.position(offset);
            
            List<Object> record = parseSingleRecord(table, buf, schema);
            results.add(record);
        }
    }

    private List<Object> parseSingleRecord(Table table, ByteBuffer buffer, Map<String, DataType> schema) {
        List<Object> record = new ArrayList<>();
        for (Map.Entry<String, DataType> entry : schema.entrySet()) {
            DataType type = entry.getValue();
            switch (type) {
                case INT -> record.add(buffer.getInt());
                case FLOAT -> record.add(buffer.getFloat());
                case STRING -> {
                    int size = table.getColumnSizes().get(entry.getKey());
                    byte[] bytes = new byte[size];
                    buffer.get(bytes);
                    record.add(new String(bytes).trim());
                }
            }
        }
        return record;
    }

    // Condition logic moved to Condition.java

    public List<List<Object>> getResults() {
        return results;
    }

    public void printResults(Table table) {
        LinkedHashMap<String, DataType> schema = table.getColumn();
        List<String> schemaKeys = new ArrayList<>(schema.keySet());

        for (List<Object> record : results) {
            StringBuilder sb = new StringBuilder();
            if (selectAll) {
                for (int i = 0; i < record.size(); i++) {
                    sb.append(schemaKeys.get(i)).append(": ").append(record.get(i)).append(", ");
                }
            } else {
                for (String requestedCol : columns) {
                    int index = schemaKeys.indexOf(requestedCol);
                    if (index != -1) {
                        sb.append(requestedCol).append(": ").append(record.get(index)).append(", ");
                    }
                }
            }
            if (!sb.isEmpty()) {
                sb.setLength(sb.length() - 2);
            }
            System.out.println(sb);
        }
    }
}

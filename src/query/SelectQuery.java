package query;

import storage.Block;
import storage.BlocksStorage;
import table.DataType;
import table.Table;

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
        String trimmedQuery = query.trim();
        if (!trimmedQuery.toUpperCase().startsWith("SELECT")) {
            throw new IllegalArgumentException("Invalid query syntax: must start with SELECT");
        }

        int fromIndex = trimmedQuery.toUpperCase().indexOf("FROM");
        if (fromIndex == -1) {
            throw new IllegalArgumentException("Invalid query syntax: missing FROM clause");
        }

        String selectPart = trimmedQuery.substring(6, fromIndex).trim();
        if (selectPart.equals("*")) {
            selectAll = true;
        } else {
            String[] colParts = selectPart.split(",");
            for (String col : colParts) {
                columns.add(col.trim());
            }
        }

        String restPart = trimmedQuery.substring(fromIndex + 4).trim();
        int whereIndex = restPart.toUpperCase().indexOf("WHERE");
        
        String fromPart;
        if (whereIndex != -1) {
            fromPart = restPart.substring(0, whereIndex).trim();
            String whereClause = restPart.substring(whereIndex + 5).trim();
            this.condition = new Condition();
            this.condition.parse(whereClause);
        } else {
            fromPart = restPart.trim();
        }

        String[] fromParts = fromPart.split("\\s+");
        if (fromParts.length == 0) {
            throw new IllegalArgumentException("Invalid query syntax: missing table name");
        }
        this.tableName = fromParts[0];
    }

    // Parsing logic moved to Condition.java

    @Override
    public void run(Table table) throws IOException {
        results.clear();
        //Full-scan
        LinkedHashMap<String, DataType> schema = table.getColumn();
        BlocksStorage blocksStorage = BlocksStorage.getInstance();

        List<String> schemaKeys = new ArrayList<>(schema.keySet());
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
                headerBuffer.getInt(); // skip current block size
                nextBlockId[0] = headerBuffer.getInt();
            });
            
            if (block == null) break;
            
            ByteBuffer buffer = ByteBuffer.wrap(block.bytes);
            int blockSize = buffer.getInt(); // Read total size of data in block
            // skip nextBlockId at index 4
            
            int currentPos = 8; // Start after the 8-byte header
            while (currentPos < 8 + blockSize) {
                buffer.position(currentPos);
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
                
                if (condition == null || condition.evaluate(record, whereColIndex, whereColType)) {
                    results.add(record);
                }
                
                currentPos = buffer.position();
            }
            blockId = nextBlockId[0];
        }
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

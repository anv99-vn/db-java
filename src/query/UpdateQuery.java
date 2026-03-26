package query;

import storage.Block;
import storage.BlocksStorage;
import table.DataType;
import table.Table;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class UpdateQuery implements Query {
    public String tableName;
    private final Map<String, String> setAssignments = new LinkedHashMap<>();
    
    // Condition parts
    private Condition condition;

    @Override
    public void parse(String query) {
        SqlTokenizer tokenizer = new SqlTokenizer(query);
        List<String> tokens = tokenizer.tokenize();

        if (tokens.isEmpty() || !tokens.get(0).equalsIgnoreCase("UPDATE")) {
            throw new IllegalArgumentException("Invalid query syntax: must start with UPDATE");
        }

        this.tableName = tokens.get(1);

        int i = 2;
        if (i >= tokens.size() || !tokens.get(i).equalsIgnoreCase("SET")) {
            throw new IllegalArgumentException("Invalid query syntax: missing SET clause");
        }
        i++;

        while (i < tokens.size() && !tokens.get(i).equalsIgnoreCase("WHERE")) {
            String colName = tokens.get(i);
            i++;
            if (!tokens.get(i).equals("=")) {
                throw new IllegalArgumentException("Invalid SET assignment: expected =");
            }
            i++;
            String value = tokens.get(i);
            setAssignments.put(colName, value);
            i++;
            if (i < tokens.size() && tokens.get(i).equals(",")) {
                i++;
            }
        }

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
        LinkedHashMap<String, DataType> schema = table.getColumn();
        BlocksStorage blocksStorage = BlocksStorage.getInstance();
        List<String> schemaKeys = new ArrayList<>(schema.keySet());
        
        int whereColIndex;
        DataType whereColType;
        if (condition != null) {
            whereColIndex = schemaKeys.indexOf(condition.getColumnName());
            if (whereColIndex == -1) {
                throw new IllegalArgumentException("Column not found: " + condition.getColumnName());
            }
            whereColType = schema.get(condition.getColumnName());
        } else {
            whereColIndex = -1;
            whereColType = null;
        }

        int blockId = table.getFirstBlock();
        while (blockId != -1) {
            int finalWhereColIndex = whereColIndex;
            DataType finalWhereColType = whereColType;
            final int[] nextBlockId = {-1};
            
            blocksStorage.updateBlock(blockId, bytes -> {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                int blockSize = buffer.getInt(Block.OFFSET_SIZE);
                nextBlockId[0] = buffer.getInt(Block.OFFSET_NEXT_BLOCK);
                
                if (blockSize == 0) return;
 
                int currentPos = Block.HEADER_TOTAL_SIZE;
                while (currentPos < Block.HEADER_TOTAL_SIZE + blockSize) {
                    buffer.position(currentPos);
                    List<Object> record = new ArrayList<>();
                    int recordStartByte = currentPos;
                    
                    for (Map.Entry<String, DataType> entry : schema.entrySet()) {
                        DataType type = entry.getValue();
                        switch (type) {
                            case INT -> record.add(buffer.getInt());
                            case FLOAT -> record.add(buffer.getFloat());
                            case STRING -> {
                                int size = table.getColumnSizes().get(entry.getKey());
                                byte[] strBytes = new byte[size];
                                buffer.get(strBytes);
                                record.add(new String(strBytes).trim());
                            }
                        }
                    }
                    int recordEndByte = buffer.position();
                    currentPos = recordEndByte;
 
                    if (condition == null || condition.evaluate(record, finalWhereColIndex, finalWhereColType)) {
                        // Apply updates to the record in-place in the bytes array
                        buffer.position(recordStartByte);
                        for (int i = 0; i < schemaKeys.size(); i++) {
                            String colName = schemaKeys.get(i);
                            DataType type = schema.get(colName);
                            String newValueStr = setAssignments.getOrDefault(colName, null);
                            Object value = record.get(i);
                            
                            if (newValueStr != null) {
                                switch (type) {
                                    case INT -> buffer.putInt(Integer.parseInt(newValueStr));
                                    case FLOAT -> buffer.putFloat(Float.parseFloat(newValueStr));
                                    case STRING -> {
                                        int size = table.getColumnSizes().get(colName);
                                        byte[] strBytes = new byte[size];
                                        byte[] src = newValueStr.getBytes();
                                        System.arraycopy(src, 0, strBytes, 0, Math.min(src.length, size));
                                        buffer.put(strBytes);
                                    }
                                }
                            } else {
                                // Write back original value at the correct position
                                switch (type) {
                                    case INT -> buffer.putInt((Integer) value);
                                    case FLOAT -> buffer.putFloat((Float) value);
                                    case STRING -> {
                                        int size = table.getColumnSizes().get(colName);
                                        byte[] strBytes = new byte[size];
                                        byte[] src = ((String) value).getBytes();
                                        System.arraycopy(src, 0, strBytes, 0, Math.min(src.length, size));
                                        buffer.put(strBytes);
                                    }
                                }
                            }
                        }
                    }
                }
            });
            blockId = nextBlockId[0];
        }
    }

    // Condition logic moved to Condition.java
}

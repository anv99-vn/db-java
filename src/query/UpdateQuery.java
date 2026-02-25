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
        String trimmedQuery = query.trim();
        if (!trimmedQuery.toUpperCase().startsWith("UPDATE")) {
            throw new IllegalArgumentException("Invalid query syntax: must start with UPDATE");
        }

        int setIndex = trimmedQuery.toUpperCase().indexOf("SET");
        if (setIndex == -1) {
            throw new IllegalArgumentException("Invalid query syntax: missing SET clause");
        }

        this.tableName = trimmedQuery.substring(6, setIndex).trim();

        int whereIndex = trimmedQuery.toUpperCase().indexOf("WHERE");
        String setPart;
        if (whereIndex != -1) {
            setPart = trimmedQuery.substring(setIndex + 3, whereIndex).trim();
            String whereClause = trimmedQuery.substring(whereIndex + 5).trim();
            this.condition = new Condition();
            this.condition.parse(whereClause);
        } else {
            setPart = trimmedQuery.substring(setIndex + 3).trim();
        }

        String[] assignments = setPart.split(",");
        for (String assignment : assignments) {
            String[] parts = assignment.split("=");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid SET assignment: " + assignment);
            }
            setAssignments.put(parts[0].trim(), Condition.removeQuotes(parts[1].trim()));
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

        for (int blockId : table.getListBlock()) {
            int finalWhereColIndex = whereColIndex;
            DataType finalWhereColType = whereColType;
            blocksStorage.updateBlock(blockId, bytes -> {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                int blockSize = buffer.getInt();
                if (blockSize == 0) return;

                int currentPos = 4;
                while (currentPos < 4 + blockSize) {
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
        }
    }

    // Condition logic moved to Condition.java
}

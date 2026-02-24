package query;

import storage.BlocksStorage;
import table.DataType;
import table.Table;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DeleteQuery implements Query {
    public String tableName;

    // Condition parts
    private Condition condition;

    @Override
    public void parse(String query) {
        String trimmedQuery = query.trim();
        if (!trimmedQuery.toUpperCase().startsWith("DELETE FROM")) {
            throw new IllegalArgumentException("Invalid query syntax: must start with DELETE FROM");
        }

        String rest = trimmedQuery.substring(11).trim();
        int whereIndex = rest.toUpperCase().indexOf("WHERE");

        if (whereIndex != -1) {
            this.tableName = rest.substring(0, whereIndex).trim();
            String whereClause = rest.substring(whereIndex + 5).trim();
            this.condition = new Condition();
            this.condition.parse(whereClause);
        } else {
            this.tableName = rest;
        }

        if (this.tableName.isEmpty()) {
            throw new IllegalArgumentException("Invalid query syntax: missing table name");
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
            whereColType = null;
            whereColIndex = -1;
        }

        List<Integer> blocksToRemove = new ArrayList<>();

        // Calculate fixed record size
        int recordSize = table.getColumnSizes().values().stream().mapToInt(size -> size).sum();

        int blockId = table.getFirstBlock();
        while (true) {
            if (blockId == -1) break;
            AtomicInteger nextBlockId = new AtomicInteger(-1);
            blocksStorage.updateBlock(blockId, bytes -> {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                int blockSize = buffer.getInt();
                nextBlockId.set(buffer.getInt());
                if (blockSize == 0) return;

                int currentPos = 8;
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
                                byte[] strBytes = new byte[size];
                                buffer.get(strBytes);
                                record.add(new String(strBytes).trim());
                            }
                        }
                    }

                    if (condition == null || condition.evaluate(record, whereColIndex, whereColType)) {
                        // lấy record cuối block thay thế vào record bị xoá
                        int lastRecordPos = 8 + blockSize - recordSize;

                        if (currentPos != lastRecordPos) {
                            // Copy last record to current position
                            System.arraycopy(bytes, lastRecordPos, bytes, currentPos, recordSize);
                        }
                        blockSize -= recordSize;
                        // Stay at same currentPos to evaluate the moved record
                    } else {
                        currentPos += recordSize;
                    }
                }
                buffer.putInt(0, blockSize);
            });
            if (nextBlockId.get() == -1) break;
            blockId = nextBlockId.get();
        }

    }
}

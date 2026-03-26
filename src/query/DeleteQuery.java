package query;

import storage.BlocksStorage;
import storage.Block;
import table.*;

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
        SqlTokenizer tokenizer = new SqlTokenizer(query);
        List<String> tokens = tokenizer.tokenize();

        if (tokens.size() < 3 || !tokens.get(0).equalsIgnoreCase("DELETE") || !tokens.get(1).equalsIgnoreCase("FROM")) {
            throw new IllegalArgumentException("Invalid query syntax: must start with DELETE FROM");
        }

        this.tableName = tokens.get(2);

        int i = 3;
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
            int finalBlockId = blockId;
            blocksStorage.updateBlock(blockId, bytes -> {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                int blockSize = buffer.getInt(Block.OFFSET_SIZE);
                nextBlockId.set(buffer.getInt(Block.OFFSET_NEXT_BLOCK));
                if (blockSize == 0) return;

                int currentPos = Block.HEADER_TOTAL_SIZE;
                while (currentPos < Block.HEADER_TOTAL_SIZE + blockSize) {
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
                        // Remove current record from index before deleting/moving
                        long deletedPointer = ((long) finalBlockId << 32) | (currentPos & 0xFFFFFFFFL);
                        removeFromIndexes(table, record, deletedPointer);

                        // lấy record cuối block thay thế vào record bị xoá
                        int lastRecordPos = Block.HEADER_TOTAL_SIZE + blockSize - recordSize;

                        if (currentPos != lastRecordPos) {
                            // Because we move the last record, we must update its pointers in all indexes
                            List<Object> lastRecord = parseRecord(table, bytes, lastRecordPos);
                            long oldLastPointer = ((long) finalBlockId << 32) | (lastRecordPos & 0xFFFFFFFFL);
                            long newLastPointer = ((long) finalBlockId << 32) | (currentPos & 0xFFFFFFFFL);

                            // Remove old pointer and add new pointer for each index
                            updateIndexesForMove(table, lastRecord, oldLastPointer, newLastPointer);

                            // Copy last record to current position
                            System.arraycopy(bytes, lastRecordPos, bytes, currentPos, recordSize);
                        }
                        blockSize -= recordSize;
                        // Stay at same currentPos to evaluate the moved record
                    } else {
                        currentPos += recordSize;
                    }
                }
                buffer.putInt(Block.OFFSET_SIZE, blockSize);
            });
            if (nextBlockId.get() == -1) break;
            blockId = nextBlockId.get();
        }

    }

    private List<Object> parseRecord(Table table, byte[] bytes, int pos) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.position(pos);
        List<Object> record = new ArrayList<>();
        LinkedHashMap<String, DataType> schema = table.getColumn();
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
        return record;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void removeFromIndexes(Table table, List<Object> record, long pointer) {
        Map<String, Index> indexes = table.getIndexes();
        for (Index idx : indexes.values()) {
            Object keyObj = collectKeyValues(table, idx, record);
            BTreeDisk tree = idx.getBTree();
            try {
                tree.delete(new BKey((Comparable) keyObj), pointer);
            } catch (IOException e) {
                System.err.println("Error removing from index: " + e.getMessage());
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void updateIndexesForMove(Table table, List<Object> record, long oldPointer, long newPointer) {
        Map<String, Index> indexes = table.getIndexes();
        for (Index idx : indexes.values()) {
            Object keyObj = collectKeyValues(table, idx, record);
            BTreeDisk tree = idx.getBTree();
            try {
                tree.delete(new BKey((Comparable) keyObj), oldPointer);
                tree.insert(new BKey((Comparable) keyObj), newPointer);
            } catch (IOException e) {
                System.err.println("Error updating index for move: " + e.getMessage());
            }
        }
    }

    private Object collectKeyValues(Table table, Index index, List<Object> record) {
        List<String> idxCols = index.getColumnNames();
        List<String> schemaKeys = new ArrayList<>(table.getColumn().keySet());
        Object[] vals = new Object[idxCols.size()];
        for (int i = 0; i < idxCols.size(); i++) {
            int pos = schemaKeys.indexOf(idxCols.get(i));
            vals[i] = record.get(pos);
        }
        if (vals.length == 1) return vals[0];
        return new CompositeKey(vals);
    }
}

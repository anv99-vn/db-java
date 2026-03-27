package query;

import b_tree.BKey;
import b_tree.BTreeDisk;
import storage.Block;
import storage.BlocksStorage;
import table.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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

            int finalBlockId = blockId;
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
                        // Apply updates to all necessary indexes first
                        long pointer = ((long) finalBlockId << 32) | (recordStartByte & 0xFFFFFFFFL);
                        try {
                            updateAllIndexesForUpdate(table, record, setAssignments, pointer);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        // Apply updates to the record in-place in the bytes array
                        buffer.position(recordStartByte);
                        for (int i = 0; i < schemaKeys.size(); i++) {
                            String colName = schemaKeys.get(i);
                            DataType type = schema.get(colName);
                            String newValueStr = setAssignments.getOrDefault(colName, null);

                            if (newValueStr != null) {
                                switch (type) {
                                    case INT -> buffer.putInt(Integer.parseInt(newValueStr));
                                    case FLOAT -> buffer.putFloat(Float.parseFloat(newValueStr));
                                    case STRING -> {
                                        int size = table.getColumnSizes().get(colName);
                                        byte[] strBytes = new byte[size];
                                        String trimmed = newValueStr;
                                        if (trimmed.startsWith("'") || trimmed.startsWith("\"")) {
                                            trimmed = trimmed.substring(1, trimmed.length() - 1);
                                        }
                                        byte[] src = trimmed.getBytes(StandardCharsets.UTF_8);
                                        System.arraycopy(src, 0, strBytes, 0, Math.min(src.length, size));
                                        buffer.put(strBytes);
                                    }
                                }
                            } else {
                                // Skip this column by moving position
                                if (type == DataType.STRING) {
                                    buffer.position(buffer.position() + table.getColumnSizes().get(colName));
                                } else {
                                    buffer.position(buffer.position() + 4); // INT and FLOAT are 4 bytes
                                }
                            }
                        }
                    } else {
                        currentPos = recordEndByte;
                    }
                }
            });
            blockId = nextBlockId[0];
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void updateAllIndexesForUpdate(Table table, List<Object> oldRecord, Map<String, String> assignments, long pointer) throws IOException {
        Map<String, Index> indexes = table.getIndexes();
        List<String> schemaKeys = new ArrayList<>(table.getColumn().keySet());
        LinkedHashMap<String, DataType> schema = table.getColumn();

        for (Index idx : indexes.values()) {
            List<String> idxCols = idx.getColumnNames();
            boolean affected = false;
            for (String col : idxCols) {
                if (assignments.containsKey(col)) {
                    affected = true;
                    break;
                }
            }
            if (!affected) continue;

            // Compute old key
            Object oldKey = collectKey(idxCols, schemaKeys, oldRecord);
            
            // Compute new key
            Object[] newVals = new Object[idxCols.size()];
            for (int i = 0; i < idxCols.size(); i++) {
                String col = idxCols.get(i);
                if (assignments.containsKey(col)) {
                    newVals[i] = parseValue(assignments.get(col), schema.get(col));
                } else {
                    newVals[i] = oldRecord.get(schemaKeys.indexOf(col));
                }
            }
            Object newKey = (newVals.length == 1) ? newVals[0] : new CompositeKey(newVals);

            BTreeDisk tree = idx.getBTree();
            try {
                tree.delete(new BKey((Comparable) oldKey), pointer);
                tree.insert(new BKey((Comparable) newKey), pointer);
            } catch (NumberFormatException e) {
                System.err.println("Error updating index for " + idx.getIndexName() + ": " + e.getMessage());
            }
        }
    }

    private Object collectKey(List<String> idxCols, List<String> schemaKeys, List<Object> record) {
        Object[] vals = new Object[idxCols.size()];
        for (int i = 0; i < idxCols.size(); i++) {
            vals[i] = record.get(schemaKeys.indexOf(idxCols.get(i)));
        }
        return (vals.length == 1) ? vals[0] : new CompositeKey(vals);
    }

    private Object parseValue(String valStr, DataType type) {
        return switch (type) {
            case INT -> Integer.parseInt(valStr);
            case FLOAT -> Float.parseFloat(valStr);
            case STRING -> Condition.removeQuotes(valStr);
            default -> valStr;
        };
    }

    // Condition logic moved to Condition.java
}

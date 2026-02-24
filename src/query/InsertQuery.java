package query;

import storage.Block;
import storage.BlocksStorage;
import table.BKey;
import table.DataType;
import table.Index;
import table.Table;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static storage.BlocksStorage.BLOCK_SIZE;

public class InsertQuery implements Query {
    public ArrayList<String> insertParams;
    public String tableName;

    @Override
    public void parse(String query) {
        String trimmedQuery = query.trim();
        // Basic validation for INSERT syntax
        if (!trimmedQuery.toUpperCase().startsWith("INSERT INTO")) {
            throw new IllegalArgumentException("Invalid query syntax: must start with INSERT INTO");
        }

        // Locate VALUES keyword
        int valuesIndex = trimmedQuery.toUpperCase().indexOf("VALUES");
        if (valuesIndex == -1) {
            throw new IllegalArgumentException("Invalid query syntax: missing VALUES clause");
        }

        // Extract table name
        String prefix = trimmedQuery.substring(0, valuesIndex).trim();
        String[] prefixParts = prefix.split("\\s+");
        if (prefixParts.length < 3) {
            throw new IllegalArgumentException("Invalid query syntax: missing table name");
        }
        this.tableName = prefixParts[2];

        // Extract values
        String suffix = trimmedQuery.substring(valuesIndex + 6).trim();
        if (!suffix.startsWith("(") || !suffix.endsWith(")")) {
            throw new IllegalArgumentException("Invalid query syntax: VALUES must be enclosed in parentheses");
        }

        String valuesContent = suffix.substring(1, suffix.length() - 1);
        this.insertParams = new ArrayList<>();

        // Split by comma and process values
        // Note: This simple split doesn't handle commas inside quotes
        String[] values = valuesContent.split(",");
        for (String val : values) {
            val = val.trim();
            // Remove surrounding quotes for string literals
            if ((val.startsWith("'") && val.endsWith("'")) || (val.startsWith("\"") && val.endsWith("\""))) {
                val = val.substring(1, val.length() - 1);
            }
            this.insertParams.add(val);
        }
    }

    @Override
    public void run(Table table) throws IOException {
        LinkedHashMap<String, DataType> column = table.getColumn();
        if (this.insertParams.size() != column.size()) {
            throw new IllegalArgumentException("Column count mismatch");
        }

        // Prepare record bytes
        int i = 0;
        ByteBuffer recordBuffer = ByteBuffer.allocate(BLOCK_SIZE);
        for (Map.Entry<String, DataType> entry : column.entrySet()) {
            DataType type = entry.getValue();
            String value = this.insertParams.get(i++);
            try {
                switch (type) {
                    case INT -> recordBuffer.putInt(Integer.parseInt(value));
                    case FLOAT -> recordBuffer.putFloat(Float.parseFloat(value));
                    case STRING -> {
                        int size = table.getColumnSizes().get(entry.getKey());
                        byte[] bytes = new byte[size];
                        byte[] src = value.getBytes();
                        System.arraycopy(src, 0, bytes, 0, Math.min(src.length, size));
                        recordBuffer.put(bytes);
                    }
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid type for column " + entry.getKey());
            }
        }
        recordBuffer.flip();
        byte[] recordData = new byte[recordBuffer.remaining()];
        recordBuffer.get(recordData);

        int lastBlockId = table.getLastBlock();
        BlocksStorage blocksStorage = BlocksStorage.getInstance();
        AtomicInteger insertedPos = new AtomicInteger(-1);

        blocksStorage.updateBlock(lastBlockId, bytes -> {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            int blockSize = buffer.getInt();
            buffer.getInt(); // skip nextBlockId
            if (blockSize + recordData.length + 8 <= BLOCK_SIZE) {
                int pos = 8 + blockSize;
                System.arraycopy(recordData, 0, bytes, pos, recordData.length);
                buffer.putInt(0, blockSize + recordData.length);
                insertedPos.set(pos);
            }
        });

        int finalBlockId;
        int finalOffset;

        if (insertedPos.get() != -1) {
            finalBlockId = lastBlockId;
            finalOffset = insertedPos.get();
        } else {
            ByteBuffer newBlockBuffer = ByteBuffer.allocate(BLOCK_SIZE);
            newBlockBuffer.putInt(recordData.length);
            newBlockBuffer.putInt(-1); // nextBlockId = -1
            newBlockBuffer.put(recordData);
            
            finalBlockId = blocksStorage.allocateAndWrite(new Block(newBlockBuffer.array()));
            
            // Link old last block to new block
            if (lastBlockId != -1) {
                blocksStorage.updateBlock(lastBlockId, bytes -> {
                    ByteBuffer buffer = ByteBuffer.wrap(bytes);
                    buffer.putInt(4, finalBlockId); // index 4 is nextBlockId
                });
            }
            
            table.setLastBlock(finalBlockId);
            finalOffset = 8; // Header is now 8 bytes (4 size + 4 nextBlockId)
        }

        // Update indexes
        if (!table.getIndexes().isEmpty()) {
            updateIndexes(table, finalBlockId, finalOffset);
        }
    }

    private void updateIndexes(Table table, int blockId, int offset) {
        long pointer = ((long) blockId << 32) | (long) offset;
        Map<String, DataType> schema = table.getColumn();
        ArrayList<String> schemaKeys = new ArrayList<>(schema.keySet());

        for (Index index : table.getIndexes().values()) {
            String colName = index.getColumnName();
            int colIndex = schemaKeys.indexOf(colName);
            String valueStr = this.insertParams.get(colIndex);
            DataType type = schema.get(colName);

            insertIntoIndex(index, valueStr, pointer, type);
        }
    }

    private void insertIntoIndex(Index index, String valueStr, long pointer, DataType type) {
        switch (type) {
            case INT ->
                    index.<Integer>getBTree().insert(new BKey<>(Integer.valueOf(valueStr)), pointer);
            case FLOAT ->
                    index.<Float>getBTree().insert(new BKey<>(Float.valueOf(valueStr)), pointer);
            case STRING -> index.<String>getBTree().insert(new BKey<>(valueStr), pointer);
            default -> throw new IllegalArgumentException("Unsupported type");
        }
    }
}

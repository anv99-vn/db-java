package query;

import storage.Block;
import storage.BlocksStorage;
import table.DataType;
import table.Table;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

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
        java.util.concurrent.atomic.AtomicBoolean inserted = new java.util.concurrent.atomic.AtomicBoolean(false);

        blocksStorage.updateBlock(lastBlockId, bytes -> {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            int blockSize = buffer.getInt();
            if (blockSize + recordData.length + 4 <= BLOCK_SIZE) {
                System.arraycopy(recordData, 0, bytes, 4 + blockSize, recordData.length);
                buffer.putInt(0, blockSize + recordData.length);
                inserted.set(true);
            }
        });

        if (!inserted.get()) {
            ByteBuffer newBlockBuffer = ByteBuffer.allocate(BLOCK_SIZE);
            newBlockBuffer.putInt(recordData.length);
            newBlockBuffer.put(recordData);
            int blockID = blocksStorage.allocateAndWrite(new Block(newBlockBuffer.array()));
            table.getListBlock().add(blockID);
        }

    }

}

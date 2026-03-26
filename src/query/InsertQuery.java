package query;

import storage.Block;
import storage.BlocksStorage;
import table.DataType;
import table.Index;
import table.Table;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static storage.Block.HEADER_TOTAL_SIZE;
import static storage.BlocksStorage.BLOCK_SIZE;

public class InsertQuery implements Query {
    public ArrayList<String> insertParams;
    public String tableName;

    @Override
    public void parse(String query) {
        SqlTokenizer tokenizer = new SqlTokenizer(query);
        List<String> tokens = tokenizer.tokenize();
        
        if (tokens.size() < 4 || !tokens.get(0).equalsIgnoreCase("INSERT") || !tokens.get(1).equalsIgnoreCase("INTO")) {
            throw new IllegalArgumentException("Invalid query syntax: must start with INSERT INTO");
        }

        this.tableName = tokens.get(2);
        
        int valuesIndex = -1;
        for (int i = 3; i < tokens.size(); i++) {
            if (tokens.get(i).equalsIgnoreCase("VALUES")) {
                valuesIndex = i;
                break;
            }
        }

        if (valuesIndex == -1) {
            throw new IllegalArgumentException("Invalid query syntax: missing VALUES clause");
        }

        if (!tokens.get(valuesIndex + 1).equals("(")) {
            throw new IllegalArgumentException("Invalid query syntax: VALUES must be followed by (");
        }

        this.insertParams = new ArrayList<>();
        int i = valuesIndex + 2;
        while (i < tokens.size() && !tokens.get(i).equals(")")) {
            String val = tokens.get(i);
            if (!val.equals(",")) {
                this.insertParams.add(val);
            }
            i++;
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

        if (lastBlockId != -1) {
            blocksStorage.updateBlock(lastBlockId, bytes -> {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                int blockSize = buffer.getInt(Block.OFFSET_SIZE);
                buffer.getInt(Block.OFFSET_NEXT_BLOCK); // read nextBlockId
                if (blockSize + recordData.length + HEADER_TOTAL_SIZE <= BLOCK_SIZE) {
                    int pos = HEADER_TOTAL_SIZE + blockSize;
                    System.arraycopy(recordData, 0, bytes, pos, recordData.length);
                    buffer.putInt(Block.OFFSET_SIZE, blockSize + recordData.length);
                    insertedPos.set(pos);
                }
            });
        }

        int finalBlockId;
        int finalOffset;

        if (insertedPos.get() != -1) {
            finalBlockId = lastBlockId;
            finalOffset = insertedPos.get();
        } else {
            ByteBuffer newBlockBuffer = ByteBuffer.allocate(BLOCK_SIZE);
            newBlockBuffer.putInt(Block.OFFSET_SIZE, recordData.length);
            newBlockBuffer.putInt(Block.OFFSET_NEXT_BLOCK, -1); // nextBlockId = -1
            // Ensure payload starts at HEADER_TOTAL_SIZE (accounts for checksum)
            newBlockBuffer.position(Block.HEADER_TOTAL_SIZE);
            newBlockBuffer.put(recordData);
            
            finalBlockId = blocksStorage.allocateAndWrite(new Block(newBlockBuffer.array()));
            
            // Link old last block to new block
            if (lastBlockId != -1) {
                blocksStorage.updateBlock(lastBlockId, bytes -> {
                    ByteBuffer buffer = ByteBuffer.wrap(bytes);
                    buffer.putInt(Block.OFFSET_NEXT_BLOCK, finalBlockId); // index 4 is nextBlockId
                });
            }
            
            table.setLastBlock(finalBlockId);
            finalOffset = HEADER_TOTAL_SIZE; // Header includes checksum (size + nextBlockId + checksum)
        }

        // Update indexes
        if (!table.getIndexes().isEmpty()) {
            updateIndexes(table, finalBlockId, finalOffset);
        }
    }

    private void updateIndexes(Table table, int blockId, int offset) {

    }

    private void insertIntoIndex(Index index, String valueStr, long pointer, DataType type) {

    }
}

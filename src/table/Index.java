package table;

import storage.BlocksStorage;
import java.io.IOException;

public class Index {
    private final String columnName;
    private final int metadataBlockId;
    private final BTreeDisk<?> bTree;
    private final DataType type;

    public Index(String columnName, int metadataBlockId, DataType type) throws IOException {
        this.columnName = columnName;
        this.metadataBlockId = metadataBlockId;
        this.type = type;
        
        switch (type) {
            case INT -> this.bTree = new BTreeDisk<Integer>(BlocksStorage.getInstance(), metadataBlockId, 3);
            case FLOAT -> this.bTree = new BTreeDisk<Float>(BlocksStorage.getInstance(), metadataBlockId, 3);
            case STRING -> this.bTree = new BTreeDisk<String>(BlocksStorage.getInstance(), metadataBlockId, 3);
            default -> throw new IllegalArgumentException("Unsupported type for index: " + type);
        }
    }

    public String getColumnName() {
        return columnName;
    }

    public int getMetadataBlockId() {
        return metadataBlockId;
    }

    public BTreeDisk<?> getBTree() {
        return bTree;
    }

    public DataType getType() {
        return type;
    }
}

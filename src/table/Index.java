package table;

import storage.BlocksStorage;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class Index {
    private final List<String> columnNames;
    private final int metadataBlockId;
    private final BTreeDisk<?> bTree;
    private final DataType type;

    public Index(String col, int metadataBlockId, DataType type) throws IOException {
        this(List.of(col), metadataBlockId, type);
    }

    public Index(List<String> columns, int metadataBlockId, DataType type) throws IOException {
        this.columnNames = new ArrayList<>(columns);
        this.metadataBlockId = metadataBlockId;
        this.type = type;
        
        if (columnNames.size() > 1 || type == DataType.COMPOSITE) {
            this.bTree = new BTreeDisk<CompositeKey>(BlocksStorage.getInstance(), metadataBlockId, 3);
        } else {
            switch (type) {
                case INT -> this.bTree = new BTreeDisk<Integer>(BlocksStorage.getInstance(), metadataBlockId, 3);
                case FLOAT -> this.bTree = new BTreeDisk<Float>(BlocksStorage.getInstance(), metadataBlockId, 3);
                case STRING -> this.bTree = new BTreeDisk<String>(BlocksStorage.getInstance(), metadataBlockId, 3);
                default -> throw new IllegalArgumentException("Unsupported type for index: " + type);
            }
        }
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public String getColumnName() {
        return columnNames.get(0);
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

    public String getIndexName() {
        return "idx_" + String.join("_", columnNames);
    }
}

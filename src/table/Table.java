package table;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * column(name, type)
 * primary key
 * arraylist<meta_data_record(block_id,offset)> records
 */
public class Table {
    LinkedHashMap<String, DataType> column;
    LinkedHashMap<String, Integer> columnSizes;
    PrimaryKey primaryKey;
    Map<String, Index> indexes = new LinkedHashMap<>();
    private int lastBlock = -1;
    private int firstBlock = -1;
    private String name;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Map<String, Index> getIndexes() {
        return indexes;
    }


    public LinkedHashMap<String, DataType> getColumn() {
        return column;
    }

    public LinkedHashMap<String, Integer> getColumnSizes() {
        return columnSizes;
    }

    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }


    public int getLastBlock() {
        return lastBlock;
    }

    public void addColumn(String name, DataType type, int size) {
        if (column == null) {
            column = new LinkedHashMap<>();
            columnSizes = new LinkedHashMap<>();
        }
        column.put(name, type);
        columnSizes.put(name, size);
    }

    public void setPrimaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
    }

    public void init() {

    }

    public void setLastBlock(int id) {
        lastBlock = id;
        if (firstBlock == -1) {
            setFirstBlock(id);
        }
    }

    public void addIndex(String colName, int metadataBlockId) throws IOException {
        DataType type = column.get(colName);
        if (type == null) throw new IllegalArgumentException("Column not found: " + colName);
        Index idx = new Index(colName, metadataBlockId, type);
        indexes.put(colName, idx);
    }

    public void setFirstBlock(int id) {
        firstBlock = id;
    }

    public int getFirstBlock() {
        return firstBlock;
    }
}

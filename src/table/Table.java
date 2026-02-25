package table;

import query.InsertQuery;

import java.nio.ByteBuffer;
import java.util.ArrayList;
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
    ArrayList<Integer> listBlock;

    public LinkedHashMap<String, DataType> getColumn() {
        return column;
    }

    public LinkedHashMap<String, Integer> getColumnSizes() {
        return columnSizes;
    }

    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public ArrayList<Integer> getListBlock() {
        return listBlock;
    }

    public int getLastBlock() {
        return listBlock.get(listBlock.size() - 1);
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
        if (listBlock == null) {
            listBlock = new ArrayList<>();
        }
    }
}

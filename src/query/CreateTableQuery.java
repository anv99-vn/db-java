package query;

import storage.Block;
import table.DataType;
import table.PrimaryKey;
import table.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public class CreateTableQuery implements Query {
    public String tableName;
    public LinkedHashMap<String, DataType> columns = new LinkedHashMap<>();
    public LinkedHashMap<String, Integer> columnSizes = new LinkedHashMap<>();
    public String primaryKeyColumn;

    @Override
    public void parse(String query) {
        String trimmedQuery = query.trim();
        if (!trimmedQuery.toUpperCase().startsWith("CREATE TABLE")) {
            throw new IllegalArgumentException("Invalid query syntax: must start with CREATE TABLE");
        }

        // Extract table name
        int openParenIndex = trimmedQuery.indexOf("(");
        if (openParenIndex == -1) {
            throw new IllegalArgumentException("Invalid query syntax: missing column definitions");
        }

        String prefix = trimmedQuery.substring(0, openParenIndex).trim();
        String[] parts = prefix.split("\\s+");
        if (parts.length < 3) {
            throw new IllegalArgumentException("Invalid query syntax: missing table name");
        }
        this.tableName = parts[2];

        // Extract columns definition
        String innerContent = trimmedQuery.substring(openParenIndex + 1, trimmedQuery.lastIndexOf(")")).trim();
        String[] definitions = innerContent.split(",");

        for (String def : definitions) {
            def = def.trim();
            if (def.toUpperCase().startsWith("PRIMARY KEY")) {
                int keyStart = def.indexOf("(") + 1;
                int keyEnd = def.indexOf(")");
                this.primaryKeyColumn = def.substring(keyStart, keyEnd).trim();
            } else {
                String[] colParts = def.split("\\s+");
                if (colParts.length < 2) {
                    throw new IllegalArgumentException("Invalid column definition: " + def);
                }
                String colName = colParts[0];
                String typePart = colParts[1].toUpperCase();
                
                DataType type;
                int size = 4; // Default for INT/FLOAT

                if (typePart.startsWith("STRING")) {
                    type = DataType.STRING;
                    if (typePart.contains("(") && typePart.contains(")")) {
                        String sizeStr = typePart.substring(typePart.indexOf("(") + 1, typePart.indexOf(")"));
                        size = Integer.parseInt(sizeStr);
                    } else {
                        size = 30; // Default size
                    }
                } else {
                    try {
                        type = DataType.valueOf(typePart);
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("Unknown data type: " + typePart);
                    }
                }
                columns.put(colName, type);
                columnSizes.put(colName, size);
            }
        }

        if (primaryKeyColumn == null) {
            // Optional: throw error if PK is required
        }
    }

    @Override
    public void run(Table table) throws IOException {
        table.init(); // Initialize lists if needed

        for (Map.Entry<String, DataType> entry : columns.entrySet()) {
            String colName = entry.getKey();
            table.addColumn(colName, entry.getValue(), columnSizes.get(colName));
        }

        if (primaryKeyColumn != null) {
            DataType pkType = columns.get(primaryKeyColumn);
            if (pkType == null) {
                throw new IllegalArgumentException("Primary key column " + primaryKeyColumn + " not defined");
            }
            table.setPrimaryKey(new PrimaryKey(primaryKeyColumn, pkType));
        }

        // Allocate initial block
        Block initialBlock = new Block();
        int blockId = storage.BlocksStorage.getInstance().allocateAndWrite(initialBlock);
        table.setLastBlock(blockId);
    }
}

package query;

import storage.Block;
import table.DataType;
import table.PrimaryKey;
import table.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CreateTableQuery implements Query {
    public String tableName;
    public LinkedHashMap<String, DataType> columns = new LinkedHashMap<>();
    public LinkedHashMap<String, Integer> columnSizes = new LinkedHashMap<>();
    public String primaryKeyColumn;

    @Override
    public void parse(String query) {
        SqlTokenizer tokenizer = new SqlTokenizer(query);
        List<String> tokens = tokenizer.tokenize();

        if (tokens.size() < 3 || !tokens.get(0).equalsIgnoreCase("CREATE") || !tokens.get(1).equalsIgnoreCase("TABLE")) {
            throw new IllegalArgumentException("Invalid query syntax: must start with CREATE TABLE");
        }

        this.tableName = tokens.get(2);

        int i = 3;
        if (!tokens.get(i).equals("(")) {
            throw new IllegalArgumentException("Invalid query syntax: missing (");
        }
        i++;

        while (i < tokens.size() && !tokens.get(i).equals(")")) {
            String token = tokens.get(i);
            
            if (token.equalsIgnoreCase("PRIMARY")) {
                i++; // Skip PRIMARY
                if (tokens.get(i).equalsIgnoreCase("KEY")) {
                    i++; // Skip KEY
                    if (tokens.get(i).equals("(")) {
                        i++; // Skip (
                        this.primaryKeyColumn = tokens.get(i);
                        i++; // Skip column name
                        if (!tokens.get(i).equals(")")) {
                            throw new IllegalArgumentException("Invalid PRIMARY KEY syntax");
                        }
                        i++; // Skip )
                    }
                }
            } else {
                String colName = token;
                i++;
                String typePart = tokens.get(i).toUpperCase();
                i++;
                
                DataType type;
                int size = 4;

                if (typePart.equals("STRING")) {
                    type = DataType.STRING;
                    if (i < tokens.size() && tokens.get(i).equals("(")) {
                        i++; // Skip (
                        size = Integer.parseInt(tokens.get(i));
                        i++; // Skip size
                        if (!tokens.get(i).equals(")")) {
                            throw new IllegalArgumentException("Invalid STRING size syntax");
                        }
                        i++; // Skip )
                    } else {
                        size = 30;
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
            
            if (i < tokens.size() && tokens.get(i).equals(",")) {
                i++;
            }
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

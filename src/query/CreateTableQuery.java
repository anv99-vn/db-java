package query;

import storage.Block;
import table.SchemaManager;
import table.Table;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CreateTableQuery implements DatabaseQuery {
    public String tableName;
    public LinkedHashMap<String, table.DataType> columns = new LinkedHashMap<>();
    public LinkedHashMap<String, Integer> columnSizes = new LinkedHashMap<>();
    public String primaryKeyColumn;

    @Override
    public void parse(String query) {
        // ... (Parsing logic remains the same)
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

                table.DataType type;
                int size = 4;

                if (typePart.equals("STRING")) {
                    type = table.DataType.STRING;
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
                        type = table.DataType.valueOf(typePart);
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
    public void run(SchemaManager schemaManager) throws IOException {
        List<Table> tables = schemaManager.loadSchemas();
        Optional<Table> option = tables.stream().filter(p -> p.getName().equals(tableName)).findAny();
        if (option.isPresent()) {
            throw new IllegalArgumentException("Table '" + tableName + "' already exists");
        }

        table.Table table = new table.Table();
        table.setName(tableName);

        for (Map.Entry<String, table.DataType> entry : columns.entrySet()) {
            String colName = entry.getKey();
            table.addColumn(colName, entry.getValue(), columnSizes.get(colName));
        }

        if (primaryKeyColumn != null) {
            table.DataType pkType = columns.get(primaryKeyColumn);
            table.setPrimaryKey(new table.PrimaryKey(primaryKeyColumn, pkType));
            int pkIndexMetaBlockId = storage.BlocksStorage.getInstance().allocateAndWrite(new Block());
            table.addIndex(primaryKeyColumn, pkIndexMetaBlockId);
        }

        Block initialBlock = new Block();
        int blockId = storage.BlocksStorage.getInstance().allocateAndWrite(initialBlock);
        table.setLastBlock(blockId);
        table.setFirstBlock(blockId);

        tables.add(table);
        schemaManager.saveSchemas(tables);
    }
}

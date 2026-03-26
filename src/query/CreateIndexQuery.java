package query;

import storage.Block;
import storage.BlocksStorage;
import table.Table;

import java.io.IOException;
import java.util.List;

/**
 * Parses and runs:  CREATE INDEX ON tableName (columnName)
 *
 * Creates a disk-backed B-Tree index for the given column.
 * The index metadata block is allocated from BlocksStorage and registered
 * on the table object, so it is persisted by SchemaManager.
 */
public class CreateIndexQuery implements Query {

    public String tableName;
    public String columnName;

    @Override
    public void parse(String query) {
        SqlTokenizer tokenizer = new SqlTokenizer(query);
        List<String> tokens = tokenizer.tokenize();

        // Syntax: CREATE INDEX ON <table> ( <column> )
        if (tokens.size() < 6
                || !tokens.get(0).equalsIgnoreCase("CREATE")
                || !tokens.get(1).equalsIgnoreCase("INDEX")
                || !tokens.get(2).equalsIgnoreCase("ON")) {
            throw new IllegalArgumentException(
                    "Invalid syntax. Expected: CREATE INDEX ON <table> (<column>)");
        }

        this.tableName = tokens.get(3);

        if (!tokens.get(4).equals("(")) {
            throw new IllegalArgumentException("Expected '(' after table name");
        }
        this.columnName = tokens.get(5);

        if (tokens.size() < 7 || !tokens.get(6).equals(")")) {
            throw new IllegalArgumentException("Expected ')' after column name");
        }
    }

    @Override
    public void run(Table table) throws IOException {
        if (table.getColumn() == null || !table.getColumn().containsKey(columnName)) {
            throw new IllegalArgumentException("Column '" + columnName + "' not found in table");
        }

        if (table.getIndexes().containsKey(columnName)) {
            throw new IllegalArgumentException("Index already exists on column '" + columnName + "'");
        }

        // Allocate a fresh block for B-Tree metadata
        int metaBlockId = BlocksStorage.getInstance().allocateAndWrite(new Block());
        table.addIndex(columnName, metaBlockId);

        System.out.println("[Index] Created index on " + table.getName() + "." + columnName
                + " (metaBlock=" + metaBlockId + ")");
    }
}

package query;

import table.SchemaManager;
import table.Table;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Parses and runs: DROP TABLE <table>
 */
public class DropTableQuery implements DatabaseQuery {
    public String tableName;

    @Override
    public void parse(String query) {
        SqlTokenizer tokenizer = new SqlTokenizer(query);
        List<String> tokens = tokenizer.tokenize();

        if (tokens.size() < 3 || !tokens.get(0).equalsIgnoreCase("DROP") || !tokens.get(1).equalsIgnoreCase("TABLE")) {
            throw new IllegalArgumentException("Invalid query syntax: must start with DROP TABLE");
        }

        this.tableName = tokens.get(2);
    }

    @Override
    public void run(SchemaManager schemaManager) throws IOException {
        List<Table> tables = schemaManager.loadSchemas();
        Optional<Table> table = tables.stream().filter(p -> p.getName().equals(tableName)).findAny();
        if (table.orElse(null) == null) {
            throw new IllegalArgumentException("Table '" + tableName + "' does not exist");
        }

        tables.remove(table.get());
        schemaManager.saveSchemas(tables);
        System.out.println("Table dropped: " + tableName);
    }
}

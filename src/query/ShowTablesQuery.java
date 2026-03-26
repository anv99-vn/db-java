package query;

import table.SchemaManager;
import table.Table;

import java.io.IOException;
import java.util.List;

/**
 * Parses and runs: SHOW TABLES
 */
public class ShowTablesQuery implements DatabaseQuery {
    @Override
    public void parse(String query) {
        if (!query.trim().equalsIgnoreCase("SHOW TABLES")) {
            throw new IllegalArgumentException("Invalid syntax. Expected: SHOW TABLES");
        }
    }

    @Override
    public void run(SchemaManager schemaManager) throws IOException {
        List<Table> tables = schemaManager.loadSchemas();
        if (tables.isEmpty()) {
            System.out.println("No tables found.");
        } else {
            System.out.println("Tables:");
            tables.stream().map(Table::getName).map(name -> " - " + name).forEach(System.out::println);
        }
    }
}

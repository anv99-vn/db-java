import query.*;
import storage.BlocksStorage;
import table.SchemaManager;
import table.Table;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        System.out.println("--- DB-Java Console ---");
        System.out.println("Type your query (e.g. SHOW TABLES, SELECT, INSERT, etc.) or 'EXIT' to quit.\n");

        BlocksStorage storage;
        storage = BlocksStorage.getInstance();

        SchemaManager schemaManager = new SchemaManager(storage);
        Map<String, Table> tablesMap = new HashMap<>();

        // 1. Initial Load of Tables
        try {
            List<Table> loadedTables = schemaManager.loadSchemas();
            for (Table t : loadedTables) {
                tablesMap.put(t.getName(), t);
            }
        } catch (IOException e) {
            System.err.println("Initial Load error: " + e.getMessage());
        }

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("db> ");
            if (!scanner.hasNextLine()) break;
            
            String line = scanner.nextLine().trim();
            if (line.isBlank()) continue;
            
            if (line.equalsIgnoreCase("EXIT") || line.equalsIgnoreCase("QUIT")) {
                System.out.println("Goodbye!");
                break;
            }

            try {
                handleQuery(line, tablesMap, schemaManager);
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                // For deep errors, you can use: e.printStackTrace();
            }
        }
    }

    private static void handleQuery(String query, Map<String, Table> tablesMap, SchemaManager schemaManager) throws Exception {
        String upperQuery = query.toUpperCase();
        
        if (upperQuery.startsWith("SHOW TABLES")) {
            DatabaseQuery showTables = new ShowTablesQuery();
            showTables.parse(query);
            showTables.run(schemaManager);
            return;
        }

        if (upperQuery.startsWith("CREATE TABLE")) {
            DatabaseQuery ctq = new CreateTableQuery();
            ctq.parse(query);
            ctq.run(schemaManager);
            System.out.println("Operation successful.");
            return;
        }

        if (upperQuery.startsWith("DROP TABLE")) {
            DatabaseQuery dtq = new DropTableQuery();
            dtq.parse(query);
            dtq.run(schemaManager);
            System.out.println("Operation successful.");
            return;
        }

        if (upperQuery.startsWith("CREATE INDEX")) {
            CreateIndexQuery ciq = new CreateIndexQuery();
            ciq.parse(query);
            ciq.run(schemaManager);
            return;
        }

        // Logic to extract table names for standard queries
        String tableName = extractTableName(query);
        Table table = tablesMap.get(tableName);
        if (table == null) throw new IllegalArgumentException("No such table: " + tableName);

        if (upperQuery.startsWith("INSERT")) {
            InsertQuery insert = new InsertQuery();
            insert.parse(query);
            insert.run(table);
            System.out.println("Inserted 1 row.");
        } else if (upperQuery.startsWith("SELECT")) {
            SelectQuery select = new SelectQuery();
            select.parse(query);
            select.run(table);
            select.printResults(table);
            System.out.println("Results count: " + select.getResults().size());
        } else if (upperQuery.startsWith("UPDATE")) {
            UpdateQuery update = new UpdateQuery();
            update.parse(query);
            update.run(table);
            System.out.println("Updated rows successfully.");
        } else if (upperQuery.startsWith("DELETE")) {
            DeleteQuery delete = new DeleteQuery();
            delete.parse(query);
            delete.run(table);
            System.out.println("Deleted rows successfully.");
        } else {
            System.err.println("Unrecognized query type: " + query);
        }
    }

    private static String extractTableName(String query) {
        SqlTokenizer tokenizer = new SqlTokenizer(query);
        List<String> tokens = tokenizer.tokenize();
        if (tokens.isEmpty()) return null;
        
        String head = tokens.get(0).toUpperCase();
        switch (head) {
            case "INSERT": { // INSERT INTO <table> ...
                if (tokens.size() > 2 && tokens.get(1).equalsIgnoreCase("INTO")) return tokens.get(2);
                break;
            }
            case "SELECT": { // SELECT ... FROM <table> ...
                for (int i = 0; i < tokens.size(); i++) {
                    if (tokens.get(i).equalsIgnoreCase("FROM") && i + 1 < tokens.size()) {
                        return tokens.get(i + 1);
                    }
                }
                break;
            }
            case "UPDATE": { // UPDATE <table> SET ...
                if (tokens.size() > 1) return tokens.get(1);
                break;
            }
            case "DELETE": { // DELETE FROM <table> ...
                if (tokens.size() > 2 && tokens.get(1).equalsIgnoreCase("FROM")) return tokens.get(2);
                break;
            }
        }
        return null;
    }
}
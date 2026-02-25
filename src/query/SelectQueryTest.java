package query;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import storage.BlocksStorage;
import table.DataType;
import table.Table;

import java.io.File;
import java.io.IOException;
import java.util.List;

class SelectQueryTest {

    private Table setupTestTable() throws IOException {
        File dbFile = new File("data.bin");
        // On Windows, if file is open, delete might fail. 
        // We try to close it if possible, but BlocksStorage singleton doesn't make it easy to re-open.
        // For testing purposes, we assume we can at least clear the file content or handle misses.
        if (dbFile.exists()) {
            dbFile.delete();
        }
        
        // Reset/Clear cache
        BlocksStorage.getInstance().clearCache();

        Table table = new Table();
        // CreateTableQuery.run will handle addColumn calls
        CreateTableQuery createTable = new CreateTableQuery();
        createTable.tableName = "users";
        createTable.parse("CREATE TABLE users (id INT, name STRING, score FLOAT)");
        createTable.run(table);

        insert(table, "1, 'Alice', 95.5");
        insert(table, "2, 'Bob', 88.0");
        insert(table, "3, 'Charlie', 72.0");
        insert(table, "4, 'David', 100.0");
        
        return table;
    }

    private void insert(Table table, String values) throws IOException {
        InsertQuery insert = new InsertQuery();
        insert.parse("INSERT INTO users VALUES (" + values + ")");
        insert.run(table);
    }

    @Test
    void parse_selectAll() {
        SelectQuery query = new SelectQuery();
        query.parse("SELECT * FROM users");
        Assertions.assertEquals("users", query.tableName);
        Assertions.assertTrue(query.selectAll);
    }

    @Test
    void parse_specificColumns() {
        SelectQuery query = new SelectQuery();
        query.parse("SELECT id, name FROM users");
        Assertions.assertEquals("users", query.tableName);
        Assertions.assertFalse(query.selectAll);
        Assertions.assertEquals(2, query.columns.size());
        Assertions.assertEquals("id", query.columns.get(0));
        Assertions.assertEquals("name", query.columns.get(1));
    }

    @Test
    void parse_withWhere() {
        SelectQuery query = new SelectQuery();
        query.parse("SELECT * FROM users WHERE score >= 90.0");
        Assertions.assertEquals("users", query.tableName);
        // Internal state check if needed, but we verify through run() mostly
    }

    @Test
    void run_selectAll() throws IOException {
        Table table = setupTestTable();
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM users");
        select.run(table);

        List<List<Object>> results = select.getResults();
        Assertions.assertEquals(4, results.size());
    }

    @Test
    void run_whereOperatorsInt() throws IOException {
        Table table = setupTestTable();
        
        // Test Equals
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM users WHERE id = 2");
        select.run(table);
        Assertions.assertEquals(1, select.getResults().size());
        Assertions.assertEquals(2, select.getResults().get(0).get(0));

        // Test Greater Than
        select = new SelectQuery();
        select.parse("SELECT * FROM users WHERE id > 2");
        select.run(table);
        Assertions.assertEquals(2, select.getResults().size()); // 3, 4

        // Test Less Than or Equal
        select = new SelectQuery();
        select.parse("SELECT * FROM users WHERE id <= 2");
        select.run(table);
        Assertions.assertEquals(2, select.getResults().size()); // 1, 2

        // Test Not Equal
        select = new SelectQuery();
        select.parse("SELECT * FROM users WHERE id != 1");
        select.run(table);
        Assertions.assertEquals(3, select.getResults().size()); // 2, 3, 4
    }

    @Test
    void run_whereOperatorsFloat() throws IOException {
        Table table = setupTestTable();
        
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM users WHERE score > 90.0");
        select.run(table);
        Assertions.assertEquals(2, select.getResults().size()); // Alice (95.5), David (100.0)
        
        select = new SelectQuery();
        select.parse("SELECT * FROM users WHERE score < 80");
        select.run(table);
        Assertions.assertEquals(1, select.getResults().size()); // Charlie (72.0)
    }

    @Test
    void run_whereOperatorsString() throws IOException {
        Table table = setupTestTable();
        
        // Equals
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM users WHERE name = 'Bob'");
        select.run(table);
        Assertions.assertEquals(1, select.getResults().size());
        Assertions.assertEquals("Bob", select.getResults().get(0).get(1));

        // Not Equals
        select = new SelectQuery();
        select.parse("SELECT * FROM users WHERE name != 'Alice'");
        select.run(table);
        Assertions.assertEquals(3, select.getResults().size());
    }

    @Test
    void run_betweenInt() throws IOException {
        Table table = setupTestTable();
        
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM users WHERE id BETWEEN 2 AND 3");
        select.run(table);
        Assertions.assertEquals(2, select.getResults().size()); // 2, 3
    }

    @Test
    void run_betweenString() throws IOException {
        Table table = setupTestTable();
        
        SelectQuery select = new SelectQuery();
        // Alice, Bob, Charlie, David
        select.parse("SELECT * FROM users WHERE name BETWEEN 'B' AND 'Czz'");
        select.run(table);
        Assertions.assertEquals(2, select.getResults().size()); // Bob, Charlie
    }

    @Test
    void run_noResults() throws IOException {
        Table table = setupTestTable();
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM users WHERE id > 100");
        select.run(table);
        Assertions.assertTrue(select.getResults().isEmpty());
    }

    @Test
    void run_edgeCaseBetweenReversed() throws IOException {
        Table table = setupTestTable();
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM users WHERE id BETWEEN 10 AND 1"); // Lower bound > Upper bound
        select.run(table);
        Assertions.assertTrue(select.getResults().isEmpty());
    }

    @Test
    void run_invalidColumnName() throws IOException {
        Table table = setupTestTable();
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM users WHERE non_existent = 10");
        Assertions.assertThrows(IllegalArgumentException.class, () -> select.run(table));
    }

    @Test
    void run_emptyTable() throws IOException {
        File dbFile = new File("data.bin");
        if (dbFile.exists()) dbFile.delete();
        BlocksStorage.getInstance().clearCache();

        Table table = new Table();
        CreateTableQuery createTable = new CreateTableQuery();
        createTable.parse("CREATE TABLE empty (id INT)");
        createTable.run(table);

        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM empty");
        select.run(table);
        Assertions.assertTrue(select.getResults().isEmpty());
    }
}

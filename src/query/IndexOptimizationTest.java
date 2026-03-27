package query;

import b_tree.BKey;
import b_tree.BTreeDisk;
import org.junit.jupiter.api.*;
import storage.BlocksStorage;
import table.*;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IndexOptimizationTest {

    @BeforeEach
    void setup() throws IOException {
        BlocksStorage.getInstance().reset();
    }

    @Test
    @DisplayName("Test Create Index and Select Optimization")
    void testIndexOptimization() throws IOException {
        BlocksStorage storage = BlocksStorage.getInstance();
        java.util.Map<String, Table> tables = new java.util.HashMap<>();
        SchemaManager schemaManager = new SchemaManager(storage);
        
        // 1. Create table
        CreateTableQuery createTable = new CreateTableQuery();
        createTable.parse("CREATE TABLE employees (id INT, name STRING(20), dept STRING(10))");
        createTable.run(schemaManager);
        Table table = schemaManager.loadSchemas().stream()
                .filter(p -> p.getName().equals("employees"))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("Table 'employees' does not exist"));

        // 2. Insert some data BEFORE creating index
        InsertQuery insert = new InsertQuery();
        insert.parse("INSERT INTO employees VALUES (1, 'Alice', 'HR')");
        insert.run(table);
        insert.parse("INSERT INTO employees VALUES (2, 'Bob', 'IT')");
        insert.run(table);
        insert.parse("INSERT INTO employees VALUES (3, 'Charlie', 'IT')");
        insert.run(table);

        // 3. Create Index on 'dept' (This will backfill)
        CreateIndexQuery createIndex = new CreateIndexQuery();
        createIndex.parse("CREATE INDEX ON employees (dept)");
        createIndex.run(schemaManager);

        // RELOAD table after persistence
        table = schemaManager.loadSchemas().stream()
                .filter(p -> p.getName().equals("employees"))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("Table 'employees' does not exist"));

        // 4. Verify Index exists and has data
        assertTrue(table.getIndexes().containsKey("dept"), "Index should be registered");
        Index deptIdx = table.getIndexes().get("dept");
        BTreeDisk<String> tree = (BTreeDisk<String>) deptIdx.getBTree();
        BKey<String> resultIT = tree.search(new BKey<>("IT"));
        assertNotNull(resultIT, "Index should contain 'IT'");
        assertEquals(2, resultIT.getRecordPointers().size(), "There should be 2 pointers for 'IT'");

        // 5. Select using Index
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM employees WHERE dept = 'IT'");
        // This will internally use handleIndexSearch
        select.run(table);
        
        List<List<Object>> results = select.getResults();
        assertEquals(2, results.size(), "Should return 2 records for IT dept");
        
        // Check content
        boolean foundBob = false;
        boolean foundCharlie = false;
        for (List<Object> row : results) {
            if (row.get(1).toString().equals("Bob")) foundBob = true;
            if (row.get(1).toString().equals("Charlie")) foundCharlie = true;
        }
        assertTrue(foundBob && foundCharlie, "Results should contain both Bob and Charlie");

        System.out.println("Index Optimization Test Passed!");
    }
}

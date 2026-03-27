package query;

import b_tree.BKey;
import b_tree.BTreeDisk;
import org.junit.jupiter.api.*;
import storage.BlocksStorage;
import table.*;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PrimaryKeyTest {

    @BeforeEach
    void setup() throws IOException {
        BlocksStorage.getInstance().reset();
    }

    @Test
    @DisplayName("Test Duplicate PK Unique Constraint")
    void testDuplicateKeyInsert() throws IOException {
        BlocksStorage storage = BlocksStorage.getInstance();
        SchemaManager schemaManager = new SchemaManager(storage);
        CreateTableQuery createQuery = new CreateTableQuery();
        createQuery.parse("CREATE TABLE accounts (acc_id INT, owner STRING(20), PRIMARY KEY (acc_id))");
        createQuery.run(schemaManager); // Ensure table is created before loading
        SchemaManager newManager = new SchemaManager(storage); // Fresh manager for loading
        Table table = newManager.loadSchemas().stream()
                .filter(p -> p.getName().equals("accounts"))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("Table 'accounts' does not exist"));

        InsertQuery insert = new InsertQuery();
        insert.parse("INSERT INTO accounts VALUES (1, 'User A')");
        insert.run(table);

        // Case 1: Duplicate with same value in-memory
        InsertQuery duplicate = new InsertQuery();
        duplicate.parse("INSERT INTO accounts VALUES (1, 'User B')");
        
        SQLException ex = assertThrows(SQLException.class, () -> duplicate.run(table),
                "Should throw SQLException for duplicate ID 1");
        assertTrue(ex.getMessage().contains("PRIMARY KEY constraint violated"));

        // Case 2: Different ID works
        insert.parse("INSERT INTO accounts VALUES (2, 'User C')");
        insert.run(table);

        // Case 3: Persistence - Save and reload
        // Important: We still need to save if Table state (like lastBlock) changed after INSERTS if schemaManager isn't global.
        // But the SchemaManager we passed to CreateTableQuery is already initialized.
        schemaManager.saveSchemas(Collections.singletonList(table));
        storage.clearCache();

        Table reloaded = schemaManager.loadSchemas().get(0);
        
        // Try duplicate ID 2 after reload
        duplicate.parse("INSERT INTO accounts VALUES (2, 'User D')");
        assertThrows(SQLException.class, () -> duplicate.run(reloaded),
                "Should still enforce PK unique constraint after reload");

        // Verify counts
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM accounts");
        select.run(reloaded);
        assertEquals(2, select.getResults().size(), "Table should still only have 2 records");
    }

    @Test
    @DisplayName("Full Primary Key Persistence Flow")
    void testPrimaryKeyFlow() throws IOException {
        BlocksStorage storage = BlocksStorage.getInstance();
        SchemaManager schemaManager = new SchemaManager(storage);

        // 1. CREATE Table with Primary Key
        CreateTableQuery createQuery = new CreateTableQuery();
        createQuery.parse("CREATE TABLE users (id INT, username STRING(20), PRIMARY KEY (id))");
        createQuery.run(schemaManager);
        Table table = schemaManager.loadSchemas().stream()
                .filter(p -> p.getName().equals("users"))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("Table 'users' does not exist"));

        assertNotNull(table.getPrimaryKey(), "Table should have a primary key");
        assertEquals("id", table.getPrimaryKey().getName());
        assertTrue(table.getIndexes().containsKey("id"), "Table should have an index on the PK column");

        // 2. INSERT a record
        InsertQuery insert = new InsertQuery();
        insert.parse("INSERT INTO users VALUES (101, 'alice')");
        insert.run(table);

        // Verify index has the key
        Index pkIndex = table.getIndexes().get("id");
        BTreeDisk<Integer> tree = (BTreeDisk<Integer>) pkIndex.getBTree();
        BKey<Integer> found = tree.search(new BKey<>(101));
        assertNotNull(found, "Index should contain the PK value 101");

        // 3. PERSIST and RELOAD
        schemaManager.saveSchemas(Collections.singletonList(table));
        storage.clearCache();

        List<Table> loadedTables = schemaManager.loadSchemas();
        Table reloadedTable = loadedTables.get(0);

        assertEquals("users", reloadedTable.getName());
        assertNotNull(reloadedTable.getPrimaryKey(), "Reloaded table should have a primary key");
        
        // 4. Test Unique Constraint on Reloaded Table
        InsertQuery insertDuplicate = new InsertQuery();
        insertDuplicate.parse("INSERT INTO users VALUES (101, 'bob')");
        assertThrows(SQLException.class, () -> insertDuplicate.run(reloadedTable));

        // 5. Insert another record with different PK
        insert.parse("INSERT INTO users VALUES (102, 'charlie')");
        insert.run(reloadedTable);

        // Verify data via Select
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM users");
        select.run(reloadedTable);
        assertEquals(2, select.getResults().size());
        
        System.out.println("PK Test passed: Create, Save, Load, and Unique Constraint verified.");
    }
}

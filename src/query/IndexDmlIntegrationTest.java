package query;

import org.junit.jupiter.api.*;
import storage.BlocksStorage;
import table.SchemaManager;
import table.Table;
import table.Index;
import table.CompositeKey;
import table.BTreeDisk;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class IndexDmlIntegrationTest {

    private SchemaManager schemaManager;

    @BeforeEach
    void setup() throws IOException {
        BlocksStorage.getInstance().reset();
        schemaManager = new SchemaManager(BlocksStorage.getInstance());
    }

    private Table createTable(String sql, String name) throws IOException {
        CreateTableQuery ctq = new CreateTableQuery();
        ctq.parse(sql);
        ctq.run(schemaManager);
        return schemaManager.loadSchemas().stream()
                .filter(t -> t.getName().equals(name)).findFirst().get();
    }

    private void createIndex(String sql) throws IOException {
        CreateIndexQuery ciq = new CreateIndexQuery();
        ciq.parse(sql);
        ciq.run(schemaManager);
    }

    private void insert(Table table, String values) throws IOException {
        InsertQuery iq = new InsertQuery();
        iq.parse("INSERT INTO " + table.getName() + " VALUES (" + values + ")");
        iq.run(table);
    }

    private void update(Table table, String sql) throws IOException {
        UpdateQuery uq = new UpdateQuery();
        uq.parse(sql);
        uq.run(table);
    }

    private void delete(Table table, String sql) throws IOException {
        DeleteQuery dq = new DeleteQuery();
        dq.parse(sql);
        dq.run(table);
    }

    @Test
    @DisplayName("Verify single-column index updates after UPDATE and DELETE")
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testSingleColumnIndexDml() throws IOException {
        createTable("CREATE TABLE tests (id INT, val STRING(10))", "tests");
        Table table;
        createIndex("CREATE INDEX ON tests (id)");
        
        // Reload table to get fresh index metadata
        table = schemaManager.loadSchemas().stream().filter(t -> t.getName().equals("tests")).findFirst().get();
        Index idx = table.getIndexes().get("id");
        BTreeDisk btree = idx.getBTree();

        // 1. Insert
        insert(table, "1, 'one'");
        assertNotNull(btree.search(1), "Index should contain 1 after insert");

        // 2. Update key col
        update(table, "UPDATE tests SET id = 2 WHERE id = 1");
        assertNull(btree.search(1), "Index should NOT contain the old key 1");
        assertNotNull(btree.search(2), "Index should contain the new key 2");

        // 3. Delete
        delete(table, "DELETE FROM tests WHERE id = 2");
        assertNull(btree.search(2), "Index should NOT contain 2 after deletion");
    }

    @Test
    @DisplayName("Verify composite index updates after UPDATE and DELETE")
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testCompositeIndexDml() throws IOException {
        createTable("CREATE TABLE tests (id INT, name STRING(10))", "tests");
        Table table;
        createIndex("CREATE INDEX ON tests (id, name)");
        
        // Reload table to get fresh index metadata
        table = schemaManager.loadSchemas().stream().filter(t -> t.getName().equals("tests")).findFirst().get();
        Index idx = table.getIndexes().get("id"); // composite keyed by first column
        BTreeDisk btree = idx.getBTree();

        // 1. Insert
        insert(table, "1, 'Alice'");
        CompositeKey key1 = new CompositeKey(1, "Alice");
        assertNotNull(btree.search(key1), "Index should contain (1, 'Alice')");

        // 2. Update ONE column of the composite key
        update(table, "UPDATE tests SET name = 'Bob' WHERE id = 1");
        assertNull(btree.search(key1), "Old composite key (1, 'Alice') should be gone");
        
        CompositeKey key2 = new CompositeKey(1, "Bob");
        assertNotNull(btree.search(key2), "New composite key (1, 'Bob') should be present");

        // 3. Delete
        delete(table, "DELETE FROM tests WHERE id = 1");
        assertNull(btree.search(key2), "Composite key (1, 'Bob') should be gone after delete");
    }
}

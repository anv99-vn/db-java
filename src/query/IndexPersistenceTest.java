package query;

import org.junit.jupiter.api.*;
import storage.BlocksStorage;
import table.SchemaManager;
import table.Table;
import table.Index;
import table.DataType;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IndexPersistenceTest {

    @BeforeEach
    void setup() throws IOException {
        BlocksStorage.getInstance().reset();
    }

    @Test
    @DisplayName("Test single and composite index persistence across restarts")
    void testIndexPersistence() throws IOException {
        BlocksStorage storage = BlocksStorage.getInstance();
        SchemaManager schemaManager = new SchemaManager(storage);

        // 1. Create Table
        CreateTableQuery ctq = new CreateTableQuery();
        ctq.parse("CREATE TABLE users (id INT, name STRING(20), email STRING(30))");
        ctq.run(schemaManager);

        // Verify No Index initially
        List<Table> initialTables = schemaManager.loadSchemas();
        Table table = initialTables.stream().filter(t -> t.getName().equals("users")).findFirst().get();
        assertTrue(table.getIndexes().isEmpty(), "Should have no indexes initially");

        // 2. Create Single-Column Index
        CreateIndexQuery ciq1 = new CreateIndexQuery();
        ciq1.parse("CREATE INDEX ON users (name)");
        ciq1.run(schemaManager);
        
        // Simular system restart: Clear cache and reload table
        storage.clearCache();
        SchemaManager reloadManager1 = new SchemaManager(storage);
        Table tableWithOneIndex = reloadManager1.loadSchemas().stream()
                .filter(t -> t.getName().equals("users")).findFirst().get();

        assertEquals(1, tableWithOneIndex.getIndexes().size(), "Should have 1 index after persistence");
        assertTrue(tableWithOneIndex.getIndexes().containsKey("name"), "Index should be on 'name'");
        assertEquals(DataType.STRING, tableWithOneIndex.getIndexes().get("name").getType());

        // 3. Create Composite Index
        CreateIndexQuery ciq2 = new CreateIndexQuery();
        ciq2.parse("CREATE INDEX ON users (id, email)");
        ciq2.run(reloadManager1);

        // Simular system restart again
        storage.clearCache();
        SchemaManager reloadManager2 = new SchemaManager(storage);
        Table tableWithTwoIndexes = reloadManager2.loadSchemas().stream()
                .filter(t -> t.getName().equals("users")).findFirst().get();

        assertEquals(2, tableWithTwoIndexes.getIndexes().size(), "Should have 2 indexes after persistence");
        
        // Composite index check (keyed by first column 'id' in current implementation)
        Index compositeIdx = tableWithTwoIndexes.getIndexes().get("id");
        assertNotNull(compositeIdx, "Composite index should be found");
        assertEquals(2, compositeIdx.getColumnNames().size());
        assertEquals("id", compositeIdx.getColumnNames().get(0));
        assertEquals("email", compositeIdx.getColumnNames().get(1));
    }
}

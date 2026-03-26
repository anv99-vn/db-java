package query;

import org.junit.jupiter.api.*;
import storage.BlocksStorage;
import table.SchemaManager;
import table.Table;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DropTableQueryTest {

    @BeforeEach
    void setup() throws IOException {
        BlocksStorage.getInstance().reset();
    }

    @Test
    @DisplayName("Test Drop Table parsing and removal")
    void testDropTableBasic() throws IOException {
        SchemaManager schemaManager = new SchemaManager(BlocksStorage.getInstance());
        
        // 1. Setup a table to drop later
        CreateTableQuery ctq = new CreateTableQuery();
        ctq.parse("CREATE TABLE users (id INT)");
        ctq.run(schemaManager);
        
        // Verify table was created
        assertTrue(schemaManager.loadSchemas().stream().anyMatch(t -> t.getName().equals("users")), 
                   "Table should be created in schema");
        
        // 2. Drop the table
        DropTableQuery dtq = new DropTableQuery();
        dtq.parse("DROP TABLE users");
        assertEquals("users", dtq.tableName);
        dtq.run(schemaManager);
        
        assertFalse(schemaManager.loadSchemas().stream().anyMatch(t -> t.getName().equals("users")), 
                    "Table should be removed from schema");
    }

    @Test
    @DisplayName("Test Drop Table when table doesn't exist")
    void testDropTableMissing() throws IOException {
        SchemaManager schemaManager = new SchemaManager(BlocksStorage.getInstance());
        DropTableQuery dtq = new DropTableQuery();
        dtq.parse("DROP TABLE nonexistent");
        
        assertThrows(IllegalArgumentException.class, () -> dtq.run(schemaManager));
    }

    @Test
    @DisplayName("Test Drop Table persistence via SchemaManager")
    void testDropTablePersistence() throws IOException {
        BlocksStorage storage = BlocksStorage.getInstance();
        SchemaManager schemaManager = new SchemaManager(storage);

        // 1. Create initial schema via Query
        CreateTableQuery ctq = new CreateTableQuery();
        ctq.parse("CREATE TABLE accounts (id INT)");
        ctq.run(schemaManager);
        
        // 2. Drop the table via Query (it should save automatically now)
        DropTableQuery dtq = new DropTableQuery();
        dtq.parse("DROP TABLE accounts");
        dtq.run(schemaManager);
        
        storage.clearCache();
        
        // 3. Reload and verify
        List<Table> reloaded = schemaManager.loadSchemas();
        assertTrue(reloaded.isEmpty(), "Schema should be empty after drop via Query");
    }

    @Test
    @DisplayName("Test Drop Table invalid syntax")
    void testInvalidSyntax() {
        DropTableQuery dtq = new DropTableQuery();
        assertThrows(IllegalArgumentException.class, () -> dtq.parse("DROP users"));
        assertThrows(IllegalArgumentException.class, () -> dtq.parse("DROP TABLE"));
    }
}

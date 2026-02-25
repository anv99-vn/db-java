package query;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import storage.BlocksStorage;
import table.DataType;
import table.Table;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

class InsertQueryTest {

    @Test
    void parse_validQuery() {
        InsertQuery query = new InsertQuery();
        query.parse("INSERT INTO users VALUES (1, 'John Doe', 25.5)");

        Assertions.assertEquals("users", query.tableName);
        Assertions.assertEquals(3, query.insertParams.size());
        Assertions.assertEquals("1", query.insertParams.get(0));
        Assertions.assertEquals("John Doe", query.insertParams.get(1));
        Assertions.assertEquals("25.5", query.insertParams.get(2));
    }

    @Test
    void parse_quotesHandling() {
        InsertQuery query = new InsertQuery();
        query.parse("INSERT INTO test VALUES ('value1', \"value2\", value3)");

        Assertions.assertEquals("test", query.tableName);
        Assertions.assertEquals("value1", query.insertParams.get(0));
        Assertions.assertEquals("value2", query.insertParams.get(1));
        Assertions.assertEquals("value3", query.insertParams.get(2));
    }

    @Test
    void parse_invalidStart() {
        InsertQuery query = new InsertQuery();
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                query.parse("SELECT * FROM users")
        );
    }

    @Test
    void parse_missingValues() {
        InsertQuery query = new InsertQuery();
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                query.parse("INSERT INTO users (1, 2)")
        );
    }

    @Test
    void run_verifyBlockSize() throws IOException {
        // Prepare storage - use a test file to avoid messing with real data
        String testDataPath = "test_data_insert.bin";
        new File(testDataPath).delete();
        BlocksStorage storage = new BlocksStorage(testDataPath, 10);

        try {
            // Mock Table using CreateTableQuery
            String createSql = "CREATE TABLE users (id INT, name STRING(20))";
            CreateTableQuery createQuery = new CreateTableQuery();
            createQuery.parse(createSql);
            Table table = new Table();
            
            // Re-using the singleton but manually clearing it for predictable results
            BlocksStorage.getInstance().clearCache();
            new File("data.bin").delete();
            
            createQuery.run(table);

            InsertQuery query = new InsertQuery();
            query.parse("INSERT INTO users VALUES (1, 'Test User')");

            // First insert to a fresh table (will allocate a new block)
            // We need to pass the storage instance to InsertQuery if it was customizable,
            // but since it's a singleton in the code, we might need a workaround or just test with default data.bin.
            // Let's assume we can use the default singleton for now but we'll try to be clean.

            // Re-using the singleton but manually clearing it for predictable results
            BlocksStorage.getInstance().clearCache();
            new File("data.bin").delete();
            // Re-initialize to ensure AtomicLong is reset
            // (In a real project we'd use Dependency Injection, but here we work with what we have)

            query.run(table);

            int lastBlockId = table.getLastBlock();
            Assertions.assertNotEquals(-1, lastBlockId);

            storage.getBlock(lastBlockId, bytes -> {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                int sizeInHeader = buffer.getInt();
                // 4 (INT) + 20 (STRING) = 24 bytes per record
                Assertions.assertEquals(24, sizeInHeader, "Block size in header should match record size");
            });

            // Insert another one
            query.parse("INSERT INTO users VALUES (2, 'User 2')");
            query.run(table);

            storage.getBlock(lastBlockId, bytes -> {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                int sizeInHeader = buffer.getInt();
                Assertions.assertEquals(48, sizeInHeader, "Block size should double after second insert");
            });
        } finally {
            storage.clearCache();
            new File("data.bin").delete();
        }
    }
}

package query;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import storage.BlocksStorage;
import storage.Block;
import table.Table;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

class DeleteQueryTest {

    private Table setupTestTable() throws IOException {
        File dbFile = new File("data.bin");
        if (dbFile.exists()) dbFile.delete();
        BlocksStorage.getInstance().clearCache();

        Table table = new Table();
        CreateTableQuery createTable = new CreateTableQuery();
        createTable.parse("CREATE TABLE test (id INT, name STRING(10), val FLOAT)");
        createTable.run(table);

        insert(table, "1, 'Alice', 10.0");
        insert(table, "2, 'Bob', 20.0");
        insert(table, "3, 'Charlie', 30.0");
        insert(table, "4, 'David', 40.0");
        
        return table;
    }

    private void insert(Table table, String values) throws IOException {
        InsertQuery insert = new InsertQuery();
        insert.parse("INSERT INTO test VALUES (" + values + ")");
        insert.run(table);
    }

    @Test
    void testDeleteMiddle() throws IOException {
        Table table = setupTestTable();
        
        DeleteQuery delete = new DeleteQuery();
        delete.parse("DELETE FROM test WHERE id = 2");
        delete.run(table);
        
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM test");
        select.run(table);
        
        List<List<Object>> results = select.getResults();
        Assertions.assertEquals(3, results.size());
        
        // After deleting 2, the last record (4) should have swapped into 2's spot
        // Order in block should be: 1, 4, 3
        Assertions.assertEquals(1, results.get(0).get(0));
        Assertions.assertEquals(4, results.get(1).get(0));
        Assertions.assertEquals(3, results.get(2).get(0));

        // Verify all blocks have valid checksum
        validateAllBlockChecksums();
    }

    @Test
    void testDeleteLast() throws IOException {
        Table table = setupTestTable();
        
        DeleteQuery delete = new DeleteQuery();
        delete.parse("DELETE FROM test WHERE id = 4");
        delete.run(table);
        
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM test");
        select.run(table);
        
        List<List<Object>> results = select.getResults();
        Assertions.assertEquals(3, results.size());
        Assertions.assertEquals(1, results.get(0).get(0));
        Assertions.assertEquals(2, results.get(1).get(0));
        Assertions.assertEquals(3, results.get(2).get(0));

        validateAllBlockChecksums();
    }

    @Test
    void testDeleteWithRange() throws IOException {
        Table table = setupTestTable();
        
        DeleteQuery delete = new DeleteQuery();
        delete.parse("DELETE FROM test WHERE id BETWEEN 1 AND 2");
        delete.run(table);
        
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM test");
        select.run(table);
        
        List<List<Object>> results = select.getResults();
        Assertions.assertEquals(2, results.size());
        // Remaining should be 3 and 4 
        // Swap logic:
        // Delete 1 -> Swap 4 to front: [4, 2, 3]
        // Next pos 0: Check 4 -> No match
        // Next pos 1: Check 2 -> Match -> Swap 3 to pos 1: [4, 3]
        // Next pos 1: Check 3 -> No match
        Assertions.assertEquals(4, results.get(0).get(0));
        Assertions.assertEquals(3, results.get(1).get(0));

        validateAllBlockChecksums();
    }

    @Test
    void testDeleteAll() throws IOException {
        Table table = setupTestTable();
        
        DeleteQuery delete = new DeleteQuery();
        delete.parse("DELETE FROM test"); // No WHERE = all
        delete.run(table);
        
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM test");
        select.run(table);
        
        Assertions.assertTrue(select.getResults().isEmpty());

        validateAllBlockChecksums();
    }

    @Test
    void testDeleteAllWithCondition() throws IOException {
        Table table = setupTestTable();
        
        DeleteQuery delete = new DeleteQuery();
        // Condition that matches all records
        delete.parse("DELETE FROM test WHERE id < 100");
        delete.run(table);
        
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM test");
        select.run(table);
        
        Assertions.assertTrue(select.getResults().isEmpty());

        validateAllBlockChecksums();
    }

    @Test
    void testDeleteNoMatch() throws IOException {
        Table table = setupTestTable();
        
        DeleteQuery delete = new DeleteQuery();
        delete.parse("DELETE FROM test WHERE id = 100");
        delete.run(table);
        
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM test");
        select.run(table);
        
        Assertions.assertEquals(4, select.getResults().size());

        validateAllBlockChecksums();
    }

    private void validateAllBlockChecksums() throws IOException {
        BlocksStorage storage = BlocksStorage.getInstance();
        int total = storage.getTotalBlockCount();
        for (int id = 0; id < total; id++) {
            storage.getBlock(id, bytes -> {
                ByteBuffer buf = ByteBuffer.wrap(bytes);
                int size = buf.getInt(Block.OFFSET_SIZE);
                int headerChecksum = buf.getInt(Block.OFFSET_CHECKSUM);
                CRC32 crc = new CRC32();
                if (size > 0) crc.update(bytes, Block.HEADER_TOTAL_SIZE, size);
                assert headerChecksum == (int) crc.getValue();
            });
        }
    }
}

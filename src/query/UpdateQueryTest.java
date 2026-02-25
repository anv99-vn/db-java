package query;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import storage.BlocksStorage;
import table.Table;

import java.io.File;
import java.io.IOException;
import java.util.List;

class UpdateQueryTest {

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
        
        return table;
    }

    private void insert(Table table, String values) throws IOException {
        InsertQuery insert = new InsertQuery();
        insert.parse("INSERT INTO test VALUES (" + values + ")");
        insert.run(table);
    }

    @Test
    void testUpdateSingleColumn() throws IOException {
        Table table = setupTestTable();
        
        UpdateQuery update = new UpdateQuery();
        update.parse("UPDATE test SET val = 99.9 WHERE id = 1");
        update.run(table);
        
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM test WHERE id = 1");
        select.run(table);
        
        Assertions.assertEquals(1, select.getResults().size());
        Assertions.assertEquals(99.9f, (float) select.getResults().get(0).get(2), 0.001);
    }

    @Test
    void testUpdateMultipleColumns() throws IOException {
        Table table = setupTestTable();
        
        UpdateQuery update = new UpdateQuery();
        update.parse("UPDATE test SET name = 'Bobby', val = 22.2 WHERE id = 2");
        update.run(table);
        
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM test WHERE id = 2");
        select.run(table);
        
        List<Object> result = select.getResults().get(0);
        Assertions.assertEquals("Bobby", result.get(1));
        Assertions.assertEquals(22.2f, (float) result.get(2), 0.001);
    }

    @Test
    void testUpdateAll() throws IOException {
        Table table = setupTestTable();
        
        UpdateQuery update = new UpdateQuery();
        update.parse("UPDATE test SET val = 0.0");
        update.run(table);
        
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM test");
        select.run(table);
        
        List<List<Object>> results = select.getResults();
        Assertions.assertEquals(3, results.size());
        for (List<Object> row : results) {
            Assertions.assertEquals(0.0f, row.get(2));
        }
    }

    @Test
    void testUpdateStringPadding() throws IOException {
        Table table = setupTestTable();
        
        UpdateQuery update = new UpdateQuery();
        // name is STRING(10). 'VeryLongName' is 12 chars, should be truncated or handled
        update.parse("UPDATE test SET name = 'Short' WHERE id = 3");
        update.run(table);
        
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM test WHERE id = 3");
        select.run(table);
        
        Assertions.assertEquals("Short", select.getResults().get(0).get(1));
    }

    @Test
    void testUpdateNoMatch() throws IOException {
        Table table = setupTestTable();
        
        UpdateQuery update = new UpdateQuery();
        update.parse("UPDATE test SET val = 100.0 WHERE id = 99");
        update.run(table);
        
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM test WHERE val = 100.0");
        select.run(table);
        
        Assertions.assertTrue(select.getResults().isEmpty());
    }
}

package query;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import table.DataType;
import table.Table;

import java.io.IOException;

class CreateTableQueryTest {

    @Test
    void parse_validQuery() {
        CreateTableQuery query = new CreateTableQuery();
        query.parse("CREATE TABLE users (id INT, name STRING, salary FLOAT, PRIMARY KEY(id))");

        Assertions.assertEquals("users", query.tableName);
        Assertions.assertEquals(3, query.columns.size());
        Assertions.assertEquals(DataType.INT, query.columns.get("id"));
        Assertions.assertEquals(DataType.STRING, query.columns.get("name"));
        Assertions.assertEquals(DataType.FLOAT, query.columns.get("salary"));
        Assertions.assertEquals("id", query.primaryKeyColumn);
    }

    @Test
    void run_populatesTable() throws IOException {
        CreateTableQuery query = new CreateTableQuery();
        query.parse("CREATE TABLE products (pid INT, price FLOAT, PRIMARY KEY(pid))");

        Table table = new Table();
        query.run(table);

        Assertions.assertEquals(2, table.getColumn().size());
        Assertions.assertEquals(DataType.INT, table.getColumn().get("pid"));
        Assertions.assertEquals(4, table.getColumnSizes().get("pid"));
        Assertions.assertEquals(DataType.FLOAT, table.getColumn().get("price"));
        Assertions.assertEquals(4, table.getColumnSizes().get("price"));

        Assertions.assertNotNull(table.getPrimaryKey());
        Assertions.assertEquals("pid", table.getPrimaryKey().getName());
        Assertions.assertEquals(DataType.INT, table.getPrimaryKey().getType());

        Assertions.assertNotNull(table.getListBlock());
        Assertions.assertEquals(1, table.getListBlock().size());
    }

    @Test
    void parse_stringWithSize() {
        CreateTableQuery query = new CreateTableQuery();
        query.parse("CREATE TABLE test (name STRING(10), bio STRING)");

        Assertions.assertEquals(DataType.STRING, query.columns.get("name"));
        Assertions.assertEquals(10, query.columnSizes.get("name"));
        
        Assertions.assertEquals(DataType.STRING, query.columns.get("bio"));
        Assertions.assertEquals(30, query.columnSizes.get("bio"));
    }

    @Test
    void parse_invalidSyntax() {
        CreateTableQuery query = new CreateTableQuery();
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                query.parse("CREATE TABLE users id INT") // missing parens
        );
    }
}

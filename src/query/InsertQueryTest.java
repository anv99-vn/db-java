package query;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
}

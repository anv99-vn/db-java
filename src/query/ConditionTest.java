package query;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import table.DataType;

import java.util.Arrays;
import java.util.List;

class ConditionTest {

    @Test
    void testParseSimpleOperators() {
        Condition cond = new Condition();
        
        cond.parse("id = 10");
        Assertions.assertEquals("id", cond.getColumnName());
        
        cond.parse("val > 5.5");
        Assertions.assertEquals("val", cond.getColumnName());
        
        cond.parse("name != 'Alice'");
        Assertions.assertEquals("name", cond.getColumnName());
        
        cond.parse("score <= 100");
        Assertions.assertEquals("score", cond.getColumnName());
    }

    @Test
    void testParseBetween() {
        Condition cond = new Condition();
        cond.parse("age BETWEEN 18 AND 30");
        Assertions.assertEquals("age", cond.getColumnName());
    }

    @Test
    void testRemoveQuotes() {
        Assertions.assertEquals("Alice", Condition.removeQuotes("'Alice'"));
        Assertions.assertEquals("Bob", Condition.removeQuotes("\"Bob\""));
        Assertions.assertEquals("Charlie", Condition.removeQuotes("Charlie"));
    }

    @Test
    void testEvaluateInt() {
        Condition cond = new Condition();
        List<Object> record = List.of(25);
        
        cond.parse("age = 25");
        Assertions.assertTrue(cond.evaluate(record, 0, DataType.INT));
        
        cond.parse("age > 20");
        Assertions.assertTrue(cond.evaluate(record, 0, DataType.INT));
        
        cond.parse("age < 30");
        Assertions.assertTrue(cond.evaluate(record, 0, DataType.INT));
        
        cond.parse("age != 20");
        Assertions.assertTrue(cond.evaluate(record, 0, DataType.INT));
        
        cond.parse("age BETWEEN 20 AND 30");
        Assertions.assertTrue(cond.evaluate(record, 0, DataType.INT));
        
        cond.parse("age = 30");
        Assertions.assertFalse(cond.evaluate(record, 0, DataType.INT)); 
    }

    @Test
    void testEvaluateFloat() {
        Condition cond = new Condition();
        List<Object> record = Arrays.asList(10.5f);
        
        cond.parse("val = 10.5");
        Assertions.assertTrue(cond.evaluate(record, 0, DataType.FLOAT));
        
        cond.parse("val > 10.0");
        Assertions.assertTrue(cond.evaluate(record, 0, DataType.FLOAT));
        
        cond.parse("val BETWEEN 10.0 AND 11.0");
        Assertions.assertTrue(cond.evaluate(record, 0, DataType.FLOAT));
    }

    @Test
    void testEvaluateString() {
        Condition cond = new Condition();
        List<Object> record = Arrays.asList("Alice");
        
        cond.parse("name = 'Alice'");
        Assertions.assertTrue(cond.evaluate(record, 0, DataType.STRING));
        
        cond.parse("name != 'Bob'");
        Assertions.assertTrue(cond.evaluate(record, 0, DataType.STRING));
        
        cond.parse("name BETWEEN 'A' AND 'B'");
        Assertions.assertTrue(cond.evaluate(record, 0, DataType.STRING));
        
        cond.parse("name = 'Bob'");
        Assertions.assertFalse(cond.evaluate(record, 0, DataType.STRING));
    }

    @Test
    void testInvalidWhereClause() {
        Condition cond = new Condition();
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            cond.parse("invalid clause");
        });
    }

    @Test
    void testMismatchedType() {
        Condition cond = new Condition();
        List<Object> record = Arrays.asList("NotAnInt");
        cond.parse("id = 10");
        // Should handle gracefully or return false
        Assertions.assertFalse(cond.evaluate(record, 0, DataType.INT));
    }
}

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;

/**
 * JUnit 5 test suite for Attribute class
 * Tests serialization, type validation, and comparison logic
 */
public class AttributeTest {

    private Kryo kryo;
    private Attribute intAttr;
    private Attribute floatAttr;
    private Attribute stringAttr;

    @BeforeEach
    public void setUp() {
        kryo = new Kryo();
        kryo.register(Attribute.class);
        kryo.register(DataTypeEnum.class);

        // Initialize test attributes
        intAttr = new Attribute((byte) DataTypeEnum.INTEGER.getId());
        intAttr.value = ByteBuffer.allocate(4).putInt(42).array();
        intAttr.dataType = DataTypeEnum.INTEGER;

        floatAttr = new Attribute((byte) DataTypeEnum.FLOAT.getId());
        floatAttr.value = ByteBuffer.allocate(4).putFloat(9.99f).array();
        floatAttr.dataType = DataTypeEnum.FLOAT;

        stringAttr = new Attribute((byte) DataTypeEnum.STRING.getId());
        String testString = "test";
        stringAttr.value = ByteBuffer.allocate(testString.length()).put(testString.getBytes()).array();
        stringAttr.dataType = DataTypeEnum.STRING;
    }

    @AfterEach
    public void tearDown() {
        // Clean up test files
        deleteTestFile("attribute_int.bin");
        deleteTestFile("attribute_float.bin");
        deleteTestFile("attribute_string.bin");
        deleteTestFile("attribute_compare.bin");
    }

    // ==================== GETTER TESTS ====================

    @Test
    public void testGetInt_ValidIntegerAttribute() {
        int value = intAttr.getInt();
        assertEquals(42, value);
    }

    @Test
    public void testGetFloat_ValidFloatAttribute() {
        float value = floatAttr.getFloat();
        assertEquals(9.99f, value, 0.01f);
    }

    @Test
    public void testGetString_ValidStringAttribute() {
        String value = stringAttr.getString();
        assertEquals("test", value, "String value should be exactly 'test'");
    }

    @Test
    public void testGetInt_OnFloatAttribute_ThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            floatAttr.getInt();
        });
    }

    @Test
    public void testGetFloat_OnStringAttribute_ThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            stringAttr.getFloat();
        });
    }

    @Test
    public void testGetString_OnIntegerAttribute_ThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            intAttr.getString();
        });
    }

    @Test
    public void testGetInt_WithNullValue_ThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> {
            Attribute nullAttr = new Attribute((byte) DataTypeEnum.INTEGER.getId());
            nullAttr.value = null;
            nullAttr.dataType = DataTypeEnum.INTEGER;
            nullAttr.getInt();
        });
    }

    // ==================== SERIALIZATION TESTS ====================

    @Test
    public void testSerializeDeserialize_IntegerAttribute() throws Exception {
        String filePath = "attribute_int.bin";

        // Serialize
        Output output = new Output(new FileOutputStream(filePath));
        kryo.writeObject(output, intAttr);
        output.close();

        // Deserialize
        Input input = new Input(new FileInputStream(filePath));
        Attribute deserialized = kryo.readObject(input, Attribute.class);
        input.close();

        // Verify
        assertEquals(intAttr.type, deserialized.type);
        assertEquals(intAttr.getInt(), deserialized.getInt());
    }

    @Test
    public void testSerializeDeserialize_FloatAttribute() throws Exception {
        String filePath = "attribute_float.bin";

        // Serialize
        Output output = new Output(new FileOutputStream(filePath));
        kryo.writeObject(output, floatAttr);
        output.close();

        // Deserialize
        Input input = new Input(new FileInputStream(filePath));
        Attribute deserialized = kryo.readObject(input, Attribute.class);
        input.close();

        // Verify
        assertEquals(floatAttr.type, deserialized.type);
        assertEquals(floatAttr.getFloat(), deserialized.getFloat(), 0.01f);
    }

    @Test
    public void testSerializeDeserialize_StringAttribute() throws Exception {
        String filePath = "attribute_string.bin";

        // Serialize
        Output output = new Output(new FileOutputStream(filePath));
        kryo.writeObject(output, stringAttr);
        output.close();

        // Deserialize
        Input input = new Input(new FileInputStream(filePath));
        Attribute deserialized = kryo.readObject(input, Attribute.class);
        input.close();

        // Verify
        assertEquals(stringAttr.type, deserialized.type);
        assertEquals(stringAttr.getString(), deserialized.getString());
    }

    // ==================== COMPARISON TESTS ====================

    @Test
    public void testCompareTo_SameTypeIntegers_LessThan() {
        Attribute smaller = new Attribute((byte) DataTypeEnum.INTEGER.getId());
        smaller.value = ByteBuffer.allocate(4).putInt(10).array();
        smaller.dataType = DataTypeEnum.INTEGER;

        int result = smaller.compareTo(intAttr); // 10 vs 42
        assertTrue(result < 0);
    }

    @Test
    public void testCompareTo_SameTypeIntegers_GreaterThan() {
        Attribute larger = new Attribute((byte) DataTypeEnum.INTEGER.getId());
        larger.value = ByteBuffer.allocate(4).putInt(100).array();
        larger.dataType = DataTypeEnum.INTEGER;

        int result = larger.compareTo(intAttr); // 100 vs 42
        assertTrue(result > 0);
    }

    @Test
    public void testCompareTo_SameTypeIntegers_Equal() {
        Attribute same = new Attribute((byte) DataTypeEnum.INTEGER.getId());
        same.value = ByteBuffer.allocate(4).putInt(42).array();
        same.dataType = DataTypeEnum.INTEGER;

        int result = intAttr.compareTo(same);
        assertEquals(0, result);
    }

    @Test
    public void testCompareTo_SameTypeFloats() {
        Attribute smaller = new Attribute((byte) DataTypeEnum.FLOAT.getId());
        smaller.value = ByteBuffer.allocate(4).putFloat(1.5f).array();
        smaller.dataType = DataTypeEnum.FLOAT;

        int result = smaller.compareTo(floatAttr); // 1.5 vs 9.99
        assertTrue(result < 0);
    }

    @Test
    public void testCompareTo_SameTypeStrings() {
        Attribute other = new Attribute((byte) DataTypeEnum.STRING.getId());
        String zebra = "zebra";
        other.value = ByteBuffer.allocate(zebra.length()).put(zebra.getBytes()).array();
        other.dataType = DataTypeEnum.STRING;

        int result = stringAttr.compareTo(other); // "test" vs "zebra"
        assertTrue(result < 0, "test should be less than zebra lexicographically");
    }

    @Test
    public void testCompareTo_IntegerAndFloat_NumericComparison() {
        // 42 (integer) should be > 3.14 (float)
        Attribute smallFloat = new Attribute((byte) DataTypeEnum.FLOAT.getId());
        smallFloat.value = ByteBuffer.allocate(4).putFloat(3.14f).array();
        smallFloat.dataType = DataTypeEnum.FLOAT;

        int result = intAttr.compareTo(smallFloat); // 42 > 3.14
        assertTrue(result > 0);
    }

    @Test
    public void testCompareTo_FloatAndInteger_NumericComparison() {
        // 9.99 (float) should be < 42 (integer)
        int result = floatAttr.compareTo(intAttr); // 9.99 < 42
        assertTrue(result < 0);
    }

    @Test
    public void testCompareTo_IntegerAndString_ThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            intAttr.compareTo(stringAttr);
        });
    }

    @Test
    public void testCompareTo_FloatAndString_ThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            floatAttr.compareTo(stringAttr);
        });
    }

    @Test
    public void testCompareTo_StringAndInteger_ThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            stringAttr.compareTo(intAttr);
        });
    }

    // ==================== EDGE CASE TESTS ====================

    @Test
    public void testAttribute_ZeroIntegerValue() {
        Attribute zero = new Attribute((byte) DataTypeEnum.INTEGER.getId());
        zero.value = ByteBuffer.allocate(4).putInt(0).array();
        zero.dataType = DataTypeEnum.INTEGER;

        assertEquals(0, zero.getInt());
    }

    @Test
    public void testAttribute_NegativeIntegerValue() {
        Attribute negative = new Attribute((byte) DataTypeEnum.INTEGER.getId());
        negative.value = ByteBuffer.allocate(4).putInt(-100).array();
        negative.dataType = DataTypeEnum.INTEGER;

        assertEquals(-100, negative.getInt());
    }

    @Test
    public void testAttribute_ZeroFloatValue() {
        Attribute zero = new Attribute((byte) DataTypeEnum.FLOAT.getId());
        zero.value = ByteBuffer.allocate(4).putFloat(0.0f).array();
        zero.dataType = DataTypeEnum.FLOAT;

        assertEquals(0.0f, zero.getFloat(), 0.001f);
    }

    @Test
    public void testAttribute_EmptyStringValue() {
        Attribute empty = new Attribute((byte) DataTypeEnum.STRING.getId());
        empty.value = new byte[0];
        empty.dataType = DataTypeEnum.STRING;

        assertNotNull(empty.getString());
    }

    // ==================== HELPER METHODS ====================

    private void deleteTestFile(String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            file.delete();
        }
    }
}

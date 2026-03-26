package query;

import org.junit.jupiter.api.*;
import storage.BlocksStorage;
import table.Table;
import table.SchemaManager;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * FullFlowTest - Unit test bao phủ CRUD và Persistence.
 * Lưu ý: Block 0 được dành riêng cho Schema (Master Block).
 */
class FullFlowTest {

    private static final String DATA_FILE = "data.bin";

    @BeforeEach
    void setup() throws IOException {
        // Use reset() instead of file.delete() – on Windows the file is held open by the
        // singleton's FileChannel and cannot be deleted while in use.
        BlocksStorage.getInstance().reset();
    }

    @Test
    @DisplayName("Full CRUD & Persistence Flow")
    void testFullPersistenceFlow() throws IOException {
        BlocksStorage storage = BlocksStorage.getInstance();
        SchemaManager schemaManager = new SchemaManager(storage);

        // --- BƯỚC QUAN TRỌNG: FORMAT DATABASE ---
        // Ghi Schema trống vào Block 0 ngay lập tức.
        schemaManager.saveSchemas(new ArrayList<>()); 
        System.out.println("--- Đã định dạng: Block 0 đã được dành riêng cho Schema ---");

        // 1. CREATE Table
        Table table = new Table();
        table.setName("products");
        CreateTableQuery createQuery = new CreateTableQuery();
        createQuery.parse("CREATE TABLE products (id INT, name STRING(20), price FLOAT)");
        createQuery.run(table);
        
        // 2. INSERT Data
        // Vì Block 0 đã bị SchemaManager chiếm, InsertQuery sẽ allocate Block 1
        InsertQuery insert = new InsertQuery();
        insert.parse("INSERT INTO products VALUES (1, 'Gaming Laptop', 1500.0)");
        insert.run(table);
        
        insert.parse("INSERT INTO products VALUES (2, 'Office Mouse', 15.0)");
        insert.run(table);
        
        // 3. UPDATE Price of id=1 to 1400.0
        UpdateQuery update = new UpdateQuery();
        update.parse("UPDATE products SET price = 1400.0 WHERE id = 1");
        update.run(table);
        
        // 4. DELETE id=2
        DeleteQuery delete = new DeleteQuery();
        delete.parse("DELETE FROM products WHERE id = 2");
        delete.run(table);
        
        System.out.println("GĐ 1: CRUD thành công. Hiện tại có 1 bản ghi Laptop giá 1400.0");

        // 5. PERSIST Schema (Lưu vào Block 0)
        schemaManager.saveSchemas(Collections.singletonList(table));
        
        // 6. RELOAD SIMULATION:
        storage.clearCache();
        System.out.println("GĐ 2: [RAM cleared, Cache cleared]. Bắt đầu khôi phục từ Disk...");

        // Khôi phục Schema từ Block 0
        List<Table> loadedTables = schemaManager.loadSchemas();
        assertFalse(loadedTables.isEmpty(), "Phải load được Schema từ disk");
        
        Table reloadedTable = loadedTables.get(0);
        assertEquals("products", reloadedTable.getName());
        
        // KIỂM TRA SỬA LỖI:
        // Lần đầu tiên InsertQuery (ở bước 2) allocate dữ liệu sau khi Block 0 đã có.
        // Vậy nên firstBlock phải là 1.
        assertEquals(1, reloadedTable.getFirstBlock(), "Block dữ liệu đầu tiên phải là 1 (vì Block 0 là Schema)");
        
        // 7. VERIFY Data bằng Select
        SelectQuery select = new SelectQuery();
        select.parse("SELECT * FROM products");
        select.run(reloadedTable);
        
        List<List<Object>> results = select.getResults();
        assertEquals(1, results.size(), "Bản ghi Laptop phải tồn tại sau khi reload");
        
        List<Object> laptop = results.get(0);
        assertEquals(1, (Integer) laptop.get(0));
        assertEquals("Gaming Laptop", laptop.get(1).toString().trim());
        assertEquals(1400.0f, (Float) laptop.get(2), 0.01);
        
        System.out.println("GĐ 3: RELOAD THÀNH CÔNG! Dữ liệu laptop khôi phục: id=1, name='Gaming Laptop', price=1400.0");
    }
}

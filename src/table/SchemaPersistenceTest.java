package table;

import org.junit.jupiter.api.*;
import storage.BlocksStorage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Persistence Schema Storage Tests")
class SchemaPersistenceTest {

    private static final String TEST_DB = "schema_persistence_test.bin";

    @BeforeEach
    void clean() {
        File f = new File(TEST_DB);
        if (f.exists()) f.delete();
    }

    @Test
    @DisplayName("Verify table schema persists after restarting storage")
    void testSchemaPersistence() throws IOException {
        
        // --- GIAI ĐOẠN 1: Tạo Schema và Lưu xuống disk ---
        {
            BlocksStorage storage = new BlocksStorage(TEST_DB);
            SchemaManager schemaManager = new SchemaManager(storage);
            
            // Tạo bảng 1: "users"
            Table usersTable = new Table();
            usersTable.setName("users");
            usersTable.addColumn("id", DataType.INT, 4);
            usersTable.addColumn("username", DataType.STRING, 50);
            usersTable.setFirstBlock(10);
            usersTable.setLastBlock(20);
            
            // Tạo bảng 2: "orders"
            Table ordersTable = new Table();
            ordersTable.setName("orders");
            ordersTable.addColumn("order_id", DataType.INT, 4);
            ordersTable.addColumn("amount", DataType.FLOAT, 8);
            ordersTable.setFirstBlock(100);
            ordersTable.setLastBlock(200);
            
            List<Table> tablesToSave = new ArrayList<>();
            tablesToSave.add(usersTable);
            tablesToSave.add(ordersTable);
            
            schemaManager.saveSchemas(tablesToSave);
            storage.close();
            System.out.println("Giai đoạn 1: Đã lưu schema của 2 bảng.");
        }

        // --- GIAI ĐOẠN 2: Load lại từ disk và kiểm tra ---
        {
            BlocksStorage storage = new BlocksStorage(TEST_DB);
            SchemaManager schemaManager = new SchemaManager(storage);
            
            List<Table> loadedTables = schemaManager.loadSchemas();
            
            assertEquals(2, loadedTables.size(), "Số lượng bảng load lên phải là 2");
            
            // Kiểm tra bảng 'users'
            Table users = loadedTables.stream().filter(t -> t.getName().equals("users")).findFirst().get();
            assertNotNull(users);
            assertEquals(2, users.getColumn().size());
            assertEquals(DataType.INT, users.getColumn().get("id"));
            assertEquals(DataType.STRING, users.getColumn().get("username"));
            assertEquals(10, users.getFirstBlock());
            assertEquals(20, users.getLastBlock());
            
            // Kiểm tra bảng 'orders'
            Table orders = loadedTables.stream().filter(t -> t.getName().equals("orders")).findFirst().get();
            assertNotNull(orders);
            assertEquals(DataType.FLOAT, orders.getColumn().get("amount"));
            assertEquals(100, orders.getFirstBlock());
            assertEquals(200, orders.getLastBlock());
            
            storage.close();
            System.out.println("Giai đoạn 2: [SUCCESS] Đã load và kiểm tra chính xác schema từ disk.");
        }
    }
}

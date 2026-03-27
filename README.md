# DB-Java

**DB-Java** là một hệ thống quản trị cơ sở dữ liệu quan hệ (RDBMS) mã nguồn mở, được viết hoàn toàn bằng ngôn ngữ Java. Dự án tập trung vào việc mô phỏng các cốt lõi của một hệ thống database: từ việc phân tích câu lệnh SQL, quản lý giao diện dòng lệnh (CLI), đến tầng lưu trữ dữ liệu bền vững trên đĩa cứng (Disk-based persistence) sử dụng các cấu trúc dữ liệu tối ưu như B-Tree.

## 🚀 Tính năng chính

- **SQL Console:** Giao diện dòng lệnh tương tác hỗ trợ các câu lệnh chuẩn: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, `DROP TABLE`, `SHOW TABLES`, và `CREATE INDEX`.
- **Disk-Based Storage:** Dữ liệu được lưu trữ trực tiếp xuống file dưới dạng các khối (Blocks), cho phép duy trì dữ liệu ngay cả khi khởi động lại hệ thống.
- **B-Tree Indexing:** Sử dụng cấu trúc cây B-Tree (phiên bản trên đĩa) để tăng tốc độ tìm kiếm và tối ưu hóa hiệu năng truy vấn cho các bảng có lượng dữ liệu lớn.
- **Custom SQL Tokenizer:** Bộ phân tích từ vựng (tokenizer) tự xây dựng giúp xử lý linh hoạt các câu lệnh SQL phức tạp, bao gồm cả chuỗi ký tự chứa dấu phẩy or khoảng trắng.
- **Hỗ trợ kiểu dữ liệu:** `INT`, `FLOAT`, `STRING(size)`.

## 🏗️ Kiến trúc dự án

Dự án được chia thành các package chính:

- **`query`**: Chứa logic phân tích (Parser) và thực thi (Execution) các câu lệnh SQL.
- **`table`**: Định nghĩa cấu trúc Bảng, Cột, Dòng, Khóa chính, và đặc biệt là triển khai **B-Tree Disk** để lưu trữ chỉ mục và dữ liệu.
- **`storage`**: Tầng quản lý lưu trữ vật lý, thực hiện các thao tác đọc/ghi các khối (Block) dữ liệu từ tệp tin (`data.bin`).

## 🛠️ Cài đặt và Chạy ứng dụng

### Yêu cầu hệ thống
- JDK 11 trở lên.

### Các bước thực hiện
1. Clone repository về máy.
2. Mở dự án trong IDE của bạn.
3. Đảm bảo các thư viện trong thư mục `lib/` (Kryo, JUnit) đã được thêm vào classpath.
4. Chạy file `src/Main.java` để bắt đầu console.

## 📝 Ví dụ các câu lệnh hỗ trợ

Sau khi chạy `Main.java`, bạn có thể nhập các truy vấn sau:

```sql
-- Tạo bảng mới
CREATE TABLE students (id INT, name STRING(50), grade FLOAT, PRIMARY KEY (id))

-- Xem danh sách các bảng
SHOW TABLES

-- Chèn dữ liệu
INSERT INTO students VALUES (1, 'Nguyen Van A', 8.5)
INSERT INTO students VALUES (2, 'Tran Thi B', 9.0)

-- Tạo index cho một cột để tìm kiếm nhanh hơn
CREATE INDEX name ON students (id)

-- Truy vấn dữ liệu
SELECT * FROM students WHERE grade > 8.0

-- Cập nhật dữ liệu
UPDATE students SET grade = 9.5 WHERE id = 1

-- Xóa dữ liệu hoặc bảng
DELETE FROM students WHERE id = 2
DROP TABLE students
```

## 🧪 Kiểm thử
Dự án bao gồm bộ unit test toàn diện cho các thành phần cốt lõi như B-Tree Disk, Blocks Storage, và logic SQL query. Bạn có thể chạy các file test như `BTreeDiskTest.java` hoặc `IndexDmlIntegrationTest.java` để kiểm tra độ ổn định.

## 📅 Định hướng phát triển
- Hỗ trợ câu lệnh `JOIN` giữa các bảng.
- Triển khai cơ chế giao dịch (Transactions - ACID).
- Tối ưu hóa bộ đệm (Buffer Pool Management).
- Hỗ trợ thêm nhiều toán tử trong mệnh đề `WHERE`.

---

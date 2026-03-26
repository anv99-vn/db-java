package query;

import table.SchemaManager;

import java.io.IOException;

/**
 * Interface for queries that operate on the whole database (e.g. SHOW TABLES, CREATE TABLE)
 */
public interface DatabaseQuery {
    void parse(String query);
    void run(SchemaManager schemaManager) throws IOException;
}

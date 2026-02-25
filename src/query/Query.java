package query;

import table.Table;

import java.io.IOException;

public interface Query {
    void parse(String query);

    void run(Table table) throws IOException;
}

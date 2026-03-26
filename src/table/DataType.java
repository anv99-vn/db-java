package table;

import java.util.Arrays;

public enum DataType {
    INT(0),
    FLOAT(1),
    STRING(2),
    COMPOSITE(3),
    ;

    private final int id;

    DataType(int id) {
        this.id = id;
    }

    private DataType() {
        this.id = 0;
    }

    public int getId() {
        return id;
    }

    public static DataType fromId(int id) {
        return Arrays.stream(DataType.values())
                .filter(type -> type.getId() == id)
                .findFirst().orElse(null);
    }
}

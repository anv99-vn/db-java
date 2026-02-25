package table;

import java.util.Arrays;

public enum DataTypeEnum {
    INTEGER(0), FLOAT(1), STRING(2),
    ;

    private final int id;

    DataTypeEnum(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static DataTypeEnum valueOf(int id) {
        return Arrays.stream(DataTypeEnum.values()).filter(type -> type.getId() == id)
                .findFirst().orElse(null);
    }
}

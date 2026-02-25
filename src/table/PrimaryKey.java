package table;

public class PrimaryKey {
    String name;
    DataType type;

    public PrimaryKey(String name, DataType type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }
}

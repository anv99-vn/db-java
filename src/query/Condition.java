package query;

import table.DataType;
import java.util.List;

public class Condition {
    private String whereColumn;
    private String whereOperator;
    private String whereValue;
    private String whereValue2;

    public void parse(String whereClause) {
        if (whereClause.toUpperCase().contains(" BETWEEN ")) {
            String[] parts = whereClause.split("(?i)\\s+BETWEEN\\s+");
            if (parts.length == 2) {
                this.whereColumn = parts[0].trim();
                this.whereOperator = "BETWEEN";
                String[] values = parts[1].split("(?i)\\s+AND\\s+");
                if (values.length == 2) {
                    this.whereValue = removeQuotes(values[0].trim());
                    this.whereValue2 = removeQuotes(values[1].trim());
                    return;
                }
            }
        }

        String[] operators = {">=", "<=", "!=", "=", ">", "<"};
        for (String op : operators) {
            int opIndex = whereClause.indexOf(op);
            if (opIndex != -1) {
                this.whereColumn = whereClause.substring(0, opIndex).trim();
                this.whereOperator = op;
                this.whereValue = removeQuotes(whereClause.substring(opIndex + op.length()).trim());
                return;
            }
        }
        throw new IllegalArgumentException("Invalid WHERE clause: " + whereClause);
    }

    public static String removeQuotes(String val) {
        if ((val.startsWith("'") && val.endsWith("'")) ||
            (val.startsWith("\"") && val.endsWith("\""))) {
            return val.substring(1, val.length() - 1);
        }
        return val;
    }

    public boolean evaluate(List<Object> record, int colIndex, DataType type) {
        if (whereColumn == null) return true;
        Object val = record.get(colIndex);
        try {
            switch (type) {
                case INT -> {
                    int actual = (int) val;
                    if (whereOperator.equals("BETWEEN")) {
                        return actual >= Integer.parseInt(whereValue) && actual <= Integer.parseInt(whereValue2);
                    }
                    return compare(actual, Integer.parseInt(whereValue));
                }
                case FLOAT -> {
                    float actual = (float) val;
                    if (whereOperator.equals("BETWEEN")) {
                        return actual >= Float.parseFloat(whereValue) && actual <= Float.parseFloat(whereValue2);
                    }
                    return compare(actual, Float.parseFloat(whereValue));
                }
                case STRING -> {
                    String actual = (String) val;
                    if (whereOperator.equals("BETWEEN")) {
                        return actual.compareTo(whereValue) >= 0 && actual.compareTo(whereValue2) <= 0;
                    }
                    if (whereOperator.equals("=")) return actual.equals(whereValue);
                    if (whereOperator.equals("!=")) return !actual.equals(whereValue);
                }
            }
        } catch (Exception e) { return false; }
        return false;
    }

    private boolean compare(double actual, double target) {
        return switch (whereOperator) {
            case "=" -> actual == target;
            case ">" -> actual > target;
            case "<" -> actual < target;
            case ">=" -> actual >= target;
            case "<=" -> actual <= target;
            case "!=" -> actual != target;
            default -> false;
        };
    }

    public String getColumnName() {
        return whereColumn;
    }
}

package query;

import table.DataType;
import java.util.List;

public class Condition {
    private String whereColumn;
    private String whereOperator;
    private String whereValue;
    private String whereValue2;

    public void parse(String whereClause) {
        SqlTokenizer tokenizer = new SqlTokenizer(whereClause);
        List<String> tokens = tokenizer.tokenize();

        if (tokens.size() < 3) {
            throw new IllegalArgumentException("Invalid WHERE clause: " + whereClause);
        }

        this.whereColumn = tokens.get(0);
        this.whereOperator = tokens.get(1);
        
        if (this.whereOperator.equalsIgnoreCase("BETWEEN")) {
            this.whereValue = removeQuotes(tokens.get(2));
            if (tokens.size() < 5 || !tokens.get(3).equalsIgnoreCase("AND")) {
                throw new IllegalArgumentException("Invalid BETWEEN syntax");
            }
            this.whereValue2 = removeQuotes(tokens.get(4));
        } else {
            // Check for multi-char operators like <=, >=, !=
            this.whereValue = removeQuotes(tokens.get(2));
        }
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

    public String getOperator() {
        return whereOperator;
    }

    public String getValue() {
        return whereValue;
    }

    public String getValue2() {
        return whereValue2;
    }
}

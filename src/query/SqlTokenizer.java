package query;

import java.util.ArrayList;
import java.util.List;

public class SqlTokenizer {
    private final String query;
    private int pos;

    public SqlTokenizer(String query) {
        this.query = query;
        this.pos = 0;
    }

    public List<String> tokenize() {
        List<String> tokens = new ArrayList<>();
        while (pos < query.length()) {
            char c = query.charAt(pos);

            if (Character.isWhitespace(c)) {
                pos++;
                continue;
            }

            if (c == '(' || c == ')' || c == ',' || c == ';' || c == '*') {
                tokens.add(String.valueOf(c));
                pos++;
                continue;
            }

            if (c == '\'' || c == '\"') {
                tokens.add(readString(c));
                continue;
            }

            if (c == '!' || c == '<' || c == '>' || c == '=') {
                tokens.add(readOperator());
                continue;
            }

            if (isIdentifierStart(c)) {
                tokens.add(readIdentifier());
                continue;
            }

            if (Character.isDigit(c) || c == '-') {
                tokens.add(readNumber());
                continue;
            }

            pos++; // Skip unknown characters
        }
        return tokens;
    }

    private String readString(char quote) {
        StringBuilder sb = new StringBuilder();
        // Keep the string WITHOUT quotes
        pos++; // Skip opening quote
        while (pos < query.length() && query.charAt(pos) != quote) {
            sb.append(query.charAt(pos));
            pos++;
        }
        if (pos < query.length()) pos++; // Skip closing quote
        return sb.toString();
    }

    private String readOperator() {
        StringBuilder sb = new StringBuilder();
        sb.append(query.charAt(pos++));
        if (pos < query.length()) {
            char next = query.charAt(pos);
            if (next == '=') {
                sb.append(next);
                pos++;
            }
        }
        return sb.toString();
    }

    private String readIdentifier() {
        StringBuilder sb = new StringBuilder();
        while (pos < query.length() && isIdentifierPart(query.charAt(pos))) {
            sb.append(query.charAt(pos));
            pos++;
        }
        return sb.toString();
    }

    private String readNumber() {
        StringBuilder sb = new StringBuilder();
        while (pos < query.length() && (Character.isDigit(query.charAt(pos)) || query.charAt(pos) == '.' || query.charAt(pos) == '-')) {
            sb.append(query.charAt(pos));
            pos++;
        }
        return sb.toString();
    }

    private boolean isIdentifierStart(char c) {
        return Character.isLetter(c) || c == '_';
    }

    private boolean isIdentifierPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }
}

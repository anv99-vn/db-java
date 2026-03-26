package table;

import java.util.Arrays;

/**
 * CompositeKey - A key consisting of multiple values for multi-column indexing.
 */
public class CompositeKey implements Comparable<CompositeKey> {
    private final Object[] values;

    public CompositeKey(Object... values) {
        this.values = values;
    }

    public Object[] getValues() {
        return values;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public int compareTo(CompositeKey other) {
        int len = Math.min(this.values.length, other.values.length);
        for (int i = 0; i < len; i++) {
            Comparable v1 = (Comparable) this.values[i];
            Comparable v2 = (Comparable) other.values[i];
            int cmp = v1.compareTo(v2);
            if (cmp != 0) return cmp;
        }
        return Integer.compare(this.values.length, other.values.length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompositeKey that = (CompositeKey) o;
        return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    public String toString() {
        return Arrays.toString(values);
    }
}

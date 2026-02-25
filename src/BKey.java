public class BKey<T extends Comparable<T>> implements Comparable<BKey<T>> {
    T key;

    // No-arg constructor required by Kryo for deserialization
    @SuppressWarnings("unused")
    private BKey() {
    }

    BKey(T key) {
        this.key = key;
    }

    @Override
    public int compareTo(BKey<T> o) {
        return this.key.compareTo(o.key);
    }

    public boolean isLessThan(BKey<T> other) {
        return compareTo(other) < 0;
    }

    public boolean isGreaterThan(BKey<T> other) {
        return compareTo(other) > 0;
    }

    public boolean isEqual(BKey<T> other) {
        return compareTo(other) == 0;
    }

    public boolean isLessThanOrEqualTo(BKey<T> other) {
        return compareTo(other) <= 0;
    }

    public boolean isGreaterThanOrEqualTo(BKey<T> other) {
        return compareTo(other) >= 0;
    }
}

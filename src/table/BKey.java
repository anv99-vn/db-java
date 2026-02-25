package table;

import java.util.ArrayList;
import java.util.List;

public class BKey<T extends Comparable<T>> implements Comparable<BKey<T>> {
    T key;
    private List<Long> recordPointers = new ArrayList<>();

    public BKey(T key) {
        this.key = key;
    }

    BKey(T key, long pointer) {
        this.key = key;
        this.recordPointers.add(pointer);
    }

    public T getKey() {
        return key;
    }

    public java.util.List<Long> getRecordPointers() {
        return recordPointers;
    }

    public void addPointer(long pointer) {
        if (!recordPointers.contains(pointer)) {
            recordPointers.add(pointer);
        }
    }

    public void removePointer(long pointer) {
        recordPointers.remove(pointer);
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

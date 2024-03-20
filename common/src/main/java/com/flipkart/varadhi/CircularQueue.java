package com.flipkart.varadhi;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

public class CircularQueue<T> implements Queue<T> {
    private T[] array;
    private int head = 0;
    private int tail = 0;
    private int size = 0;

    /**
     * call it with power of 2, for better perf.
     * @param initialCapacity
     */
    public CircularQueue(int initialCapacity) {
        this.array = (T[]) new Object[initialCapacity];
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean contains(Object o) {
        for (int i = 0; i < size; i++) {
            if (array[(head + i) % array.length].equals(o)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < size;
            }

            @Override
            public T next() {
                return array[(head + index++) % array.length];
            }
        };
    }

    @Override
    public Object[] toArray() {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public boolean add(T t) {
        if (size == array.length) {
            grow();
        }
        array[tail] = t;
        tail = (tail + 1) % array.length;
        size++;
        return true;
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        // if enough space is not left, then grow by twice, and then copy all elements.
        if (size + c.size() > array.length) {
            grow();
        }
        for (T t : c) {
            array[tail] = t;
            tail = (tail + 1) % array.length;
            size++;
        }
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void clear() {
        head = 0;
        tail = 0;
        size = 0;
        Arrays.fill(array, null);
    }

    @Override
    public boolean offer(T t) {
        return add(t);
    }

    @Override
    public T remove() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public T poll() {
        if (size == 0) {
            return null;
        }
        T t = array[head];
        head = (head + 1) % array.length;
        size--;
        return t;
    }

    @Override
    public T element() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public T peek() {
        if (size == 0) {
            return null;
        }
        return array[head];
    }

    private void grow() {
        T[] newArray = (T[]) new Object[array.length * 2];
        for (int i = 0; i < size; i++) {
            newArray[i] = array[(head + i) % array.length];
        }
        array = newArray;
        head = 0;
        tail = size;
    }
}

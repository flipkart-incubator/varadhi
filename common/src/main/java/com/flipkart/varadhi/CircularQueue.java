package com.flipkart.varadhi;

import org.apache.commons.collections4.queue.CircularFifoQueue;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

public class CircularQueue<T> implements Queue<T> {

    private CircularFifoQueue<T> delegate;

    public CircularQueue(int size) {
        delegate = new CircularFifoQueue<>(size);
    }

    public CircularQueue() {
        delegate = new CircularFifoQueue<>();
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return delegate.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return delegate.iterator();
    }

    @Override
    public Object[] toArray() {
        return delegate.toArray();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return delegate.toArray(a);
    }

    /**
     * If the delegate is full, then create a new queue with double the size and copy all previous elements to the new
     * queue, and then add this new element.
     *
     * @param t element whose presence in this collection is to be ensured
     * @return Must always return true.
     */
    @Override
    public boolean add(T t) {
        if (delegate.isAtFullCapacity()) {
            CircularFifoQueue<T> newDelegate = new CircularFifoQueue<>(delegate.maxSize() * 2);
            newDelegate.addAll(delegate);
            delegate = newDelegate;
        }
        return delegate.add(t);
    }

    @Override
    public boolean remove(Object o) {
        return delegate.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return delegate.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return delegate.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return delegate.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return delegate.retainAll(c);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public boolean offer(T t) {
        return add(t);
    }

    @Override
    public T remove() {
        return delegate.remove();
    }

    @Override
    public T poll() {
        return delegate.poll();
    }

    @Override
    public T element() {
        return delegate.element();
    }

    @Override
    public T peek() {
        return delegate.peek();
    }
}

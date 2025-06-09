package com.redis.spring.batch;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class UniqueBlockingQueue<E> extends LinkedBlockingQueue<E> {

    private final Set<E> set;

    public UniqueBlockingQueue() {
        this.set = Collections.synchronizedSet(new HashSet<>());
    }

    public UniqueBlockingQueue(int capacity) {
        super(capacity);
        this.set = Collections.synchronizedSet(new HashSet<>(capacity));
    }

    @Override
    public boolean offer(E e) {
        if (set.add(e)) {
            return super.offer(e);
        }
        return true;
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        if (set.add(e)) {
            return super.offer(e, timeout, unit);
        }
        return true;
    }

    @Override
    public E poll() {
        E element = super.poll();
        if (element != null) {
            set.remove(element);
        }
        return element;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E element = super.poll(timeout, unit);
        if (element != null) {
            set.remove(element);
        }
        return element;
    }

    @Override
    public void put(E e) throws InterruptedException {
        if (set.add(e)) {
            super.put(e);
        }
    }

}

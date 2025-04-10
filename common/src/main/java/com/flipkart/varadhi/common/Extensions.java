package com.flipkart.varadhi.common;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

public class Extensions {

    public static class LockExtensions {
        public static <T> T lockAndCall(Lock lock, Callable<T> task) throws Exception {
            lock.lock();
            try {
                return task.call();
            } finally {
                lock.unlock();
            }
        }

        public static <T> T lockAndSupply(Lock lock, Supplier<T> task) {
            lock.lock();
            try {
                return task.get();
            } finally {
                lock.unlock();
            }
        }

        public static void lockAndRun(Lock lock, Runnable task) {
            lock.lock();
            try {
                task.run();
            } finally {
                lock.unlock();
            }
        }
    }
}

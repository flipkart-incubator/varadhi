package com.flipkart.varadhi.consumer.concurrent;

import io.netty.util.concurrent.FastThreadLocal;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class Context {

    private static final FastThreadLocal<Context> currentThreadCtx = new FastThreadLocal<>();

    private final EventExecutor executor;

    public interface Task extends Runnable {
        Context getContext();
    }

    public void updateCurrentThreadContext() {
        Thread currentThread = Thread.currentThread();
        if (currentThread instanceof CustomThread) {
            ((CustomThread) currentThread).setContext(this);
        } else {
            currentThreadCtx.set(this);
        }
    }

    public static Context getCurrentTheadContext() {
        Thread currentThread = Thread.currentThread();
        if (currentThread instanceof CustomThread) {
            return ((CustomThread) currentThread).getContext();
        } else {
            return currentThreadCtx.get();
        }
    }
}

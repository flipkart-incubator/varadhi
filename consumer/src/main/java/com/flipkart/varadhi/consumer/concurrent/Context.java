package com.flipkart.varadhi.consumer.concurrent;

import io.netty.util.concurrent.FastThreadLocal;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class Context {

    private static final FastThreadLocal<Context> currentThreadCtx = new FastThreadLocal<>();

    @Getter(lombok.AccessLevel.PACKAGE)
    final EventExecutor executor;

    public interface Task extends Runnable {
        Context getContext();
    }

    /**
     * Update the current thread's context to this context.
     */
    public void updateCurrentThreadContext() {
        Thread currentThread = Thread.currentThread();
        if (currentThread instanceof CustomThread) {
            ((CustomThread) currentThread).setContext(this);
        } else {
            currentThreadCtx.set(this);
        }
    }

    public static Context getCurrentThreadContext() {
        Thread currentThread = Thread.currentThread();
        if (currentThread instanceof CustomThread) {
            return ((CustomThread) currentThread).getContext();
        } else {
            return currentThreadCtx.get();
        }
    }

    public boolean isInContext() {
        return Thread.currentThread() == executor.getThread();
    }

    public Task wrap(Runnable runnable) {
        return new Task() {
            @Override
            public Context getContext() {
                return Context.this;
            }

            @Override
            public void run() {
                runnable.run();
            }
        };
    }

    /**
     * It makes sure that the task is run on the thread bound to the context. If already on the context thread, it
     * runs the task directly.
     *
     * @param runnable
     */
    public void executeOnContext(Runnable runnable) {
        if (isInContext()) {
            runnable.run();
        } else {
            executor.execute(wrap(runnable));
        }
    }

    public void execute(Task task) {
        executor.execute(task);
    }
}

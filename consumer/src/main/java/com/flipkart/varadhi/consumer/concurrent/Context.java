package com.flipkart.varadhi.consumer.concurrent;

import com.flipkart.varadhi.SafeCallable;
import io.netty.util.concurrent.FastThreadLocal;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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
    public void runOnContext(Runnable runnable) {
        if (isInContext()) {
            runnable.run();
        } else {
            executor.execute(wrap(runnable));
        }
    }

    public <T> CompletableFuture<T> executeOnContext(SafeCallable<T> callable) {
        if (isInContext()) {
            return CompletableFuture.completedFuture(callable.call());
        } else {
            CompletableFuture<T> promise = new CompletableFuture<>();
            executor.execute(wrap(() -> {
                try {
                    promise.complete(callable.call());
                } catch (Exception e) {
                    promise.completeExceptionally(e);
                }
            }));
            return promise;
        }
    }

    public ScheduledFuture<?> scheduleOnContext(Runnable command, long delay, TimeUnit unit) {
        return executor.schedule(wrap(command), delay, unit);
    }

    public void run(Task task) {
        executor.execute(task);
    }
}

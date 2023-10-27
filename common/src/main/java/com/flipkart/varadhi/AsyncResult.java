package com.flipkart.varadhi;

public class AsyncResult<T> {
    private final T result;
    private final Throwable cause;

    private AsyncResult(T result, Throwable cause) {
        this.result = result;
        this.cause = cause;
    }

    public static <T> AsyncResult<T> of(T result, Throwable cause) {
        return new AsyncResult<>(result, cause);
    }

    public static <T> AsyncResult<T> of(T result) {
        return AsyncResult.of(result, null);
    }

    public static AsyncResult of(Throwable cause) {
        return AsyncResult.of(null, cause);
    }

    public boolean hasResult() {
        return result != null;
    }

    public boolean hasThrowable() {
        return cause != null;
    }

    public T result() {
        return result;
    }

    public Throwable cause() {
        return cause;
    }
}

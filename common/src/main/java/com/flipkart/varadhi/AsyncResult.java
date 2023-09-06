package com.flipkart.varadhi;

public class AsyncResult<T> {
    private T result;
    private Throwable cause;

    public static <T> AsyncResult<T> of(T result, Throwable cause) {
        AsyncResult<T> asyncResult = new AsyncResult<>();
        asyncResult.result = result;
        asyncResult.cause = cause;
        return asyncResult;
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

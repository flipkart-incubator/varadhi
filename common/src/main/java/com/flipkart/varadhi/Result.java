package com.flipkart.varadhi;

public class Result<T> {
    private final T result;
    private final Throwable cause;

    private Result(T result, Throwable cause) {
        this.result = result;
        this.cause = cause;
    }

    public static <T> Result<T> of(T result, Throwable cause) {
        if (result != null && cause != null) {
            throw new IllegalArgumentException("Both result and cause can't be non null.");
        }
        return new Result<>(result, cause);
    }

    public static <T> Result<T> of(T result) {
        return Result.of(result, null);
    }

    public static Result of(Throwable cause) {
        return Result.of(null, cause);
    }

    public boolean hasResult() {
        return result != null;
    }

    public boolean hasFailed() {
        return cause != null;
    }

    public T result() {
        return result;
    }

    public Throwable cause() {
        return cause;
    }
}

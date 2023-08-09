package com.flipkart.varadhi;


public class ExceptionOrResult<E extends Exception, R> {
    private final E exception;
    private final R result;

    private ExceptionOrResult(E exception, R result) {
        this.exception = exception;
        this.result = result;
    }

    public static <E extends Exception, R> ExceptionOrResult<E, R> Result(R result) {
        return new ExceptionOrResult<>(null, result);
    }

    public static <E extends Exception, R> ExceptionOrResult<E, R> Failure(E exception) {
        return new ExceptionOrResult<>(exception, null);
    }

    public boolean hasResult() {
        return result != null;
    }

    public R getResult() {
        return result;
    }

    public E getException() {
        return exception;
    }
}

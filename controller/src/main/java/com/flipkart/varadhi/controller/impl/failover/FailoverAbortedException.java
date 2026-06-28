package com.flipkart.varadhi.controller.impl.failover;

/**
 * Raised to fail an in-flight stage barrier when a topic failover is aborted (by an operator or a
 * pre-flight failure). It unwinds the executor's stage chain so the operation terminates without
 * applying further changes.
 */
public class FailoverAbortedException extends RuntimeException {
    public FailoverAbortedException(String message) {
        super(message);
    }
}

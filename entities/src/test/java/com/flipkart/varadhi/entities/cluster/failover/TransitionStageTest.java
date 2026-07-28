package com.flipkart.varadhi.entities.cluster.failover;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransitionStageTest {

    @Test
    void isTerminal_onlyCompletedAndAborted() {
        assertFalse(TransitionStage.PENDING.isTerminal());
        assertFalse(TransitionStage.PREPARE.isTerminal());
        assertFalse(TransitionStage.SWITCH.isTerminal());
        assertFalse(TransitionStage.DRAIN.isTerminal());
        assertTrue(TransitionStage.COMPLETED.isTerminal());
        assertTrue(TransitionStage.ABORTED.isTerminal());
    }
}

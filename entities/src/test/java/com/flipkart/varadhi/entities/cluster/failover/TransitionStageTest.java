package com.flipkart.varadhi.entities.cluster.failover;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransitionStageTest {

    @Test
    void isTerminal_onlyCompletedAndAborted() {
        assertFalse(TransitionStage.PENDING.isTerminal());
        assertFalse(TransitionStage.PREPARE.isTerminal());
        assertFalse(TransitionStage.FENCE.isTerminal());
        assertFalse(TransitionStage.MIGRATE.isTerminal());
        assertTrue(TransitionStage.COMPLETED.isTerminal());
        assertTrue(TransitionStage.ABORTED.isTerminal());
    }

    @Test
    void isAbortable_throughFenceAndMigrate() {
        assertTrue(TransitionStage.PENDING.isAbortable());
        assertTrue(TransitionStage.PREPARE.isAbortable());
        assertTrue(TransitionStage.FENCE.isAbortable());
        assertTrue(TransitionStage.MIGRATE.isAbortable());
        assertFalse(TransitionStage.COMPLETED.isAbortable());
        assertFalse(TransitionStage.ABORTED.isAbortable());
    }

    @Test
    void needsTopicVersionSync_prepareFenceMigrate() {
        assertTrue(TransitionStage.PREPARE.needsTopicVersionSync());
        assertTrue(TransitionStage.FENCE.needsTopicVersionSync());
        assertTrue(TransitionStage.MIGRATE.needsTopicVersionSync());
        assertFalse(TransitionStage.COMPLETED.needsTopicVersionSync());
    }
}

package com.flipkart.varadhi.common.utils;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Non-blocking, deadline-bounded polling helper that runs entirely on a caller-supplied
 * {@link ScheduledExecutorService} — no thread is ever blocked between attempts.
 *
 * <p>It repeatedly evaluates a {@code probe} until it yields a terminal result, the deadline
 * passes, or the probe throws. Exactly one terminal callback fires:
 * <ul>
 *   <li>{@code onResult} — the first time {@code probe} returns a non-empty {@link Optional};</li>
 *   <li>{@code onTimeout} — when {@code probe} is still empty at/after {@code deadlineMs};</li>
 *   <li>{@code onError} — if {@code probe} throws (the loop stops).</li>
 * </ul>
 *
 * <p>This centralizes the "poll-on-a-scheduler-until-condition-or-deadline" mechanics so callers
 * keep only their domain probe/handlers and do not hand-roll rescheduling.
 */
public final class ScheduledPoller {

    private ScheduledPoller() {
    }

    /**
     * Starts a poll loop. The first attempt is submitted immediately to {@code scheduler};
     * subsequent attempts are scheduled {@code intervalMs} apart.
     *
     * @param scheduler  executor the probe and rescheduling run on
     * @param probe      evaluated each attempt; non-empty result terminates the loop
     * @param intervalMs delay between attempts, in milliseconds
     * @param deadlineMs absolute wall-clock deadline ({@code System.currentTimeMillis()} basis)
     * @param onResult   invoked with the terminal probe result
     * @param onTimeout  invoked when the deadline is reached before a terminal result
     * @param onError    invoked if the probe throws
     * @param <R>        the terminal result type
     */
    public static <R> void pollUntil(
        ScheduledExecutorService scheduler,
        Supplier<Optional<R>> probe,
        long intervalMs,
        long deadlineMs,
        Consumer<? super R> onResult,
        Runnable onTimeout,
        Consumer<? super Exception> onError
    ) {
        scheduler.execute(new PollTask<>(scheduler, probe, intervalMs, deadlineMs, onResult, onTimeout, onError));
    }

    private static final class PollTask<R> implements Runnable {
        private final ScheduledExecutorService scheduler;
        private final Supplier<Optional<R>> probe;
        private final long intervalMs;
        private final long deadlineMs;
        private final Consumer<? super R> onResult;
        private final Runnable onTimeout;
        private final Consumer<? super Exception> onError;

        private PollTask(
            ScheduledExecutorService scheduler,
            Supplier<Optional<R>> probe,
            long intervalMs,
            long deadlineMs,
            Consumer<? super R> onResult,
            Runnable onTimeout,
            Consumer<? super Exception> onError
        ) {
            this.scheduler = scheduler;
            this.probe = probe;
            this.intervalMs = intervalMs;
            this.deadlineMs = deadlineMs;
            this.onResult = onResult;
            this.onTimeout = onTimeout;
            this.onError = onError;
        }

        @Override
        public void run() {
            try {
                Optional<R> outcome = probe.get();
                if (outcome.isPresent()) {
                    onResult.accept(outcome.get());
                    return;
                }
                if (System.currentTimeMillis() >= deadlineMs) {
                    onTimeout.run();
                    return;
                }
                scheduler.schedule(this, intervalMs, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                onError.accept(e);
            }
        }
    }
}

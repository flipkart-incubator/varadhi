package com.flipkart.varadhi.consumer.concurrent;

import io.netty.util.concurrent.FastThreadLocalThread;
import lombok.Getter;
import lombok.Setter;

/**
 * A thread can be managing or running tasks from many contexts.
 */
public class CustomThread extends FastThreadLocalThread {

    /**
     * The context which this thread is currently running tasks for.
     */
    @Getter
    @Setter
    private Context context;

    public CustomThread(Runnable target) {
        super(target);
    }
}

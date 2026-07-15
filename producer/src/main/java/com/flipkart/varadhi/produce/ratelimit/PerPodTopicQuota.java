package com.flipkart.varadhi.produce.ratelimit;

/**
 * This pod's enforceable produce quota for one topic.
 *
 * @param qpsQuota permits per second
 * @param bytesQuota bytes per second
 */
public record PerPodTopicQuota(int qpsQuota, long bytesQuota) {
}

package com.flipkart.varadhi.produce.ratelimit;

import com.flipkart.varadhi.entities.VaradhiTopic;

/**
 * Source of each pod's per-topic quota. v1 uses {@link EvenSplitPerPodTopicQuotaProvider}; a future
 * coordinator plugs in as another implementation without changing buckets or the produce hook.
 */
public interface PerPodTopicQuotaProvider {

    PerPodTopicQuota quotaFor(VaradhiTopic topic);
}

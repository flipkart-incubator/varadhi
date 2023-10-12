package com.flipkart.varadhi.produce.services;

import com.flipkart.varadhi.core.VaradhiTopicService;
import com.flipkart.varadhi.entities.InternalTopic;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.exceptions.ProduceException;
import com.flipkart.varadhi.exceptions.VaradhiException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class InternalTopicCache {

    //TODO::Add topic event listener kind of semantics for updating topic status (blocked/throttled) etc.
    private final Cache<String, VaradhiTopic> varadhiTopicCache;
    private final VaradhiTopicService varadhiTopicService;

    public InternalTopicCache(VaradhiTopicService varadhiTopicService, String topicCacheBuilderSpec) {
        this.varadhiTopicService = varadhiTopicService;
        // If no specification, this will create a default cache of unlimited size and not expiring.
        this.varadhiTopicCache = CacheBuilder.from(topicCacheBuilderSpec).build();
    }

    public InternalTopic getProduceTopicForRegion(String varadhiTopicName, String region) {
        try {
            VaradhiTopic varadhiTopic = this.varadhiTopicCache.get(varadhiTopicName, () ->
                    this.varadhiTopicService.get(varadhiTopicName));
            return varadhiTopic.getProduceTopicForRegion(region);
        } catch (Exception e) {
            Throwable realFailure = e.getCause() == null ? e : e.getCause();
            if (realFailure instanceof VaradhiException) {
                throw (VaradhiException) realFailure;
            }
            throw new ProduceException(
                    String.format("Failed to get Produce Topic(%s): %s", varadhiTopicName, realFailure.getMessage()),
                    realFailure
            );
        }
    }

}

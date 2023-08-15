package com.flipkart.varadhi.produce.services;

import com.flipkart.varadhi.core.VaradhiTopicService;
import com.flipkart.varadhi.entities.InternalTopic;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.ExecutionException;

public class InternalTopicCache {

    //TODO::Add topic event listener kind of semantics for updating topic status (blocked/throttled) etc.
    private final Cache<String, VaradhiTopic> varadhiTopicCache;
    private final VaradhiTopicService varadhiTopicService;

    public InternalTopicCache(VaradhiTopicService varadhiTopicService, String topicCacheBuilderSpec) {
        this.varadhiTopicService = varadhiTopicService;
        // If no specification, this will create a default cache of unlimited size and not expiring.
        this.varadhiTopicCache = CacheBuilder.from(topicCacheBuilderSpec).build();
    }

    public InternalTopic getProduceTopicForRegion(String varadhiTopicName, String region) throws ExecutionException {
        VaradhiTopic varadhiTopic = this.varadhiTopicCache.get(varadhiTopicName, () ->
                this.varadhiTopicService.get(varadhiTopicName));
        return varadhiTopic.getProduceTopicForRegion(region);
    }

}

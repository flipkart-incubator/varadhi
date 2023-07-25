package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.InternalTopic;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.exceptions.VaradhiException;
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

    public InternalTopic getInternalMainTopicForRegion(String varadhiTopicName, String region) {
        try {
            VaradhiTopic varadhiTopic = this.varadhiTopicCache.get(varadhiTopicName, () ->
                    this.varadhiTopicService.get(varadhiTopicName));
            return varadhiTopic.getInternalMainTopic(varadhiTopicName, region);
        } catch (ExecutionException e) {
            throw new VaradhiException(e);
        }
    }

}

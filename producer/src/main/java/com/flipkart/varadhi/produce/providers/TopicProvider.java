package com.flipkart.varadhi.produce.providers;

import com.flipkart.varadhi.common.EntityProvider;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.spi.db.MetaStore;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;

/**
 * Provider for Varadhi topic entities with caching capabilities.
 * <p>
 * This class extends the generic {@link EntityProvider} to provide specialized
 * topic management functionality. It maintains an in-memory cache of topics
 * that can be preloaded and dynamically updated through entity events.
 * <p>
 * The provider is designed for high-performance, thread-safe access to topic
 * information in a distributed environment. It supports:
 * <ul>
 *   <li>Efficient topic lookup by name</li>
 *   <li>Preloading topics from the metadata store</li>
 *   <li>Automatic cache updates via entity events</li>
 * </ul>
 *
 * @see EntityProvider
 * @see VaradhiTopic
 */
@Slf4j
public class TopicProvider extends EntityProvider<VaradhiTopic> {

    /**
     * The metadata store used to load topics.
     */
    private final MetaStore metaStore;

    /**
     * Creates a new TopicProvider with the specified metadata store.
     *
     * @param metaStore the metadata store to load topics from
     * @throws NullPointerException if metaStore is null
     */
    public TopicProvider(MetaStore metaStore) {
        super(ResourceType.TOPIC, "topic");
        this.metaStore = Objects.requireNonNull(metaStore, "MetaStore cannot be null");
    }

    /**
     * {@inheritDoc}
     * <p>
     * Loads all topics from the metadata store.
     *
     * @return a list of all topics
     */
    @Override
    protected List<VaradhiTopic> loadAllEntities() {
        return metaStore.getAllTopics();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the name of the given topic.
     *
     * @param topic the topic to get the name for
     * @return the name of the topic
     */
    @Override
    protected String getEntityName(VaradhiTopic topic) {
        return topic.getName();
    }
}

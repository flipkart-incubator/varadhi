package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.EventMarker;
import com.flipkart.varadhi.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.EventStore;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import lombok.Getter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.retry.RetryNTimes;

import java.util.List;

import static com.flipkart.varadhi.db.ZNode.EVENT;

/**
 * Implementation of {@link EventStore} that uses ZooKeeper as the underlying storage mechanism.
 * This class provides functionality to store and manage events in a distributed system using ZooKeeper
 * for persistence and coordination.
 *
 * <p>The implementation uses the following ZooKeeper features:
 * <ul>
 *     <li>Persistent ZNodes for event storage</li>
 *     <li>Distributed atomic counter for sequence generation</li>
 *     <li>Curator cache for event notifications</li>
 * </ul>
 *
 * <p>The events are stored in the following ZooKeeper path structure:
 * <ul>
 *     <li>Events: /varadhi/entities/Event/[eventName]</li>
 *     <li>Sequence: /varadhi/sequences/Event</li>
 * </ul>
 */
public class EventStoreImpl implements EventStore {
    private static final int RETRY_COUNT = 10;
    private static final int RETRY_SLEEP_MS = 1000;

    private final ZKMetaStore zkMetaStore;
    private final DistributedAtomicLong sequenceGenerator;

    @Getter
    private final CuratorCache curatorCache;

    /**
     * Creates a new event store instance with the given ZooKeeper curator framework.
     *
     * @param zkCurator The curator framework instance for ZooKeeper operations
     * @throws MetaStoreException if initialization of required ZooKeeper paths fails
     */
    public EventStoreImpl(CuratorFramework zkCurator) {
        this.zkMetaStore = new ZKMetaStore(zkCurator);
        ensureEntityTypePathExists();

        this.sequenceGenerator = new DistributedAtomicLong(
            zkCurator,
            ZNode.ofSequence(EVENT).getPath(),
            new RetryNTimes(RETRY_COUNT, RETRY_SLEEP_MS)
        );

        this.curatorCache = CuratorCache.build(zkCurator, ZNode.ofEntityType(EVENT).getPath());
    }

    /**
     * Ensures that required ZooKeeper paths exist.
     * Creates the base paths for events and sequences if they don't exist.
     *
     * @throws MetaStoreException if path creation fails
     */
    private void ensureEntityTypePathExists() {
        zkMetaStore.createZNode(ZNode.ofEntityType(EVENT));
        zkMetaStore.createZNode(ZNode.ofSequence(EVENT));
    }

    /**
     * Creates a new event in the store.
     *
     * @param event The event to create
     * @throws MetaStoreException         if the event creation fails
     * @throws DuplicateResourceException if an event with the same name already exists
     */
    @Override
    public void createEvent(EventMarker event) {
        ZNode znode = ZNode.ofEvent(event.getName());
        zkMetaStore.createZNodeWithData(znode, event);
    }

    /**
     * Retrieves an event by its ID.
     *
     * @param eventName The name of the event to retrieve
     * @return The event if found
     * @throws ResourceNotFoundException if the event does not exist
     * @throws MetaStoreException        if the retrieval operation fails
     */
    @Override
    public EventMarker getEvent(String eventName) {
        ZNode znode = ZNode.ofEvent(eventName);
        return zkMetaStore.getZNodeDataAsPojo(znode, EventMarker.class);
    }

    /**
     * Retrieves all pending events in the system.
     *
     * @return A list of all pending events
     * @throws MetaStoreException if the retrieval operation fails
     */
    @Override
    public List<EventMarker> getPendingEvents() {
        return zkMetaStore.listChildren(ZNode.ofEntityType(EVENT)).stream().map(this::getEvent).toList();
    }

    /**
     * Deletes an event from the store.
     *
     * @param eventName The name of the event to delete
     * @throws ResourceNotFoundException if the event does not exist
     * @throws MetaStoreException        if the deletion operation fails
     */
    @Override
    public void deleteEvent(String eventName) {
        ZNode znode = ZNode.ofEvent(eventName);
        zkMetaStore.deleteZNode(znode);
    }

    /**
     * Generates the next sequence number for events in a distributed manner.
     * Uses ZooKeeper's distributed atomic counter to ensure uniqueness across the cluster.
     *
     * @return The next sequence number
     * @throws MetaStoreException if sequence generation fails after the configured number of retries
     */
    @Override
    public long getNextSequenceNumber() {
        try {
            AtomicValue<Long> value = sequenceGenerator.increment();
            if (value.succeeded()) {
                return value.postValue();
            }
            throw new MetaStoreException("Failed to get next sequence number after " + RETRY_COUNT + " attempts");
        } catch (Exception e) {
            throw new MetaStoreException("Failed to get next sequence number", e);
        }
    }

    /**
     * Returns the CuratorCache instance used for event notifications.
     *
     * @return A CuratorCache instance as an Object. Callers should cast this to
     *         {@code org.apache.curator.framework.recipes.cache.CuratorCache}
     */
    @Override
    public Object getEventCache() {
        return getCuratorCache();
    }
}

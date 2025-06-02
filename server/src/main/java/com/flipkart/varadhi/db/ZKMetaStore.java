package com.flipkart.varadhi.db;

import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.JsonMapper;
import com.flipkart.varadhi.db.entities.ZKMetadataEvent;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.MetaStoreEntityType;
import com.flipkart.varadhi.spi.db.MetaStoreEventListener;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.api.transaction.OperationType;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.flipkart.varadhi.db.ZNode.EVENT;

/**
 * A ZooKeeper-based implementation of the metadata store.
 *
 * <p>This class provides persistent storage and retrieval of metadata entities using ZooKeeper,
 * with support for:
 * <ul>
 *   <li>Versioned entities</li>
 *   <li>Atomic transactions</li>
 *   <li>Event notifications</li>
 *   <li>Hierarchical data organization</li>
 * </ul>
 *
 * <h2>Version Management</h2>
 * <p>For entities extending {@code MetaStoreEntity}, version management has the following characteristics:
 * <ul>
 *   <li>ZooKeeper's stat object version is considered the source of truth</li>
 *   <li>Data object versions are always overwritten with stat versions during create/get/update operations</li>
 *   <li>Manual updates to ZooKeeper will automatically increment versions without client awareness</li>
 * </ul>
 *
 * <h2>Event Tracking</h2>
 * <p>Changes to resources can be tracked through event notifications, which are implemented using
 * sequential ZNodes under a dedicated event path.
 *
 * @see MetaStoreEntity
 * @see CuratorFramework
 * @see CuratorCache
 */
@Slf4j
public class ZKMetaStore implements AutoCloseable {

    private final CuratorFramework zkCurator;
    private final CuratorCache eventCache;

    /**
     * Constants for event node naming and structure.
     */
    private static final String EVENT_PREFIX = "event";
    private static final String EVENT_DELIMITER = "-";
    private static final String LISTENER_NODE = "change_event_listener";

    /**
     * Constructs a new ZKMetaStore instance.
     *
     * <p>Initializes the store with a ZooKeeper curator framework and sets up event tracking
     * infrastructure. The event cache is initialized but not started - it will be started
     * when the first event listener is registered.
     *
     * @param zkCurator The curator framework instance for ZooKeeper operations
     * @throws IllegalArgumentException if zkCurator is null
     */
    public ZKMetaStore(CuratorFramework zkCurator) {
        this.zkCurator = Objects.requireNonNull(zkCurator, "zkCurator must not be null");
        this.eventCache = CuratorCache.build(zkCurator, ZNode.ofEntityType(EVENT).getPath());
    }

    /**
     * Creates a new ZNode in ZooKeeper if it doesn't already exist.
     * This method will create parent nodes if they don't exist.
     *
     * @param znode The ZNode to create
     * @throws MetaStoreException if creation fails
     */
    void createZNode(ZNode znode) {
        try {
            if (!zkPathExist(znode)) {
                zkCurator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(znode.getPath());
                log.debug("Created ZNode: {}", znode);
            }
        } catch (Exception e) {
            throw new MetaStoreException(String.format("Failed to create ZNode at path %s", znode.getPath()), e);
        }
    }

    /**
     * Creates a new ZNode with associated data.
     *
     * @param <T>        Type of the MetaStoreEntity
     * @param znode      The ZNode to create
     * @param dataObject The entity to store in the ZNode
     * @throws DuplicateResourceException if the ZNode already exists
     * @throws MetaStoreException         if creation fails
     */
    <T extends MetaStoreEntity> void createZNodeWithData(ZNode znode, T dataObject) {
        try {
            byte[] jsonData = JsonMapper.jsonSerialize(dataObject).getBytes(StandardCharsets.UTF_8);
            zkCurator.create().withMode(CreateMode.PERSISTENT).forPath(znode.getPath(), jsonData);

            dataObject.setVersion(0);
            log.debug("Created ZNode {} with data", znode);
        } catch (Exception e) {
            handleCreateException(znode, e);
        }
    }

    /**
     * Creates a new ZNode with data and tracks the creation with an event node.
     * This operation is performed atomically in a transaction.
     *
     * @param <T>          Type of the MetaStoreEntity
     * @param znode        The ZNode to create
     * @param dataObject   The entity to store in the ZNode
     * @param metaStoreEntityType The type of resource being created
     * @throws DuplicateResourceException if the ZNode already exists
     * @throws MetaStoreException         if creation fails
     */
    public <T extends MetaStoreEntity> void createTrackedZNodeWithData(
        ZNode znode,
        T dataObject,
        MetaStoreEntityType metaStoreEntityType
    ) {
        try {
            byte[] jsonData = JsonMapper.jsonSerialize(dataObject).getBytes(StandardCharsets.UTF_8);
            var ops = List.of(
                zkCurator.transactionOp().create().withMode(CreateMode.PERSISTENT).forPath(znode.getPath(), jsonData),
                createChangeEventZNode(znode.getName(), metaStoreEntityType)
            );

            zkCurator.transaction().forOperations(ops);
            dataObject.setVersion(0);
        } catch (Exception e) {
            handleCreateException(znode, e);
        }
    }

    /**
     * Handles exceptions that occur during ZNode creation operations.
     * Translates ZooKeeper exceptions into appropriate application exceptions.
     *
     * @param znode The ZNode that was being created
     * @param e     The exception that occurred
     * @throws DuplicateResourceException if the ZNode already exists
     * @throws MetaStoreException         for all other errors
     */
    private void handleCreateException(ZNode znode, Exception e) {
        if (e instanceof KeeperException.NodeExistsException) {
            throw new DuplicateResourceException(
                String.format("%s(%s) already exists.", znode.getKind(), znode.getName()),
                e
            );
        }

        throw new MetaStoreException(
            String.format("Failed to create %s(%s) at %s", znode.getKind(), znode.getName(), znode.getPath()),
            e
        );
    }

    /**
     * Updates the data of an existing ZNode with the provided entity.
     *
     * @param <T>        Type of the MetaStoreEntity
     * @param znode      The ZNode to update
     * @param dataObject The entity containing the new data
     * @throws ResourceNotFoundException            if the ZNode does not exist
     * @throws InvalidOperationForResourceException if there's a version conflict
     * @throws MetaStoreException                   for other update failures
     */
    <T extends MetaStoreEntity> void updateZNodeWithData(ZNode znode, T dataObject) {
        try {
            byte[] jsonData = JsonMapper.jsonSerialize(dataObject).getBytes(StandardCharsets.UTF_8);
            var stat = zkCurator.setData().withVersion(dataObject.getVersion()).forPath(znode.getPath(), jsonData);

            dataObject.setVersion(stat.getVersion());
            log.debug(
                "Updated {}({}) at {}: version {}",
                znode.getKind(),
                znode.getName(),
                znode.getPath(),
                stat.getVersion()
            );
        } catch (Exception e) {
            handleUpdateException(znode, e);
        }
    }

    /**
     * Updates a ZNode's data and creates an event node to track the update.
     * This operation is performed atomically in a transaction.
     *
     * @param <T>          Type of the MetaStoreEntity
     * @param znode        The ZNode to update
     * @param dataObject   The entity containing the new data
     * @param metaStoreEntityType The type of resource being updated
     * @throws ResourceNotFoundException            if the ZNode does not exist
     * @throws InvalidOperationForResourceException if there's a version conflict
     * @throws MetaStoreException                   for other update failures
     */
    public <T extends MetaStoreEntity> void updateTrackedZNodeWithData(
        ZNode znode,
        T dataObject,
        MetaStoreEntityType metaStoreEntityType
    ) {
        try {
            byte[] jsonData = JsonMapper.jsonSerialize(dataObject).getBytes(StandardCharsets.UTF_8);

            var ops = List.of(
                zkCurator.transactionOp()
                         .setData()
                         .withVersion(dataObject.getVersion())
                         .forPath(znode.getPath(), jsonData),
                createChangeEventZNode(znode.getName(), metaStoreEntityType)
            );

            var results = zkCurator.transaction().forOperations(ops);
            updateDataObjectVersion(dataObject, results);
        } catch (Exception e) {
            handleUpdateException(znode, e);
        }
    }

    /**
     * Updates the version of the data object based on the transaction results.
     * Finds the SET_DATA operation result and updates the entity version accordingly.
     *
     * @param <T>        Type of the MetaStoreEntity
     * @param dataObject The entity to update version for
     * @param results    List of transaction operation results
     */
    private <T extends MetaStoreEntity> void updateDataObjectVersion(
        T dataObject,
        List<CuratorTransactionResult> results
    ) {
        results.stream()
               .filter(r -> OperationType.SET_DATA.equals(r.getType()))
               .findFirst()
               .ifPresent(r -> dataObject.setVersion(r.getResultStat().getVersion()));
    }

    /**
     * Handles exceptions that occur during ZNode update operations.
     * Translates ZooKeeper exceptions into appropriate application exceptions.
     *
     * @param znode The ZNode that was being operated on
     * @param e     The exception that occurred
     * @throws ResourceNotFoundException            if the ZNode does not exist
     * @throws InvalidOperationForResourceException if there's a version conflict
     * @throws MetaStoreException                   for all other errors
     */
    private void handleUpdateException(ZNode znode, Exception e) {
        if (e instanceof KeeperException.NoNodeException) {
            throw new ResourceNotFoundException(
                String.format("%s(%s) not found.", znode.getKind(), znode.getName()),
                e
            );
        }

        if (e instanceof KeeperException.BadVersionException) {
            throw new InvalidOperationForResourceException(
                String.format(
                    "Conflicting update, %s(%s) has been modified. Fetch latest and try again.",
                    znode.getKind(),
                    znode.getName()
                ),
                e
            );
        }

        throw new MetaStoreException(
            String.format("Failed to update %s(%s) at %s", znode.getKind(), znode.getName(), znode.getPath()),
            e
        );
    }

    /**
     * Retrieves and deserializes data from a ZNode into a specified POJO type.
     *
     * @param <T>       The type of MetaStoreEntity to deserialize into
     * @param znode     The ZNode to retrieve data from
     * @param pojoClazz The class object representing the target POJO type
     * @return Deserialized instance of the specified type
     * @throws ResourceNotFoundException if the ZNode does not exist
     * @throws MetaStoreException        if there's an error retrieving or deserializing the data
     */
    <T extends MetaStoreEntity> T getZNodeDataAsPojo(ZNode znode, Class<T> pojoClazz) {
        try {
            var stat = new Stat();
            byte[] jsonData = zkCurator.getData().storingStatIn(stat).forPath(znode.getPath());
            var res = JsonMapper.jsonDeserialize(new String(jsonData, StandardCharsets.UTF_8), pojoClazz);
            res.setVersion(stat.getVersion());
            return res;
        } catch (KeeperException.NoNodeException e) {
            throw new ResourceNotFoundException(
                String.format("%s(%s) not found.", znode.getKind(), znode.getName()),
                e
            );
        } catch (Exception e) {
            throw new MetaStoreException(
                String.format("Failed to find %s(%s) at %s.", znode.getKind(), znode.getName(), znode.getPath()),
                e
            );
        }
    }

    /**
     * Checks if a ZNode exists at the specified path.
     *
     * @param znode The ZNode to check
     * @return true if the ZNode exists, false otherwise
     * @throws MetaStoreException if there's an error checking the ZNode existence
     */
    boolean zkPathExist(ZNode znode) {
        try {
            return zkCurator.checkExists().forPath(znode.getPath()) != null;
        } catch (Exception e) {
            throw new MetaStoreException(
                String.format("Failed to check existence of %s at %s", znode.getName(), znode.getPath()),
                e
            );
        }
    }

    /**
     * Deletes a ZNode from ZooKeeper.
     *
     * @param znode The ZNode to delete
     * @throws ResourceNotFoundException if the ZNode does not exist
     * @throws MetaStoreException        if there's an error during deletion
     */
    void deleteZNode(ZNode znode) {
        try {
            zkCurator.delete().forPath(znode.getPath());
        } catch (Exception e) {
            handleDeleteException(znode, e);
        }
    }

    /**
     * Deletes a ZNode and creates an event node to track the deletion.
     * This operation is performed atomically in a transaction.
     *
     * @param znode        The ZNode to delete
     * @param metaStoreEntityType The type of resource being deleted
     * @throws ResourceNotFoundException if the ZNode does not exist
     * @throws MetaStoreException        if there's an error during the transaction
     */
    public void deleteTrackedZNode(ZNode znode, MetaStoreEntityType metaStoreEntityType) {
        try {
            var ops = List.of(
                zkCurator.transactionOp().delete().forPath(znode.getPath()),
                createChangeEventZNode(znode.getName(), metaStoreEntityType)
            );
            zkCurator.transaction().forOperations(ops);
        } catch (Exception e) {
            handleDeleteException(znode, e);
        }
    }

    /**
     * Handles exceptions that occur during ZNode deletion operations.
     * Translates ZooKeeper exceptions into appropriate application exceptions.
     *
     * @param znode The ZNode that was being operated on
     * @param e     The exception that occurred
     * @throws ResourceNotFoundException if the ZNode does not exist
     * @throws MetaStoreException        for all other errors
     */
    private void handleDeleteException(ZNode znode, Exception e) {
        if (e instanceof KeeperException.NoNodeException) {
            throw new ResourceNotFoundException(
                String.format("%s(%s) not found.", znode.getKind(), znode.getName()),
                e
            );
        }

        throw new MetaStoreException(
            String.format("Failed to delete %s(%s) at %s", znode.getKind(), znode.getName(), znode.getPath()),
            e
        );
    }

    /**
     * Lists all child nodes under the specified ZNode.
     *
     * @param znode The parent ZNode whose children are to be listed
     * @return List of child node names
     * @throws ResourceNotFoundException if the parent ZNode does not exist
     * @throws MetaStoreException        if there's an error retrieving the children
     */
    List<String> listChildren(ZNode znode) {
        if (!zkPathExist(znode)) {
            throw new ResourceNotFoundException(
                String.format("Path(%s) not found for entity %s.", znode.getPath(), znode.getName())
            );
        }
        try {
            return zkCurator.getChildren().forPath(znode.getPath());
        } catch (Exception e) {
            throw new MetaStoreException(
                String.format(
                    "Failed to list children for entity type %s at path %s.",
                    znode.getName(),
                    znode.getPath()
                ),
                e
            );
        }
    }

    /**
     * Executes multiple ZNode operations in a single atomic transaction.
     * All operations will either succeed or fail together - there is no partial success.
     *
     * <p>While the transaction itself is atomic, multiple operations might fail for different
     * reasons. All such failures are logged before throwing an exception.
     *
     * @param toAdd List of ZNodes to create
     * @param toDelete List of ZNodes to delete
     * @throws MetaStoreException if any operation in the transaction fails
     */
    public void executeInTransaction(List<ZNode> toAdd, List<ZNode> toDelete) {
        if (toAdd.isEmpty() && toDelete.isEmpty()) {
            return;
        }

        var ops = new ArrayList<CuratorOp>(toAdd.size() + toDelete.size());

        try {
            toAdd.forEach(zNode -> ops.add(addCreateZNodeOp(zNode)));
            toDelete.forEach(zNode -> ops.add(addDeleteZNodeOp(zNode)));

            var results = zkCurator.transaction().forOperations(ops);
            logFailedOperations(results);
        } catch (KeeperException e) {
            logKeeperExceptionDetails(e, ops);
            throw new MetaStoreException(String.format("Transaction failed with %d operations", ops.size()), e);
        } catch (Exception e) {
            throw new MetaStoreException(String.format("Transaction failed with %d operations", ops.size()), e);
        }
    }

    /**
     * Logs any failed operations from the transaction results.
     * Note: If any operation failed, the entire transaction was rolled back.
     *
     * @param results List of transaction operation results
     */
    private void logFailedOperations(List<CuratorTransactionResult> results) {
        var failedOps = results.stream().filter(r -> r.getError() != 0).toList();

        if (!failedOps.isEmpty()) {
            log.error("Transaction rolled back. Failed operations:");
            failedOps.forEach(
                r -> log.error(
                    "Operation failed - Type: {}, Path: {}, Error: {}, Result Path: {}",
                    r.getType(),
                    r.getForPath(),
                    r.getError(),
                    r.getResultPath()
                )
            );
        }
    }

    /**
     * Logs detailed information about failed operations in case of a KeeperException.
     * This provides additional context about why specific operations failed.
     *
     * @param exception  The KeeperException containing results
     * @param operations List of operations that were attempted
     */
    private void logKeeperExceptionDetails(KeeperException exception, List<CuratorOp> operations) {
        var results = exception.getResults();
        if (results == null) {
            log.error("Transaction failed with no detailed results available");
            return;
        }

        log.error("Transaction rolled back due to Keeper Exception. Failed operations:");
        for (int i = 0; i < results.size(); i++) {
            if (results.get(i) instanceof OpResult.ErrorResult errorResult) {
                var operation = operations.get(i).get();
                log.error(
                    "Operation failed - Type: {}, Path: {}, Error: {}",
                    operation.getType(),
                    operation.getPath(),
                    errorResult.getErr()
                );
            }
        }
    }

    /**
     * Creates a transaction operation for creating a new ZNode.
     *
     * @param zNode The ZNode to be created
     * @return CuratorOp representing the create operation
     * @throws MetaStoreException if operation creation fails
     */
    private CuratorOp addCreateZNodeOp(ZNode zNode) {
        try {
            return zkCurator.transactionOp().create().withMode(CreateMode.PERSISTENT).forPath(zNode.getPath());
        } catch (Exception e) {
            throw new MetaStoreException(
                String.format("Failed to create Create Operation for path %s.", zNode.getPath()),
                e
            );
        }
    }

    /**
     * Creates a transaction operation for deleting a ZNode.
     *
     * @param zNode The ZNode to be deleted
     * @return CuratorOp representing the delete operation
     * @throws MetaStoreException if operation creation fails
     */
    private CuratorOp addDeleteZNodeOp(ZNode zNode) {
        try {
            return zkCurator.transactionOp().delete().forPath(zNode.getPath());
        } catch (Exception e) {
            throw new MetaStoreException(
                String.format("Failed to create Delete operation for path %s.", zNode.getPath()),
                e
            );
        }
    }

    /**
     * Creates a sequential ZNode for event tracking under the events' path.
     * The node name follows the format: event-{resourceType}-{resourceName}-{sequence}
     *
     * @param resourceName Name of the resource being tracked
     * @param metaStoreEntityType Type of the resource being tracked
     * @throws MetaStoreException if the creation operation fails
     */
    private CuratorOp createChangeEventZNode(String resourceName, MetaStoreEntityType metaStoreEntityType) {
        try {
            var nodeName = String.join(EVENT_DELIMITER, EVENT_PREFIX, metaStoreEntityType.name(), resourceName, "");
            log.debug(
                "Adding event znode creation operation for resource {} of type {}",
                resourceName,
                metaStoreEntityType
            );

            var eventsPath = ZNode.ofEntityChange(nodeName).getPath();
            return zkCurator.transactionOp().create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(eventsPath);
        } catch (Exception e) {
            throw new MetaStoreException(
                String.format(
                    "Failed to create event znode for resource %s of type %s",
                    resourceName,
                    metaStoreEntityType
                ),
                e
            );
        }
    }

    /**
     * Registers an event listener for metadata changes.
     * Creates an ephemeral node to track the listener's presence and initializes the cache listener.
     *
     * @param listener The metadata event listener to register
     * @return true if registration is successful, false otherwise
     */
    public boolean registerEventListener(MetaStoreEventListener listener) {
        var listenerPath = Path.of(ZNode.ofEntityType(EVENT).getPath(), LISTENER_NODE).toString();

        try {
            zkCurator.create().withMode(CreateMode.EPHEMERAL).forPath(listenerPath);

            var eventListener = buildEventListener(listener);
            eventCache.listenable().addListener(eventListener);
            eventCache.start();
            return true;
        } catch (Exception e) {
            log.error("Failed to register event listener", e);
            return false;
        }
    }

    /**
     * Builds a curator cache listener that processes both existing and newly created event nodes.
     *
     * <p>The listener handles events in two ways:
     * <ul>
     *   <li>Initial population: Processes all existing event nodes when cache is initialized</li>
     *   <li>Ongoing updates: Processes new event nodes as they are created</li>
     * </ul>
     *
     * @param listener The metadata event listener to notify of changes
     * @return A configured CuratorCacheListener
     */
    private CuratorCacheListener buildEventListener(MetaStoreEventListener listener) {
        return CuratorCacheListener.builder().forInitialized(() -> {
            log.info("Event cache initialized");
            eventCache.stream()
                      .filter(data -> isEventNode(data.getPath()))
                      .forEach(data -> processEventZNode(data.getPath(), listener));
        }).forCreates(childData -> {
            if (isEventNode(childData.getPath())) {
                processEventZNode(childData.getPath(), listener);
            }
        }).build();
    }

    /**
     * Checks if the given path corresponds to an event node.
     *
     * @param path ZNode path to check
     * @return true if the path is an event node, false if it's a listener node
     */
    private static boolean isEventNode(String path) {
        return !path.endsWith(LISTENER_NODE);
    }

    /**
     * Processes a newly created event ZNode by parsing its name and notifying the listener.
     * Node name format: event-{resourceType}-{resourceName}-{sequence}
     *
     * @param path     Path of the event ZNode
     * @param listener Listener to notify of the event
     */
    private void processEventZNode(String path, MetaStoreEventListener listener) {
        try {
            var name = Path.of(path).getFileName().toString();
            var parts = name.split(EVENT_DELIMITER, 4);
            if (parts.length >= 4 && EVENT_PREFIX.equals(parts[0])) {
                var event = new ZKMetadataEvent(
                    path,
                    parts[2],
                    MetaStoreEntityType.valueOf(parts[1].toUpperCase()),
                    this
                );
                listener.onEvent(event);
            }
        } catch (Exception e) {
            log.error("Failed to process event znode {}", path, e);
        }
    }

    /**
     * Deletes an event ZNode at the specified path.
     *
     * @param path Path of the event ZNode to delete
     * @throws MetaStoreException if the deletion fails
     */
    public void deleteEventZNode(String path) {
        try {
            zkCurator.delete().forPath(path);
            log.debug("Deleted event znode at path {}", path);
        } catch (Exception e) {
            throw new MetaStoreException(String.format("Failed to delete event znode at path %s", path), e);
        }
    }

    @Override
    public void close() throws Exception {
        if (eventCache != null) {
            eventCache.close();
        }
    }
}

package com.flipkart.varadhi.db;

import com.flipkart.varadhi.db.entities.ZkEvent;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.EventCallback;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.utils.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.flipkart.varadhi.db.ZNode.EVENT;

@Slf4j
public class ZKMetaStore {
    /*
    Implementation Details: < T extends VaradhiResource>
    Some APIs works on VaradhiResource abstraction. VaradhiResource implements Versioned entities.
    However, for ZKMetaStore, version is of stat object and not data object.
    So there will be a mismatch in data object version and stat object version. Stat object version is a valid version.
    create/get/update APIs overwrites the data version with stat version always.
    Another implication -- manual data update when done, will automatically bump the entity version w/o user being aware of it.
     */
    private final CuratorFramework zkCurator;
    private final CuratorCache eventCache;
    private CuratorCacheListener eventListener;

    public ZKMetaStore(CuratorFramework zkCurator) {
        this.zkCurator = zkCurator;
        this.eventCache = CuratorCache.build(zkCurator, ZNode.ofEntityType(EVENT).getPath());
    }

    void createZNode(ZNode znode) {
        try {
            if (!zkPathExist(znode)) {
                zkCurator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(znode.getPath());
                log.debug("Created zk node {}", znode);
            }
        } catch (Exception e) {
            throw new MetaStoreException(String.format("Failed to create path %s.", znode.getPath()), e);
        }
    }

    <T extends MetaStoreEntity> void createZNodeWithData(ZNode znode, T dataObject) {
        try {
            String jsonData = JsonMapper.jsonSerialize(dataObject);
            zkCurator.create()
                     .withMode(CreateMode.PERSISTENT)
                     .forPath(znode.getPath(), jsonData.getBytes(StandardCharsets.UTF_8));
            log.debug("Created zk node {} with data: {}", znode, jsonData);
            dataObject.setVersion(0);
        } catch (Exception e) {
            handleCreateException(znode, e);
        }
    }

    public <T extends MetaStoreEntity> void createTrackedResource(ZNode znode, T dataObject, ResourceType resourceType) {
        try {
            List<CuratorOp> ops = new ArrayList<>();

            String jsonData = JsonMapper.jsonSerialize(dataObject);
            ops.add(zkCurator.transactionOp()
                    .create()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(znode.getPath(), jsonData.getBytes(StandardCharsets.UTF_8)));

            createEventMarker(znode.getName(), resourceType, ops);

            zkCurator.transaction().forOperations(ops);
            dataObject.setVersion(0);

        } catch (Exception e) {
            handleCreateException(znode, e);
        }
    }

    private void handleCreateException(ZNode znode, Exception e) {
        if (e instanceof KeeperException.NodeExistsException) {
            throw new DuplicateResourceException(
                    String.format("%s(%s) already exists.", znode.getKind(), znode.getName()),
                    e
            );
        }

        throw new MetaStoreException(
                String.format("Failed to create %s(%s) at %s.", znode.getKind(), znode.getName(), znode.getPath()),
                e
        );
    }

    <T extends MetaStoreEntity> void updateZNodeWithData(ZNode znode, T dataObject) {
        try {
            String jsonData = JsonMapper.jsonSerialize(dataObject);
            Stat stat = zkCurator.setData()
                                 .withVersion(dataObject.getVersion())
                                 .forPath(znode.getPath(), jsonData.getBytes(StandardCharsets.UTF_8));
            log.debug(
                "Updated {}({}) in at {}: New Version{}.",
                znode.getKind(),
                znode.getName(),
                znode.getPath(),
                stat.getVersion()
            );
            dataObject.setVersion(stat.getVersion());
        } catch (Exception e) {
            handleUpdateException(znode, dataObject, e);
        }
    }

    public <T extends MetaStoreEntity> void updateTrackedResource(ZNode znode, T dataObject, ResourceType resourceType) {
        try {
            List<CuratorOp> ops = new ArrayList<>();

            String jsonData = JsonMapper.jsonSerialize(dataObject);
            ops.add(zkCurator.transactionOp()
                    .setData()
                    .withVersion(dataObject.getVersion())
                    .forPath(znode.getPath(), jsonData.getBytes(StandardCharsets.UTF_8)));

            createEventMarker(znode.getName(), resourceType, ops);

            zkCurator.transaction().forOperations(ops);

        } catch (Exception e) {
            handleUpdateException(znode, dataObject, e);
        }
    }

    private void handleUpdateException(ZNode znode, MetaStoreEntity dataObject, Exception e) {
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
                String.format("Failed to update %s(%s) at %s.", znode.getKind(), znode.getName(), znode.getPath()),
                e
        );
    }

    <T extends MetaStoreEntity> T getZNodeDataAsPojo(ZNode znode, Class<T> pojoClazz) {
        byte[] jsonData;
        Stat stat;
        try {
            stat = new Stat();
            jsonData = zkCurator.getData().storingStatIn(stat).forPath(znode.getPath());
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
        T resource = JsonMapper.jsonDeserialize(new String(jsonData), pojoClazz);
        resource.setVersion(stat.getVersion());
        return resource;
    }

    boolean zkPathExist(ZNode znode) {
        try {
            return null != zkCurator.checkExists().forPath(znode.getPath());
        } catch (Exception e) {
            throw new MetaStoreException(
                String.format("Failed to check if %s(%s) exists.", znode.getName(), znode.getPath()),
                e
            );
        }
    }

    void deleteZNode(ZNode znode) {
        try {
            zkCurator.delete().forPath(znode.getPath());
        } catch (Exception e) {
            handleDeleteException(znode, e);
        }
    }

    public void deleteTrackedResource(ZNode znode, ResourceType resourceType) {
        try {
            List<CuratorOp> ops = new ArrayList<>();

            ops.add(zkCurator.transactionOp()
                    .delete()
                    .forPath(znode.getPath()));

            createEventMarker(znode.getName(), resourceType, ops);

            zkCurator.transaction().forOperations(ops);

        } catch (Exception e) {
            handleDeleteException(znode, e);
        }
    }

    private void handleDeleteException(ZNode znode, Exception e) {
        if (e instanceof KeeperException.NoNodeException) {
            throw new ResourceNotFoundException(
                    String.format("%s(%s) not found.", znode.getKind(), znode.getName()),
                    e
            );
        }

        throw new MetaStoreException(
                String.format("Failed to delete %s(%s) at %s.", znode.getKind(), znode.getName(), znode.getPath()),
                e
        );
    }

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

    public void executeInTransaction(List<ZNode> toAdd, List<ZNode> toDelete) {
        if (!toAdd.isEmpty() || !toDelete.isEmpty()) {
            List<CuratorOp> ops = new ArrayList<>();
            toAdd.forEach(zNode -> ops.add(addCreateZNodeOp(zNode)));
            toDelete.forEach(zNode -> ops.add(addDeleteZNodeOp(zNode)));
            try {
                List<CuratorTransactionResult> results = zkCurator.transaction().forOperations(ops);
                //TODO::understand the partial failure scenario (if possible) ?
                results.forEach(r -> {
                    if (r.getError() != 0) {
                        log.error(
                            "Operation({}, {}) failed: code-{},{}",
                            r.getType(),
                            r.getForPath(),
                            r.getError(),
                            r.getResultPath()
                        );
                    }
                });
            } catch (KeeperException e) {
                e.getResults().forEach(r -> {
                    Op op = ops.get(e.getResults().indexOf(r)).get();
                    if (r instanceof OpResult.ErrorResult er) {
                        log.error(
                            "Operation({}, {}, {}) failed: {}.",
                            op.getKind(),
                            op.getType(),
                            op.getPath(),
                            er.getErr()
                        );
                    }
                });
                throw new MetaStoreException(
                    String.format("Failed to execute a batch operation %s.", e.getMessage()),
                    e
                );
            } catch (Exception e) {
                throw new MetaStoreException(
                    String.format("Failed to execute a batch operation %s.", e.getMessage()),
                    e
                );
            }
        }
    }

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

    private void createEventMarker(String resourceName, ResourceType resourceType, List<CuratorOp> ops) {
        try {
            String eventsPath = ZNode.ofEntityType(EVENT).getPath();

            // Format: event-{resourceType}-{resourceName}-{sequence}
            String nodeName = String.format("event-%s-%s-", resourceType, resourceName);

            log.debug("Adding event marker creation operation for resource {} of type {}", resourceName, resourceType);
            ops.add(zkCurator.transactionOp()
                    .create()
                    .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                    .forPath(eventsPath + "/" + nodeName));
        } catch (Exception e) {
            throw new MetaStoreException(
                    String.format("Failed to create event marker for resource %s of type %s", resourceName, resourceType),
                    e
            );
        }
    }

    public boolean registerEventListener(EventCallback callback) {
        String listenerNode = "listener";
        String listenerPath = ZNode.ofEntityType(EVENT).getPath() + "/" + listenerNode;

        try {
            zkCurator.create()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(listenerPath);

            eventListener = CuratorCacheListener.builder()
                    .forInitialized(() -> log.info("Event cache initialized"))
                    .forAll((type, oldData, data) -> {
                        if (type == CuratorCacheListener.Type.NODE_CREATED) {
                            String path = data.getPath();
                            if (!path.endsWith(listenerNode)) {
                                handleEvent(path, callback);
                            }
                        }
                    })
                    .build();

            eventCache.listenable().addListener(eventListener);
            eventCache.start();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void handleEvent(String path, EventCallback callback) {
        try {
            String name = path.substring(path.lastIndexOf('/') + 1);
            // Parse: event-resourceType-resourceName-sequence
            String[] parts = name.split("-", 4);
            if (parts.length >= 4 && parts[0].equals("event")) {
                ZkEvent marker = new ZkEvent(
                        path,
                        parts[2],
                        ResourceType.valueOf(parts[1].toUpperCase()),
                        this
                );
                callback.onEvent(marker);
            }
        } catch (Exception e) {
            log.error("Failed to process event {}", path, e);
        }
    }

    public void deleteEventMarker(String path) {
        try {
            zkCurator.delete().forPath(path);
            log.debug("Deleted event marker at path {}", path);
        } catch (Exception e) {
            throw new MetaStoreException(
                    String.format("Failed to delete event marker at path %s", path),
                    e
            );
        }
    }
}

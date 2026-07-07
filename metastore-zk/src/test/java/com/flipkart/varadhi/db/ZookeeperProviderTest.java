package com.flipkart.varadhi.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.flipkart.varadhi.common.ZookeeperConnectConfig;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZookeeperProviderTest {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    static {
        YAML_MAPPER.findAndRegisterModules();
    }

    private TestingServer globalStoreServer;
    private TestingServer localStoreServer;
    private ZookeeperProvider provider;

    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        globalStoreServer = new TestingServer();
        localStoreServer = new TestingServer();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (provider != null) {
            provider.close();
        }
        globalStoreServer.close();
        localStoreServer.close();
    }

    @Test
    void routesMetaStoreToGlobalZkAndConsumeStateStoresToLocalZk() throws Exception {
        initProvider(globalStoreServer.getConnectString(), localStoreServer.getConnectString());

        provider.getMetaStore();
        provider.getOpStore();
        provider.getAssignmentStore();

        try (
                CuratorFramework globalClient = newClient(globalStoreServer.getConnectString());
                CuratorFramework localClient = newClient(localStoreServer.getConnectString())
        ) {
            String orgPath = ZNode.ofEntityType(ZNode.ORG).getPath();
            assertNotNull(globalClient.checkExists().forPath(orgPath));
            assertNull(localClient.checkExists().forPath(orgPath));

            String subOpPath = ZNode.ofEntityType(ZNode.SUB_OP).getPath();
            assertNotNull(localClient.checkExists().forPath(subOpPath));
            assertNull(globalClient.checkExists().forPath(subOpPath));
        }
    }

    @Test
    @Timeout(5)
    void createFailsWhenZookeeperIsUnreachable() {
        MetaStoreException ex = assertThrows(
                MetaStoreException.class,
                () -> ZookeeperProvider.create(connectConfig("127.0.0.1:1", 500))
        );
        assertTrue(ex.getMessage().contains("connectTimeout"));
    }

    @Test
    @Timeout(5)
    void initFailsWhenLocalZookeeperIsUnreachable() throws Exception {
        Path configFile = metastoreConfigFile(
                connectConfig(globalStoreServer.getConnectString()),
                connectConfig("127.0.0.1:1", 500)
        );

        try (ZookeeperProvider failedProvider = new ZookeeperProvider()) {
            IllegalStateException ex = assertThrows(
                    IllegalStateException.class,
                    () -> failedProvider.init(metaStoreOptions(configFile))
            );
            assertInstanceOf(MetaStoreException.class, ex.getCause());
            assertThrows(IllegalStateException.class, failedProvider::getMetaStore);
        }
    }

    @Test
    void closeCanBeCalledMultipleTimes() throws Exception {
        initProvider(globalStoreServer.getConnectString(), localStoreServer.getConnectString());

        provider.close();
        assertDoesNotThrow(() -> provider.close());
        assertThrows(IllegalStateException.class, () -> provider.getMetaStore());
    }

    private void initProvider(String globalConnectUrl, String localConnectUrl) throws Exception {
        Path configFile = metastoreConfigFile(connectConfig(globalConnectUrl), connectConfig(localConnectUrl));
        provider = new ZookeeperProvider();
        provider.init(metaStoreOptions(configFile));
    }

    private Path metastoreConfigFile(ZookeeperConnectConfig global, ZookeeperConnectConfig local) throws IOException {
        ZKMetaStoreConfig config = new ZKMetaStoreConfig();
        config.setGlobalZookeeperOptions(global);
        config.setLocalZookeeperOptions(local);

        Path file = tempDir.resolve("metastore.yml");
        YAML_MAPPER.writeValue(file.toFile(), config);
        return file;
    }

    private static ZookeeperConnectConfig connectConfig(String connectUrl) {
        return connectConfig(connectUrl, 2000);
    }

    private static ZookeeperConnectConfig connectConfig(String connectUrl, int connectTimeoutMs) {
        ZookeeperConnectConfig config = new ZookeeperConnectConfig();
        config.setConnectUrl(connectUrl);
        config.setSessionTimeoutMs(60000);
        config.setConnectTimeoutMs(connectTimeoutMs);
        return config;
    }

    private static MetaStoreOptions metaStoreOptions(Path configFile) {
        MetaStoreOptions opts = new MetaStoreOptions();
        opts.setProviderClassName(ZookeeperProvider.class.getName());
        opts.setConfigFile(configFile.toString());
        return opts;
    }

    private CuratorFramework newClient(String connectString) {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                connectString,
                new ExponentialBackoffRetry(1000, 1)
        );
        client.start();
        return client;
    }
}

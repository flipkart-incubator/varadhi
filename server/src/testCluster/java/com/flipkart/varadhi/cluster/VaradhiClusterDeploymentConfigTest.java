package com.flipkart.varadhi.cluster;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Fast unit checks for config/nginx generation (no Docker). */
class VaradhiClusterDeploymentConfigTest {

    @Test
    void writeNginxConf_listsAllPods() throws Exception {
        Path conf = Files.createTempFile("nginx-", ".conf");
        VaradhiClusterDeployment.writeNginxConf(conf, List.of("varadhi-0", "varadhi-1"));
        String text = Files.readString(conf);
        assertTrue(text.contains("server varadhi-0:18488;"));
        assertTrue(text.contains("server varadhi-1:18488;"));
        assertTrue(text.contains("X-Upstream-Addr"));
        assertTrue(text.contains("underscores_in_headers on"));
    }

    @Test
    void writePodConfigs_rewritesZkAndPulsarAndJmx() throws Exception {
        Path template = Path.of(
            System.getProperty("varadhi.repo.root", findRepoRoot()),
            "setup/docker/configs/varadhi-auto-generated"
        );
        if (!Files.isDirectory(template)) {
            return; // unit test may run without repo property in IDEs
        }
        Path dest = Files.createTempDirectory("cfg-");
        VaradhiClusterDeployment.writePodConfigs(dest, template);
        String metastore = Files.readString(dest.resolve("metastore.yml"));
        assertTrue(metastore.contains(VaradhiClusterDeployment.ZK_ALIAS + ":2181"));
        assertTrue(!metastore.contains("10.5.5.4"));
        String messaging = Files.readString(dest.resolve("messaging.yml"));
        assertTrue(messaging.contains(VaradhiClusterDeployment.PULSAR_ALIAS));
        String configuration = Files.readString(dest.resolve("configuration.yml"));
        assertTrue(configuration.contains("exporter: \"jmx\""));
        assertTrue(!configuration.contains("exporter: \"otlp\""));
    }

    private static String findRepoRoot() {
        Path p = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        for (int i = 0; i < 6; i++) {
            if (Files.isDirectory(p.resolve("setup/docker/configs/varadhi-auto-generated"))) {
                return p.toString();
            }
            p = p.getParent();
            if (p == null) {
                break;
            }
        }
        return System.getProperty("user.dir");
    }
}

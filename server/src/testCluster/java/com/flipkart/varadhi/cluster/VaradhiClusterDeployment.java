package com.flipkart.varadhi.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Programmatic multi-pod Varadhi cluster for Docker-backed e2e (Viesti {@code PulsarDeployment}-shaped).
 *
 * <p>Pods are <strong>not</strong> automatically behind an ELB — Testcontainers only puts them on a
 * shared Docker network. Optional nginx LB, Toxiproxy (network faults), and Byteman (in-JVM faults)
 * bolt onto the same harness.
 *
 * <p>Lifecycle: {@link #start()} → {@link #addPod()} / {@link #killPod(String)} / … → {@link #close()}.
 */
public final class VaradhiClusterDeployment implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(VaradhiClusterDeployment.class);

    public static final int HTTP_PORT = 18_488;
    public static final int BYTEMAN_PORT = 9_091;
    public static final String ZK_ALIAS = "zookeeper";
    public static final String PULSAR_ALIAS = "pulsar";
    public static final String LB_ALIAS = "varadhi-lb";
    public static final String TOXIPROXY_ALIAS = "toxiproxy";

    private static final String ZK_IMAGE = "zookeeper:3.9.2";
    private static final String PULSAR_IMAGE = "apachepulsar/pulsar:3.3.2";
    private static final String NGINX_IMAGE = "nginx:1.27-alpine";
    private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName
        .parse("ghcr.io/shopify/toxiproxy:2.5.0")
        .asCompatibleSubstituteFor("shopify/toxiproxy");

    private final Network network;
    private final String varadhiImage;
    private final Path configDir;
    private final Path nginxConfDir;
    private final boolean withLoadBalancer;
    private final boolean withToxiproxy;
    private final boolean withByteman;
    private final Path bytemanJar;
    private final int initialPods;
    private final AtomicInteger podSeq = new AtomicInteger();
    private final Map<String, GenericContainer<?>> pods = new LinkedHashMap<>();
    private final Map<String, ToxiproxyContainer.ContainerProxy> httpProxies = new ConcurrentHashMap<>();

    private GenericContainer<?> zookeeper;
    private GenericContainer<?> pulsar;
    private GenericContainer<?> loadBalancer;
    private ToxiproxyContainer toxiproxy;
    private boolean started;

    private VaradhiClusterDeployment(Builder builder) {
        this.network = Network.newNetwork();
        this.varadhiImage = builder.varadhiImage;
        this.withLoadBalancer = builder.withLoadBalancer;
        this.withToxiproxy = builder.withToxiproxy;
        this.withByteman = builder.withByteman;
        this.bytemanJar = builder.bytemanJar;
        this.initialPods = builder.initialPods;
        try {
            this.configDir = Files.createTempDirectory("varadhi-cluster-cfg-");
            this.nginxConfDir = Files.createTempDirectory("varadhi-cluster-nginx-");
            writePodConfigs(configDir, builder.configTemplateDir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (withByteman && (bytemanJar == null || !Files.isRegularFile(bytemanJar))) {
            throw new IllegalStateException(
                "withByteman(true) requires byteman jar; set -Dvaradhi.byteman.jar=... (Gradle wires this)"
            );
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public void start() {
        if (started) {
            return;
        }
        if (withToxiproxy) {
            startToxiproxy();
        }
        startZookeeper();
        startPulsar();
        for (int i = 0; i < initialPods; i++) {
            addPod();
        }
        if (withLoadBalancer) {
            refreshLoadBalancer();
        }
        started = true;
    }

    /** Docker network alias for the pod (stable identity for kill/restart/remove). */
    public String addPod() {
        String alias = "varadhi-" + podSeq.getAndIncrement();
        GenericContainer<?> pod = newVaradhiContainer(alias);
        pod.start();
        pods.put(alias, pod);
        log.info("Started Varadhi pod {} on {}:{}", alias, pod.getHost(), pod.getMappedPort(HTTP_PORT));
        if (started && withLoadBalancer) {
            refreshLoadBalancer();
        }
        return alias;
    }

    public void removePod(String alias) {
        httpProxies.remove(alias);
        GenericContainer<?> pod = requirePod(alias);
        pod.stop();
        pods.remove(alias);
        if (withLoadBalancer) {
            refreshLoadBalancer();
        }
    }

    /** SIGKILL — container stays registered until {@link #removePod} or {@link #restartPod}. */
    public void killPod(String alias) {
        requirePod(alias).getDockerClient()
                         .killContainerCmd(requirePod(alias).getContainerId())
                         .exec();
    }

    public void restartPod(String alias) {
        httpProxies.remove(alias);
        GenericContainer<?> old = requirePod(alias);
        old.stop();
        pods.remove(alias);
        GenericContainer<?> fresh = newVaradhiContainer(alias);
        fresh.start();
        pods.put(alias, fresh);
        if (withLoadBalancer) {
            refreshLoadBalancer();
        }
    }

    public List<String> podAliases() {
        return List.copyOf(pods.keySet());
    }

    public String podBaseUri(String alias) {
        GenericContainer<?> pod = requirePod(alias);
        return "http://" + pod.getHost() + ":" + pod.getMappedPort(HTTP_PORT);
    }

    /**
     * Client entrypoint when LB is enabled. Without LB, prefer {@link #podBaseUri(String)} per pod —
     * there is no automatic cloud ELB in this harness.
     */
    public String loadBalancerBaseUri() {
        if (loadBalancer == null) {
            throw new IllegalStateException("Load balancer not enabled; use builder.withLoadBalancer(true)");
        }
        return "http://" + loadBalancer.getHost() + ":" + loadBalancer.getMappedPort(HTTP_PORT);
    }

    /**
     * HTTP base URI for a pod routed through Toxiproxy (same Docker network, client uses host-mapped port).
     * Create-on-first-use; reuse the returned {@link ToxiproxyContainer.ContainerProxy} for toxics.
     */
    public String podBaseUriViaProxy(String alias) {
        return "http://" + toxiproxy().getHost() + ":" + httpProxy(alias).getProxyPort();
    }

    /**
     * Client → Toxiproxy → nginx LB → pods. Toxiproxy is TCP middleware on the produce path, not an
     * in-process Varadhi filter chain.
     */
    public String loadBalancerBaseUriViaProxy() {
        return "http://" + toxiproxy().getHost() + ":" + lbHttpProxy().getProxyPort();
    }

    /** Lazily creates a Toxiproxy TCP proxy {@code listen → alias:18488}. */
    public ToxiproxyContainer.ContainerProxy httpProxy(String alias) {
        requirePod(alias);
        ToxiproxyContainer toxi = toxiproxy();
        return httpProxies.computeIfAbsent(alias, a -> toxi.getProxy(a, HTTP_PORT));
    }

    /** Toxiproxy in front of the nginx LB (same listen→upstream pattern as {@link #httpProxy}). */
    public ToxiproxyContainer.ContainerProxy lbHttpProxy() {
        if (loadBalancer == null) {
            throw new IllegalStateException("Load balancer not enabled; use builder.withLoadBalancer(true)");
        }
        ToxiproxyContainer toxi = toxiproxy();
        return httpProxies.computeIfAbsent(LB_ALIAS, a -> toxi.getProxy(a, HTTP_PORT));
    }

    public ToxiproxyContainer toxiproxy() {
        if (toxiproxy == null) {
            throw new IllegalStateException("Toxiproxy not enabled; use builder.withToxiproxy(true)");
        }
        return toxiproxy;
    }

    /** Submit client for dynamic Byteman rules on a pod started with {@code withByteman(true)}. */
    public BytemanControl byteman(String alias) {
        if (!withByteman) {
            throw new IllegalStateException("Byteman not enabled; use builder.withByteman(true)");
        }
        GenericContainer<?> pod = requirePod(alias);
        return new BytemanControl(pod.getHost(), pod.getMappedPort(BYTEMAN_PORT));
    }

    public Network network() {
        return network;
    }

    @Override
    public void close() {
        httpProxies.clear();
        if (loadBalancer != null) {
            loadBalancer.stop();
            loadBalancer = null;
        }
        new ArrayList<>(pods.keySet()).forEach(alias -> {
            try {
                pods.get(alias).stop();
            } catch (Exception e) {
                log.warn("Failed stopping pod {}", alias, e);
            }
        });
        pods.clear();
        if (pulsar != null) {
            pulsar.stop();
        }
        if (zookeeper != null) {
            zookeeper.stop();
        }
        if (toxiproxy != null) {
            toxiproxy.stop();
            toxiproxy = null;
        }
        network.close();
        deleteDirQuietly(configDir);
        deleteDirQuietly(nginxConfDir);
        started = false;
    }

    private static void deleteDirQuietly(Path dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    log.warn("Failed deleting {}", p, e);
                }
            });
        } catch (IOException e) {
            log.warn("Failed cleaning {}", dir, e);
        }
    }

    private void startToxiproxy() {
        toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
            .withNetwork(network)
            .withNetworkAliases(TOXIPROXY_ALIAS)
            .withLogConsumer(new Slf4jLogConsumer(log).withPrefix(TOXIPROXY_ALIAS));
        toxiproxy.start();
    }

    private void startZookeeper() {
        zookeeper = new GenericContainer<>(DockerImageName.parse(ZK_IMAGE))
            .withNetwork(network)
            .withNetworkAliases(ZK_ALIAS)
            .withEnv("ZOO_4LW_COMMANDS_WHITELIST", "*")
            .withEnv("ZOO_ADMINSERVER_ENABLED", "false")
            .waitingFor(Wait.forLogMessage(".*binding to port.*", 1).withStartupTimeout(Duration.ofMinutes(2)))
            .withLogConsumer(new Slf4jLogConsumer(log).withPrefix(ZK_ALIAS));
        zookeeper.start();
    }

    private void startPulsar() {
        pulsar = new GenericContainer<>(DockerImageName.parse(PULSAR_IMAGE))
            .withNetwork(network)
            .withNetworkAliases(PULSAR_ALIAS)
            .withExposedPorts(8080)
            .withCommand("bin/pulsar", "standalone", "--advertised-address", PULSAR_ALIAS)
            .waitingFor(
                Wait.forHttp("/admin/v2/clusters")
                    .forPort(8080)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(3))
            )
            .withLogConsumer(new Slf4jLogConsumer(log).withPrefix(PULSAR_ALIAS));
        pulsar.start();
    }

    private GenericContainer<?> newVaradhiContainer(String alias) {
        GenericContainer<?> pod = new GenericContainer<>(DockerImageName.parse(varadhiImage))
            .withNetwork(network)
            .withNetworkAliases(alias)
            .withExposedPorts(HTTP_PORT)
            // Image runs as uid 10222; copied configs must be world-readable.
            .withCopyFileToContainer(MountableFile.forHostPath(configDir, 0755), "/etc/varadhi")
            .waitingFor(
                Wait.forHttp("/v1/health-check")
                    .forPort(HTTP_PORT)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(3))
            )
            .withLogConsumer(new Slf4jLogConsumer(log).withPrefix(alias));

        if (withByteman) {
            Path rules = writeDefaultBytemanRules();
            pod.withExposedPorts(HTTP_PORT, BYTEMAN_PORT)
               .withCopyFileToContainer(MountableFile.forHostPath(bytemanJar, 0644), "/opt/byteman/byteman.jar")
               .withCopyFileToContainer(MountableFile.forHostPath(rules, 0644), "/opt/byteman/rules.btm")
               .withEnv(
                   "JAVA_TOOL_OPTIONS",
                   "-javaagent:/opt/byteman/byteman.jar="
                       + "script:/opt/byteman/rules.btm,"
                       + "listener:true,port:" + BYTEMAN_PORT + ",address:0.0.0.0,"
                       + "boot:/opt/byteman/byteman.jar,"
                       + "prop:org.jboss.byteman.transform.all=true"
               );
        } else {
            // Compose jacoco agent path does not exist in this harness.
            pod.withEnv("JAVA_TOOL_OPTIONS", "");
        }
        return pod;
    }

    /**
     * Boot-time rules keyed off system properties so tests can arm/disarm faults via
     * {@link BytemanControl} without relying on late retransform.
     */
    private static Path writeDefaultBytemanRules() {
        try {
            Path rules = Files.createTempFile("varadhi-byteman-", ".btm");
            Files.writeString(
                rules,
                """
                    RULE varadhi fail health check
                    CLASS com.flipkart.varadhi.web.v1.HealthCheckHandler
                    METHOD handle
                    AT ENTRY
                    IF Boolean.getBoolean("org.jboss.byteman.failHealth")
                    DO throw new java.lang.RuntimeException("byteman-injected-health-fault")
                    ENDRULE

                    RULE varadhi fail produce not found
                    CLASS com.flipkart.varadhi.web.v1.producer.ProduceHandlers
                    METHOD produce
                    AT ENTRY
                    IF Boolean.getBoolean("org.jboss.byteman.failProduceNotFound")
                    DO throw new com.flipkart.varadhi.common.exceptions.ResourceNotFoundException("TOPIC 'byteman-injected' not found")
                    ENDRULE
                    """,
                StandardCharsets.UTF_8
            );
            makeWorldReadable(rules);
            return rules;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void refreshLoadBalancer() {
        writeNginxConf(nginxConfDir.resolve("default.conf"), pods.keySet());
        if (loadBalancer != null) {
            loadBalancer.stop();
        }
        if (pods.isEmpty()) {
            loadBalancer = null;
            return;
        }
        loadBalancer = new GenericContainer<>(DockerImageName.parse(NGINX_IMAGE))
            .withNetwork(network)
            .withNetworkAliases(LB_ALIAS)
            .withExposedPorts(HTTP_PORT)
            .withCopyFileToContainer(
                MountableFile.forHostPath(nginxConfDir.resolve("default.conf")),
                "/etc/nginx/conf.d/default.conf"
            )
            .waitingFor(Wait.forHttp("/v1/health-check").forPort(HTTP_PORT).forStatusCode(200))
            .withLogConsumer(new Slf4jLogConsumer(log).withPrefix(LB_ALIAS));
        loadBalancer.start();
        log.info("LB listening on {}", loadBalancerBaseUri());
    }

    private GenericContainer<?> requirePod(String alias) {
        GenericContainer<?> pod = pods.get(alias);
        if (pod == null) {
            throw new IllegalArgumentException("Unknown pod alias: " + alias + "; known=" + pods.keySet());
        }
        return pod;
    }

    static void writePodConfigs(Path dest, Path templateDir) throws IOException {
        Files.createDirectories(dest);
        try (var stream = Files.list(templateDir)) {
            stream.filter(Files::isRegularFile).forEach(src -> {
                try {
                    String text = Files.readString(src);
                    text = text.replace("10.5.5.4", ZK_ALIAS).replace("10.5.5.3", PULSAR_ALIAS);
                    // Avoid depending on an otel collector in the harness.
                    if (src.getFileName().toString().equals("configuration.yml")) {
                        text = text.replaceAll(
                            "(?s)metricsExporterOptions:.*?(?=\\ntracesEnabled:)",
                            "metricsExporterOptions:\n  exporter: \"jmx\"\n"
                        );
                    }
                    Files.writeString(dest.resolve(src.getFileName()), text, StandardCharsets.UTF_8);
                    makeWorldReadable(dest.resolve(src.getFileName()));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
        makeWorldReadable(dest);
    }

    private static void makeWorldReadable(Path path) {
        try {
            Set<PosixFilePermission> perms = Files.isDirectory(path)
                ? EnumSet.copyOf(PosixFilePermissions.fromString("rwxr-xr-x"))
                : EnumSet.copyOf(PosixFilePermissions.fromString("rw-r--r--"));
            Files.setPosixFilePermissions(path, perms);
        } catch (UnsupportedOperationException | IOException ignored) {
            // non-POSIX FS — MountableFile mode above is the fallback
        }
    }

    static void writeNginxConf(Path confFile, Iterable<String> podAliases) {
        StringBuilder upstream = new StringBuilder();
        for (String alias : podAliases) {
            upstream.append("    server ").append(alias).append(':').append(HTTP_PORT).append(";\n");
        }
        String conf = """
            underscores_in_headers on;
            upstream varadhi_backends {
            %s}
            server {
                listen %d;
                location / {
                    proxy_http_version 1.1;
                    proxy_set_header Host $host;
                    proxy_set_header Connection "";
                    proxy_set_header x_user_id $http_x_user_id;
                    proxy_pass http://varadhi_backends;
                    # Surface which backend handled the request (proves multi-pod routing).
                    add_header X-Upstream-Addr $upstream_addr always;
                }
            }
            """.formatted(upstream, HTTP_PORT);
        try {
            Files.writeString(confFile, conf, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static final class Builder {
        private String varadhiImage = System.getProperty(
            "varadhi.cluster.image",
            "varadhi.docker.registry/varadhi:latest"
        );
        private Path configTemplateDir;
        private boolean withLoadBalancer = true;
        private boolean withToxiproxy = false;
        private boolean withByteman = false;
        private Path bytemanJar = Path.of(System.getProperty("varadhi.byteman.jar", ""));
        private int initialPods = 2;

        public Builder varadhiImage(String image) {
            this.varadhiImage = Objects.requireNonNull(image);
            return this;
        }

        public Builder configTemplateDir(Path dir) {
            this.configTemplateDir = dir;
            return this;
        }

        public Builder withLoadBalancer(boolean enabled) {
            this.withLoadBalancer = enabled;
            return this;
        }

        public Builder withToxiproxy(boolean enabled) {
            this.withToxiproxy = enabled;
            return this;
        }

        public Builder withByteman(boolean enabled) {
            this.withByteman = enabled;
            return this;
        }

        public Builder bytemanJar(Path jar) {
            this.bytemanJar = jar;
            return this;
        }

        public Builder initialPods(int n) {
            if (n < 0) {
                throw new IllegalArgumentException("initialPods >= 0");
            }
            this.initialPods = n;
            return this;
        }

        public VaradhiClusterDeployment build() {
            if (configTemplateDir == null) {
                String root = System.getProperty("varadhi.repo.root", System.getProperty("user.dir"));
                configTemplateDir = Path.of(root, "setup/docker/configs/varadhi-auto-generated");
            }
            if (!Files.isDirectory(configTemplateDir)) {
                throw new IllegalStateException("Config template dir missing: " + configTemplateDir);
            }
            return new VaradhiClusterDeployment(this);
        }
    }
}

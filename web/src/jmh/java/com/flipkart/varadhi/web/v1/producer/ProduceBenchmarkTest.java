package com.flipkart.varadhi.web.v1.producer;

import com.flipkart.varadhi.common.TestExtensions;
import com.flipkart.varadhi.common.utils.YamlLoader;
import com.flipkart.varadhi.core.CoreServices;
import com.flipkart.varadhi.core.OrgReadCache;
import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.ResourceReadCacheRegistry;

import com.flipkart.varadhi.entities.InternalQueueCategory;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.OrgDetails;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.SegmentedStorageTopic;
import com.flipkart.varadhi.entities.StdHeaders;
import com.flipkart.varadhi.entities.TestStdHeaders;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.spi.mock.InMemoryMessagingStackProvider;
import com.flipkart.varadhi.spi.mock.InMemoryMetaStore;
import com.flipkart.varadhi.spi.mock.InMemoryStorageTopic;
import com.flipkart.varadhi.spi.services.MessagingStackProvider;
import com.flipkart.varadhi.web.WebServerVerticle;
import com.flipkart.varadhi.web.config.WebConfiguration;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import lombok.experimental.ExtensionMethod;

import org.apache.commons.lang3.RandomStringUtils;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

import static java.util.Arrays.asList;

/**
 * JMH benchmark for producer API using WebServerVerticle.
 * 
 * Uses WebServerVerticle with PRODUCE-only APIUsecases to set up a lightweight
 * producer-only server for benchmarking. This ensures all handlers and configurations
 * are set up exactly like in production but with in-memory implementations.
 */
@State (Scope.Benchmark)
@Fork (1)
@ExtensionMethod ({TestExtensions.FutureExtensions.class})
public class ProduceBenchmarkTest {

    /**
     * Thread-local state for topic iteration and message handling
     */
    @State (Scope.Thread)
    public static class ThreadState {
        private List<String> threadTopics;
        private int topicIndex = 0;

        @Setup
        public void setup() {
            // Create a randomized copy of topics for this thread
            threadTopics = new ArrayList<>(TOPIC_NAMES);
            Collections.shuffle(threadTopics);
        }

        public String getNextTopic() {
            String topic = threadTopics.get(topicIndex);
            topicIndex = (topicIndex + 1) % threadTopics.size();
            return topic;
        }
    }

    ///////////////////////////////
    /// Setup
    ///////////////////////////////

    static final String DEFAULT_PROJECT_NAME = "testProject";
    static final String DEFAULT_ORG_NAME = "testOrg";
    static final String DEFAULT_TEAM_NAME = "testTeam";
    static final int NUM_TOPICS = 10;

    // Fixed set of topics for cycling
    // Note: JMH doesn't allow static fields in benchmark classes, so we'll initialize this in setup
    private List<String> topicNames;

    final byte[] SAMPLE_PAYLOAD = RandomStringUtils.insecure().nextAlphanumeric(1000).getBytes(StandardCharsets.UTF_8);

    private Vertx vertx;
    private HttpServer httpServer;
    private WebServerVerticle webServerVerticle;

    // Load generation 
    private Vertx loadGenVertx;
    private WebClient loadGenClient;

    @Setup
    public void setup() throws Exception {
        // Initialize StdHeaders
        if (!StdHeaders.isGlobalInstanceInitialized()) {
            StdHeaders.init(TestStdHeaders.get());
        }

        // Setup Vertx
        vertx = Vertx.vertx(serverVertxOptions);

        VertxOptions loadGenVertxOptions = new VertxOptions().setEventLoopPoolSize(2)
                                                             .setWorkerPoolSize(20)
                                                             .setInternalBlockingPoolSize(20);
        loadGenVertx = Vertx.vertx(loadGenVertxOptions);
        WebClientOptions webClientOptions = new WebClientOptions().setKeepAlive(true)
                                                                  .setTcpNoDelay(true)
                                                                  .setMaxPoolSize(128);
        loadGenClient = WebClient.create(loadGenVertx, webClientOptions);

        // Create WebServerVerticle with PRODUCE-only APIs
        webServerVerticle = createProducerWebServer();

        // Deploy and start the verticle
        vertx.deployVerticle(webServerVerticle).blockingGet();

        // Wait for HTTP server to be ready
        Thread.sleep(1000); // Give time for server startup

    }

    /**
     * Creates a WebServerVerticle configured for producer-only APIs with in-memory implementations
     */
    private WebServerVerticle createProducerWebServer() throws Exception {
        // Create minimal web configuration
        WebConfiguration webConfig = createMinimalWebConfig();

        // Create in-memory messaging stack
        InMemoryMessagingStackProvider messagingStackProvider = new InMemoryMessagingStackProvider();
        messagingStackProvider.init(null, null);

        // Create test data and caches
        Project testProject = Project.of(DEFAULT_PROJECT_NAME, "", DEFAULT_TEAM_NAME, DEFAULT_ORG_NAME);
        OrgDetails testOrg = new OrgDetails(com.flipkart.varadhi.entities.Org.of(DEFAULT_ORG_NAME), null);

        // Create caches with test data
        ResourceReadCache<Resource.EntityResource<Project>> projectCache = ResourceReadCache.create(
            ResourceType.PROJECT,
            () -> asList(Resource.of(testProject, ResourceType.PROJECT)),
            vertx
        ).blockingGet();

        OrgReadCache orgCache = new OrgReadCache(ResourceType.ORG, () -> asList(testOrg));
        ResourceReadCache.preload(orgCache, vertx).blockingGet();

        List<Resource.EntityResource<VaradhiTopic>> testTopics = createTestTopics(testProject, messagingStackProvider);
        ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache = ResourceReadCache.create(
            ResourceType.TOPIC,
            () -> testTopics,
            vertx
        ).blockingGet();

        // Create cache registry
        ResourceReadCacheRegistry cacheRegistry = new ResourceReadCacheRegistry();
        cacheRegistry.addCache(ResourceType.ORG, orgCache);
        cacheRegistry.addCache(ResourceType.PROJECT, projectCache);
        cacheRegistry.addCache(ResourceType.TOPIC, topicCache);

        // Create core services with minimal implementations
        CoreServices coreServices = new CoreServices() {
            @Override
            public MessagingStackProvider getMessagingStackProvider() {
                return messagingStackProvider;
            }

            @Override
            public MetaStoreProvider getMetaStoreProvider() {
                return () -> new InMemoryMetaStore();
            }

            @Override
            public SimpleMeterRegistry getMeterRegistry() {
                return new SimpleMeterRegistry();
            }

            @Override
            public io.opentelemetry.api.trace.Tracer getTracer(String instrumentationName) {
                return OpenTelemetry.noop().getTracer(instrumentationName);
            }
        };

        // Create WebServerVerticle with PRODUCE-only APIs
        return new WebServerVerticle(
            webConfig,
            coreServices,
            null, // No cluster manager needed for produce-only
            cacheRegistry,
            WebServerVerticle.APIUsecases.PRODUCE
        );
    }

    private WebConfiguration createMinimalWebConfig() {

        String configStr =
            """
                deployedRegion: "default"

                vertxOptions:
                    eventLoopPoolSize: 1
                    workerPoolSize: 20
                    internalBlockingPoolSize: 20

                producerOptions:
                    producerCacheTtlSeconds: 3600

                messagingStackOptions:
                    providerClassName: "com.flipkart.varadhi.spi.inmemory.InMemoryMessagingStackProvider"

                metaStoreOptions:
                providerClassName: "com.flipkart.varadhi.spi.mock.InMemoryMetastoreProvider"

                featureFlags:
                    leanDeployment: false
                    defaultOrg: "default"
                    defaultTeam: "public"
                    defaultProject: "public"

                messageConfiguration:
                    maxIdHeaderSize: 100
                    maxRequestSize: 5242880
                    filterNonCompliantHeaders: true

                tracesEnabled: true

                httpServerOptions:
                    port: 8989
                    host: 0.0.0.0
                    tcpKeepAlive: true
                    reuseAddress: true
                    reusePort: true
                    acceptBacklog: 1024
                    soLinger: 0
                    tcpNoDelay: true
                    tcpFastOpen: true
                    alpnVersions: [ "HTTP_1_1", "HTTP_2" ]
                    decompressionSupported: false
                    useAlpn: true
                    tracingPolicy: "ALWAYS"

                authenticationOptions:
                    handlerProviderClassName: "com.flipkart.varadhi.web.authn.CustomAuthenticationHandler"
                    authenticationProviderClassName: "com.flipkart.varadhi.web.authn.UserHeaderAuthenticationHandler.AuthnProvider"

                authorizationOptions:
                    enabled: true
                    providerClassName: "com.flipkart.varadhi.web.spi.authz.AuthorizationProvider.NoAuthorizationProvider"
                """;

        WebConfiguration webConfig = YamlLoader.loadConfigFromString(configStr, WebConfiguration.class, false);

        return webConfig;
    }

    private List<Resource.EntityResource<VaradhiTopic>> createTestTopics(
        Project project,
        InMemoryMessagingStackProvider messagingStackProvider
    ) {
        List<Resource.EntityResource<VaradhiTopic>> topics = new ArrayList<>();
        TopicCapacityPolicy policy = new TopicCapacityPolicy(100, 1000, 1);

        for (int i = 0; i < NUM_TOPICS; i++) {
            String topicName = "topic" + (i + 1);
            VaradhiTopic topic = VaradhiTopic.of(
                project.getName(),
                topicName,
                i % 2 == 0, // alternate grouped/ungrouped
                policy,
                LifecycleStatus.ActionCode.USER_ACTION
            );
            topic.markCreated(); // Set state to active

            // Create storage topic using messaging stack provider
            InMemoryStorageTopic storageTopic = messagingStackProvider.getStorageTopicFactory()
                                                                      .getTopic(
                                                                          i,
                                                                          topic.getName(),
                                                                          project,
                                                                          policy,
                                                                          InternalQueueCategory.MAIN
                                                                      );

            // Create the topic in the messaging stack
            messagingStackProvider.getStorageTopicService().create(storageTopic, project);

            // Add as internal topic with proper state
            SegmentedStorageTopic segmentedTopic = SegmentedStorageTopic.of(storageTopic);
            segmentedTopic.setTopicState(TopicState.Producing);
            topic.addInternalTopic("testRegion", segmentedTopic);

            topics.add(Resource.of(topic, ResourceType.TOPIC));
        }
        return topics;
    }

    @TearDown
    public void tearDown() throws Exception {
        if (vertx != null) {
            vertx.close().blockingGet();
        }
        if (loadGenVertx != null) {
            loadGenVertx.close().blockingGet();
        }
    }

    ///////////////////////////////
    /// Benchmark Test
    ///////////////////////////////

    // @Benchmark
    // @BenchmarkMode (Mode.Throughput)
    // @Threads (4)
    //     @Warmup (iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
    // @Measurement (iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
    // @OutputTimeUnit (TimeUnit.SECONDS)
    // public void benchmarkProduceMessages(ThreadState threadState) throws Exception {
    //     // Get next topic from thread-specific randomized list
    //     String topicName = threadState.getNextTopic();

    //     // Use pre-created reusable message to avoid object creation overhead
    //     String topicFQN = VaradhiTopic.fqn(DEFAULT_PROJECT_NAME, topicName);

    //     // Call producer service directly for better performance measurement

    //     var result = producerService.produceToTopic(reusableMessage, topicFQN).join();
    //     Assertions.assertTrue(result.isSuccess());
    // }

    @Benchmark
    @BenchmarkMode (Mode.AverageTime)
    @Threads (1)
    @Warmup (iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement (iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
    @OutputTimeUnit (TimeUnit.MICROSECONDS)
    public void benchmarkProduceHttpMessages(ThreadState threadState, Blackhole blackhole) throws Exception {


        // Get next topic from thread-specific randomized list
        String topicName = threadState.getNextTopic();
        String endpoint = String.format("/v1/projects/%s/topics/%s/produce", DEFAULT_PROJECT_NAME, topicName);

        // Make HTTP POST request to the produce endpoint
        var response = loadGenClient.post(8989, "localhost", endpoint)
                                    .putHeader("Content-Type", "application/json")
                                    .putHeader("X_PRODUCE_REGION", "testRegion")
                                    .putHeader("X_MESSAGE_ID", "benchmark-msg-" + System.nanoTime())
                                    .sendJsonObject(
                                        new JsonObject().put(
                                            "payload",
                                            new String(SAMPLE_PAYLOAD, StandardCharsets.UTF_8)
                                        )
                                    )
                                    .blockingGet();  // Block to wait for response in JMH

        // Consume the result to prevent JVM optimization
        blackhole.consume(response.statusCode());
        String responseBody = response.bodyAsString();
        blackhole.consume(responseBody);

        // Assert success for correctness
        if (response.statusCode() != 200) {
            throw new RuntimeException(
                "HTTP request failed with status: " + response.statusCode() + ", body: " + responseBody
            );
        }
    }
}

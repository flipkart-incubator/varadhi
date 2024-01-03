package com.flipkart.varadhi;

import com.flipkart.varadhi.config.ServerConfiguration;
import com.flipkart.varadhi.deployment.FullDeploymentVerticleDeployer;
import com.flipkart.varadhi.deployment.LeanDeploymentVerticleDeployer;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import com.flipkart.varadhi.metrices.CustomMetrics;
import com.flipkart.varadhi.utils.HostUtils;
import io.micrometer.core.instrument.Tag;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.observability.HttpRequest;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.tracing.opentelemetry.OpenTelemetryOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Slf4j
public class Server {

    public static void main(String[] args) {

        try {
            String hostName = HostUtils.getHostName();
            log.info("Server Starting on {}.", hostName);
            ServerConfiguration configuration = readConfiguration(args);
            CoreServices services = new CoreServices(configuration);
            Vertx vertx = createVertex(configuration, services);
            deployVerticle(hostName, configuration, services, vertx);
            log.info("Server Started on {}.", hostName);
        } catch (Exception e) {
            log.error("Failed to initialise the server.", e);
            System.exit(-1);
        }
        // TODO: check need for shutdown hook
    }

    private static Vertx createVertex(ServerConfiguration configuration, CoreServices services) {
        log.debug("Creating Vertex");
        VertxOptions vertxOptions = configuration.getVertxOptions()
                .setTracingOptions(new OpenTelemetryOptions(services.getOpenTelemetry()))
                .setMetricsOptions(new MicrometerMetricsOptions()
                        .setMicrometerRegistry(services.getMetricsRegistry())
                        .setRegistryName("default")
                        .setJvmMetricsEnabled(true)
                        .setFactory(options -> new CustomMetrics(services.getMetricsRegistry()))
                        .setClientRequestTagsProvider(httpRequest -> getCustomHttpHeaders(httpRequest))
                        .setEnabled(true));
        Vertx vertx = Vertx.vertx(vertxOptions);
        log.debug("Created Vertex");
        return vertx;
    }

    private static List<Tag> getCustomHttpHeaders(HttpRequest httpRequest) {

        MultiMap multiMap = httpRequest.headers();
        String topicName = multiMap.get(Constants.Tags.TAG_NAME_TOPIC);
        Tag topicTag = Tag.of(Constants.Tags.TAG_NAME_TOPIC, Optional.ofNullable(topicName).orElse(StringUtils.EMPTY));

        String projectName = multiMap.get(Constants.Tags.TAG_NAME_PROJECT);
        Tag projectTag = Tag.of(Constants.Tags.TAG_NAME_PROJECT, Optional.ofNullable(projectName).orElse(StringUtils.EMPTY));

        String regionName = multiMap.get(Constants.Tags.TAG_NAME_REGION);
        Tag regionTag = Tag.of(Constants.Tags.TAG_NAME_REGION, Optional.ofNullable(regionName).orElse(StringUtils.EMPTY));

        return Arrays.asList(topicTag, projectTag, regionTag);
    }

    private static void deployVerticle(
            String hostName, ServerConfiguration configuration, CoreServices services, Vertx vertx
    ) {
        log.debug("Verticle deployment started.");
        VerticleDeployer verticleDeployer;
        if (configuration.getFeatureFlags().isLeanDeployment()) {
            verticleDeployer = new LeanDeploymentVerticleDeployer(
                    hostName,
                    vertx,
                    configuration,
                    services.getMessagingStackProvider(),
                    services.getMetaStoreProvider(),
                    services.getMetricsRegistry()
            );
        } else {
            verticleDeployer = new FullDeploymentVerticleDeployer(
                    hostName,
                    vertx,
                    configuration,
                    services.getMessagingStackProvider(),
                    services.getMetaStoreProvider(),
                    services.getMetricsRegistry()
            );
        }

        verticleDeployer.deployVerticle(vertx, configuration);
        log.debug("Verticle deployment completed.");
    }


    public static ServerConfiguration readConfiguration(String[] args) {
        if (args.length < 1) {
            log.error("Usage: java com.flipkart.varadhi.Server configuration.yml");
            System.exit(-1);
        }
        return readConfigFromFile(args[0]);
    }

    public static ServerConfiguration readConfigFromFile(String filePath) throws InvalidConfigException {
        log.info("Loading Configuration.");
        Vertx vertx = Vertx.vertx();

        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
                .setOptional(false)
                .setFormat("yaml")
                .setConfig(new JsonObject().put("path", filePath));

        ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(fileStore);
        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        try {
            JsonObject content = retriever.getConfig().toCompletionStage().toCompletableFuture().join();
            return content.mapTo(ServerConfiguration.class);
        } catch (Exception e) {
            throw new InvalidConfigException("Failed to load Application Configuration", e);
        } finally {
            retriever.close();
            vertx.close();
        }
    }
}

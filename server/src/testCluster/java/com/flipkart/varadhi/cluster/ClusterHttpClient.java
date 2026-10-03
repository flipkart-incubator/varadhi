package com.flipkart.varadhi.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.varadhi.entities.JsonMapper;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ContextResolver;
import jakarta.ws.rs.ext.Provider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import java.util.Map;

import static com.flipkart.varadhi.common.Constants.USER_ID_HEADER;

/** Minimal HTTP client against a Varadhi base URI (pod or LB). */
public final class ClusterHttpClient implements AutoCloseable {

    public static final String SUPER_USER = "thanos";

    private final String baseUri;
    private final Client client;

    public ClusterHttpClient(String baseUri) {
        this.baseUri = baseUri.endsWith("/") ? baseUri.substring(0, baseUri.length() - 1) : baseUri;
        ClientConfig cfg = new ClientConfig().register(new ObjectMapperContextResolver());
        this.client = ClientBuilder.newClient(cfg);
        this.client.property(ClientProperties.CONNECT_TIMEOUT, 10_000);
        this.client.property(ClientProperties.READ_TIMEOUT, 60_000);
    }

    public String baseUri() {
        return baseUri;
    }

    public Response get(String path) {
        return client.target(baseUri + path)
                     .request(MediaType.APPLICATION_JSON_TYPE)
                     .header(USER_ID_HEADER, SUPER_USER)
                     .get();
    }

    public <T> Response postJson(String path, T body) {
        return client.target(baseUri + path)
                     .request(MediaType.APPLICATION_JSON_TYPE)
                     .header(USER_ID_HEADER, SUPER_USER)
                     .post(Entity.json(body));
    }

    public Response produce(String project, String topic, byte[] payload, Map<String, String> headers) {
        var inv = client.target(baseUri + "/v1/projects/" + project + "/topics/" + topic + "/produce")
                        .request(MediaType.APPLICATION_JSON_TYPE)
                        .header(USER_ID_HEADER, SUPER_USER);
        if (headers != null) {
            headers.forEach(inv::header);
        }
        return inv.post(Entity.entity(payload != null ? payload : new byte[0], MediaType.APPLICATION_OCTET_STREAM_TYPE));
    }

    public void ensureOrgTeamProject(String orgName, String teamName, String projectName) {
        try (Response r = postJson("/v1/orgs", Org.of(orgName))) {
            if (r.getStatus() != 200 && r.getStatus() != 409) {
                throw new IllegalStateException("create org failed: " + r.getStatus() + " " + r.readEntity(String.class));
            }
        }
        try (Response r = postJson("/v1/orgs/" + orgName + "/teams", Team.of(teamName, orgName))) {
            if (r.getStatus() != 200 && r.getStatus() != 409) {
                throw new IllegalStateException("create team failed: " + r.getStatus() + " " + r.readEntity(String.class));
            }
        }
        try (Response r = postJson("/v1/projects", Project.of(projectName, "", teamName, orgName))) {
            if (r.getStatus() != 200 && r.getStatus() != 409) {
                throw new IllegalStateException(
                    "create project failed: " + r.getStatus() + " " + r.readEntity(String.class)
                );
            }
        }
        awaitProjectReadable(projectName);
    }

    public void awaitProjectReadable(String projectName) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try (Response r = get("/v1/projects/" + projectName)) {
                if (r.getStatus() == 200) {
                    return;
                }
            } catch (Exception ignored) {
                // retry
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new IllegalStateException("project not readable: " + projectName);
    }

    /** Poll produce until the topic is on the hot path (not still provisioning). */
    public void awaitProduceReady(String project, String topic) throws InterruptedException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            Map<String, String> headers = Map.of("X_MESSAGE_ID", "warmup-" + System.nanoTime());
            try (Response response = produce(project, topic, "warmup".getBytes(), headers)) {
                int status = response.getStatus();
                if (status == 200) {
                    return;
                }
                if (status != 404 && status != 422) {
                    throw new AssertionError(
                        "unexpected produce status " + status + ": " + response.readEntity(String.class)
                    );
                }
            }
            Thread.sleep(250);
        }
        throw new AssertionError("produce path not ready for " + project + "/" + topic);
    }

    @Override
    public void close() {
        client.close();
    }

    @Provider
    static final class ObjectMapperContextResolver implements ContextResolver<ObjectMapper> {
        @Override
        public ObjectMapper getContext(Class<?> type) {
            return JsonMapper.getMapper();
        }
    }
}

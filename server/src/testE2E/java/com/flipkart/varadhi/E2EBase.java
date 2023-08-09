package com.flipkart.varadhi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.utils.JsonMapper;
import com.flipkart.varadhi.web.ErrorResponse;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.junit.jupiter.api.Assertions;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import java.util.List;

public class E2EBase {

    protected static final String VaradhiBaseUri = "http://localhost:8080";
    private static final int ConnectTimeoutMs = 10 * 1000;
    private static final int ReadTimeoutMs = 10 * 1000;


    String getOrgsUri() {
        return String.format("%s/v1/orgs", VaradhiBaseUri);
    }

    String getOrgUri(Org org) {
        return String.join("/", getOrgsUri(), org.getName());
    }

    String getTeamsUri(String orgName) {
        return String.join("/", String.join("/", getOrgsUri(), orgName), "teams");
    }

    String getTeamUri(Team team) {
        return String.join("/", getTeamsUri(team.getOrgName()), team.getName());
    }

    String getProjectListUri(String orgName, String teamName) {
        return String.join("/", getTeamsUri(orgName), teamName, "projects");
    }

    String getProjectCreateUri() {
        return String.join("/", VaradhiBaseUri, "v1", "projects");
    }

    String getProjectUri(Project project) {
        return String.join("/", getProjectCreateUri(), project.getName());
    }

    List<Org> getOrgs(Response response) {
        return response.readEntity(new GenericType<>() {
        });
    }

    List<Team> getTeams(Response response) {
        return response.readEntity(new GenericType<>() {
        });
    }

    List<Project> getProjects(Response response) {
        return response.readEntity(new GenericType<>() {
        });
    }

    void cleanupOrgs(List<Org> orgs) {
        List<Org> existingOrgs = getOrgs(makeListRequest(getOrgsUri(), 200));
        existingOrgs.forEach(o -> {
            if (orgs.contains(o)) {
                cleanupOrg(o);
            }
        });
    }

    void cleanupOrg(Org org) {
        List<Team> existingTeams = getTeams(makeListRequest(getTeamsUri(org.getName()), 200));
        existingTeams.forEach(t -> cleanupTeam(t));
        makeDeleteRequest(getOrgUri(org), 200);
    }

    void cleanupTeam(Team team) {
        List<Project> existingProjects =
                getProjects(makeListRequest(getProjectListUri(team.getOrgName(), team.getName()), 200));
        existingProjects.forEach(p -> makeDeleteRequest(getProjectUri(p), 200));
        makeDeleteRequest(getTeamUri(team), 200);
    }

    Client getClient() {
        ClientConfig clientConfig = new ClientConfig().register(new ObjectMapperContextResolver());
        Client client = ClientBuilder.newClient(clientConfig);
        client.property(ClientProperties.CONNECT_TIMEOUT, ConnectTimeoutMs);
        client.property(ClientProperties.READ_TIMEOUT, ReadTimeoutMs);
        return client;
    }

    <T> T makeCreateRequest(String targetUrl, T entity, int expectedStatus) {
        Response response = makeHttpPostRequest(targetUrl, entity);
        Assertions.assertNotNull(response);
        Assertions.assertEquals(expectedStatus, response.getStatus());
        Class<T> clazz = (Class<T>) entity.getClass();
        return response.readEntity(clazz);
    }

    <T> void makeCreateRequest(
            String targetUrl, T entity, int expectedStatus, String expectedResponse, boolean isErrored
    ) {
        Response response = makeHttpPostRequest(targetUrl, entity);
        Assertions.assertEquals(expectedStatus, response.getStatus());
        if (null != expectedResponse) {
            String responseMsg =
                    isErrored ? response.readEntity(ErrorResponse.class).reason() : response.readEntity(String.class);
            Assertions.assertEquals(expectedResponse, responseMsg);
        }
    }

    <T> T makeGetRequest(String targetUrl, Class<T> clazz, int expectedStatus) {
        Response response = makeHttpGetRequest(targetUrl);
        Assertions.assertEquals(expectedStatus, response.getStatus());
        return response.readEntity(clazz);
    }

    void makeGetRequest(String targetUrl, int expectedStatus, String expectedResponse, boolean isErrored) {
        Response response = makeHttpGetRequest(targetUrl);
        Assertions.assertEquals(expectedStatus, response.getStatus());
        if (null != expectedResponse) {
            String responseMsg =
                    isErrored ? response.readEntity(ErrorResponse.class).reason() : response.readEntity(String.class);
            Assertions.assertEquals(expectedResponse, responseMsg);
        }
    }

    Response makeListRequest(String targetUrl, int expectedStatus) {
        Response response = makeHttpGetRequest(targetUrl);
        Assertions.assertEquals(expectedStatus, response.getStatus());
        return response;
    }

    void makeListRequest(String targetUrl, int expectedStatus, String expectedResponse, boolean isErrored) {
        Response response = makeHttpGetRequest(targetUrl);
        Assertions.assertEquals(expectedStatus, response.getStatus());
        if (null != expectedResponse) {
            String responseMsg =
                    isErrored ? response.readEntity(ErrorResponse.class).reason() : response.readEntity(String.class);
            Assertions.assertEquals(expectedResponse, responseMsg);
        }
    }

    <T> T makeUpdateRequest(String targetUrl, T entity, int expectedStatus) {
        Response response = makeHttpPutRequest(targetUrl, entity);
        Assertions.assertNotNull(response);
        Assertions.assertEquals(expectedStatus, response.getStatus());
        Class<T> clazz = (Class<T>) entity.getClass();
        return response.readEntity(clazz);
    }

    <T> void makeUpdateRequest(
            String targetUrl, T entity, int expectedStatus, String expectedResponse, boolean isErrored
    ) {
        Response response = makeHttpPutRequest(targetUrl, entity);
        Assertions.assertEquals(expectedStatus, response.getStatus());
        if (null != expectedResponse) {
            String responseMsg =
                    isErrored ? response.readEntity(ErrorResponse.class).reason() : response.readEntity(String.class);
            Assertions.assertEquals(expectedResponse, responseMsg);
        }
    }

    void makeDeleteRequest(String targetUrl, int expectedStatus) {
        Response response = makeHttpDeleteRequest(targetUrl);
        Assertions.assertEquals(expectedStatus, response.getStatus());
    }

    void makeDeleteRequest(String targetUrl, int expectedStatus, String expectedResponse, boolean isErrored) {
        Response response = makeHttpDeleteRequest(targetUrl);
        Assertions.assertEquals(expectedStatus, response.getStatus());
        if (null != expectedResponse) {
            String responseMsg =
                    isErrored ? response.readEntity(ErrorResponse.class).reason() : response.readEntity(String.class);
            Assertions.assertEquals(expectedResponse, responseMsg);
        }
    }

    <T> Response makeHttpPostRequest(String targetUrl, T entityToCreate) {
        return getClient()
                .target(targetUrl)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.entity(entityToCreate, MediaType.APPLICATION_JSON_TYPE));
    }

    Response makeHttpGetRequest(String targetUrl) {
        return getClient()
                .target(targetUrl)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .get();
    }

    <T> Response makeHttpPutRequest(String targetUrl, T entityToCreate) {
        return getClient()
                .target(targetUrl)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .put(Entity.entity(entityToCreate, MediaType.APPLICATION_JSON_TYPE));
    }

    Response makeHttpDeleteRequest(String targetUrl) {
        return getClient()
                .target(targetUrl)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .delete();
    }


    @Provider
    public class ObjectMapperContextResolver implements ContextResolver<ObjectMapper> {

        private final ObjectMapper mapper = JsonMapper.getMapper();

        @Override
        public ObjectMapper getContext(Class<?> type) {
            return mapper;
        }
    }
}

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


    static String getOrgsUri() {
        return String.format("%s/v1/orgs", VaradhiBaseUri);
    }

    static String getOrgUri(Org org) {
        return String.join("/", getOrgsUri(), org.getName());
    }

    static String getTeamsUri(String orgName) {
        return String.join("/", String.join("/", getOrgsUri(), orgName), "teams");
    }

    static String getTeamUri(Team team) {
        return String.join("/", getTeamsUri(team.getOrg()), team.getName());
    }

    static String getProjectListUri(String orgName, String teamName) {
        return String.join("/", getTeamsUri(orgName), teamName, "projects");
    }

    static String getProjectCreateUri() {
        return String.join("/", VaradhiBaseUri, "v1", "projects");
    }

    static String getProjectUri(Project project) {
        return String.join("/", getProjectCreateUri(), project.getName());
    }


    static String getTopicsUri(Project project) {
        return String.join("/", getProjectUri(project), "topics");
    }

    static List<Org> getOrgs(Response response) {
        return response.readEntity(new GenericType<>() {
        });
    }

    static List<Team> getTeams(Response response) {
        return response.readEntity(new GenericType<>() {
        });
    }

    static List<Project> getProjects(Response response) {
        return response.readEntity(new GenericType<>() {
        });
    }

    static void cleanupOrgs(List<Org> orgs) {
        List<Org> existingOrgs = getOrgs(makeListRequest(getOrgsUri(), 200));
        existingOrgs.forEach(o -> {
            if (orgs.contains(o)) {
                cleanupOrg(o);
            }
        });
    }

    static void cleanupOrg(Org org) {
        List<Team> existingTeams = getTeams(makeListRequest(getTeamsUri(org.getName()), 200));
        existingTeams.forEach(t -> cleanupTeam(t));
        makeDeleteRequest(getOrgUri(org), 200);
    }

    static void cleanupTeam(Team team) {
        List<Project> existingProjects =
                getProjects(makeListRequest(getProjectListUri(team.getOrg(), team.getName()), 200));
        existingProjects.forEach(p -> cleanupProject(p));
        makeDeleteRequest(getTeamUri(team), 200);
    }

    static void cleanupProject(Project project) {
        //TODO:: add cleanup of other resources when implemented.
        makeDeleteRequest(getProjectUri(project), 200);
    }

    static Client getClient() {
        ClientConfig clientConfig = new ClientConfig().register(new ObjectMapperContextResolver());
        Client client = ClientBuilder.newClient(clientConfig);
        client.property(ClientProperties.CONNECT_TIMEOUT, ConnectTimeoutMs);
        client.property(ClientProperties.READ_TIMEOUT, ReadTimeoutMs);
        return client;
    }

    static <T> T makeCreateRequest(String targetUrl, T entity, int expectedStatus) {
        Response response = makeHttpPostRequest(targetUrl, entity);
        Assertions.assertNotNull(response);
        Assertions.assertEquals(expectedStatus, response.getStatus());
        Class<T> clazz = (Class<T>) entity.getClass();
        return response.readEntity(clazz);
    }

    static <T> void makeCreateRequest(
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

    static <T> T makeGetRequest(String targetUrl, Class<T> clazz, int expectedStatus) {
        Response response = makeHttpGetRequest(targetUrl);
        Assertions.assertEquals(expectedStatus, response.getStatus());
        return response.readEntity(clazz);
    }

    static void makeGetRequest(String targetUrl, int expectedStatus, String expectedResponse, boolean isErrored) {
        Response response = makeHttpGetRequest(targetUrl);
        Assertions.assertEquals(expectedStatus, response.getStatus());
        if (null != expectedResponse) {
            String responseMsg =
                    isErrored ? response.readEntity(ErrorResponse.class).reason() : response.readEntity(String.class);
            Assertions.assertEquals(expectedResponse, responseMsg);
        }
    }

    static Response makeListRequest(String targetUrl, int expectedStatus) {
        Response response = makeHttpGetRequest(targetUrl);
        Assertions.assertEquals(expectedStatus, response.getStatus());
        return response;
    }

    static void makeListRequest(String targetUrl, int expectedStatus, String expectedResponse, boolean isErrored) {
        Response response = makeHttpGetRequest(targetUrl);
        Assertions.assertEquals(expectedStatus, response.getStatus());
        if (null != expectedResponse) {
            String responseMsg =
                    isErrored ? response.readEntity(ErrorResponse.class).reason() : response.readEntity(String.class);
            Assertions.assertEquals(expectedResponse, responseMsg);
        }
    }

    static <T> T makeUpdateRequest(String targetUrl, T entity, int expectedStatus) {
        Response response = makeHttpPutRequest(targetUrl, entity);
        Assertions.assertNotNull(response);
        Assertions.assertEquals(expectedStatus, response.getStatus());
        Class<T> clazz = (Class<T>) entity.getClass();
        return response.readEntity(clazz);
    }

    static <T> void makeUpdateRequest(
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

    static void makeDeleteRequest(String targetUrl, int expectedStatus) {
        Response response = makeHttpDeleteRequest(targetUrl);
        Assertions.assertEquals(expectedStatus, response.getStatus());
    }

    static void makeDeleteRequest(String targetUrl, int expectedStatus, String expectedResponse, boolean isErrored) {
        Response response = makeHttpDeleteRequest(targetUrl);
        Assertions.assertEquals(expectedStatus, response.getStatus());
        if (null != expectedResponse) {
            String responseMsg =
                    isErrored ? response.readEntity(ErrorResponse.class).reason() : response.readEntity(String.class);
            Assertions.assertEquals(expectedResponse, responseMsg);
        }
    }

    static <T> Response makeHttpPostRequest(String targetUrl, T entityToCreate) {
        return getClient()
                .target(targetUrl)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.entity(entityToCreate, MediaType.APPLICATION_JSON_TYPE));
    }

    static Response makeHttpGetRequest(String targetUrl) {
        return getClient()
                .target(targetUrl)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .get();
    }

    static <T> Response makeHttpPutRequest(String targetUrl, T entityToCreate) {
        return getClient()
                .target(targetUrl)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .put(Entity.entity(entityToCreate, MediaType.APPLICATION_JSON_TYPE));
    }

    static Response makeHttpDeleteRequest(String targetUrl) {
        return getClient()
                .target(targetUrl)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .delete();
    }


    @Provider
    public static class ObjectMapperContextResolver implements ContextResolver<ObjectMapper> {

        private final ObjectMapper mapper = JsonMapper.getMapper();

        @Override
        public ObjectMapper getContext(Class<?> type) {
            return mapper;
        }
    }
}

package com.flipkart.varadhi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.common.utils.JsonMapper;
import com.flipkart.varadhi.web.ErrorResponse;
import com.flipkart.varadhi.web.entities.SubscriptionResource;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ContextResolver;
import jakarta.ws.rs.ext.Provider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;

import java.util.List;

import static com.flipkart.varadhi.common.Constants.QueryParams.QUERY_PARAM_DELETION_TYPE;
import static com.flipkart.varadhi.common.Constants.USER_ID_HEADER;
import static com.flipkart.varadhi.entities.Versioned.NAME_SEPARATOR_REGEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class E2EBase {

    protected static final String VARADHI_BASE_URI = "http://localhost:18488";
    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int READ_TIMEOUT_MS = 60_000;
    public static final String SUPER_USER = "thanos";
    public static final int EXPECTED_STATUS_OK = 200;

    private static final Client CLIENT = createClient();

    private static Client createClient() {
        ClientConfig clientConfig = new ClientConfig().register(new ObjectMapperContextResolver());
        Client client = ClientBuilder.newClient(clientConfig);
        client.property(ClientProperties.CONNECT_TIMEOUT, CONNECT_TIMEOUT_MS);
        client.property(ClientProperties.READ_TIMEOUT, READ_TIMEOUT_MS);
        return client;
    }

    private static String buildUri(String... segments) {
        return String.join("/", segments);
    }

    public static String getOrgsUri() {
        return buildUri(VARADHI_BASE_URI, "v1", "orgs");
    }

    public static String getOrgUri(Org org) {
        return buildUri(getOrgsUri(), org.getName());
    }

    public static String getTeamsUri(String orgName) {
        return buildUri(getOrgsUri(), orgName, "teams");
    }

    public static String getTeamUri(Team team) {
        return buildUri(getTeamsUri(team.getOrg()), team.getName());
    }

    public static String getProjectListUri(String orgName, String teamName) {
        return buildUri(getTeamsUri(orgName), teamName, "projects");
    }

    public static String getProjectCreateUri() {
        return buildUri(VARADHI_BASE_URI, "v1", "projects");
    }

    public static String getProjectUri(Project project) {
        return buildUri(getProjectCreateUri(), project.getName());
    }

    public static String getTopicsUri(Project project) {
        return buildUri(getProjectUri(project), "topics");
    }

    public static String getTopicsUri(Project project, String topicName) {
        return buildUri(getTopicsUri(project), topicName);
    }

    public static String getSubscriptionsUri(Project project) {
        return buildUri(getProjectUri(project), "subscriptions");
    }

    public static String getSubscriptionsUri(Project project, String subscriptionName) {
        return buildUri(getSubscriptionsUri(project), subscriptionName);
    }

    public static List<Org> getOrgs(Response response) {
        return response.readEntity(new GenericType<>() {
        });
    }

    public static List<Team> getTeams(Response response) {
        return response.readEntity(new GenericType<>() {
        });
    }

    public static List<Project> getProjects(Response response) {
        return response.readEntity(new GenericType<>() {
        });
    }

    public static List<String> getTopics(Response response) {
        return response.readEntity(new GenericType<>() {
        });
    }

    public static List<String> getSubscriptions(Response response) {
        return response.readEntity(new GenericType<>() {
        });
    }

    public static void cleanupOrgs(List<Org> orgs) {
        getOrgs(makeListRequest(getOrgsUri(), EXPECTED_STATUS_OK)).stream()
                                                                  .filter(orgs::contains)
                                                                  .forEach(E2EBase::cleanupOrg);
    }

    public static void cleanupOrg(Org org) {
        getTeams(makeListRequest(getTeamsUri(org.getName()), EXPECTED_STATUS_OK)).forEach(E2EBase::cleanupTeam);
        makeDeleteRequest(getOrgUri(org), EXPECTED_STATUS_OK);
    }

    public static void cleanupTeam(Team team) {
        getProjects(makeListRequest(getProjectListUri(team.getOrg(), team.getName()), EXPECTED_STATUS_OK)).forEach(
            E2EBase::cleanupProject
        );
        makeDeleteRequest(getTeamUri(team), EXPECTED_STATUS_OK);
    }

    public static void cleanupProject(Project project) {
        cleanupSubscriptionsOnProject(project);
        List<String> existingTopics = getTopics(makeListRequest(getTopicsUri(project), EXPECTED_STATUS_OK));
        if (!existingTopics.isEmpty()) {
            cleanupSubscriptionsOnTopics(existingTopics, project.getName());
            existingTopics.forEach(topic -> cleanupTopic(topic, project));
        }
        makeDeleteRequest(getProjectUri(project), EXPECTED_STATUS_OK);
    }

    public static void cleanupTopic(String topicName, Project project) {
        makeDeleteRequest(
            getTopicsUri(project, topicName),
            ResourceDeletionType.HARD_DELETE.toString(),
            EXPECTED_STATUS_OK
        );
    }

    public static void cleanupSubscriptionsOnTopics(List<String> topicNames, String projectName) {
        getOrgs(makeListRequest(getOrgsUri(), EXPECTED_STATUS_OK)).forEach(
            org -> getTeams(makeListRequest(getTeamsUri(org.getName()), EXPECTED_STATUS_OK)).forEach(
                team -> getProjects(
                    makeListRequest(getProjectListUri(team.getOrg(), team.getName()), EXPECTED_STATUS_OK)
                ).forEach(
                    project -> getSubscriptions(makeListRequest(getSubscriptionsUri(project), EXPECTED_STATUS_OK))
                                                                                                                  .forEach(
                                                                                                                      sub -> {
                                                                                                                          SubscriptionResource res =
                                                                                                                              makeGetRequest(
                                                                                                                                  getSubscriptionsUri(
                                                                                                                                      project,
                                                                                                                                      sub
                                                                                                                                  ),
                                                                                                                                  SubscriptionResource.class,
                                                                                                                                  EXPECTED_STATUS_OK
                                                                                                                              );
                                                                                                                          if (topicNames.contains(
                                                                                                                              res.getTopic()
                                                                                                                          ) && projectName.equals(
                                                                                                                              res.getTopicProject()
                                                                                                                          )) {
                                                                                                                              makeDeleteRequest(
                                                                                                                                  getSubscriptionsUri(
                                                                                                                                      project,
                                                                                                                                      sub
                                                                                                                                  ),
                                                                                                                                  ResourceDeletionType.HARD_DELETE.toString(),
                                                                                                                                  EXPECTED_STATUS_OK
                                                                                                                              );
                                                                                                                          }
                                                                                                                      }
                                                                                                                  )
                )
            )
        );
    }

    public static void cleanupSubscriptionsOnProject(Project project) {
        getSubscriptions(makeListRequest(getSubscriptionsUri(project), EXPECTED_STATUS_OK)).forEach(
            sub -> makeDeleteRequest(
                getSubscriptionsUri(project, sub.split(NAME_SEPARATOR_REGEX)[1]),
                ResourceDeletionType.HARD_DELETE.toString(),
                EXPECTED_STATUS_OK
            )
        );
    }

    public static <T> T makeCreateRequest(String targetUrl, T entity, int expectedStatus) {
        return processRequest(makeHttpPostRequest(targetUrl, entity), expectedStatus, (Class<T>)entity.getClass());
    }

    public static <T> void makeCreateRequest(
        String targetUrl,
        T entity,
        int expectedStatus,
        String expectedResponse,
        boolean isErrored
    ) {
        processRequest(makeHttpPostRequest(targetUrl, entity), expectedStatus, expectedResponse, isErrored);
    }

    public static <T> T makeGetRequest(String targetUrl, Class<T> clazz, int expectedStatus) {
        return processRequest(makeHttpGetRequest(targetUrl), expectedStatus, clazz);
    }

    public static void makeGetRequest(
        String targetUrl,
        int expectedStatus,
        String expectedResponse,
        boolean isErrored
    ) {
        processRequest(makeHttpGetRequest(targetUrl), expectedStatus, expectedResponse, isErrored);
    }

    public static Response makeListRequest(String targetUrl, int expectedStatus) {
        return processRequest(makeHttpGetRequest(targetUrl), expectedStatus);
    }

    public static void makeListRequest(
        String targetUrl,
        int expectedStatus,
        String expectedResponse,
        boolean isErrored
    ) {
        processRequest(makeHttpGetRequest(targetUrl), expectedStatus, expectedResponse, isErrored);
    }

    public static <T> T makeUpdateRequest(String targetUrl, T entity, int expectedStatus) {
        return processRequest(makeHttpPutRequest(targetUrl, entity), expectedStatus, (Class<T>)entity.getClass());
    }

    public static <T> void makeUpdateRequest(
        String targetUrl,
        T entity,
        int expectedStatus,
        String expectedResponse,
        boolean isErrored
    ) {
        processRequest(makeHttpPutRequest(targetUrl, entity), expectedStatus, expectedResponse, isErrored);
    }

    public static void makeDeleteRequest(String targetUrl, int expectedStatus) {
        processRequest(makeHttpDeleteRequest(targetUrl), expectedStatus);
    }

    public static void makeDeleteRequest(String targetUrl, String deletionType, int expectedStatus) {
        processRequest(makeHttpDeleteRequest(targetUrl, deletionType), expectedStatus);
    }

    public static void makeDeleteRequest(
        String targetUrl,
        int expectedStatus,
        String expectedResponse,
        boolean isErrored
    ) {
        processRequest(makeHttpDeleteRequest(targetUrl), expectedStatus, expectedResponse, isErrored);
    }

    public static void makePatchRequest(String targetUrl, int expectedStatus) {
        processRequest(makeHttpPatchRequest(targetUrl), expectedStatus);
    }

    private static <T> T processRequest(Response response, int expectedStatus, Class<T> clazz) {
        assertNotNull(response);
        assertEquals(expectedStatus, response.getStatus());
        return response.readEntity(clazz);
    }

    private static void processRequest(
        Response response,
        int expectedStatus,
        String expectedResponse,
        boolean isErrored
    ) {
        assertEquals(expectedStatus, response.getStatus());
        if (expectedResponse != null) {
            String responseMsg = isErrored ?
                response.readEntity(ErrorResponse.class).reason() :
                response.readEntity(String.class);
            assertEquals(expectedResponse, responseMsg);
        }
    }

    private static Response processRequest(Response response, int expectedStatus) {
        assertEquals(expectedStatus, response.getStatus());
        return response;
    }

    public static <T> Response makeHttpPostRequest(String targetUrl, T entityToCreate) {
        return CLIENT.target(targetUrl)
                     .request(MediaType.APPLICATION_JSON_TYPE)
                     .header(USER_ID_HEADER, SUPER_USER)
                     .post(Entity.entity(entityToCreate, MediaType.APPLICATION_JSON_TYPE));
    }

    public static Response makeHttpGetRequest(String targetUrl) {
        return CLIENT.target(targetUrl)
                     .request(MediaType.APPLICATION_JSON_TYPE)
                     .header(USER_ID_HEADER, SUPER_USER)
                     .get();
    }

    public static <T> Response makeHttpPutRequest(String targetUrl, T entityToCreate) {
        return CLIENT.target(targetUrl)
                     .request(MediaType.APPLICATION_JSON_TYPE)
                     .header(USER_ID_HEADER, SUPER_USER)
                     .put(Entity.entity(entityToCreate, MediaType.APPLICATION_JSON_TYPE));
    }

    public static Response makeHttpDeleteRequest(String targetUrl) {
        return CLIENT.target(targetUrl)
                     .request(MediaType.APPLICATION_JSON_TYPE)
                     .header(USER_ID_HEADER, SUPER_USER)
                     .delete();
    }

    public static Response makeHttpDeleteRequest(String targetUrl, String deletionType) {
        return CLIENT.target(targetUrl)
                     .queryParam(QUERY_PARAM_DELETION_TYPE, deletionType)
                     .request(MediaType.APPLICATION_JSON_TYPE)
                     .header(USER_ID_HEADER, SUPER_USER)
                     .delete();
    }

    public static Response makeHttpPatchRequest(String targetUrl) {
        return CLIENT.target(targetUrl)
                     .request(MediaType.APPLICATION_JSON_TYPE)
                     .header(USER_ID_HEADER, SUPER_USER)
                     .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
                     .method("PATCH", Entity.json("{}"));
    }

    @Provider
    public static class ObjectMapperContextResolver implements ContextResolver<ObjectMapper> {

        private final ObjectMapper mapper = JsonMapper.getMapper();

        @Override
        public ObjectMapper getContext(Class<?> type) {
            return mapper;
        }
    }

    protected E2EBase() {
        // Protected constructor to allow subclassing
    }
}

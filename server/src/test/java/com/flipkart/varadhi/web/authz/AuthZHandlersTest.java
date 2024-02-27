package com.flipkart.varadhi.web.authz;

import com.flipkart.varadhi.entities.auth.IAMPolicyRecord;
import com.flipkart.varadhi.entities.auth.IAMPolicyRequest;
import com.flipkart.varadhi.entities.auth.IAMPolicyResponse;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.services.AuthZService;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.web.ErrorResponse;
import com.flipkart.varadhi.web.WebTestBase;
import com.flipkart.varadhi.web.v1.authz.AuthZHandlers;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.flipkart.varadhi.utils.AuthZHelper.getAuthResourceFQN;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AuthZHandlersTest extends WebTestBase {
    public static final String AUTHZ_DEBUG_BASE = "/authz/debug";
    public static final String AUTHZ_ORG_POLICY = "/orgs/:org/policy";
    public static final String AUTHZ_TEAM_POLICY = "/orgs/:org/teams/:team/policy";
    public static final String AUTHZ_PROJECT_POLICY = "/projects/:project/policy";
    public static final String AUTHZ_TOPIC_POLICY = "/projects/:project/topics/:topic/policy";
    AuthZHandlers authZHandlers;
    AuthZService authZService;

    @BeforeEach
    public void PreTest() throws InterruptedException {
        super.setUp();
        authZService = mock(AuthZService.class);
        authZHandlers = new AuthZHandlers(authZService);

        Route routeGetAllNodes =
                router.get(AUTHZ_DEBUG_BASE).handler(wrapBlocking(authZHandlers::getAllIAMPolicy));
        setupFailureHandler(routeGetAllNodes);

        Route routeGetIAMPolicy = router.get(AUTHZ_ORG_POLICY)
                .handler(wrapBlocking(authZHandlers.getIAMPolicyHandler(ResourceType.ORG)));
        setupFailureHandler(routeGetIAMPolicy);

        Route routeSetIAMPolicy = router.put(AUTHZ_ORG_POLICY).handler(bodyHandler)
                .handler(wrapBlocking(authZHandlers.setIAMPolicyHandler(ResourceType.ORG)));
        setupFailureHandler(routeSetIAMPolicy);

        Route routeDelIAMPolicy = router.delete(AUTHZ_ORG_POLICY).handler(bodyHandler)
                .handler(wrapBlocking(authZHandlers.deleteIAMPolicyHandler(ResourceType.ORG)));
        setupFailureHandler(routeDelIAMPolicy);
    }

    @AfterEach
    public void PostTest() throws InterruptedException {
        super.tearDown();
    }

    private String getOrgIAMPolicyUrl(String orgId) {
        return AUTHZ_ORG_POLICY.replace(":org", orgId);
    }

    private String getTeamIAMPolicyUrl(String orgId, String teamId) {
        return AUTHZ_TEAM_POLICY.replace(":org", orgId).replace(":team", teamId);
    }

    private String getProjectIAMPolicyUrl(String projectId) {
        return AUTHZ_PROJECT_POLICY.replace(":project", projectId);
    }

    private String getTopicIAMPolicyUrl(String projectId, String topicId) {
        return AUTHZ_TOPIC_POLICY.replace(":project", projectId).replace(":topic", topicId);
    }

    @Test
    void testGetAllIAMPolicyRecords() throws Exception {
        List<IAMPolicyRecord> recordsList = List.of(
                new IAMPolicyRecord(
                        getAuthResourceFQN(ResourceType.ORG, "testNode1"), Map.of(), 0),
                new IAMPolicyRecord(
                        getAuthResourceFQN(ResourceType.ORG, "testNode2"), Map.of(), 0)
        );

        List<IAMPolicyResponse> expected = List.of(
                new IAMPolicyResponse(
                        getAuthResourceFQN(ResourceType.ORG, "testNode1"), "testNode1", ResourceType.ORG, Map.of(), 0),
                new IAMPolicyResponse(
                        getAuthResourceFQN(ResourceType.ORG, "testNode2"), "testNode2", ResourceType.ORG, Map.of(), 0)
        );

        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, AUTHZ_DEBUG_BASE);
        doReturn(recordsList).when(authZService).getAll();

        HttpResponse<Buffer> responseBuffer = sendRequest(request, null);
        List<IAMPolicyRecord> response =
                jsonDeserialize(responseBuffer.bodyAsString(), List.class, IAMPolicyResponse.class);

        assertEquals(expected.size(), response.size());
        assertArrayEquals(expected.toArray(), response.toArray());
        verify(authZService, times(1)).getAll();
    }

    @Test
    void testDeleteIAMPolicyRecord() throws Exception {
        String resourceId = "testNode";
        IAMPolicyRecord node = new IAMPolicyRecord(getAuthResourceFQN(ResourceType.ORG, "testNode"), Map.of(), 0);

        HttpRequest<Buffer> request =
                createRequest(HttpMethod.DELETE, getOrgIAMPolicyUrl(resourceId));
        doNothing().when(authZService).deleteIAMPolicy(eq(ResourceType.ORG), eq(resourceId));

        sendRequestWithoutBody(request, null);
        verify(authZService, times(1)).deleteIAMPolicy(eq(ResourceType.ORG), eq(resourceId));

        String notFoundError = String.format("IAMPolicyRecord on resource(%s) not found.", resourceId);
        doThrow(new ResourceNotFoundException(notFoundError)).when(authZService)
                .deleteIAMPolicy(ResourceType.ORG, resourceId);
        ErrorResponse errResponse = sendRequestWithoutBody(request, 404, notFoundError, ErrorResponse.class);
        assertEquals(notFoundError, errResponse.reason());
    }

    @Test
    void testSetOrgIAMPolicy() throws InterruptedException {
        String orgName = "myOrg";
        String user = "user.a";
        Set<String> roles = Set.of("role1", "role2");

        IAMPolicyRecord policyRecord =
                new IAMPolicyRecord(getAuthResourceFQN(ResourceType.ORG, orgName), Map.of(user, roles), 0);
        IAMPolicyResponse expected =
                new IAMPolicyResponse(
                        policyRecord.getName(), orgName, ResourceType.ORG, policyRecord.getRoleBindings(), 0);
        IAMPolicyRequest assignmentUpdate = new IAMPolicyRequest(user, roles);

        HttpRequest<Buffer> request = createRequest(HttpMethod.PUT, getOrgIAMPolicyUrl(orgName));
        doReturn(policyRecord).when(authZService)
                .setIAMPolicy(eq(ResourceType.ORG), eq(orgName), eq(assignmentUpdate));

        IAMPolicyResponse response = sendRequestWithBody(request, assignmentUpdate, IAMPolicyResponse.class);
        assertEquals(expected, response);
        verify(authZService, times(1)).setIAMPolicy(eq(ResourceType.ORG), eq(orgName), eq(assignmentUpdate));

        String someInternalError = "Some internal error";
        doThrow(new MetaStoreException(someInternalError)).when(authZService)
                .setIAMPolicy(eq(ResourceType.ORG), eq(orgName), eq(assignmentUpdate));
        ErrorResponse errResponse =
                sendRequestWithBody(request, assignmentUpdate, 500, someInternalError, ErrorResponse.class);
        assertEquals(someInternalError, errResponse.reason());
    }
}

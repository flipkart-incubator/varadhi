package com.flipkart.varadhi.web.authz;

import com.flipkart.varadhi.entities.auth.IamPolicyRecord;
import com.flipkart.varadhi.entities.auth.IamPolicyRequest;
import com.flipkart.varadhi.entities.auth.IamPolicyResponse;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.services.IamPolicyService;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.web.ErrorResponse;
import com.flipkart.varadhi.web.WebTestBase;
import com.flipkart.varadhi.web.v1.authz.IamPolicyHandlers;
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

import static com.flipkart.varadhi.utils.IamPolicyHelper.getAuthResourceFQN;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class IamPolicyHandlersTest extends WebTestBase {
    public static final String AUTHZ_DEBUG_BASE = "/authz/debug";
    public static final String AUTHZ_ORG_POLICY = "/orgs/:org/policy";
    public static final String AUTHZ_TEAM_POLICY = "/orgs/:org/teams/:team/policy";
    public static final String AUTHZ_PROJECT_POLICY = "/projects/:project/policy";
    public static final String AUTHZ_TOPIC_POLICY = "/projects/:project/topics/:topic/policy";
    IamPolicyHandlers iamPolicyHandlers;
    IamPolicyService iamPolicyService;

    @BeforeEach
    public void PreTest() throws InterruptedException {
        super.setUp();
        iamPolicyService = mock(IamPolicyService.class);
        iamPolicyHandlers = new IamPolicyHandlers(iamPolicyService);

        Route routeGetAllNodes =
                router.get(AUTHZ_DEBUG_BASE).handler(wrapBlocking(iamPolicyHandlers::getAllIamPolicy));
        setupFailureHandler(routeGetAllNodes);

        Route routeGetIamPolicy = router.get(AUTHZ_ORG_POLICY)
                .handler(wrapBlocking(iamPolicyHandlers.getIamPolicyHandler(ResourceType.ORG)));
        setupFailureHandler(routeGetIamPolicy);

        Route routeSetIamPolicy = router.put(AUTHZ_ORG_POLICY).handler(bodyHandler)
                .handler(wrapBlocking(iamPolicyHandlers.setIamPolicyHandler(ResourceType.ORG)));
        setupFailureHandler(routeSetIamPolicy);

        Route routeDelIamPolicy = router.delete(AUTHZ_ORG_POLICY).handler(bodyHandler)
                .handler(wrapBlocking(iamPolicyHandlers.deleteIamPolicyHandler(ResourceType.ORG)));
        setupFailureHandler(routeDelIamPolicy);
    }

    @AfterEach
    public void PostTest() throws InterruptedException {
        super.tearDown();
    }

    private String getOrgIamPolicyUrl(String orgId) {
        return AUTHZ_ORG_POLICY.replace(":org", orgId);
    }

    private String getTeamIamPolicyUrl(String orgId, String teamId) {
        return AUTHZ_TEAM_POLICY.replace(":org", orgId).replace(":team", teamId);
    }

    private String getProjectIamPolicyUrl(String projectId) {
        return AUTHZ_PROJECT_POLICY.replace(":project", projectId);
    }

    private String getTopicIamPolicyUrl(String projectId, String topicId) {
        return AUTHZ_TOPIC_POLICY.replace(":project", projectId).replace(":topic", topicId);
    }

    @Test
    void testGetAllIamPolicyRecords() throws Exception {
        List<IamPolicyRecord> recordsList = List.of(
                new IamPolicyRecord(
                        getAuthResourceFQN(ResourceType.ORG, "testNode1"), Map.of(), 0),
                new IamPolicyRecord(
                        getAuthResourceFQN(ResourceType.ORG, "testNode2"), Map.of(), 0)
        );

        List<IamPolicyResponse> expected = List.of(
                new IamPolicyResponse(
                        getAuthResourceFQN(ResourceType.ORG, "testNode1"), "testNode1", ResourceType.ORG, Map.of(), 0),
                new IamPolicyResponse(
                        getAuthResourceFQN(ResourceType.ORG, "testNode2"), "testNode2", ResourceType.ORG, Map.of(), 0)
        );

        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, AUTHZ_DEBUG_BASE);
        doReturn(recordsList).when(iamPolicyService).getAll();

        HttpResponse<Buffer> responseBuffer = sendRequest(request, null);
        List<IamPolicyRecord> response =
                jsonDeserialize(responseBuffer.bodyAsString(), List.class, IamPolicyResponse.class);

        assertEquals(expected.size(), response.size());
        assertArrayEquals(expected.toArray(), response.toArray());
        verify(iamPolicyService, times(1)).getAll();
    }

    @Test
    void testDeleteIamPolicyRecord() throws Exception {
        String resourceId = "testNode";
        IamPolicyRecord node = new IamPolicyRecord(getAuthResourceFQN(ResourceType.ORG, "testNode"), Map.of(), 0);

        HttpRequest<Buffer> request =
                createRequest(HttpMethod.DELETE, getOrgIamPolicyUrl(resourceId));
        doNothing().when(iamPolicyService).deleteIamPolicy(eq(ResourceType.ORG), eq(resourceId));

        sendRequestWithoutBody(request, null);
        verify(iamPolicyService, times(1)).deleteIamPolicy(eq(ResourceType.ORG), eq(resourceId));

        String notFoundError = String.format("IamPolicyRecord on resource(%s) not found.", resourceId);
        doThrow(new ResourceNotFoundException(notFoundError)).when(iamPolicyService)
                .deleteIamPolicy(ResourceType.ORG, resourceId);
        ErrorResponse errResponse = sendRequestWithoutBody(request, 404, notFoundError, ErrorResponse.class);
        assertEquals(notFoundError, errResponse.reason());
    }

    @Test
    void testSetOrgIamPolicy() throws InterruptedException {
        String orgName = "myOrg";
        String user = "user.a";
        Set<String> roles = Set.of("role1", "role2");

        IamPolicyRecord policyRecord =
                new IamPolicyRecord(getAuthResourceFQN(ResourceType.ORG, orgName), Map.of(user, roles), 0);
        IamPolicyResponse expected =
                new IamPolicyResponse(
                        policyRecord.getName(), orgName, ResourceType.ORG, policyRecord.getRoleBindings(), 0);
        IamPolicyRequest assignmentUpdate = new IamPolicyRequest(user, roles);

        HttpRequest<Buffer> request = createRequest(HttpMethod.PUT, getOrgIamPolicyUrl(orgName));
        doReturn(policyRecord).when(iamPolicyService)
                .setIamPolicy(eq(ResourceType.ORG), eq(orgName), eq(assignmentUpdate));

        IamPolicyResponse response = sendRequestWithBody(request, assignmentUpdate, IamPolicyResponse.class);
        assertEquals(expected, response);
        verify(iamPolicyService, times(1)).setIamPolicy(eq(ResourceType.ORG), eq(orgName), eq(assignmentUpdate));

        String someInternalError = "Some internal error";
        doThrow(new MetaStoreException(someInternalError)).when(iamPolicyService)
                .setIamPolicy(eq(ResourceType.ORG), eq(orgName), eq(assignmentUpdate));
        ErrorResponse errResponse =
                sendRequestWithBody(request, assignmentUpdate, 500, someInternalError, ErrorResponse.class);
        assertEquals(someInternalError, errResponse.reason());
    }
}

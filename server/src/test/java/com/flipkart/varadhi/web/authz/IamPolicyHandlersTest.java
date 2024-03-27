package com.flipkart.varadhi.web.authz;

import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.auth.IAMPolicyRequest;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.entities.auth.RoleBindingNode;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.services.IamPolicyService;
import com.flipkart.varadhi.services.ProjectService;
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class IamPolicyHandlersTest extends WebTestBase {
    public static final String AUTHZ_DEBUG_BASE = "/authz/debug";
    public static final String AUTHZ_DEBUG_PATH = AUTHZ_DEBUG_BASE + "/:resource_type/:resource";
    public static final String AUTHZ_ORG_POLICY = "/orgs/:org/policy";
    public static final String AUTHZ_TEAM_POLICY = "/orgs/:org/teams/:team/policy";
    public static final String AUTHZ_PROJECT_POLICY = "/projects/:project/policy";
    public static final String AUTHZ_TOPIC_POLICY = "/projects/:project/topics/:topic/policy";
    IamPolicyHandlers iamPolicyHandlers;
    IamPolicyService iamPolicyService;
    ProjectService projectService;
    Org o1 = new Org("OrgOne", 0);

    @BeforeEach
    public void PreTest() throws InterruptedException {
        super.setUp();
        projectService = mock(ProjectService.class);
        iamPolicyService = mock(IamPolicyService.class);
        iamPolicyHandlers = new IamPolicyHandlers(projectService, iamPolicyService);

        Route routeGetNode =
                router.get(AUTHZ_DEBUG_PATH).handler(wrapBlocking(iamPolicyHandlers::findRoleBindingNode));
        setupFailureHandler(routeGetNode);

        Route routeGetAllNodes =
                router.get(AUTHZ_DEBUG_BASE).handler(wrapBlocking(iamPolicyHandlers::getAllRoleBindingNodes));
        setupFailureHandler(routeGetAllNodes);

        Route routeGetIAMPolicy = router.get(AUTHZ_ORG_POLICY)
                .handler(wrapBlocking(iamPolicyHandlers.getIAMPolicyHandler(ResourceType.ORG)));
        setupFailureHandler(routeGetIAMPolicy);

        Route routeSetIAMPolicy = router.put(AUTHZ_ORG_POLICY).handler(bodyHandler)
                .handler(wrapBlocking(iamPolicyHandlers.setIAMPolicyHandler(ResourceType.ORG)));
        setupFailureHandler(routeSetIAMPolicy);

        Route routeDeleteNode = router.delete(AUTHZ_DEBUG_PATH)
                .handler(wrapBlocking(iamPolicyHandlers::deleteRoleBindingNode));
        setupFailureHandler(routeDeleteNode);
    }

    @AfterEach
    public void PostTest() throws InterruptedException {
        super.tearDown();
    }

    private String getRoleBindingNodeUrl(ResourceType resourceType, String resourceId) {
        return String.join("/", AUTHZ_DEBUG_BASE, resourceType.name(), resourceId);
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
    public void testGetRoleBindingNode() throws Exception {
        RoleBindingNode expected = new RoleBindingNode("testNode", ResourceType.ORG, Map.of(), 0);

        HttpRequest<Buffer> request = createRequest(
                HttpMethod.GET,
                getRoleBindingNodeUrl(expected.getResourceType(), expected.getResourceId())
        );
        doReturn(expected).when(iamPolicyService).findRoleBindingNode(expected.getResourceType(), expected.getResourceId());

        RoleBindingNode response = sendRequestWithoutBody(request, RoleBindingNode.class);
        assertEquals(expected, response);
        verify(iamPolicyService, times(1)).findRoleBindingNode(expected.getResourceType(), expected.getResourceId());

        String notFoundError = String.format("RoleBinding on resource(%s) not found.", expected.getResourceId());
        doThrow(new ResourceNotFoundException(notFoundError)).when(iamPolicyService)
                .findRoleBindingNode(expected.getResourceType(), expected.getResourceId());
        ErrorResponse errResponse = sendRequestWithoutBody(request, 404, notFoundError, ErrorResponse.class);
        assertEquals(notFoundError, errResponse.reason());
    }

    @Test
    public void testGetAllRoleBindingNodes() throws Exception {
        List<RoleBindingNode> expected = List.of(
                new RoleBindingNode("testNode1", ResourceType.ORG, Map.of(), 0),
                new RoleBindingNode("testNode2", ResourceType.ORG, Map.of(), 0)
        );

        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, AUTHZ_DEBUG_BASE);
        doReturn(expected).when(iamPolicyService).getAllRoleBindingNodes();

        HttpResponse<Buffer> responseBuffer = sendRequest(request, null);
        List<RoleBindingNode> response =
                jsonDeserialize(responseBuffer.bodyAsString(), List.class, RoleBindingNode.class);

        assertEquals(expected.size(), response.size());
        assertArrayEquals(expected.toArray(), response.toArray());
        verify(iamPolicyService, times(1)).getAllRoleBindingNodes();
    }

    @Test
    public void testDeleteRoleBindingNode() throws Exception {
        RoleBindingNode node = new RoleBindingNode("testNode", ResourceType.ORG, Map.of(), 0);

        HttpRequest<Buffer> request =
                createRequest(HttpMethod.DELETE, getRoleBindingNodeUrl(node.getResourceType(), node.getResourceId()));
        doNothing().when(iamPolicyService).deleteRoleBindingNode(node.getResourceType(), node.getResourceId());

        sendRequestWithoutBody(request, null);
        verify(iamPolicyService, times(1)).deleteRoleBindingNode(node.getResourceType(), node.getResourceId());

        String notFoundError = String.format("RoleBinding on resource(%s) not found.", node.getResourceId());
        doThrow(new ResourceNotFoundException(notFoundError)).when(iamPolicyService)
                .deleteRoleBindingNode(node.getResourceType(), node.getResourceId());
        ErrorResponse errResponse = sendRequestWithoutBody(request, 404, notFoundError, ErrorResponse.class);
        assertEquals(notFoundError, errResponse.reason());
    }

    @Test
    public void testSetOrgIAMPolicy() throws InterruptedException {
        RoleBindingNode existingNode = new RoleBindingNode("testNode", ResourceType.ORG, Map.of(
                "user.a", Set.of("role1", "role2")
        ), 0);
        IAMPolicyRequest assignmentUpdate = new IAMPolicyRequest(
                "user.a", Set.of("role1", "role2")
        );

        HttpRequest<Buffer> request = createRequest(HttpMethod.PUT, getOrgIAMPolicyUrl("testNode"));
        doReturn(existingNode).when(iamPolicyService)
                .setIAMPolicy(eq(ResourceType.ORG), eq("testNode"), eq(assignmentUpdate));

        RoleBindingNode response = sendRequestWithBody(request, assignmentUpdate, RoleBindingNode.class);
        assertEquals(existingNode, response);
        verify(iamPolicyService, times(1)).setIAMPolicy(eq(ResourceType.ORG), eq("testNode"), eq(assignmentUpdate));

        String someInternalError = "Some internal error";
        doThrow(new MetaStoreException(someInternalError)).when(iamPolicyService)
                .setIAMPolicy(eq(ResourceType.ORG), eq("testNode"), eq(assignmentUpdate));
        ErrorResponse errResponse =
                sendRequestWithBody(request, assignmentUpdate, 500, someInternalError, ErrorResponse.class);
        assertEquals(someInternalError, errResponse.reason());
    }
}

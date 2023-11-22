package com.flipkart.varadhi.web.authz;

import com.flipkart.varadhi.auth.RoleBindingNode;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.RoleAssignmentUpdate;
import com.flipkart.varadhi.exceptions.MetaStoreException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.services.DefaultAuthZService;
import com.flipkart.varadhi.web.ErrorResponse;
import com.flipkart.varadhi.web.WebTestBase;
import com.flipkart.varadhi.web.v1.authz.DefaultAuthZHandlers;
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

public class DefaultAuthZHandlersTest extends WebTestBase {
    DefaultAuthZHandlers defaultAuthZHandlers;
    DefaultAuthZService defaultAuthZService;

    Org o1 = new Org("OrgOne", 0);

    @BeforeEach
    public void preTest() throws InterruptedException {
        super.setUp();
        defaultAuthZService = mock(DefaultAuthZService.class);
        defaultAuthZHandlers = new DefaultAuthZHandlers(defaultAuthZService);

        Route routeGetNode = router.get("/authz/bindings/:resource").handler(wrapBlocking(defaultAuthZHandlers::getRoleBindingNode));
        setupFailureHandler(routeGetNode);
        Route routeGetAllNodes = router.get("/authz/bindings").handler(wrapBlocking(defaultAuthZHandlers::getAllRoleBindingNodes));
        setupFailureHandler(routeGetAllNodes);
        Route routeUpdateAssignment = router.put("/authz/bindings").handler(bodyHandler).handler(wrapBlocking(defaultAuthZHandlers::updateRoleAssignment));
        setupFailureHandler(routeUpdateAssignment);
        Route routeDeleteNode = router.delete("/authz/bindings/:resource").handler(wrapBlocking(defaultAuthZHandlers::deleteRoleBindingNode));
        setupFailureHandler(routeDeleteNode);
    }

    @AfterEach
    public void postTest() throws InterruptedException {
        super.tearDown();
    }

    private String getRoleBindingNodeUrl(String resourceId) {
        return String.join("/", "/authz/bindings", resourceId);
    }

    @Test
    public void testGetRoleBindingNode() throws Exception {
        RoleBindingNode expected = new RoleBindingNode("testNode", ResourceType.ORG, Map.of(), 0);

        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, getRoleBindingNodeUrl(expected.getResourceId()));
        doReturn(expected).when(defaultAuthZService).getRoleBindingNode(expected.getResourceId());

        RoleBindingNode response = sendRequestWithoutBody(request, RoleBindingNode.class);
        assertEquals(expected, response);
        verify(defaultAuthZService, times(1)).getRoleBindingNode(expected.getResourceId());

        String notFoundError = String.format("RoleBinding on resource(%s) not found.", expected.getResourceId());
        doThrow(new ResourceNotFoundException(notFoundError)).when(defaultAuthZService).getRoleBindingNode(expected.getResourceId());
        ErrorResponse errResponse = sendRequestWithoutBody(request, 404, notFoundError, ErrorResponse.class);
        assertEquals(notFoundError, errResponse.reason());
    }

    @Test
    public void testGetAllRoleBindingNodes() throws Exception {
        List<RoleBindingNode> expected = List.of(
                new RoleBindingNode("testNode1", ResourceType.ORG, Map.of(), 0),
                new RoleBindingNode("testNode2", ResourceType.ORG, Map.of(), 0));

        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, "/authz/bindings");
        doReturn(expected).when(defaultAuthZService).getAllRoleBindingNodes();

        HttpResponse<Buffer> responseBuffer = sendRequest(request, null);
        List<RoleBindingNode> response = jsonDeserialize(responseBuffer.bodyAsString(), List.class, RoleBindingNode.class);

        assertEquals(expected.size(), response.size());
        assertArrayEquals(expected.toArray(), response.toArray());
        verify(defaultAuthZService, times(1)).getAllRoleBindingNodes();
    }

    @Test
    public void testDeleteRoleBindingNode() throws Exception {
        RoleBindingNode node = new RoleBindingNode("testNode", ResourceType.ORG, Map.of(), 0);

        HttpRequest<Buffer> request = createRequest(HttpMethod.DELETE, getRoleBindingNodeUrl(node.getResourceId()));
        doNothing().when(defaultAuthZService).deleteRoleBindingNode(node.getResourceId());

        sendRequestWithoutBody(request, null);
        verify(defaultAuthZService, times(1)).deleteRoleBindingNode(node.getResourceId());

        String notFoundError = String.format("RoleBinding on resource(%s) not found.", node.getResourceId());
        doThrow(new ResourceNotFoundException(notFoundError)).when(defaultAuthZService).deleteRoleBindingNode(node.getResourceId());
        ErrorResponse errResponse = sendRequestWithoutBody(request, 404, notFoundError, ErrorResponse.class);
        assertEquals(notFoundError, errResponse.reason());
    }

    @Test
    public void testUpdateRoleAssignment() throws InterruptedException {
        RoleBindingNode existingNode = new RoleBindingNode("testNode", ResourceType.ORG, Map.of(
                "user.a", Set.of("role1", "role2")
        ), 0);
        RoleAssignmentUpdate assignmentUpdate = new RoleAssignmentUpdate(existingNode.getResourceId(),
                existingNode.getResourceType(),
                "user.a", Set.of("role1", "role2"));

        HttpRequest<Buffer> request = createRequest(HttpMethod.PUT, "/authz/bindings");
        doReturn(existingNode).when(defaultAuthZService).updateRoleAssignment(eq(assignmentUpdate));

        RoleBindingNode response = sendRequestWithBody(request, assignmentUpdate, RoleBindingNode.class);
        assertEquals(existingNode, response);
        verify(defaultAuthZService, times(1)).updateRoleAssignment(eq(assignmentUpdate));

        String someInternalError = "Some internal error";
        doThrow(new MetaStoreException(someInternalError)).when(defaultAuthZService).updateRoleAssignment(eq(assignmentUpdate));
        ErrorResponse errResponse = sendRequestWithBody(request, assignmentUpdate, 500, someInternalError, ErrorResponse.class);
        assertEquals(someInternalError, errResponse.reason());
    }
}

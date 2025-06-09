package com.flipkart.varadhi.web.authz;

import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.auth.IamPolicyRecord;
import com.flipkart.varadhi.entities.auth.IamPolicyRequest;
import com.flipkart.varadhi.entities.auth.IamPolicyResponse;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static com.flipkart.varadhi.utils.IamPolicyHelper.getAuthResourceFQN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class IamPolicyHandlersTest extends WebTestBase {
    public static final String AUTHZ_ORG_POLICY = "/orgs/:org/policy";
    public static final String AUTHZ_TEAM_POLICY = "/orgs/:org/teams/:team/policy";
    public static final String AUTHZ_PROJECT_POLICY = "/projects/:project/policy";
    public static final String AUTHZ_TOPIC_POLICY = "/projects/:project/topics/:topic/policy";
    IamPolicyHandlers iamPolicyHandlers;
    IamPolicyService iamPolicyService;
    ProjectService projectService;

    @BeforeEach
    public void PreTest() throws InterruptedException {
        super.setUp();
        iamPolicyService = mock(IamPolicyService.class);
        projectService = mock(ProjectService.class);

        iamPolicyHandlers = new IamPolicyHandlers(projectService, iamPolicyService);

        Route routeGetIamPolicy = router.get(AUTHZ_ORG_POLICY)
                                        .handler(wrapBlocking(iamPolicyHandlers.get(ResourceType.ORG)));
        setupFailureHandler(routeGetIamPolicy);

        Route routeSetIamPolicy = router.put(AUTHZ_ORG_POLICY)
                                        .handler(bodyHandler)
                                        .handler(wrapBlocking(iamPolicyHandlers.set(ResourceType.ORG)));
        setupFailureHandler(routeSetIamPolicy);

        Route routeDelIamPolicy = router.delete(AUTHZ_ORG_POLICY)
                                        .handler(bodyHandler)
                                        .handler(wrapBlocking(iamPolicyHandlers.delete(ResourceType.ORG)));
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
    void testDeleteIamPolicyRecord() throws Exception {
        String resourceId = "testNode";

        HttpRequest<Buffer> request = createRequest(HttpMethod.DELETE, getOrgIamPolicyUrl(resourceId));
        doNothing().when(iamPolicyService).deleteIamPolicy(eq(ResourceType.ORG), eq(resourceId));

        sendRequestWithoutPayload(request, null);
        verify(iamPolicyService, times(1)).deleteIamPolicy(eq(ResourceType.ORG), eq(resourceId));

        String notFoundError = String.format("IamPolicyRecord on resource(%s) not found.", resourceId);
        doThrow(new ResourceNotFoundException(notFoundError)).when(iamPolicyService)
                                                             .deleteIamPolicy(ResourceType.ORG, resourceId);
        sendRequestWithoutPayload(request, 404, notFoundError);
    }

    @Test
    void testSetOrgIamPolicy() throws InterruptedException {
        String orgName = "myOrg";
        String user = "user.a";
        Set<String> roles = Set.of("role1", "role2");

        IamPolicyRecord policyRecord = new IamPolicyRecord(
            getAuthResourceFQN(ResourceType.ORG, orgName),
            0,
            Map.of(user, roles)
        );
        IamPolicyResponse expected = new IamPolicyResponse(policyRecord.getName(), policyRecord.getRoleBindings(), 0);
        IamPolicyRequest assignmentUpdate = new IamPolicyRequest(user, roles);

        HttpRequest<Buffer> request = createRequest(HttpMethod.PUT, getOrgIamPolicyUrl(orgName));
        doReturn(policyRecord).when(iamPolicyService)
                              .setIamPolicy(eq(ResourceType.ORG), eq(orgName), eq(assignmentUpdate));

        IamPolicyResponse response = sendRequestWithEntity(request, assignmentUpdate, c(IamPolicyResponse.class));
        assertEquals(expected, response);
        verify(iamPolicyService, times(1)).setIamPolicy(eq(ResourceType.ORG), eq(orgName), eq(assignmentUpdate));

        String someInternalError = "Some internal error";
        doThrow(new MetaStoreException(someInternalError)).when(iamPolicyService)
                                                          .setIamPolicy(
                                                              eq(ResourceType.ORG),
                                                              eq(orgName),
                                                              eq(assignmentUpdate)
                                                          );
        ErrorResponse errResponse = sendRequestWithEntity(
            request,
            assignmentUpdate,
            500,
            someInternalError,
            c(ErrorResponse.class)
        );
        assertEquals(someInternalError, errResponse.reason());
    }
}
